package ableops

import (
	"context"
	"net/url"
	"strconv"
	"strings"
)

// GraphNodeAttributes는 공개 가능한 관계 요약만 해석한다. 인증 설정과 자유 형식 속성은 제외한다.
type GraphNodeAttributes struct {
	Principal        string `json:"principal,omitempty"`
	HasCredential    *bool  `json:"hasCredential,omitempty"`
	CredentialLocked *bool  `json:"credentialLocked,omitempty"`
	ACLCount         int    `json:"aclCount,omitempty"`
	Orphan           bool   `json:"orphan,omitempty"`
	Grouped          bool   `json:"grouped,omitempty"`
	Count            int    `json:"count,omitempty"`
}

type GraphNode struct {
	ID    string               `json:"id"`
	Type  string               `json:"type"`
	Label string               `json:"label"`
	Risk  string               `json:"risk,omitempty"`
	Attrs *GraphNodeAttributes `json:"attrs,omitempty"`
}

// GraphEdgeAttributes는 ACL 원문 대신 백엔드 집계 관계의 허용 필드만 보존한다.
type GraphEdgeAttributes struct {
	Principal        string   `json:"principal,omitempty"`
	ResourceType     string   `json:"resourceType,omitempty"`
	Resource         string   `json:"resource,omitempty"`
	Operations       []string `json:"operations,omitempty"`
	DeniedOperations []string `json:"deniedOperations,omitempty"`
	PermissionTypes  []string `json:"permissionTypes,omitempty"`
	PatternTypes     []string `json:"patternTypes,omitempty"`
	Hosts            []string `json:"hosts,omitempty"`
	ACLCount         int      `json:"aclCount,omitempty"`
}

type GraphEdge struct {
	ID     string               `json:"id"`
	Source string               `json:"source"`
	Target string               `json:"target"`
	Label  string               `json:"label"`
	Kind   string               `json:"kind"`
	Risk   string               `json:"risk,omitempty"`
	Attrs  *GraphEdgeAttributes `json:"attrs,omitempty"`
}

type AssetGraphSummary struct {
	ShownNodes  int `json:"shownNodes"`
	ShownEdges  int `json:"shownEdges"`
	HiddenNodes int `json:"hiddenNodes"`
	ACLs        int `json:"acls"`
	Relations   int `json:"relations"`
}

type AssetGraph struct {
	ClusterID string            `json:"clusterId"`
	Center    string            `json:"center"`
	View      string            `json:"view"`
	Depth     int               `json:"depth"`
	Nodes     []GraphNode       `json:"nodes"`
	Edges     []GraphEdge       `json:"edges"`
	Summary   AssetGraphSummary `json:"summary"`
	SyncedAt  string            `json:"syncedAt"`
	Errors    []string          `json:"errors,omitempty"`
}

type AssetImpact struct {
	Target        GraphNode `json:"target"`
	Producers     []string  `json:"producers,omitempty"`
	Consumers     []string  `json:"consumers,omitempty"`
	Topics        []string  `json:"topics,omitempty"`
	Groups        []string  `json:"groups,omitempty"`
	ACLs          []string  `json:"acls,omitempty"`
	Users         []string  `json:"users,omitempty"`
	ClusterScoped bool      `json:"clusterScoped,omitempty"`
	Risks         []string  `json:"risks,omitempty"`
	Notes         []string  `json:"notes,omitempty"`
}

func validImpactTarget(kind, key string) bool {
	if strings.TrimSpace(key) == "" || strings.ContainsAny(key, "\x00\r\n") {
		return false
	}
	switch kind {
	case "TOPIC", "PRINCIPAL", "CONSUMER_GROUP":
		return !(kind == "PRINCIPAL" && strings.HasPrefix(key, "group:"))
	default:
		return false
	}
}

// AssetRelations는 서버의 클러스터 acl.view 검사를 거친 제한 깊이의 그래프만 조회한다.
func (c *Client) AssetRelations(ctx context.Context, cluster, kind, key string, depth int) (AssetGraph, error) {
	var out AssetGraph
	if !validImpactTarget(kind, key) || depth < 1 || depth > 2 {
		return out, publicError("invalid_request", 0)
	}
	center := kind + ":" + key
	q := url.Values{"center": {center}, "depth": {strconv.Itoa(depth)}, "view": {"full"}, "expandStructural": {"false"}}
	if err := c.getCluster(ctx, cluster, []string{"asset-graph"}, q, &out); err != nil {
		return AssetGraph{}, err
	}
	if out.ClusterID != cluster || out.Center != center || out.Depth != depth || out.View != "full" || out.Nodes == nil || out.Edges == nil {
		return AssetGraph{}, publicError("invalid_response", 200)
	}
	ids := make(map[string]bool, len(out.Nodes))
	for _, node := range out.Nodes {
		if node.ID == "" || ids[node.ID] {
			return AssetGraph{}, publicError("invalid_response", 200)
		}
		ids[node.ID] = true
	}
	if len(out.Nodes) > 0 && !ids[center] {
		return AssetGraph{}, publicError("invalid_response", 200)
	}
	for _, edge := range out.Edges {
		if !ids[edge.Source] || !ids[edge.Target] {
			return AssetGraph{}, publicError("invalid_response", 200)
		}
	}
	// 수집 오류에는 브로커 주소와 인증 설정이 포함될 수 있어 원문은 공개하지 않는다.
	for i := range out.Errors {
		out.Errors[i] = "백엔드 자산 수집 일부가 실패했습니다. 내부 오류 원문은 생략합니다."
	}
	return out, nil
}

func (c *Client) AssetImpact(ctx context.Context, cluster, kind, key string) (AssetImpact, error) {
	var out AssetImpact
	if !validImpactTarget(kind, key) {
		return out, publicError("invalid_request", 0)
	}
	q := url.Values{"type": {kind}, "key": {key}}
	if err := c.getCluster(ctx, cluster, []string{"asset-graph", "impact"}, q, &out); err != nil {
		return AssetImpact{}, err
	}
	if out.Target.ID != kind+":"+key || out.Target.Type != kind {
		return AssetImpact{}, publicError("invalid_response", 200)
	}
	return out, nil
}

// RequestTarget은 입력 payload 전체를 노출하지 않고 계약에 존재하는 대상 식별자만 읽는다.
type RequestTarget struct {
	TopicName string `json:"topicName,omitempty"`
	Principal string `json:"principal,omitempty"`
}

type RequestPolicyViolation struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
}

type RequestPolicyResult struct {
	Passed     bool                     `json:"passed"`
	RiskLevel  string                   `json:"riskLevel"`
	Violations []RequestPolicyViolation `json:"violations"`
}

// RequestHistory는 자유 입력 의견과 내부 실패 원문을 공개하지 않는다.
type RequestHistory struct {
	Time  string `json:"time"`
	State string `json:"state"`
	Actor string `json:"actor"`
}

type RequestStatus struct {
	RequestID    string               `json:"requestId"`
	Type         string               `json:"type"`
	ClusterID    string               `json:"clusterId"`
	Status       string               `json:"status"`
	RiskLevel    string               `json:"riskLevel"`
	Payload      RequestTarget        `json:"payload"`
	PolicyResult *RequestPolicyResult `json:"policyResult,omitempty"`
	History      []RequestHistory     `json:"history,omitempty"`
	CreatedAt    string               `json:"createdAt"`
	UpdatedAt    string               `json:"updatedAt"`
}

// RequestStatus는 백엔드 canSeeRequest 객체 권한 검사 후 반환된 객체의 클러스터도 대조한다.
func (c *Client) RequestStatus(ctx context.Context, cluster, request string) (RequestStatus, error) {
	var out RequestStatus
	if strings.TrimSpace(cluster) == "" || strings.TrimSpace(request) == "" {
		return out, publicError("invalid_request", 0)
	}
	if err := c.Get(ctx, []string{"requests", request}, nil, &out); err != nil {
		return RequestStatus{}, err
	}
	// 레거시 빈 clusterId도 기본 클러스터로 대체하지 않는다.
	if out.ClusterID != cluster || out.RequestID != request || !validRequestState(out.Status) {
		return RequestStatus{}, publicError("invalid_response", 200)
	}
	for _, h := range out.History {
		if !validRequestState(h.State) {
			return RequestStatus{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

func validRequestState(state string) bool {
	switch state {
	case "DRAFT", "REQUESTED", "POLICY_CHECKED", "APPROVED", "READY_TO_APPLY", "APPLYING", "APPLIED", "VERIFIED", "REJECTED", "NEED_MORE_INFO", "POLICY_REJECTED", "APPLY_FAILED", "CANCELED", "DISCARDED":
		return true
	default:
		return false
	}
}
