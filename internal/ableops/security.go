package ableops

import (
	"context"
	"net/url"
	"strings"
)

// SecurityACL은 인증 설정과 자유 입력 메모를 제외한 권한 규칙이다.
type SecurityACL struct {
	Principal      string `json:"principal"`
	ResourceType   string `json:"resourceType"`
	ResourceName   string `json:"resourceName"`
	PatternType    string `json:"patternType"`
	Operation      string `json:"operation"`
	PermissionType string `json:"permissionType"`
	Host           string `json:"host"`
}

type IdentityOwner struct {
	Manager    string `json:"manager"`
	Department string `json:"department"`
	Service    string `json:"service"`
}

type IdentityLifecycle struct {
	Stage    string   `json:"stage" jsonschema:"현재 메타와 관측값으로 백엔드가 계산한 단계이며 선언 상태와 다르다"`
	Severity string   `json:"severity"`
	Reasons  []string `json:"reasons"`
}

// IdentityCredential은 비밀번호 없이 존재·알고리즘·변경 잠금 메타만 공개한다.
type IdentityCredential struct {
	Has       bool                     `json:"has"`
	Supported bool                     `json:"supported"`
	Locked    bool                     `json:"locked" jsonschema:"포털의 삭제·재발급 잠금이며 Kafka 접속 차단 여부가 아니다"`
	Items     []IdentityCredentialItem `json:"items"`
}

type IdentityCredentialItem struct {
	Username   string `json:"username"`
	Mechanism  string `json:"mechanism"`
	Iterations int    `json:"iterations"`
	Locked     bool   `json:"locked"`
}

type IdentityEntitlement struct {
	ACLCount           int  `json:"aclCount"`
	ResourceCount      int  `json:"resourceCount"`
	TopicCount         int  `json:"topicCount"`
	ConsumerGroupCount int  `json:"consumerGroupCount"`
	AllowCount         int  `json:"allowCount"`
	DenyCount          int  `json:"denyCount"`
	HasWildcardHost    bool `json:"hasWildcardHost"`
	HighRisk           bool `json:"highRisk"`
}

type IdentityFinding struct {
	Code      string `json:"code"`
	Severity  string `json:"severity"`
	Attention string `json:"attention"`
}

// Identity는 별도 감사 권한이 없는 recentAudit와 클러스터 키가 없는 grants를 수신하지 않는다.
type Identity struct {
	Principal          string              `json:"principal"`
	PrincipalType      string              `json:"principalType"`
	PrincipalName      string              `json:"principalName"`
	IdentityKind       string              `json:"identityKind"`
	IdentityKindSource string              `json:"identityKindSource" jsonschema:"meta는 명시 분류, inferred는 이름에 의한 추론"`
	Managed            bool                `json:"managed"`
	Owner              IdentityOwner       `json:"owner"`
	Environment        string              `json:"environment,omitempty"`
	Status             string              `json:"status,omitempty" jsonschema:"담당자가 선언한 거버넌스 상태"`
	Lifecycle          IdentityLifecycle   `json:"lifecycle"`
	Credential         IdentityCredential  `json:"credential"`
	Entitlement        IdentityEntitlement `json:"entitlement"`
	Findings           []IdentityFinding   `json:"findings"`
	Severity           string              `json:"severity"`
	ExpiresAt          *string             `json:"expiresAt,omitempty"`
	ReviewedAt         *string             `json:"reviewedAt,omitempty"`
	MetaUpdatedAt      *string             `json:"metaUpdatedAt,omitempty"`
	ACLs               []SecurityACL       `json:"acls,omitempty"`
}

type IdentitySummary struct {
	Total     int `json:"total"`
	Managed   int `json:"managed"`
	Unmanaged int `json:"unmanaged"`
	Critical  int `json:"critical"`
	Warn      int `json:"warn"`
	OK        int `json:"ok"`
}

type IdentityInventory struct {
	ClusterID string          `json:"clusterId"`
	Items     []Identity      `json:"items"`
	Summary   IdentitySummary `json:"summary"`
}

type IdentityFilters struct{ Query, Kind, Stage, Department, Environment, Severity, Finding string }

func (c *Client) ListIdentities(ctx context.Context, cluster string, filters IdentityFilters) (IdentityInventory, error) {
	var out IdentityInventory
	q := url.Values{}
	for key, value := range map[string]string{"q": filters.Query, "kind": filters.Kind, "stage": filters.Stage, "department": filters.Department, "environment": filters.Environment, "severity": filters.Severity, "finding": filters.Finding} {
		if value != "" {
			q.Set(key, value)
		}
	}
	if err := c.getCluster(ctx, cluster, []string{"identities"}, q, &out); err != nil {
		return out, err
	}
	if out.ClusterID != cluster || out.Items == nil {
		return IdentityInventory{}, publicError("invalid_response", 200)
	}
	for _, item := range out.Items {
		if !validIdentity(item) {
			return IdentityInventory{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

func (c *Client) IdentityDetail(ctx context.Context, cluster, principal string) (Identity, error) {
	var out struct {
		ClusterID string   `json:"clusterId"`
		Item      Identity `json:"items"`
	}
	if strings.TrimSpace(principal) == "" || principal != strings.TrimSpace(principal) {
		return Identity{}, publicError("invalid_request", 0)
	}
	if err := c.getCluster(ctx, cluster, []string{"identities", principal}, nil, &out); err != nil {
		return Identity{}, err
	}
	if out.ClusterID != cluster || out.Item.Principal != principal || !validIdentity(out.Item) {
		return Identity{}, publicError("invalid_response", 200)
	}
	return out.Item, nil
}

func validIdentity(item Identity) bool {
	if strings.TrimSpace(item.Principal) == "" || (item.IdentityKindSource != "meta" && item.IdentityKindSource != "inferred") {
		return false
	}
	switch item.Lifecycle.Stage {
	case "UNMANAGED", "DRAFT", "INCOMPLETE", "ACTIVE", "REVIEW_DUE", "EXPIRING", "EXPIRED", "LOCKED", "DORMANT", "REVOKED":
	default:
		return false
	}
	return true
}

func (c *Client) ListACLs(ctx context.Context, cluster string) (Snapshot[SecurityACL], error) {
	var out Snapshot[SecurityACL]
	if err := c.getCluster(ctx, cluster, []string{"acls"}, nil, &out); err != nil {
		return out, err
	}
	if out.ClusterID != cluster {
		return Snapshot[SecurityACL]{}, publicError("invalid_response", 200)
	}
	return out, nil
}

func (c *Client) ListPrincipalACLs(ctx context.Context, cluster, principal string) (Snapshot[SecurityACL], error) {
	var out Snapshot[SecurityACL]
	if strings.TrimSpace(principal) == "" || principal != strings.TrimSpace(principal) {
		return out, publicError("invalid_request", 0)
	}
	if err := c.getCluster(ctx, cluster, []string{"principals", principal, "acls"}, nil, &out); err != nil {
		return out, err
	}
	if out.ClusterID != cluster || out.Items == nil {
		return Snapshot[SecurityACL]{}, publicError("invalid_response", 200)
	}
	for _, item := range out.Items {
		if item.Principal != principal {
			return Snapshot[SecurityACL]{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

// SecurityFinding은 비밀 설정과 내부 실패 원문을 제외한 정책 진단 근거다.
type SecurityFinding struct {
	Code     string `json:"code"`
	Status   string `json:"status"`
	Target   string `json:"target,omitempty"`
	Actual   string `json:"actual,omitempty"`
	Exempted bool   `json:"exempted,omitempty"`
}

type ACLRiskPrincipal struct {
	Principal string `json:"principal"`
}

type ACLRiskReport struct {
	ClusterID   string             `json:"clusterId"`
	Status      string             `json:"status"`
	Findings    []SecurityFinding  `json:"findings"`
	Principals  []ACLRiskPrincipal `json:"principals"`
	Total       int                `json:"total"`
	Critical    int                `json:"critical"`
	Warn        int                `json:"warn"`
	GeneratedAt string             `json:"generatedAt"`
}

type ScramAuditReport struct {
	ClusterID     string            `json:"clusterId"`
	Supported     bool              `json:"supported"`
	Status        string            `json:"status"`
	Findings      []SecurityFinding `json:"findings"`
	Unresolved    int               `json:"unresolved"`
	AssetSyncedAt string            `json:"assetSyncedAt"`
	GeneratedAt   string            `json:"generatedAt"`
}

func validSecurityReport(cluster, actual, status, generated string, findings []SecurityFinding) bool {
	if actual != cluster || !validProbeStatus(status) || generated == "" || findings == nil {
		return false
	}
	for _, f := range findings {
		if f.Code == "" || !validProbeStatus(f.Status) {
			return false
		}
	}
	return true
}

func (c *Client) ACLRisk(ctx context.Context, cluster string) (ACLRiskReport, error) {
	var out ACLRiskReport
	if err := c.getCluster(ctx, cluster, []string{"acl-risk"}, nil, &out); err != nil {
		return out, err
	}
	if !validSecurityReport(cluster, out.ClusterID, out.Status, out.GeneratedAt, out.Findings) {
		return ACLRiskReport{}, publicError("invalid_response", 200)
	}
	return out, nil
}

func (c *Client) ScramAudit(ctx context.Context, cluster string) (ScramAuditReport, error) {
	var out ScramAuditReport
	if err := c.getCluster(ctx, cluster, []string{"scram-audit"}, nil, &out); err != nil {
		return out, err
	}
	if !validSecurityReport(cluster, out.ClusterID, out.Status, out.GeneratedAt, out.Findings) || out.Unresolved < 0 {
		return ScramAuditReport{}, publicError("invalid_response", 200)
	}
	return out, nil
}

// RequestListItem은 신청 내용·감사 의견 없이 상태와 대상만 공개한다.
type RequestListItem struct {
	RequestID string        `json:"requestId"`
	Type      string        `json:"type"`
	ClusterID string        `json:"clusterId"`
	Status    string        `json:"status"`
	RiskLevel string        `json:"riskLevel"`
	Requester string        `json:"requester"`
	Payload   RequestTarget `json:"payload"`
	CreatedAt string        `json:"createdAt"`
	UpdatedAt string        `json:"updatedAt"`
}

func (c *Client) ListRequests(ctx context.Context, cluster string) ([]RequestListItem, error) {
	if strings.TrimSpace(cluster) == "" {
		return nil, publicError("invalid_request", 0)
	}
	var out []RequestListItem
	// 클러스터 경로는 객체 권한 검사 누락: canSeeRequest가 적용되는 전역 목록의 지원 필터를 쓴다.
	if err := c.Get(ctx, []string{"requests"}, url.Values{"cluster": {cluster}}, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, publicError("invalid_response", 200)
	}
	for _, item := range out {
		if item.ClusterID != cluster || item.RequestID == "" || !validRequestState(item.Status) {
			return nil, publicError("invalid_response", 200)
		}
	}
	return out, nil
}
