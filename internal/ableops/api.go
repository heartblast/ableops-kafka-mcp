package ableops

import (
	"context"
	"net/url"
	"strconv"
	"strings"
)

// CurrentUserID는 기존 /api/me의 공개 ID만 해석하고 권한·개인정보를 노출하지 않는다.
func (c *Client) CurrentUserID(ctx context.Context) (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	if err := c.Get(ctx, []string{"me"}, nil, &out); err != nil {
		return "", err
	}
	if strings.TrimSpace(out.ID) == "" || len(out.ID) > 256 || strings.ContainsAny(out.ID, "\r\n\x00") {
		return "", publicError("invalid_response", 200)
	}
	return out.ID, nil
}

// ListClusters는 백엔드가 현재 사용자에게 허용한 클러스터만 조회한다.
func (c *Client) ListClusters(ctx context.Context) ([]Cluster, error) {
	var out []Cluster
	if err := c.Get(ctx, []string{"clusters"}, nil, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, publicError("invalid_response", 200)
	}
	return out, nil
}

func (c *Client) ClusterHealth(ctx context.Context, id string) (ClusterHealthView, error) {
	var out ClusterHealthView
	if err := c.getCluster(ctx, id, []string{"health"}, nil, &out); err != nil {
		return out, err
	}
	if out.ClusterID != id {
		return ClusterHealthView{}, publicError("invalid_response", 200)
	}
	if out.Error != "" {
		out.Error = "백엔드가 클러스터 조회 실패를 보고했습니다. 내부 오류 원문은 생략합니다."
	}
	return out, nil
}

func (c *Client) PartitionHealth(ctx context.Context, id string) (PartitionHealthView, error) {
	var out PartitionHealthView
	if err := c.getCluster(ctx, id, []string{"partition-health"}, nil, &out); err != nil {
		return out, err
	}
	if !validProbeStatus(out.Status) {
		return PartitionHealthView{}, publicError("invalid_response", 200)
	}
	out.Reasons = safeProbeReasons(out.Status, out.Reasons)
	for i := range out.Issues {
		if !validProbeStatus(out.Issues[i].Status) {
			return PartitionHealthView{}, publicError("invalid_response", 200)
		}
		out.Issues[i].Reasons = safeProbeReasons(out.Issues[i].Status, out.Issues[i].Reasons)
	}
	return out, nil
}

func (c *Client) ListTopics(ctx context.Context, id string) (Snapshot[Topic], error) {
	var out Snapshot[Topic]
	if err := c.getCluster(ctx, id, []string{"topics"}, nil, &out); err != nil {
		return out, err
	}
	if out.ClusterID != id {
		return Snapshot[Topic]{}, publicError("invalid_response", 200)
	}
	return out, nil
}

func (c *Client) ListConsumerGroups(ctx context.Context, id string) (Snapshot[ConsumerGroup], error) {
	var out Snapshot[ConsumerGroup]
	if err := c.getCluster(ctx, id, []string{"consumer-groups"}, nil, &out); err != nil {
		return out, err
	}
	if out.ClusterID != id {
		return Snapshot[ConsumerGroup]{}, publicError("invalid_response", 200)
	}
	return out, nil
}

func (c *Client) ConsumerGroupLag(ctx context.Context, id, name string) (ConsumerGroupLagView, error) {
	var out ConsumerGroupLagView
	if strings.TrimSpace(name) == "" {
		return out, publicError("invalid_request", 0)
	}
	if err := c.getCluster(ctx, id, []string{"consumer-groups", name, "lag"}, nil, &out); err != nil {
		return out, err
	}
	if !validProbeStatus(out.Status) || out.Group != name {
		return ConsumerGroupLagView{}, publicError("invalid_response", 200)
	}
	out.Reasons = safeProbeReasons(out.Status, out.Reasons)
	for i := range out.Partitions {
		p := &out.Partitions[i]
		if !validProbeStatus(p.Status) {
			return ConsumerGroupLagView{}, publicError("invalid_response", 200)
		}
		if p.Error != "" {
			p.Error = "백엔드가 파티션 조회 실패를 보고했습니다. 내부 오류 원문은 생략합니다."
		}
		p.Reasons = safeProbeReasons(p.Status, p.Reasons)
	}
	return out, nil
}

func (c *Client) ListClusterEvents(ctx context.Context, id string, filter EventFilters) (EventPage, error) {
	var out EventPage
	q := url.Values{}
	if filter.Page != 0 {
		q.Set("page", strconv.Itoa(filter.Page))
	}
	if filter.PageSize != 0 {
		q.Set("pageSize", strconv.Itoa(filter.PageSize))
	}
	for key, values := range map[string][]string{"status": filter.Status, "severity": filter.Severity, "category": filter.Category, "attention": filter.Attention, "eventCode": filter.EventCode} {
		for _, value := range values {
			q.Add(key, value)
		}
	}
	for key, value := range map[string]string{"search": filter.Search, "from": filter.From, "to": filter.To, "resourceType": filter.ResourceType, "resourceId": filter.ResourceID, "module": filter.Module, "assignedTo": filter.AssignedTo, "sort": filter.Sort} {
		if value != "" {
			q.Set(key, value)
		}
	}
	if filter.ExcludeSynthetic {
		q.Set("excludeSynthetic", "true")
	}
	if err := c.getCluster(ctx, id, []string{"events"}, q, &out); err != nil {
		return out, err
	}
	if out.Page < 1 || out.PageSize < 1 || out.Total < 0 || out.Items == nil {
		return EventPage{}, publicError("invalid_response", 200)
	}
	for _, item := range out.Items {
		if item.ClusterID != id {
			return EventPage{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

// getCluster는 클러스터 생략을 기본 클러스터 조회로 바꾸지 않는다.
func (c *Client) getCluster(ctx context.Context, id string, suffix []string, q url.Values, out any) error {
	if strings.TrimSpace(id) == "" {
		return publicError("invalid_request", 0)
	}
	segments := append([]string{"clusters", id}, suffix...)
	return c.Get(ctx, segments, q, out)
}

// validProbeStatus는 실제 REST 계약의 enum만 허용한다. 미정의 상태를 성공으로 해석하지 않는다.
func validProbeStatus(status string) bool {
	switch status {
	case "OK", "WARN", "CRITICAL", "UNAVAILABLE", "FORBIDDEN":
		return true
	default:
		return false
	}
}

// 조회 실패 원문에는 접속 프로필 등 내부 정보가 포함될 수 있어 상태 의미만 전달한다.
// WARN/CRITICAL 진단은 백엔드의 기존 판정으로 보존하며 원인을 새로 계산하지 않는다.
func safeProbeReasons(status string, reasons []string) []string {
	switch status {
	case "FORBIDDEN":
		return []string{"백엔드가 Kafka 조회 권한 부족을 보고했습니다. 내부 오류 원문은 생략합니다."}
	case "UNAVAILABLE":
		return []string{"백엔드가 조회 불가 상태를 보고했습니다. 내부 오류 원문은 생략합니다."}
	default:
		return reasons
	}
}
