package ableops

import (
	"context"
	"net/url"
	"strconv"
	"strings"
)

// EventEvidence는 로컬 수집원에서 확인한 관측값만 공개한다. 원문 오류,
// 임의 라벨, 인증 설정, 메시지, 자유 형식 중첩 객체는 선언하지 않는다.
type EventEvidence struct {
	Group                string `json:"group,omitempty"`
	Topic                string `json:"topic,omitempty"`
	State                string `json:"state,omitempty"`
	CheckedAt            string `json:"checkedAt,omitempty"`
	Members              *int   `json:"members,omitempty"`
	TotalLag             *int64 `json:"totalLag,omitempty"`
	TopicLag             *int64 `json:"topicLag,omitempty"`
	GroupTotalLag        *int64 `json:"groupTotalLag,omitempty"`
	MaxPartitionLag      *int64 `json:"maxPartitionLag,omitempty"`
	Partitions           *int   `json:"partitions,omitempty"`
	ErrorPartitions      *int   `json:"errorPartitions,omitempty"`
	LagCritical          *int64 `json:"lagCritical,omitempty"`
	PartitionLagCritical *int64 `json:"partitionLagCritical,omitempty"`
	TopicLagCritical     *int64 `json:"topicLagCritical,omitempty"`
	Monitored            *bool  `json:"monitored,omitempty"`
	GroupStalled         *bool  `json:"groupStalled,omitempty"`
	BrokerCount          *int   `json:"brokerCount,omitempty"`
	TotalTopics          *int   `json:"totalTopics,omitempty"`
	TotalPartitions      *int   `json:"totalPartitions,omitempty"`
	Offline              *int   `json:"offline,omitempty"`
	Leaderless           *int   `json:"leaderless,omitempty"`
	UnderReplicated      *int   `json:"underReplicated,omitempty"`
	UnderMinISR          *int   `json:"underMinIsr,omitempty"`
	Unjudged             *int   `json:"unjudged,omitempty"`
	MinISRResolved       *bool  `json:"minIsrResolved,omitempty"`
}

// EventDetail은 발생 근거와 권장 조치를 실제 상태 전이 시각과 분리한다.
type EventDetail struct {
	Event
	Evidence          *EventEvidence `json:"evidence,omitempty"`
	RecommendedAction string         `json:"recommendedAction,omitempty"`
	UpdatedAt         string         `json:"updatedAt,omitempty"`
}

type EventOccurrence struct {
	ID         int64          `json:"id"`
	EventID    string         `json:"eventId"`
	ObservedAt string         `json:"observedAt"`
	Severity   string         `json:"severity"`
	Source     string         `json:"source"`
	DataMode   string         `json:"dataMode"`
	IsFiring   bool           `json:"isFiring"`
	Summary    string         `json:"summary"`
	Evidence   *EventEvidence `json:"evidence,omitempty"`
}

// Total은 전체 저장 이력 수가 아니라 이 REST 응답의 항목 수다.
type EventOccurrencePage struct {
	Items []EventOccurrence `json:"items"`
	Total int               `json:"total"`
}

type EventRelatedIssue struct {
	ID                string `json:"id"`
	ClusterID         string `json:"clusterId"`
	RootEventID       string `json:"rootEventId,omitempty"`
	Status            string `json:"status"`
	AttentionLevel    string `json:"attentionLevel"`
	ImpactLevel       string `json:"impactLevel"`
	Actionability     string `json:"actionability"`
	TrendState        string `json:"trendState"`
	Title             string `json:"title"`
	Summary           string `json:"summary"`
	DataMode          string `json:"dataMode"`
	FirstSeenAt       string `json:"firstSeenAt"`
	LastSeenAt        string `json:"lastSeenAt"`
	ActiveMemberCount int    `json:"activeMemberCount"`
}

// PlaybookStep은 안내 데이터다. 실행 명령·인증 설정은 공개하지 않는다.
type EventPlaybookStep struct {
	Title   string `json:"title"`
	Detail  string `json:"detail,omitempty"`
	Caution string `json:"caution,omitempty"`
}

type EventPlaybook struct {
	Code     string              `json:"code"`
	Title    string              `json:"title"`
	Impact   string              `json:"impact"`
	Urgency  string              `json:"urgency"`
	Checks   []EventPlaybookStep `json:"checks,omitempty"`
	Causes   []string            `json:"causes,omitempty"`
	Steps    []EventPlaybookStep `json:"steps,omitempty"`
	Verify   []EventPlaybookStep `json:"verify,omitempty"`
	Prevent  []string            `json:"prevent,omitempty"`
	Escalate string              `json:"escalate,omitempty"`
}

func (c *Client) EventDetail(ctx context.Context, clusterID, eventID string) (EventDetail, error) {
	var out EventDetail
	if strings.TrimSpace(clusterID) == "" || strings.TrimSpace(eventID) == "" {
		return out, publicError("invalid_request", 0)
	}
	// eventOr가 요청 사용자와 객체 소속 클러스터의 event.view를 검사한다.
	if err := c.Get(ctx, []string{"events", eventID}, nil, &out); err != nil {
		return EventDetail{}, err
	}
	if out.ID != eventID || out.ClusterID != clusterID || !validEventStatus(out.Status) || out.EventCode == "" {
		return EventDetail{}, publicError("invalid_response", 200)
	}
	return out, nil
}

func validEventStatus(status string) bool {
	switch status {
	case "OPEN", "ACKNOWLEDGED", "SUPPRESSED", "RESOLVED":
		return true
	default:
		return false
	}
}

// EventOccurrences는 검증된 상세의 ID로만 후속 조회한다. 서버 제한 200 이내다.
func (c *Client) EventOccurrences(ctx context.Context, eventID string, limit int) (EventOccurrencePage, error) {
	var out EventOccurrencePage
	if strings.TrimSpace(eventID) == "" || limit < 1 || limit > 101 {
		return out, publicError("invalid_request", 0)
	}
	if err := c.Get(ctx, []string{"events", eventID, "occurrences"}, url.Values{"limit": {strconv.Itoa(limit)}}, &out); err != nil {
		return EventOccurrencePage{}, err
	}
	if out.Items == nil || out.Total != len(out.Items) || len(out.Items) > limit {
		return EventOccurrencePage{}, publicError("invalid_response", 200)
	}
	for _, item := range out.Items {
		if item.EventID != eventID {
			return EventOccurrencePage{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

func (c *Client) EventIssue(ctx context.Context, clusterID, eventID string) (*EventRelatedIssue, error) {
	var out struct {
		Issue EventRelatedIssue `json:"issue"`
	}
	// limit은 응답에 함께 실리는 Issue 조치 이력 비용도 최소화한다.
	found, err := c.GetOptional(ctx, []string{"events", eventID, "issue"}, url.Values{"limit": {"1"}}, &out)
	if err != nil || !found {
		return nil, err
	}
	if out.Issue.ID == "" || out.Issue.ClusterID != clusterID || out.Issue.Status == "" {
		return nil, publicError("invalid_response", 200)
	}
	return &out.Issue, nil
}

func (c *Client) EventPlaybook(ctx context.Context, eventCode string) (EventPlaybook, error) {
	var out EventPlaybook
	if err := c.Get(ctx, []string{"event-playbooks", eventCode}, nil, &out); err != nil {
		return out, err
	}
	if out.Code != eventCode {
		return EventPlaybook{}, publicError("invalid_response", 200)
	}
	return out, nil
}
