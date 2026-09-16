package ableops

import (
	"context"
	"net/url"
	"strconv"
	"strings"
)

// EventSummaryScope의 범위는 기본 권한 범위이며 집계 대상은 ClusterID 하나다.
// 다른 클러스터의 ID 목록은 공개하지 않는다.
type EventSummaryScope struct {
	All           bool   `json:"all"`
	ClusterCount  int    `json:"clusterCount"`
	TotalClusters int    `json:"totalClusters"`
	ClusterID     string `json:"clusterId"`
	ClusterDenied bool   `json:"clusterDenied"`
}

type EventCollection struct {
	Enabled     bool `json:"enabled"`
	IntervalSec int  `json:"intervalSec,omitempty"`
}

type EventSummary struct {
	UnacknowledgedCritical int                `json:"unacknowledgedCritical"`
	OpenTotal              int                `json:"openTotal"`
	SecurityEvents         int                `json:"securityEvents"`
	AvailabilityEvents     int                `json:"availabilityEvents"`
	ResolvedLast24h        int                `json:"resolvedLast24h"`
	NotificationFailures   int                `json:"notificationFailures"`
	MonitoredClusters      int                `json:"monitoredClusters"`
	CoverageGapClusters    int                `json:"coverageGapClusters"`
	AttentionUrgent        int                `json:"attentionUrgent"`
	AttentionReview        int                `json:"attentionReview"`
	AttentionObserve       int                `json:"attentionObserve"`
	AttentionUnevaluated   int                `json:"attentionUnevaluated"`
	SyntheticExcluded      int                `json:"syntheticExcluded"`
	GeneratedAt            string             `json:"generatedAt"`
	Scope                  *EventSummaryScope `json:"scope"`
	Collection             *EventCollection   `json:"collection"`
}

func (c *Client) GetEventSummary(ctx context.Context, clusterID string) (EventSummary, error) {
	var out EventSummary
	if strings.TrimSpace(clusterID) == "" {
		return out, publicError("invalid_request", 0)
	}
	if err := c.Get(ctx, []string{"events", "summary"}, url.Values{"clusterId": {clusterID}}, &out); err != nil {
		return EventSummary{}, err
	}
	if out.Scope == nil || out.Collection == nil || out.Scope.ClusterID != clusterID || out.GeneratedAt == "" {
		return EventSummary{}, publicError("invalid_response", 200)
	}
	if out.Scope.ClusterDenied || (!out.Scope.All && out.Scope.ClusterCount == 0) {
		return EventSummary{}, publicError("access_denied", 200)
	}
	if out.MonitoredClusters == 0 {
		return EventSummary{}, publicError("not_found", 200)
	}
	for _, count := range []int{out.UnacknowledgedCritical, out.OpenTotal, out.SecurityEvents, out.AvailabilityEvents, out.ResolvedLast24h, out.NotificationFailures, out.MonitoredClusters, out.CoverageGapClusters, out.AttentionUrgent, out.AttentionReview, out.AttentionObserve, out.AttentionUnevaluated, out.SyntheticExcluded, out.Scope.ClusterCount, out.Scope.TotalClusters, out.Collection.IntervalSec} {
		if count < 0 {
			return EventSummary{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

// OperationalIssue는 임의 영향 JSON과 조치 사유 원문을 제외한다.
type OperationalIssue struct {
	EventRelatedIssue
	Module                 string   `json:"module"`
	CorrelationGroup       string   `json:"correlationGroup,omitempty"`
	ClusterName            string   `json:"clusterName,omitempty"`
	Environment            string   `json:"environment,omitempty"`
	ReasonCodes            []string `json:"reasonCodes,omitempty"`
	OpenedAt               string   `json:"openedAt"`
	AcknowledgedAt         *string  `json:"acknowledgedAt,omitempty"`
	ResolvedAt             *string  `json:"resolvedAt,omitempty"`
	SuppressedUntil        *string  `json:"suppressedUntil,omitempty"`
	NotificationSuppressed bool     `json:"notificationSuppressed"`
	RecurrenceCount        int      `json:"recurrenceCount"`
	DurationSeconds        int64    `json:"durationSeconds"`
	Demo                   bool     `json:"demo"`
}

type OperationalIssuePage struct {
	Items    []OperationalIssue `json:"items"`
	Total    int                `json:"total"`
	Page     int                `json:"page"`
	PageSize int                `json:"pageSize"`
}

type IssueFilters struct {
	Page, PageSize      int
	Status, Attention   []string
	Group, Search, Sort string
	ExcludeSynthetic    bool
}

func (c *Client) ListOperationalIssues(ctx context.Context, clusterID string, f IssueFilters) (OperationalIssuePage, error) {
	var out OperationalIssuePage
	if strings.TrimSpace(clusterID) == "" || f.Page < 1 || f.PageSize < 1 || f.PageSize > 100 {
		return out, publicError("invalid_request", 0)
	}
	q := url.Values{"clusterId": {clusterID}, "page": {strconv.Itoa(f.Page)}, "pageSize": {strconv.Itoa(f.PageSize)}}
	for key, values := range map[string][]string{"status": f.Status, "attention": f.Attention} {
		for _, value := range values {
			q.Add(key, value)
		}
	}
	for key, value := range map[string]string{"group": f.Group, "search": f.Search, "sort": f.Sort} {
		if value != "" {
			q.Set(key, value)
		}
	}
	if f.ExcludeSynthetic {
		q.Set("excludeSynthetic", "true")
	}
	if err := c.Get(ctx, []string{"operational-issues"}, q, &out); err != nil {
		return OperationalIssuePage{}, err
	}
	if out.Items == nil || out.Total < 0 || out.Page != f.Page || out.PageSize != f.PageSize || len(out.Items) > f.PageSize || out.Total < len(out.Items) {
		return OperationalIssuePage{}, publicError("invalid_response", 200)
	}
	for _, item := range out.Items {
		if item.ID == "" || item.ClusterID != clusterID || !validEventStatus(item.Status) {
			return OperationalIssuePage{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

type EventRuleValues struct {
	Severity                string `json:"severity"`
	Enabled                 bool   `json:"enabled"`
	ConsecutiveHits         int    `json:"consecutiveHits"`
	MinDurationSec          int    `json:"minDurationSec"`
	AutoResolveMisses       int    `json:"autoResolveMisses"`
	NotifyEnabled           bool   `json:"notifyEnabled"`
	RenotifyIntervalSec     int    `json:"renotifyIntervalSec"`
	RecordDuringMaintenance bool   `json:"recordDuringMaintenance"`
	NotifyDuringMaintenance bool   `json:"notifyDuringMaintenance"`
}

// Override의 포인터는 미설정과 명시적 false·0을 구분한다.
type EventRuleOverride struct {
	EventCode               string `json:"eventCode"`
	ClusterID               string `json:"clusterId"`
	Enabled                 *bool  `json:"enabled,omitempty"`
	Severity                string `json:"severity,omitempty"`
	ConsecutiveHits         *int   `json:"consecutiveHits,omitempty"`
	MinDurationSec          *int   `json:"minDurationSec,omitempty"`
	AutoResolveMisses       *int   `json:"autoResolveMisses,omitempty"`
	NotifyEnabled           *bool  `json:"notifyEnabled,omitempty"`
	RenotifyIntervalSec     *int   `json:"renotifyIntervalSec,omitempty"`
	RecordDuringMaintenance *bool  `json:"recordDuringMaintenance,omitempty"`
	NotifyDuringMaintenance *bool  `json:"notifyDuringMaintenance,omitempty"`
}

type EventRule struct {
	EventCode         string             `json:"eventCode"`
	Category          string             `json:"category"`
	Source            string             `json:"source"`
	ResourceType      string             `json:"resourceType"`
	Description       string             `json:"description,omitempty"`
	RecommendedAction string             `json:"recommendedAction,omitempty"`
	SourceImplemented bool               `json:"sourceImplemented"`
	Effective         *EventRuleValues   `json:"effective"`
	Default           *EventRuleValues   `json:"default"`
	Overridden        bool               `json:"overridden"`
	GlobalOverride    *EventRuleOverride `json:"globalOverride,omitempty"`
	ClusterOverride   *EventRuleOverride `json:"clusterOverride,omitempty"`
}

type AttentionValues struct {
	DefaultAttention     string `json:"defaultAttention"`
	MinimumAttention     string `json:"minimumAttention"`
	Actionability        string `json:"actionability"`
	TrendEnabled         bool   `json:"trendEnabled"`
	CorrelationEnabled   bool   `json:"correlationEnabled"`
	CorrelationGroup     string `json:"correlationGroup,omitempty"`
	CorrelationWindowSec int    `json:"correlationWindowSec"`
	NotifyMode           string `json:"notifyMode"`
	MinDurationSec       int    `json:"minDurationSec"`
}

type AttentionOverride struct {
	EventCode            string  `json:"eventCode"`
	ClusterID            string  `json:"clusterId"`
	DefaultAttention     *string `json:"defaultAttention,omitempty"`
	MinimumAttention     *string `json:"minimumAttention,omitempty"`
	Actionability        *string `json:"actionability,omitempty"`
	TrendEnabled         *bool   `json:"trendEnabled,omitempty"`
	CorrelationEnabled   *bool   `json:"correlationEnabled,omitempty"`
	CorrelationGroup     *string `json:"correlationGroup,omitempty"`
	CorrelationWindowSec *int    `json:"correlationWindowSec,omitempty"`
	NotifyMode           *string `json:"notifyMode,omitempty"`
}

type AttentionRule struct {
	EventCode       string             `json:"eventCode"`
	Category        string             `json:"category"`
	ResourceType    string             `json:"resourceType"`
	Description     string             `json:"description,omitempty"`
	Effective       *AttentionValues   `json:"effective"`
	Default         *AttentionValues   `json:"default"`
	Overridden      bool               `json:"overridden"`
	ProfileApplied  string             `json:"profileApplied"`
	GlobalOverride  *AttentionOverride `json:"globalOverride,omitempty"`
	ClusterOverride *AttentionOverride `json:"clusterOverride,omitempty"`
}

type AttentionProfile struct {
	ClusterID string `json:"clusterId"`
	Profile   string `json:"profile"`
	Explicit  bool   `json:"explicit"`
}

func (c *Client) GetEventRule(ctx context.Context, clusterID, eventCode string) (EventRule, error) {
	var out struct {
		ClusterID string      `json:"clusterId"`
		Items     []EventRule `json:"items"`
		Total     int         `json:"total"`
	}
	if strings.TrimSpace(clusterID) == "" || strings.TrimSpace(eventCode) == "" {
		return EventRule{}, publicError("invalid_request", 0)
	}
	if err := c.Get(ctx, []string{"event-rules"}, url.Values{"clusterId": {clusterID}}, &out); err != nil {
		return EventRule{}, err
	}
	if out.ClusterID != clusterID || out.Items == nil || out.Total != len(out.Items) {
		return EventRule{}, publicError("invalid_response", 200)
	}
	var selected *EventRule
	for _, item := range out.Items {
		if item.EventCode != eventCode {
			continue
		}
		if selected != nil || item.Effective == nil || item.Default == nil || (item.GlobalOverride != nil && (item.GlobalOverride.ClusterID != "" || item.GlobalOverride.EventCode != eventCode)) || (item.ClusterOverride != nil && (item.ClusterOverride.ClusterID != clusterID || item.ClusterOverride.EventCode != eventCode)) {
			return EventRule{}, publicError("invalid_response", 200)
		}
		copy := item
		selected = &copy
	}
	if selected == nil {
		return EventRule{}, publicError("not_found", 200)
	}
	return *selected, nil
}

func (c *Client) GetAttentionRule(ctx context.Context, clusterID, eventCode string) (AttentionRule, error) {
	var out struct {
		ClusterID string          `json:"clusterId"`
		Profile   string          `json:"profile"`
		Items     []AttentionRule `json:"items"`
		Total     int             `json:"total"`
	}
	if strings.TrimSpace(clusterID) == "" || strings.TrimSpace(eventCode) == "" {
		return AttentionRule{}, publicError("invalid_request", 0)
	}
	if err := c.Get(ctx, []string{"event-attention-overrides"}, url.Values{"clusterId": {clusterID}}, &out); err != nil {
		return AttentionRule{}, err
	}
	if out.ClusterID != clusterID || out.Profile == "" || out.Items == nil || out.Total != len(out.Items) {
		return AttentionRule{}, publicError("invalid_response", 200)
	}
	var selected *AttentionRule
	for _, item := range out.Items {
		if item.EventCode != eventCode {
			continue
		}
		if selected != nil || item.Effective == nil || item.Default == nil || item.ProfileApplied != out.Profile || (item.GlobalOverride != nil && (item.GlobalOverride.ClusterID != "" || item.GlobalOverride.EventCode != eventCode)) || (item.ClusterOverride != nil && (item.ClusterOverride.ClusterID != clusterID || item.ClusterOverride.EventCode != eventCode)) {
			return AttentionRule{}, publicError("invalid_response", 200)
		}
		copy := item
		selected = &copy
	}
	if selected == nil {
		return AttentionRule{}, publicError("not_found", 200)
	}
	return *selected, nil
}

func (c *Client) GetAttentionProfile(ctx context.Context, clusterID string) (AttentionProfile, error) {
	var out AttentionProfile
	if err := c.getCluster(ctx, clusterID, []string{"attention-profile"}, nil, &out); err != nil {
		return AttentionProfile{}, err
	}
	if out.ClusterID != clusterID || out.Profile == "" {
		return AttentionProfile{}, publicError("invalid_response", 200)
	}
	return out, nil
}

// MaintenanceWindow에는 자유 서술 사유·등록자·인증 정보를 선언하지 않는다.
type MaintenanceWindow struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	StartsAt   string   `json:"startsAt"`
	EndsAt     string   `json:"endsAt"`
	ClusterID  string   `json:"clusterId"`
	EventCodes []string `json:"eventCodes,omitempty"`
	Enabled    bool     `json:"enabled"`
	Active     bool     `json:"active"`
}

func (c *Client) ListMaintenanceWindows(ctx context.Context, clusterID string, activeOnly bool) ([]MaintenanceWindow, error) {
	var out struct {
		Items []MaintenanceWindow `json:"items"`
		Total int                 `json:"total"`
	}
	if strings.TrimSpace(clusterID) == "" {
		return nil, publicError("invalid_request", 0)
	}
	q := url.Values{"clusterId": {clusterID}}
	if activeOnly {
		q.Set("activeOnly", "true")
	}
	if err := c.Get(ctx, []string{"event-maintenance-windows"}, q, &out); err != nil {
		return nil, err
	}
	if out.Items == nil || out.Total != len(out.Items) {
		return nil, publicError("invalid_response", 200)
	}
	for _, item := range out.Items {
		if item.ID == "" || (item.ClusterID != "" && item.ClusterID != clusterID) || (activeOnly && !item.Active) {
			return nil, publicError("invalid_response", 200)
		}
	}
	return out.Items, nil
}
