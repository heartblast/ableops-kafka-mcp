package ableops

import (
	"context"
	"net/url"
	"strings"
)

// Consumer Group × Topic 처리량 DTO는 로컬 REST 계약(GET /consumer-lag/throughput·/throughput/history·
// /consumer-lag)에서 확인한 공개 필드만 선언한다. PromQL 원문(query)·오류 원문은 수신해도 공개하지 않는다.

// ThroughputRate는 처리량·파생 값 1건이다. value는 status=MEASURED일 때만 존재한다(미수집을 0으로 채우지 않는다).
type ThroughputRate struct {
	Status    string   `json:"status"`
	Value     *float64 `json:"value,omitempty"`
	Source    string   `json:"source,omitempty"`
	WindowSec int      `json:"windowSec,omitempty"`
	Reason    string   `json:"reason,omitempty"`
}

// Measured는 실측값인지 반환한다.
func (r ThroughputRate) Measured() bool { return r.Status == "MEASURED" && r.Value != nil }

// ThroughputSourceInfo는 수집 경로 진단이다. 백엔드의 query(PromQL)는 선언하지 않아 수신해도 버린다.
type ThroughputSourceInfo struct {
	Kind      string `json:"kind"`
	Label     string `json:"label"`
	WindowSec int    `json:"windowSec,omitempty"`
	Warning   string `json:"warning,omitempty"`
}

// ThroughputTarget은 대상(Group × Topic) 1건의 현재 처리량과 백엔드 처리상태 판정이다.
type ThroughputTarget struct {
	Group            string         `json:"group"`
	Topic            string         `json:"topic"`
	BusinessName     string         `json:"businessName,omitempty"`
	CurrentLag       int64          `json:"currentLag"`
	LagObservation   string         `json:"lagObservation"`
	Monitored        bool           `json:"monitored"`
	ProduceRate      ThroughputRate `json:"produceRate"`
	ConsumeRate      ThroughputRate `json:"consumeRate"`
	LagGrowthRate    ThroughputRate `json:"lagGrowthRate"`
	ProcessingRatio  ThroughputRate `json:"processingRatio"`
	RecoveryEtaSec   ThroughputRate `json:"recoveryEtaSec"`
	ProcessingState  string         `json:"processingState"`
	ProcessingReason string         `json:"processingReason,omitempty"`
}

// ThroughputReport는 GET /clusters/{id}/consumer-lag/throughput 응답이다(요약·Topic 목록은 대상 1건 도구에 불필요해 받지 않는다).
type ThroughputReport struct {
	ClusterID     string               `json:"clusterId"`
	Status        string               `json:"status"`
	Partial       bool                 `json:"partial"`
	Reasons       []string             `json:"reasons"`
	CheckedAt     string               `json:"checkedAt"`
	ProduceSource ThroughputSourceInfo `json:"produceSource"`
	ConsumeSource ThroughputSourceInfo `json:"consumeSource"`
	Targets       []ThroughputTarget   `json:"targets"`
}

// ThroughputPoint는 시계열 점 1개다. 백엔드는 없는 구간에 점을 만들지 않는다.
type ThroughputPoint struct {
	Ts    string  `json:"ts"`
	Value float64 `json:"value"`
}

// ThroughputSeries는 유입·처리·Lag 계열 1개다. query(PromQL)는 선언하지 않는다.
type ThroughputSeries struct {
	Status    string            `json:"status"`
	Source    string            `json:"source,omitempty"`
	Unit      string            `json:"unit"`
	WindowSec int               `json:"windowSec,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Points    []ThroughputPoint `json:"points"`
}

// ThroughputHistory는 GET /clusters/{id}/consumer-lag/throughput/history 응답이다.
type ThroughputHistory struct {
	ClusterID string           `json:"clusterId"`
	Group     string           `json:"group"`
	Topic     string           `json:"topic"`
	Range     string           `json:"range"`
	StepSec   int              `json:"stepSec"`
	From      string           `json:"from"`
	To        string           `json:"to"`
	Produce   ThroughputSeries `json:"produce"`
	Consume   ThroughputSeries `json:"consume"`
	Lag       ThroughputSeries `json:"lag"`
}

// RebalanceCorrelation은 재조정 관측과 처리상태의 상관 힌트다(인과 판정이 아니다).
type RebalanceCorrelation struct {
	State       string `json:"state"`
	Reason      string `json:"reason"`
	SinceEndSec *int64 `json:"sinceEndSec,omitempty"`
}

// RebalanceObservation은 그룹의 포털 관측 기반 재조정 요약이다. 시각·지속시간은 관측 하한값이다.
type RebalanceObservation struct {
	State               string  `json:"state"`
	FirstObservedAt     *string `json:"firstObservedAt,omitempty"`
	LastObservedAt      *string `json:"lastObservedAt,omitempty"`
	ObservedDurationSec *int64  `json:"observedDurationSec,omitempty"`
	RecentCount         int     `json:"recentCount"`
	RecentWindowSec     int     `json:"recentWindowSec"`
	Prolonged           bool    `json:"prolonged"`
}

// lagRebalanceView는 /consumer-lag 응답에서 재조정 상관에 필요한 필드만 읽는 내부 투영이다.
type lagRebalanceView struct {
	ClusterID string `json:"clusterId"`
	Status    string `json:"status"`
	CheckedAt string `json:"checkedAt"`
	Groups    []struct {
		Group       string                `json:"group"`
		State       string                `json:"state"`
		Rebalancing bool                  `json:"rebalancing"`
		Observation string                `json:"observation"`
		Rebalance   *RebalanceObservation `json:"rebalance,omitempty"`
	} `json:"groups"`
	Targets []struct {
		Group                string                `json:"group"`
		Topic                string                `json:"topic"`
		Observation          string                `json:"observation"`
		ProcessingState      string                `json:"processingState,omitempty"`
		RebalanceCorrelation *RebalanceCorrelation `json:"rebalanceCorrelation,omitempty"`
	} `json:"targets"`
}

// TargetRebalance는 Group × Topic 1건의 재조정 상관 조회 결과다.
type TargetRebalance struct {
	CheckedAt       string                `json:"checkedAt,omitempty"`
	GroupFound      bool                  `json:"groupFound"`
	TargetFound     bool                  `json:"targetFound"`
	GroupState      string                `json:"groupState,omitempty"`
	Rebalancing     bool                  `json:"rebalancing"`
	GroupRebalance  *RebalanceObservation `json:"groupRebalance,omitempty"`
	Correlation     *RebalanceCorrelation `json:"correlation,omitempty"`
	ProcessingState string                `json:"processingState,omitempty"`
}

// ThroughputHistoryRange는 백엔드가 지원하는 처리량 시계열 기간과 기대 버킷 수를 반환한다.
// 백엔드는 미지원 값을 15m으로 바꾸므로 MCP가 먼저 거부해 요청과 다른 기간의 결과를 받지 않는다.
func ThroughputHistoryRange(value string) (points int, ok bool) {
	switch value {
	case "5m":
		return 21, true
	case "15m":
		return 31, true
	case "1h":
		return 61, true
	case "6h":
		return 73, true
	default:
		return 0, false
	}
}

func validRateStatus(value string) bool {
	return oneOfMonitoring(value, "MEASURED", "NO_DATA", "QUERY_FAILED", "FORBIDDEN", "NOT_COLLECTED", "NOT_APPLICABLE")
}

func validProcessingState(value string) bool {
	return oneOfMonitoring(value, "HEALTHY", "DEGRADING", "RECOVERING", "STALLED", "UNOBSERVED")
}

// rateReasons는 상태별 고정 사유다. 백엔드 사유에는 Prometheus 오류 원문(promFailure·historySeries의 err.Error())이
// 이어 붙을 수 있어 어떤 상태든 원문을 옮기지 않는다. 상태 자체가 의미를 전달한다.
var rateReasons = map[string]string{
	"NO_DATA":        "수집원에 도달했으나 이 대상의 표본이 없습니다. 오류 원문은 생략합니다.",
	"QUERY_FAILED":   "처리량 수집원 조회에 실패했습니다. 오류 원문은 생략합니다.",
	"FORBIDDEN":      "백엔드가 Kafka 조회 권한 부족을 보고했습니다. 오류 원문은 생략합니다.",
	"NOT_COLLECTED":  "수집원이 구성되지 않았거나 표본이 아직 부족합니다. 수집 실패와 0을 구분하세요.",
	"NOT_APPLICABLE": "관측은 정상이나 계산이 정의되지 않습니다(예: 유입 0의 처리율, 처리가 유입보다 느린 ETA, 대상 단위로 좁힐 수 없는 수집원).",
}

// safeRate는 상태 enum과 value 존재 규약을 검증하고 사유를 고정 문구로 바꾼다.
func safeRate(r *ThroughputRate) bool {
	if !validRateStatus(r.Status) || (r.Status == "MEASURED") != (r.Value != nil) {
		return false
	}
	r.Reason = rateReasons[r.Status]
	return true
}

func safeSeries(s *ThroughputSeries) bool {
	if !validRateStatus(s.Status) || (s.Status != "MEASURED" && len(s.Points) > 0) {
		return false
	}
	if s.Points == nil {
		s.Points = []ThroughputPoint{}
	}
	s.Reason = rateReasons[s.Status]
	return true
}

func safeSourceInfo(s *ThroughputSourceInfo) {
	if s.Warning != "" {
		// 경고에는 Prometheus 접속 오류 원문이 포함될 수 있다.
		s.Warning = "이 수집 경로가 일부 값을 만들지 못했습니다. 오류 원문은 생략합니다."
	}
}

// ConsumerThroughputTarget은 클러스터 처리량 리포트에서 group과 topic이 정확히 일치하는 대상 1건만 고른다.
// 대상이 없으면 found=false이며 다른 대상으로 대체하지 않는다.
func (c *Client) ConsumerThroughputTarget(ctx context.Context, clusterID, group, topic string) (ThroughputReport, *ThroughputTarget, error) {
	var out ThroughputReport
	if strings.TrimSpace(group) == "" || strings.TrimSpace(topic) == "" {
		return out, nil, publicError("invalid_request", 0)
	}
	if err := c.getCluster(ctx, clusterID, []string{"consumer-lag", "throughput"}, nil, &out); err != nil {
		return out, nil, err
	}
	if out.ClusterID != clusterID || !validProbeStatus(out.Status) {
		return ThroughputReport{}, nil, publicError("invalid_response", 200)
	}
	out.Reasons = safeProbeReasons(out.Status, out.Reasons)
	safeSourceInfo(&out.ProduceSource)
	safeSourceInfo(&out.ConsumeSource)
	var found *ThroughputTarget
	for i := range out.Targets {
		item := out.Targets[i]
		if item.Group != group || item.Topic != topic {
			continue
		}
		for _, rate := range []*ThroughputRate{&item.ProduceRate, &item.ConsumeRate, &item.LagGrowthRate, &item.ProcessingRatio, &item.RecoveryEtaSec} {
			if !safeRate(rate) {
				return ThroughputReport{}, nil, publicError("invalid_response", 200)
			}
		}
		if !validProcessingState(item.ProcessingState) || !validLagObservation(item.LagObservation) {
			return ThroughputReport{}, nil, publicError("invalid_response", 200)
		}
		if item.ProcessingState == "UNOBSERVED" {
			// UNOBSERVED 사유는 입력 값의 미수집 사유(오류 원문 포함 가능)를 그대로 옮긴 것이다.
			item.ProcessingReason = "판정에 필요한 입력이 실측이 아니어서 백엔드가 처리상태를 판정하지 않았습니다."
		}
		found = &item
		break
	}
	out.Targets = nil
	return out, found, nil
}

// ConsumerThroughputHistory는 대상 1건의 유입·처리·Lag 시계열을 조회한다.
func (c *Client) ConsumerThroughputHistory(ctx context.Context, clusterID, group, topic, period string) (ThroughputHistory, error) {
	var out ThroughputHistory
	if _, ok := ThroughputHistoryRange(period); !ok || strings.TrimSpace(group) == "" || strings.TrimSpace(topic) == "" {
		return out, publicError("invalid_request", 0)
	}
	q := url.Values{"group": {group}, "topic": {topic}, "range": {period}}
	if err := c.getCluster(ctx, clusterID, []string{"consumer-lag", "throughput", "history"}, q, &out); err != nil {
		return out, err
	}
	if out.ClusterID != clusterID || out.Group != group || out.Topic != topic || out.Range != period || out.StepSec <= 0 {
		return ThroughputHistory{}, publicError("invalid_response", 200)
	}
	for _, series := range []*ThroughputSeries{&out.Produce, &out.Consume, &out.Lag} {
		if !safeSeries(series) {
			return ThroughputHistory{}, publicError("invalid_response", 200)
		}
	}
	if out.Produce.Unit != "msg/s" || out.Consume.Unit != "msg/s" || out.Lag.Unit != "messages" {
		return ThroughputHistory{}, publicError("invalid_response", 200)
	}
	return out, nil
}

// ConsumerTargetRebalance는 /consumer-lag 관측에서 group·topic이 정확히 일치하는 재조정 상관 힌트만 읽는다.
func (c *Client) ConsumerTargetRebalance(ctx context.Context, clusterID, group, topic string) (TargetRebalance, error) {
	var view lagRebalanceView
	if strings.TrimSpace(group) == "" || strings.TrimSpace(topic) == "" {
		return TargetRebalance{}, publicError("invalid_request", 0)
	}
	if err := c.getCluster(ctx, clusterID, []string{"consumer-lag"}, nil, &view); err != nil {
		return TargetRebalance{}, err
	}
	if view.ClusterID != clusterID || !validProbeStatus(view.Status) {
		return TargetRebalance{}, publicError("invalid_response", 200)
	}
	out := TargetRebalance{CheckedAt: view.CheckedAt}
	for _, g := range view.Groups {
		if g.Group != group {
			continue
		}
		if g.Rebalance != nil && !oneOfMonitoring(g.Rebalance.State, "STABLE", "REBALANCING", "UNKNOWN") {
			return TargetRebalance{}, publicError("invalid_response", 200)
		}
		out.GroupFound, out.GroupState, out.Rebalancing, out.GroupRebalance = true, g.State, g.Rebalancing, g.Rebalance
		break
	}
	for _, t := range view.Targets {
		if t.Group != group || t.Topic != topic {
			continue
		}
		if t.RebalanceCorrelation != nil && !validCorrelationState(t.RebalanceCorrelation.State) {
			return TargetRebalance{}, publicError("invalid_response", 200)
		}
		out.TargetFound, out.Correlation, out.ProcessingState = true, t.RebalanceCorrelation, t.ProcessingState
		break
	}
	return out, nil
}

func validCorrelationState(value string) bool {
	return oneOfMonitoring(value, "NONE", "REBALANCE_ACTIVE", "REBALANCE_RECENT", "REBALANCE_RECOVERY",
		"REBALANCE_PERSISTENT_IMPACT", "REBALANCE_REPEATED", "UNOBSERVED")
}
