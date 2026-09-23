package tools

import (
	"context"
	"strings"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// DefaultThroughputRange는 포털 대상 상세 추이 카드(TargetThroughputHistory)의 기본 기간이며 백엔드 기본값과 같다.
const DefaultThroughputRange = "15m"

type TargetThroughputInput struct {
	ClusterID string `json:"cluster_id"`
	GroupName string `json:"group_name"`
	TopicName string `json:"topic_name"`
	Range     string `json:"range,omitempty"`
}

// TargetThroughputData는 Consumer Group × Topic 1건의 처리량 관측이다. 세 출처는 서로 독립적으로 성공·실패한다.
type TargetThroughputData struct {
	Group string `json:"group"`
	Topic string `json:"topic"`
	// Current는 현재 처리량과 백엔드 처리상태 판정이다. 조회 실패·대상 없음이면 null이다.
	Current *TargetThroughputCurrent `json:"current"`
	// History는 요청 기간의 유입·처리·Lag 시계열이다. 조회 실패면 null이다.
	History *TargetThroughputHistory `json:"history"`
	// Rebalance는 재조정 관측과 처리상태의 상관 힌트다(인과 아님). 조회 실패면 null이다.
	Rebalance *TargetRebalanceHint `json:"rebalance"`
	// Sources는 출처별 조회 결과다(ok·partial·not_found·error). 실패한 출처를 0이나 빈 값으로 채우지 않는다.
	Sources []TargetThroughputSource `json:"sources"`
}

type TargetThroughputSource struct {
	Source string `json:"source" jsonschema:"current, history, rebalance 중 하나"`
	Result string `json:"result" jsonschema:"ok, partial, not_found, error 중 하나"`
}

type TargetThroughputCurrent struct {
	ableops.ThroughputTarget
	// CheckedAt은 처리량 계산에 쓴 Lag 관측 시각이다.
	CheckedAt string `json:"checkedAt"`
	// ClusterObservationStatus는 클러스터 Lag 관측 종합 상태이며 이 대상의 판정이 아니다.
	ClusterObservationStatus string                       `json:"cluster_observation_status"`
	ProduceSource            ableops.ThroughputSourceInfo `json:"produceSource"`
	ConsumeSource            ableops.ThroughputSourceInfo `json:"consumeSource"`
}

type TargetThroughputHistory struct {
	ableops.ThroughputHistory
	ExpectedPoints int `json:"expected_points"`
}

type TargetRebalanceHint struct {
	ableops.TargetRebalance
	// Semantics는 상관 힌트가 원인 판정이 아니라는 고정 표기다.
	Semantics string `json:"semantics"`
}

const rebalanceSemantics = "correlation_hint_not_causation"

func registerThroughputTools(server *mcp.Server, s *service) {
	nonDestructive := false
	schema := inputSchema(true, true, false)
	props := schema["properties"].(map[string]any)
	delete(props, "limit")
	props["topic_name"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 249, "description": "대상 Topic 이름. group_name과 함께 정확히 일치하는 대상 1건만 반환합니다."}
	props["range"] = map[string]any{"type": "string", "enum": []string{"5m", "15m", "1h", "6h"}, "description": "시계열 기간. 기본 15m(포털 대상 상세 기본값). 해상도는 백엔드 고정값입니다."}
	schema["required"] = []string{"cluster_id", "group_name", "topic_name"}
	// 현재값 조회는 /consumer-lag와 같은 Lag 관측을 거쳐 자산 스냅샷 저장·포털 샘플러 표본 축적이 일어날 수 있다.
	registerWithAnnotations(server, s, "get_consumer_target_throughput",
		"Consumer Group × Topic 1건의 현재 유입·처리 속도, Lag 증감, 처리율, 회복 ETA, 백엔드 처리상태, 재조정 상관 힌트와 기간 시계열을 조회합니다. 미수집·실패를 0으로 채우지 않습니다.",
		schema, func(i TargetThroughputInput) string { return i.ClusterID },
		&mcp.ToolAnnotations{ReadOnlyHint: false, IdempotentHint: false, DestructiveHint: &nonDestructive}, s.targetThroughput)
}

func (s *service) targetThroughput(ctx context.Context, input TargetThroughputInput) Envelope[TargetThroughputData] {
	out := newEnvelope[TargetThroughputData](s.client.RedactContext(ctx, input.ClusterID), "backend_consumer_throughput")
	period := input.Range
	if period == "" {
		period = DefaultThroughputRange
	}
	expected, rangeOK := ableops.ThroughputHistoryRange(period)
	group, topic := input.GroupName, input.TopicName
	if !validMonitoringInput(input.ClusterID, 0) || !rangeOK || !validName(group, 1024) || !validName(topic, 249) {
		return invalid(out, "cluster_id, group_name, topic_name과 지원 기간(5m·15m·1h·6h)을 확인하세요.")
	}
	data := TargetThroughputData{Group: group, Topic: topic, Sources: []TargetThroughputSource{}}
	out.Data = &data
	out.Limitations = []string{
		monitoringModeLimitation,
		"lagGrowthRate는 produceRate-consumeRate(msg/s)이며 양수가 Lag 증가입니다. processingRatio는 consumeRate/produceRate 비율로 1을 넘을 수 있고, recoveryEtaSec은 처리가 유입보다 빠르고 Lag가 있을 때만 계산됩니다.",
		"processingState·processingReason은 백엔드 판정을 그대로 옮긴 값입니다. Lag 위험도(OK/WARN/CRITICAL)와 다른 축이며 서로 대응시키지 마세요. UNOBSERVED는 정상이 아니라 판정 불가입니다.",
		"status가 MEASURED가 아닌 값·계열에는 value·points가 없습니다. 미수집·조회 실패를 0이나 정상으로 해석하지 마세요.",
	}
	var failures []Failure
	succeeded := 0
	mark := func(source, result string) {
		data.Sources = append(data.Sources, TargetThroughputSource{Source: source, Result: result})
		if result == "ok" || result == "partial" {
			succeeded++
		}
	}
	incomplete := false

	// 1. 현재값 — 클러스터 리포트에서 group+topic이 정확히 일치하는 대상만 고른다.
	report, target, err := s.client.ConsumerThroughputTarget(ctx, input.ClusterID, group, topic)
	switch {
	case err != nil:
		failures = append(failures, failure(err, "throughput_current"))
		mark("current", "error")
	case target == nil:
		failures = append(failures, Failure{Component: "throughput_current", Code: "target_not_found", Message: "처리량 리포트에 요청한 Consumer Group × Topic 대상이 없습니다. 다른 대상으로 대체하지 않았습니다.", HTTPStatus: 200})
		mark("current", "not_found")
	default:
		data.Current = &TargetThroughputCurrent{ThroughputTarget: *target, CheckedAt: report.CheckedAt, ClusterObservationStatus: report.Status, ProduceSource: report.ProduceSource, ConsumeSource: report.ConsumeSource}
		result := "ok"
		if !target.ProduceRate.Measured() || !target.ConsumeRate.Measured() || target.LagObservation != "OBSERVED" {
			result = "partial"
			out.Limitations = append(out.Limitations, "현재값 일부가 실측이 아니거나 Lag 관측이 부분적입니다. 값별 status와 lagObservation을 함께 확인하세요.")
		}
		mark("current", result)
		incomplete = incomplete || result == "partial"
	}

	// 2. 시계열 — 대상 식별자로 좁힌 Prometheus 레인지 조회다.
	history, err := s.client.ConsumerThroughputHistory(ctx, input.ClusterID, group, topic, period)
	if err != nil {
		failures = append(failures, failure(err, "throughput_history"))
		mark("history", "error")
	} else {
		data.History = &TargetThroughputHistory{ThroughputHistory: history, ExpectedPoints: expected}
		result := "ok"
		for _, series := range []ableops.ThroughputSeries{history.Produce, history.Consume, history.Lag} {
			if series.Status != "MEASURED" || len(series.Points) < expected {
				result = "partial"
			}
		}
		if result == "partial" {
			out.Limitations = append(out.Limitations, "시계열 일부 계열이 실측이 아니거나 기대 버킷보다 점이 적습니다. 빈 구간은 보간·0 채움 없이 비어 있으며 마지막 점을 현재값으로 단정하지 마세요.")
		}
		mark("history", result)
		incomplete = incomplete || result == "partial"
	}

	// 3. 재조정 상관 — /consumer-lag 관측의 같은 대상 행만 읽는다.
	rebalance, err := s.client.ConsumerTargetRebalance(ctx, input.ClusterID, group, topic)
	switch {
	case err != nil:
		failures = append(failures, failure(err, "rebalance"))
		mark("rebalance", "error")
	case !rebalance.GroupFound:
		failures = append(failures, Failure{Component: "rebalance", Code: "not_found", Message: "Lag 관측에 요청한 Consumer Group이 없습니다.", HTTPStatus: 200})
		mark("rebalance", "not_found")
	case !rebalance.TargetFound:
		data.Rebalance = &TargetRebalanceHint{TargetRebalance: rebalance, Semantics: rebalanceSemantics}
		failures = append(failures, Failure{Component: "rebalance", Code: "target_not_found", Message: "Lag 관측에 요청한 Consumer Group × Topic 대상이 없습니다.", HTTPStatus: 200})
		mark("rebalance", "not_found")
	default:
		data.Rebalance = &TargetRebalanceHint{TargetRebalance: rebalance, Semantics: rebalanceSemantics}
		result := "ok"
		if rebalance.Correlation == nil || rebalance.Correlation.State == "UNOBSERVED" || rebalance.GroupRebalance == nil || rebalance.GroupRebalance.State == "UNKNOWN" {
			result = "partial"
			out.Limitations = append(out.Limitations, "백엔드가 재조정 상관을 판정하지 않았거나 그룹 상태를 확인하지 못했습니다. 재조정 없음으로 해석하지 마세요.")
		}
		mark("rebalance", result)
		incomplete = incomplete || result == "partial"
	}
	out.Limitations = append(out.Limitations, "rebalance는 재조정과 처리 저하가 같은 시점에 관측됐다는 상관 힌트이며 원인 판정이 아닙니다. 관측 시각·지속시간은 포털 관측 기준 하한값입니다.")

	out.Errors = failures
	switch {
	case succeeded == 0:
		out.Status = "error"
		out.Data = nil
	case len(failures) > 0 || incomplete:
		out.Status = "partial"
	}
	bound(&out, func() bool {
		if data.History == nil {
			return false
		}
		changed := false
		for _, series := range []*ableops.ThroughputSeries{&data.History.Produce, &data.History.Consume, &data.History.Lag} {
			if len(series.Points) > 0 {
				series.Points = series.Points[(len(series.Points)+1)/2:]
				changed = true
			}
		}
		return changed
	})
	return out
}

func validName(value string, max int) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= max
}
