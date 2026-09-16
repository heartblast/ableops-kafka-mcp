package tools

import (
	"context"
	"strings"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type LagOverviewInput struct {
	ClusterID string `json:"cluster_id"`
	Refresh   bool   `json:"refresh,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type LagPolicyInput struct {
	ClusterID string `json:"cluster_id"`
	TopicName string `json:"topic_name,omitempty"`
	GroupName string `json:"group_name,omitempty"`
}

type MetricSeriesInput struct {
	ClusterID string `json:"cluster_id"`
	Metric    string `json:"metric"`
	Range     string `json:"range,omitempty"`
	MaxPoints int    `json:"max_points,omitempty"`
}

type MetricSeriesData struct {
	ableops.MetricSeries
	StepSeconds     int    `json:"step_seconds"`
	ExpectedPoints  int    `json:"expected_points"`
	ReceivedPoints  int    `json:"received_points"`
	ReturnedPoints  int    `json:"returned_points"`
	PointLimit      int    `json:"point_limit"`
	DataMode        string `json:"data_mode"`
	QualityVerified bool   `json:"quality_verified"`
}

func registerMonitoringTools(server *mcp.Server, s *service) {
	nonDestructive := false
	overview := inputSchema(true, false, false)
	overview["properties"].(map[string]any)["refresh"] = map[string]any{"type": "boolean", "description": "백엔드 관측 캐시를 우회합니다. 기본 false"}
	registerWithAnnotations(server, s, "get_consumer_lag_overview", "그룹×토픽 Lag 관측·현재 판정·발생 경보를 구분해 조회합니다. 최초 조회 시 백엔드가 자산 스냅샷을 저장할 수 있습니다.", overview, func(i LagOverviewInput) string { return i.ClusterID }, &mcp.ToolAnnotations{ReadOnlyHint: false, IdempotentHint: false, DestructiveHint: &nonDestructive}, s.consumerLagOverview)
	policy := inputSchema(true, false, false)
	props := policy["properties"].(map[string]any)
	delete(props, "limit")
	props["topic_name"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 249, "description": "생략하면 클러스터 정책, 지정하면 토픽 정책"}
	props["group_name"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 255, "description": "그룹×토픽 정책. topic_name을 함께 지정해야 합니다."}
	register(server, s, "get_consumer_lag_policy", "공통→클러스터→토픽→그룹×토픽 정책의 상속·직접 설정·비활성과 지속/회복 조건을 조회합니다.", policy, func(i LagPolicyInput) string { return i.ClusterID }, s.consumerLagPolicy)
	metric := inputSchema(true, false, false)
	props = metric["properties"].(map[string]any)
	delete(props, "limit")
	props["metric"] = map[string]any{"type": "string", "enum": []string{"consumer-lag", "topic-throughput", "cluster-stability"}}
	props["range"] = map[string]any{"type": "string", "enum": []string{"15m", "1h", "6h", "24h", "7d"}, "description": "기본 1h. 기간별 해상도는 백엔드 고정값입니다."}
	props["max_points"] = map[string]any{"type": "integer", "minimum": 1, "maximum": 96, "description": "최근 포인트 출력 상한. 기본은 기간별 포인트 수이며 전송 크기를 줄이지 않습니다."}
	metric["required"] = []string{"cluster_id", "metric"}
	register(server, s, "get_metric_series", "허용된 지표 추이를 조회합니다. 실측·데모와 단위를 보존하며 클러스터 범위가 확인되지 않는 메트릭은 반환하지 않습니다.", metric, func(i MetricSeriesInput) string { return i.ClusterID }, s.metricSeries)
	registerWithAnnotations(server, s, "get_cluster_storage", "Kafka 데이터 bytes와 Leader/Replica 분포를 조회합니다. 백엔드가 STORAGE_VIEW 감사 기록을 생성합니다.", inputSchema(true, false, false), func(i ClusterInput) string { return i.ClusterID }, &mcp.ToolAnnotations{ReadOnlyHint: false, IdempotentHint: false, DestructiveHint: &nonDestructive}, s.clusterStorage)
	register(server, s, "get_cluster_config_audit", "토픽 설정 정책 진단을 조회합니다. 실제 설정·오류 원문은 제외하고 규칙 코드·상태·예외·임계치를 반환합니다.", inputSchema(true, false, false), func(i ClusterInput) string { return i.ClusterID }, s.clusterConfigAudit)
	register(server, s, "get_partition_reassignments", "진행 중인 파티션 재배치를 조회합니다. 경과 시간은 포털 관측 기준 추정치이며 실행·중단 기능이 아닙니다.", inputSchema(true, false, false), func(i ClusterInput) string { return i.ClusterID }, s.partitionReassignments)
}

func validMonitoringInput(cluster string, limit int) bool {
	return validCluster(cluster) && len(cluster) <= 512 && limit >= 0 && limit <= MaxLimit
}

const monitoringModeLimitation = "이 백엔드 응답은 어댑터의 실측/mock 구분을 제공하지 않습니다. 조회 성공만으로 실측 또는 Kafka 정상 상태를 단정하지 마세요."
const monitoringListLimitation = "limit은 각 목록의 MCP 출력에만 적용됩니다. 백엔드 집계 수치는 수신 범위를 유지하며 잘린 목록을 전체 목록이나 순위로 해석하지 마세요."

func monitoringProbe[T any](out *Envelope[T], status, component string, partial, observed bool) {
	if err := backendFailure(status, component); err != nil {
		out.Status = "error"
		if observed {
			out.Status = "partial"
		}
		out.Errors = append(out.Errors, *err)
	} else if partial {
		out.Status = "partial"
		out.Errors = append(out.Errors, Failure{Component: component, Code: "partial_failure", Message: "일부 대상의 조회가 완료되지 않았습니다.", HTTPStatus: 200, BackendStatus: status})
	}
}

func (s *service) consumerLagOverview(ctx context.Context, input LagOverviewInput) Envelope[ableops.ConsumerLagOverview] {
	out := newEnvelope[ableops.ConsumerLagOverview](s.client.RedactContext(ctx, input.ClusterID), "backend_consumer_lag_snapshot")
	if !validMonitoringInput(input.ClusterID, input.Limit) {
		return invalid(out, "cluster_id와 1~100 범위의 limit을 확인하세요.")
	}
	data, err := s.client.ConsumerLagOverview(ctx, input.ClusterID, input.Refresh)
	if err != nil {
		return failed(out, err, "consumer_lag")
	}
	out.Data = &data
	observed, partial := false, data.Partial
	for _, group := range data.Groups {
		observed = observed || group.Observation == "OBSERVED" || group.Observation == "PARTIAL"
		partial = partial || group.Observation == "PARTIAL" || group.Observation == "UNAVAILABLE" || group.Observation == "FORBIDDEN"
	}
	monitoringProbe(&out, data.Status, "consumer_lag", partial, observed)
	if data.AlertsStatus != "OK" {
		if out.Status == "ok" {
			out.Status = "partial"
		}
		out.Errors = append(out.Errors, *backendFailure(data.AlertsStatus, "lag_alerts"))
		out.Limitations = append(out.Limitations, "경보 비활성 또는 조회 실패를 경보 0건으로 해석하지 마세요.")
	}
	if data.TopicsSyncedAt == nil && out.Status == "ok" {
		out.Status = "partial"
	}
	out.Limitations = append(out.Limitations, monitoringModeLimitation, monitoringListLimitation,
		"다른 그룹의 Lag를 합산해 토픽 미처리량으로 해석하지 마세요. Kafka state, 현재 status, alert.phase는 서로 다른 정보입니다.",
		"checkedAt은 Lag 관측 시각이고 topicsSyncedAt은 소비 그룹 없음 판정에 사용한 토픽 자산 시각입니다. 후자가 없으면 토픽 목록의 완전성을 확인할 수 없습니다.",
		"백엔드 활성 경보 조회에는 최대 50페이지 상한이 있으며 잘림 정보가 없습니다. 경보 집계의 전체 범위 완전성을 보장하지 않습니다.",
		"최초 토픽 자산 조회 시 백엔드가 스냅샷을 저장할 수 있습니다. 업무명은 토픽 이름 기준의 클러스터 공통 카탈로그 메타입니다.")
	limit := limitOrDefault(input.Limit)
	out.Truncated = capItems(&data.Groups, limit) || out.Truncated
	out.Truncated = capItems(&data.Topics, limit) || out.Truncated
	out.Truncated = capItems(&data.Targets, limit) || out.Truncated
	out.Truncated = capItems(&data.Reasons, limit) || out.Truncated
	for i := range data.Groups {
		g := &data.Groups[i]
		out.Truncated = capItems(&g.Reasons, limit) || out.Truncated
		out.Truncated = capItems(&g.Codes, limit) || out.Truncated
		out.Truncated = capItems(&g.EventCauses, limit) || out.Truncated
	}
	for i := range data.Targets {
		t := &data.Targets[i]
		out.Truncated = capItems(&t.Reasons, limit) || out.Truncated
		out.Truncated = capItems(&t.Codes, limit) || out.Truncated
		out.Truncated = capItems(&t.EventCauses, limit) || out.Truncated
	}
	bound(&out, func() bool {
		return shrinkItems(&data.Targets) || shrinkItems(&data.Groups) || shrinkItems(&data.Topics) || shrinkItems(&data.Reasons)
	})
	return out
}

func (s *service) consumerLagPolicy(ctx context.Context, input LagPolicyInput) Envelope[ableops.ConsumerLagPolicyDetail] {
	out := newEnvelope[ableops.ConsumerLagPolicyDetail](s.client.RedactContext(ctx, input.ClusterID), "backend_lag_policy")
	if !validMonitoringInput(input.ClusterID, 0) || len(input.TopicName) > 249 || len(input.GroupName) > 255 ||
		input.TopicName != strings.TrimSpace(input.TopicName) || input.GroupName != strings.TrimSpace(input.GroupName) || (input.GroupName != "" && input.TopicName == "") {
		return invalid(out, "cluster_id와 정책 범위를 확인하세요. group_name에는 topic_name이 필요합니다.")
	}
	data, err := s.client.ConsumerLagPolicy(ctx, input.ClusterID, input.TopicName, input.GroupName)
	if err != nil {
		return failed(out, err, "lag_policy")
	}
	out.Data = &data
	out.Limitations = []string{"stored/layers는 저장된 설정이고 inherited/effective는 적용 결과입니다. INHERIT·SET·DISABLED를 구분하며 ruleBased이면 지속/회복 시간은 이벤트 규칙을 따릅니다. 이 API는 이벤트 규칙의 실제 횟수·시간을 제공하지 않으므로 필요하면 get_consumer_lag_overview의 eventRule을 별도로 확인하세요. 이 도구의 canEdit은 권한 승인이나 변경 기능을 의미하지 않습니다."}
	bound(&out, nil)
	return out
}

func (s *service) metricSeries(ctx context.Context, input MetricSeriesInput) Envelope[MetricSeriesData] {
	out := newEnvelope[MetricSeriesData](s.client.RedactContext(ctx, input.ClusterID), "backend_metric_series")
	period := input.Range
	if period == "" {
		period = "1h"
	}
	expected, step, ok := ableops.MetricRange(period)
	if !validMonitoringInput(input.ClusterID, 0) || !ok || input.MaxPoints < 0 || input.MaxPoints > 96 {
		return invalid(out, "cluster_id, 지원 기간, 1~96 범위의 max_points를 확인하세요.")
	}
	series, err := s.client.MetricSeries(ctx, input.ClusterID, input.Metric, period)
	if err != nil {
		out.Limitations = []string{"clusterScoped=false인 전역 메트릭은 반환하지 않습니다. 백엔드의 클러스터별 메트릭 소스 설정이 필요합니다."}
		return failed(out, err, "metric_series")
	}
	limit := input.MaxPoints
	if limit == 0 || limit > expected {
		limit = expected
	}
	data := MetricSeriesData{MetricSeries: series, StepSeconds: step, ExpectedPoints: expected, ReceivedPoints: len(series.Points), PointLimit: limit, DataMode: "measured"}
	out.Data = &data
	out.Limitations = []string{
		"해상도는 기간별 백엔드 고정값입니다. max_points는 최근 시점의 MCP 출력만 제한하며 다운샘플링이나 백엔드 조회 크기 조절이 아닙니다.",
		"points.ts는 원본 시계열 시각이고 queried_at은 MCP 조회 시각입니다. 전체 Consumer Group Lag 합계는 토픽 미처리 메시지 수가 아닙니다.",
		"백엔드는 시계열 획득 실패 사유·표본별 품질을 제공하지 않습니다. 빈 시계열을 정상 또는 0으로 바꾸지 않습니다.",
	}
	if series.Demo {
		data.DataMode = "demo"
		out.Status = "partial"
		out.Limitations = append(out.Limitations, "합성·추정값이 포함된 데모 시계열입니다. 실측 추세 분석·장애 판정에 사용하지 마세요. 처리량 데모 msg/s는 실측 bytes/s와 단위가 다릅니다.")
	} else if input.Metric != "consumer-lag" {
		out.Status = "partial"
		out.Limitations = append(out.Limitations, "백엔드가 다중 시리즈의 누락 표본·조회 실패를 0으로 채울 수 있습니다. quality_verified=false이며 0을 실측 0 또는 정상으로 확정할 수 없습니다.")
	}
	if len(series.Points) == 0 || len(series.Points) < expected {
		out.Status = "partial"
		out.Limitations = append(out.Limitations, "요청 기간의 포인트 수보다 적은 시계열입니다. 미수집·조회 실패·수집 시작 여부는 이 API만으로 구별되지 않습니다.")
	}
	if len(data.Points) > limit {
		data.Points = data.Points[len(data.Points)-limit:]
		out.Truncated = true
	}
	// 합성 Lag는 상위 5개 그룹과 합계, 나머지 지표는 최대 3개 시리즈만 사용한다.
	out.Truncated = capItems(&data.Series, 6) || out.Truncated
	allowed := make(map[string]bool, len(data.Series))
	for _, name := range data.Series {
		allowed[name] = true
	}
	for i := range data.Points {
		for name := range data.Points[i].Values {
			if !allowed[name] {
				delete(data.Points[i].Values, name)
				out.Truncated = true
			}
		}
	}
	data.ReturnedPoints = len(data.Points)
	bound(&out, func() bool {
		if len(data.Points) == 0 {
			return false
		}
		data.Points = data.Points[(len(data.Points)+1)/2:]
		data.ReturnedPoints = len(data.Points)
		return true
	})
	return out
}

func (s *service) clusterStorage(ctx context.Context, input ClusterInput) Envelope[ableops.ClusterStorageView] {
	out := newEnvelope[ableops.ClusterStorageView](s.client.RedactContext(ctx, input.ClusterID), "backend_storage_observation")
	if !validMonitoringInput(input.ClusterID, input.Limit) {
		return invalid(out, "cluster_id와 1~100 범위의 limit을 확인하세요.")
	}
	data, err := s.client.ClusterStorage(ctx, input.ClusterID)
	if err != nil {
		return failed(out, err, "cluster_storage")
	}
	out.Data = &data
	partial := data.Partial || len(data.FailedBrokers) > 0
	for _, broker := range data.Brokers {
		partial = partial || broker.Status == "UNAVAILABLE" || broker.Status == "FORBIDDEN" || broker.State == "LOGDIR_ERROR"
	}
	monitoringProbe(&out, data.Status, "cluster_storage", partial, data.BrokerCount > 0)
	out.Limitations = []string{monitoringModeLimitation, monitoringListLimitation, "크기 단위는 Kafka 로그 데이터 bytes이며 OS 디스크 사용률이 아닙니다. OffsetLag는 offset 차이이고 시간 지연이 아닙니다. totalSize/avgSize는 성공 브로커 범위입니다.", "skewEvaluated/leaders.evaluated=false는 판정 생략입니다. 로그 디렉터리 경로는 제외하며 백엔드는 STORAGE_VIEW 감사 기록을 생성합니다."}
	limit := limitOrDefault(input.Limit)
	out.Truncated = capItems(&data.Brokers, limit) || out.Truncated
	out.Truncated = capItems(&data.Leaders.Brokers, limit) || out.Truncated
	out.Truncated = capItems(&data.FailedBrokers, limit) || out.Truncated
	out.Truncated = capItems(&data.Reasons, limit) || out.Truncated
	out.Truncated = capItems(&data.Leaders.Reasons, limit) || out.Truncated
	for i := range data.Brokers {
		out.Truncated = capItems(&data.Brokers[i].Reasons, limit) || out.Truncated
	}
	for i := range data.Leaders.Brokers {
		out.Truncated = capItems(&data.Leaders.Brokers[i].Reasons, limit) || out.Truncated
	}
	bound(&out, func() bool {
		return shrinkItems(&data.Brokers) || shrinkItems(&data.Leaders.Brokers) || shrinkItems(&data.FailedBrokers) || shrinkItems(&data.Reasons)
	})
	return out
}

func (s *service) clusterConfigAudit(ctx context.Context, input ClusterInput) Envelope[ableops.ConfigAuditReport] {
	out := newEnvelope[ableops.ConfigAuditReport](s.client.RedactContext(ctx, input.ClusterID), "backend_config_policy_diagnosis")
	if !validMonitoringInput(input.ClusterID, input.Limit) {
		return invalid(out, "cluster_id와 1~100 범위의 limit을 확인하세요.")
	}
	data, err := s.client.ClusterConfigAudit(ctx, input.ClusterID)
	if err != nil {
		return failed(out, err, "config_audit")
	}
	out.Data = &data
	monitoringProbe(&out, data.Status, "config_audit", data.Unavailable > 0, data.Total > data.Unavailable)
	out.Truncated = data.Truncated
	out.Limitations = []string{monitoringModeLimitation, monitoringListLimitation, "total은 백엔드 진단 대상 토픽 수입니다. 백엔드 상한 기본 200개를 초과하면 truncated=true이며 전체 클러스터 진단이 아닙니다. 설정 원문과 진단 메시지·actual은 제외합니다.", "정책 예외는 백엔드의 기본 클러스터에만 적용됩니다. exempted와 limits를 함께 확인하세요."}
	limit := limitOrDefault(input.Limit)
	out.Truncated = capItems(&data.Topics, limit) || out.Truncated
	for i := range data.Topics {
		out.Truncated = capItems(&data.Topics[i].Findings, limit) || out.Truncated
	}
	bound(&out, func() bool { return shrinkItems(&data.Topics) })
	return out
}

func (s *service) partitionReassignments(ctx context.Context, input ClusterInput) Envelope[ableops.PartitionReassignmentView] {
	out := newEnvelope[ableops.PartitionReassignmentView](s.client.RedactContext(ctx, input.ClusterID), "backend_reassignment_observation")
	if !validMonitoringInput(input.ClusterID, input.Limit) {
		return invalid(out, "cluster_id와 1~100 범위의 limit을 확인하세요.")
	}
	data, err := s.client.PartitionReassignments(ctx, input.ClusterID)
	if err != nil {
		return failed(out, err, "reassignments")
	}
	out.Data = &data
	partial := false
	for _, item := range data.Items {
		partial = partial || item.Status == "UNAVAILABLE" || item.Status == "FORBIDDEN"
	}
	monitoringProbe(&out, data.Status, "reassignments", partial, false)
	out.Limitations = []string{monitoringModeLimitation, monitoringListLimitation, "observedSince/observedMinutes는 포털 관측 기준 추정치입니다. progressPct는 추가 복제본의 ISR 진입 비율이며 실제 이관 bytes 비율이 아닙니다."}
	limit := limitOrDefault(input.Limit)
	out.Truncated = capItems(&data.Items, limit) || out.Truncated
	out.Truncated = capItems(&data.Reasons, limit) || out.Truncated
	for i := range data.Items {
		item := &data.Items[i]
		out.Truncated = capItems(&item.Replicas, limit) || out.Truncated
		out.Truncated = capItems(&item.AddingReplicas, limit) || out.Truncated
		out.Truncated = capItems(&item.RemovingReplicas, limit) || out.Truncated
		out.Truncated = capItems(&item.Reasons, limit) || out.Truncated
	}
	bound(&out, func() bool { return shrinkItems(&data.Items) || shrinkItems(&data.Reasons) })
	return out
}
