package ableops

import (
	"context"
	"net/url"
	"strings"
)

// MetricRange는 백엔드가 실제 지원하는 기간과 고정 해상도·포인트 예산을 반환한다.
func MetricRange(value string) (points, stepSeconds int, ok bool) {
	switch value {
	case "15m":
		return 15, 60, true
	case "1h":
		return 60, 60, true
	case "6h":
		return 72, 300, true
	case "24h":
		return 96, 900, true
	case "7d":
		return 84, 7200, true
	default:
		return 0, 0, false
	}
}

func (c *Client) ConsumerLagOverview(ctx context.Context, clusterID string, refresh bool) (ConsumerLagOverview, error) {
	var out ConsumerLagOverview
	q := url.Values{}
	if refresh {
		q.Set("refresh", "true")
	}
	if err := c.getCluster(ctx, clusterID, []string{"consumer-lag"}, q, &out); err != nil {
		return out, err
	}
	if out.ClusterID != clusterID || !validProbeStatus(out.Status) || !oneOfMonitoring(out.AlertsStatus, "OK", "DISABLED", "UNAVAILABLE") {
		return ConsumerLagOverview{}, publicError("invalid_response", 200)
	}
	out.Reasons = safeProbeReasons(out.Status, out.Reasons)
	if out.AlertsStatus == "UNAVAILABLE" {
		// 경보 저장소 실패 사유에는 내부 DB 오류가 포함된다.
		out.Reasons = []string{"경보 상태 조회에 실패했습니다. 내부 오류 원문은 생략합니다."}
	}
	for i := range out.Groups {
		item := &out.Groups[i]
		if !validProbeStatus(item.Status) || !validProbeStatus(item.GroupStatus) || !validLagObservation(item.Observation) {
			return ConsumerLagOverview{}, publicError("invalid_response", 200)
		}
		item.Reasons = safeProbeReasons(item.Observation, item.Reasons)
	}
	for i := range out.Targets {
		item := &out.Targets[i]
		if !validProbeStatus(item.Status) || !validLagObservation(item.Observation) {
			return ConsumerLagOverview{}, publicError("invalid_response", 200)
		}
		item.Reasons = safeProbeReasons(item.Observation, item.Reasons)
	}
	for _, item := range out.Topics {
		if !validProbeStatus(item.Status) || !validLagObservation(item.Observation) {
			return ConsumerLagOverview{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

func validLagObservation(value string) bool {
	return oneOfMonitoring(value, "OBSERVED", "PARTIAL", "UNAVAILABLE", "FORBIDDEN", "NO_CONSUMER")
}

func oneOfMonitoring(value string, values ...string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func (c *Client) ConsumerLagPolicy(ctx context.Context, clusterID, topic, group string) (ConsumerLagPolicyDetail, error) {
	var out ConsumerLagPolicyDetail
	if group != "" && topic == "" {
		return out, publicError("invalid_request", 0)
	}
	q := url.Values{}
	scope := "CLUSTER"
	if topic != "" {
		q.Set("topic", topic)
		scope = "TOPIC"
	}
	if group != "" {
		q.Set("group", group)
		scope = "GROUP_TOPIC"
	}
	if err := c.getCluster(ctx, clusterID, []string{"consumer-lag", "policy"}, q, &out); err != nil {
		return out, err
	}
	if out.ClusterID != clusterID || out.Scope != scope || out.Topic != topic || out.Group != group {
		return ConsumerLagPolicyDetail{}, publicError("invalid_response", 200)
	}
	for _, layer := range []*ConsumerLagPolicy{out.Stored, out.Layers.Cluster, out.Layers.Topic, out.Layers.GroupTopic} {
		if layer != nil && layer.ClusterID != clusterID {
			return ConsumerLagPolicyDetail{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

func (c *Client) MetricSeries(ctx context.Context, clusterID, metric, period string) (MetricSeries, error) {
	var out MetricSeries
	if !oneOfMonitoring(metric, "consumer-lag", "topic-throughput", "cluster-stability") {
		return out, publicError("invalid_request", 0)
	}
	if _, _, ok := MetricRange(period); !ok {
		return out, publicError("invalid_request", 0)
	}
	if err := c.getCluster(ctx, clusterID, []string{"charts", metric}, url.Values{"range": {period}}, &out); err != nil {
		return out, err
	}
	if out.ClusterID != clusterID || out.Metric != metric || out.Range != period || len(out.Series) == 0 {
		return MetricSeries{}, publicError("invalid_response", 200)
	}
	if !out.Scoped {
		// 백엔드가 전역 Prometheus 결과라고 표시하면 클러스터별 데이터로 노출하지 않는다.
		return MetricSeries{}, publicError("unsupported", 200)
	}
	unitOK := (metric == "consumer-lag" && out.Unit == "messages") ||
		(metric == "cluster-stability" && out.Unit == "partitions") ||
		(metric == "topic-throughput" && ((!out.Demo && out.Unit == "bytes/s") || (out.Demo && out.Unit == "msg/s")))
	if !unitOK {
		return MetricSeries{}, publicError("invalid_response", 200)
	}
	return out, nil
}

func (c *Client) ClusterStorage(ctx context.Context, clusterID string) (ClusterStorageView, error) {
	var out ClusterStorageView
	if strings.ContainsAny(clusterID, "/;,") {
		return out, publicError("unsupported", 0)
	}
	if err := c.getCluster(ctx, clusterID, []string{"storage"}, nil, &out); err != nil {
		return out, err
	}
	if !validProbeStatus(out.Status) {
		return ClusterStorageView{}, publicError("invalid_response", 200)
	}
	out.Reasons = safeProbeReasons(out.Status, out.Reasons)
	out.Leaders.Reasons = safeProbeReasons(out.Leaders.Status, out.Leaders.Reasons)
	for i := range out.Brokers {
		item := &out.Brokers[i]
		if !validProbeStatus(item.Status) {
			return ClusterStorageView{}, publicError("invalid_response", 200)
		}
		item.Reasons = safeProbeReasons(item.Status, item.Reasons)
		if item.State == "LOGDIR_ERROR" {
			item.Reasons = []string{"백엔드가 로그 디렉터리 조회 오류를 보고했습니다. 경로와 오류 원문은 생략합니다."}
		}
	}
	for i := range out.Leaders.Brokers {
		item := &out.Leaders.Brokers[i]
		if !validProbeStatus(item.Status) {
			return ClusterStorageView{}, publicError("invalid_response", 200)
		}
		item.Reasons = safeProbeReasons(item.Status, item.Reasons)
	}
	return out, nil
}

func (c *Client) ClusterConfigAudit(ctx context.Context, clusterID string) (ConfigAuditReport, error) {
	var out ConfigAuditReport
	if err := c.getCluster(ctx, clusterID, []string{"config-audit"}, nil, &out); err != nil {
		return out, err
	}
	if out.ClusterID != clusterID || !validProbeStatus(out.Status) || out.Total < 0 || out.Unavailable < 0 {
		return ConfigAuditReport{}, publicError("invalid_response", 200)
	}
	if out.Note != "" {
		out.Note = "백엔드가 진단 제한 또는 조회 실패를 보고했습니다. 내부 오류 원문은 생략합니다."
	}
	for _, item := range out.Topics {
		if !validProbeStatus(item.Status) {
			return ConfigAuditReport{}, publicError("invalid_response", 200)
		}
		for _, finding := range item.Findings {
			if !validProbeStatus(finding.Status) {
				return ConfigAuditReport{}, publicError("invalid_response", 200)
			}
		}
	}
	return out, nil
}

func (c *Client) PartitionReassignments(ctx context.Context, clusterID string) (PartitionReassignmentView, error) {
	var out PartitionReassignmentView
	if strings.ContainsAny(clusterID, "/;,") {
		return out, publicError("unsupported", 0)
	}
	if err := c.getCluster(ctx, clusterID, []string{"reassignments"}, nil, &out); err != nil {
		return out, err
	}
	if !validProbeStatus(out.Status) || out.Total < 0 {
		return PartitionReassignmentView{}, publicError("invalid_response", 200)
	}
	out.Reasons = safeProbeReasons(out.Status, out.Reasons)
	for i := range out.Items {
		item := &out.Items[i]
		if !validProbeStatus(item.Status) {
			return PartitionReassignmentView{}, publicError("invalid_response", 200)
		}
		item.Reasons = safeProbeReasons(item.Status, item.Reasons)
	}
	return out, nil
}
