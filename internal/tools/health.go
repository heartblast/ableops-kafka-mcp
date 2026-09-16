package tools

import (
	"context"
	"strings"
	"sync"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
)

type HealthData struct {
	Connectivity *ableops.ClusterHealthView   `json:"connectivity,omitempty"`
	Partitions   *ableops.PartitionHealthView `json:"partitions,omitempty"`
}

func backendFailure(status, component string) *Failure {
	var code, message string
	switch strings.ToUpper(status) {
	case "FORBIDDEN":
		code, message = "access_denied", "백엔드가 Kafka 조회 권한 부족을 보고했습니다."
	case "UNAVAILABLE":
		code, message = "backend_unavailable", "백엔드가 조회 불가 상태를 보고했습니다."
	case "NOT_FOUND":
		code, message = "not_found", "백엔드가 요청한 대상을 찾지 못했습니다."
	case "UNSUPPORTED", "DISABLED":
		code, message = "unsupported", "백엔드에서 이 조회 기능을 사용할 수 없습니다."
	case "ERROR":
		code, message = "backend_unavailable", "백엔드가 조회 실패 상태를 보고했습니다."
	default:
		return nil
	}
	return &Failure{Component: component, Code: code, Message: message, HTTPStatus: 200, BackendStatus: status}
}

func capItems[T any](items *[]T, limit int) bool {
	if len(*items) <= limit {
		return false
	}
	*items = (*items)[:limit]
	return true
}

func (s *service) clusterHealth(ctx context.Context, input ClusterInput) Envelope[HealthData] {
	out := newEnvelope[HealthData](s.client.RedactContext(ctx, input.ClusterID), "backend_live_adapter")
	if !validCluster(input.ClusterID) {
		return invalid(out, "cluster_id를 명시해야 합니다.")
	}
	var health ableops.ClusterHealthView
	var partitions ableops.PartitionHealthView
	var healthErr, partitionErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); health, healthErr = s.client.ClusterHealth(ctx, input.ClusterID) }()
	go func() { defer wg.Done(); partitions, partitionErr = s.client.PartitionHealth(ctx, input.ClusterID) }()
	wg.Wait()
	data := HealthData{}
	out.Data = &data
	available := 0
	if healthErr != nil {
		out.Errors = append(out.Errors, failure(healthErr, "connectivity"))
	} else {
		data.Connectivity = &health
		if !health.Reachable || health.Error != "" {
			out.Errors = append(out.Errors, Failure{Component: "connectivity", Code: "backend_unavailable", Message: "백엔드가 클러스터 연결 또는 조회 실패를 보고했습니다.", HTTPStatus: 200})
		} else {
			available++
			if health.Cluster == nil {
				out.Errors = append(out.Errors, Failure{Component: "cluster_metadata", Code: "metadata_unavailable", Message: "연결은 가능하지만 백엔드가 클러스터 상세 정보를 제공하지 않았습니다.", HTTPStatus: 200})
			}
		}
	}
	if partitionErr != nil {
		out.Errors = append(out.Errors, failure(partitionErr, "partitions"))
	} else {
		data.Partitions = &partitions
		if err := backendFailure(partitions.Status, "partitions"); err != nil {
			out.Errors = append(out.Errors, *err)
			// 실패 카운터는 토픽/파티션 단위가 섞인다. 이 충분조건일 때만 일부 성공을 확인한다.
			if partitions.TotalPartitions > partitions.Forbidden+partitions.Unavailable {
				available++
			}
			out.Limitations = append(out.Limitations, "파티션 조회 실패 카운터는 토픽·파티션 실패를 함께 셉니다. 명확한 성공 관측이 확인된 경우에만 MCP 부분 성공으로 구분하며 백엔드 건강 판정은 그대로 유지합니다.")
		} else {
			available++
			if partitions.Forbidden > 0 || partitions.Unavailable > 0 {
				out.Errors = append(out.Errors, Failure{Component: "partitions", Code: "partial_failure", Message: "일부 파티션의 권한 또는 가용성 조회에 실패했습니다.", HTTPStatus: 200, BackendStatus: partitions.Status})
			}
		}
		out.Truncated = partitions.Truncated
		if !partitions.MinISRResolved {
			out.Limitations = append(out.Limitations, "최소 ISR 판정이 미평가 상태이거나 평가 대상이 없습니다. minIsrResolved와 파티션 수를 함께 확인하세요.")
		}
		if !partitions.ReassignApplied {
			out.Limitations = append(out.Limitations, "백엔드에서 재배치 정보를 연계한 판정이 적용되지 않았습니다. reassignApplied를 확인하세요.")
		}
	}
	if len(out.Errors) > 0 {
		out.Status = "partial"
	}
	if available == 0 {
		out.Status = "error"
	}
	out.Limitations = append(out.Limitations, "연결 상태 API는 원본 관측 시각을 제공하지 않습니다. 파티션 checkedAt은 백엔드 판정 시각입니다. 두 API는 동일 순간의 원자적 스냅샷이 아닙니다.")
	limit := limitOrDefault(input.Limit)
	if health.Cluster != nil {
		out.Truncated = capItems(&health.Cluster.Brokers, limit) || out.Truncated
		out.Truncated = capItems(&health.Cluster.UnderReplicatedSample, limit) || out.Truncated
		out.Truncated = capItems(&health.Cluster.OfflineSample, limit) || out.Truncated
	}
	out.Truncated = capItems(&partitions.Issues, limit) || out.Truncated
	out.Truncated = capItems(&partitions.Reasons, limit) || out.Truncated
	for i := range partitions.Issues {
		issue := &partitions.Issues[i]
		out.Truncated = capItems(&issue.Reasons, limit) || out.Truncated
		out.Truncated = capItems(&issue.Replicas, limit) || out.Truncated
		out.Truncated = capItems(&issue.ISR, limit) || out.Truncated
		out.Truncated = capItems(&issue.OfflineReplicas, limit) || out.Truncated
	}
	bound(&out, func() bool {
		if shrinkItems(&partitions.Issues) {
			return true
		}
		if shrinkItems(&partitions.Reasons) {
			return true
		}
		if health.Cluster != nil {
			if shrinkItems(&health.Cluster.Brokers) {
				return true
			}
			if shrinkItems(&health.Cluster.UnderReplicatedSample) {
				return true
			}
			if shrinkItems(&health.Cluster.OfflineSample) {
				return true
			}
		}
		return false
	})
	return out
}

func (s *service) groupLag(ctx context.Context, input GroupInput) Envelope[ableops.ConsumerGroupLagView] {
	out := newEnvelope[ableops.ConsumerGroupLagView](s.client.RedactContext(ctx, input.ClusterID), "backend_live_adapter")
	if !validCluster(input.ClusterID) || strings.TrimSpace(input.GroupName) == "" {
		return invalid(out, "cluster_id와 group_name을 명시해야 합니다.")
	}
	data, err := s.client.ConsumerGroupLag(ctx, input.ClusterID, input.GroupName)
	if err != nil {
		return failed(out, err, "consumer_group_lag")
	}
	out.Data = &data
	if !data.Found && data.Code == "NOT_FOUND" {
		out.Status = "error"
		out.Errors = []Failure{{Component: "consumer_group_lag", Code: "not_found", Message: "백엔드가 Consumer Group을 찾지 못했습니다.", HTTPStatus: 200, BackendStatus: data.Status}}
	} else if bodyErr := backendFailure(data.Status, "consumer_group_lag"); bodyErr != nil {
		out.Status = "error"
		if data.Found && data.CountedPartitions > 0 && data.ErrorPartitions > 0 {
			out.Status = "partial"
		}
		out.Errors = []Failure{*bodyErr}
	} else if !data.Found {
		out.Status = "error"
		out.Errors = []Failure{{Component: "consumer_group_lag", Code: "not_found", Message: "백엔드가 Consumer Group을 찾지 못했습니다.", HTTPStatus: 200, BackendStatus: data.Code}}
	} else if data.ErrorPartitions > 0 {
		out.Status = "partial"
		if data.CountedPartitions == 0 {
			out.Status = "error"
		}
		out.Errors = []Failure{{Component: "consumer_group_lag", Code: "partial_failure", Message: "일부 또는 전체 파티션 Lag 조회에 실패했습니다. 집계와 0 값을 전체 정상 Lag로 해석하지 마세요.", HTTPStatus: 200, BackendStatus: data.Status}}
	}
	limit := limitOrDefault(input.Limit)
	out.Truncated = capItems(&data.Partitions, limit) || out.Truncated
	out.Truncated = capItems(&data.TopicLag, limit) || out.Truncated
	out.Truncated = capItems(&data.Reasons, limit) || out.Truncated
	for i := range data.Partitions {
		out.Truncated = capItems(&data.Partitions[i].Reasons, limit) || out.Truncated
	}
	out.Limitations = []string{"백엔드의 Lag 집계와 정책 판정을 그대로 반환합니다. 표시 목록이 잘려도 집계값은 전체 백엔드 결과를 유지합니다."}
	bound(&out, func() bool {
		if shrinkItems(&data.Partitions) {
			return true
		}
		if shrinkItems(&data.TopicLag) {
			return true
		}
		return shrinkItems(&data.Reasons)
	})
	return out
}
