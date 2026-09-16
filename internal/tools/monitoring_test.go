package tools_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

func TestMonitoringDiscoveryAndInput(t *testing.T) {
	var requests atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1) })
	listed, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, tool := range listed.Tools {
		switch tool.Name {
		case "get_consumer_lag_overview", "get_cluster_storage":
			if tool.Annotations == nil || tool.Annotations.ReadOnlyHint || tool.Annotations.IdempotentHint {
				t.Errorf("백엔드 저장 부작용 annotation 누락: %s", tool.Name)
			}
			found[tool.Name] = true
		case "get_consumer_lag_policy", "get_metric_series", "get_cluster_config_audit", "get_partition_reassignments":
			found[tool.Name] = true
		}
	}
	if len(found) != 6 {
		t.Fatalf("모니터링 도구 발견 실패: %v", found)
	}
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{"get_consumer_lag_overview", map[string]any{}},
		{"get_consumer_lag_overview", map[string]any{"cluster_id": "c1", "limit": 101}},
		{"get_consumer_lag_policy", map[string]any{"cluster_id": "c1", "group_name": "billing"}},
		{"get_consumer_lag_policy", map[string]any{"cluster_id": "c1", "topic_name": " "}},
		{"get_metric_series", map[string]any{"cluster_id": "c1", "metric": "arbitrary"}},
		{"get_metric_series", map[string]any{"cluster_id": "c1", "metric": "consumer-lag", "range": "30d"}},
		{"get_metric_series", map[string]any{"cluster_id": "c1", "metric": "consumer-lag", "max_points": 97}},
		{"get_metric_series", map[string]any{"cluster_id": "c1", "metric": "consumer-lag", "url": "/api/system"}},
		{"get_cluster_storage", map[string]any{"cluster_id": " "}},
		{"get_cluster_config_audit", map[string]any{"cluster_id": "c1", "limit": 0}},
		{"get_partition_reassignments", map[string]any{"cluster_id": "c1", "execute": true}},
	} {
		if !call(t, cs, tc.name, tc.args).IsError {
			t.Errorf("잘못된 인자를 허용함: %s %v", tc.name, tc.args)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("검증 실패 후 REST 요청 발생: %d", requests.Load())
	}
}

func TestMonitoringLagOverviewPreservesAxes(t *testing.T) {
	var requests atomic.Int32
	cs, logs := connect(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/api/clusters/c1/consumer-lag" || r.URL.RawQuery != "refresh=true" {
			t.Errorf("통합 Lag 계약 불일치: %s", r.URL.RequestURI())
		}
		io.WriteString(w, `{"clusterId":"c1","status":"WARN","partial":true,"checkedAt":"2026-09-16T00:00:00Z","topicsSyncedAt":"2026-09-15T23:00:00Z","alertsStatus":"OK","groups":[{"group":"billing","state":"Stable","status":"WARN","groupStatus":"OK","observation":"PARTIAL","totalLag":24,"errorPartitions":1}],"topics":[{"topic":"orders","groups":2,"maxGroupLag":24,"status":"WARN","observation":"PARTIAL"}],"targets":[{"group":"billing","topic":"orders","status":"WARN","observation":"PARTIAL","countedPartitions":1,"errorPartitions":1,"topicLag":24,"monitored":true,"eventFiring":false,"alert":{"phase":"OPEN","recovering":true},"policy":{"partitionLagWarn":{"enabled":true,"value":20,"source":"TOPIC"}},"messages":["private-marker"]},{"group":"shipping","topic":"orders","status":"OK","observation":"OBSERVED","topicLag":3}],"summary":{"targets":2,"partialGroups":1,"activeAlerts":1},"password":"private-marker"}`)
	})
	res := call(t, cs, "get_consumer_lag_overview", map[string]any{"cluster_id": "c1", "refresh": true, "limit": 1})
	out := decoded[ableops.ConsumerLagOverview](t, res)
	if out.Status != "partial" || res.IsError || !out.Truncated || len(out.Data.Targets) != 1 || out.Data.Summary.Targets != 2 || requests.Load() != 1 {
		t.Fatalf("Lag 부분 실패·상한·범위 보존 실패: %+v", out)
	}
	item := out.Data.Targets[0]
	if item.EventFiring || item.Alert.Phase != "OPEN" || !item.Alert.Recovering || item.Policy.PartitionLagWarn.Source != "TOPIC" || out.Data.Groups[0].State != "Stable" || out.Data.Topics[0].MaxGroupLag != 24 {
		t.Fatalf("Kafka 상태·현재 판정·경보·정책 구분 실패: %+v", out.Data)
	}
	raw, _ := json.Marshal(res)
	if strings.Contains(string(raw), "private-marker") || strings.Contains(logs.String(), "private-marker") {
		t.Fatal("메시지 또는 민감 필드 노출")
	}
}

func TestMonitoringLagPolicyScopeAndInheritance(t *testing.T) {
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/clusters/c1/consumer-lag/policy" || r.URL.Query().Get("group") != "billing & reports" || r.URL.Query().Get("topic") != "orders" || len(r.URL.Query()) != 2 {
			t.Errorf("정책 대상 또는 필터 불일치: %s", r.URL.RequestURI())
		}
		io.WriteString(w, `{"clusterId":"c1","scope":"GROUP_TOPIC","topic":"orders","group":"billing & reports","stored":{"clusterId":"c1","scope":"GROUP_TOPIC","topic":"orders","group":"billing & reports","topicLagWarn":{"mode":"DISABLED"},"partitionLagCritical":{"mode":"SET","value":50},"alertAfterSec":{"mode":"SET","value":0},"monitoring":"INHERIT","updatedBy":"private-marker"},"layers":{"cluster":null,"topic":null,"groupTopic":null},"inherited":{"topicLagWarn":{"enabled":true,"value":30,"source":"TOPIC"}},"effective":{"topicLagWarn":{"enabled":false,"source":"GROUP_TOPIC"},"alertAfterSec":{"enabled":true,"value":0,"source":"GROUP_TOPIC"},"recoverAfterSec":{"ruleBased":true,"source":"COMMON"}},"canEdit":true}`)
	})
	res := call(t, cs, "get_consumer_lag_policy", map[string]any{"cluster_id": "c1", "topic_name": "orders", "group_name": "billing & reports"})
	out := decoded[ableops.ConsumerLagPolicyDetail](t, res)
	if out.Status != "ok" || out.Data.Stored.TopicLagWarn.Mode != "DISABLED" || out.Data.Stored.AlertAfterSec.Value == nil || *out.Data.Stored.AlertAfterSec.Value != 0 || !out.Data.Effective.RecoverAfterSec.RuleBased || !out.Data.Inherited.TopicLagWarn.Enabled {
		t.Fatalf("상속·비활성·0초 지속 조건 소실: %+v", out)
	}
	raw, _ := json.Marshal(res)
	if strings.Contains(string(raw), "private-marker") {
		t.Fatal("정책 수정자 필드 노출")
	}
}

func TestMonitoringMetricRangesAndLatestLimit(t *testing.T) {
	for _, tc := range []struct {
		period         string
		step, expected int
	}{{"15m", 60, 15}, {"1h", 60, 60}, {"6h", 300, 72}, {"24h", 900, 96}, {"7d", 7200, 84}} {
		t.Run(tc.period, func(t *testing.T) {
			cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/clusters/c1/charts/consumer-lag" || r.URL.Query().Get("range") != tc.period || len(r.URL.Query()) != 1 {
					t.Errorf("시계열 계약 불일치: %s", r.URL.RequestURI())
				}
				fmt.Fprintf(w, `{"clusterId":"c1","clusterScoped":true,"metric":"consumer-lag","range":%q,"unit":"messages","source":"Prometheus","series":["합계"],"points":[{"ts":"2026-09-16T00:00:00Z","values":{"합계":7}},{"ts":"2026-09-16T00:01:00Z","values":{"합계":8}},{"ts":"2026-09-16T00:02:00Z","values":{"합계":9}}]}`, tc.period)
			})
			out := decoded[tools.MetricSeriesData](t, call(t, cs, "get_metric_series", map[string]any{"cluster_id": "c1", "metric": "consumer-lag", "range": tc.period, "max_points": 2}))
			if out.Status != "partial" || !out.Truncated || out.Data.StepSeconds != tc.step || out.Data.ExpectedPoints != tc.expected || out.Data.ReceivedPoints != 3 || out.Data.ReturnedPoints != 2 || out.Data.Points[0].Values["합계"] != 8 || out.Data.DataMode != "measured" {
				t.Fatalf("기간·해상도·최근 포인트·시계열 공백 보존 실패: %+v", out)
			}
		})
	}
}

func TestMonitoringMetricDemoQualityAndScope(t *testing.T) {
	for _, tc := range []struct {
		name         string
		demo, scoped bool
		unit, code   string
	}{
		{"demo", true, true, "msg/s", ""}, {"measured_quality_unknown", false, true, "bytes/s", ""},
		{"global_scope", false, false, "bytes/s", "unsupported"}, {"wrong_unit", false, true, "msg/s", "invalid_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"clusterId":"c1","clusterScoped":%t,"metric":"topic-throughput","range":"1h","unit":%q,"demo":%t,"series":["유입","유출"],"points":[{"ts":"2026-09-16T00:00:00Z","values":{"유입":42,"유출":0}}],"authorization":"private-marker"}`, tc.scoped, tc.unit, tc.demo)
			})
			res := call(t, cs, "get_metric_series", map[string]any{"cluster_id": "c1", "metric": "topic-throughput"})
			out := decoded[tools.MetricSeriesData](t, res)
			if tc.code != "" {
				if !res.IsError || out.Data != nil || out.Errors[0].Code != tc.code {
					t.Fatalf("범위·단위 실패에서 데이터 반환: %+v", out)
				}
			} else if out.Status != "partial" || out.Data.QualityVerified || out.Data.Demo != tc.demo || out.Data.Unit != tc.unit || out.Data.Points[0].Values["유출"] != 0 {
				t.Fatalf("데모·품질 미확인 의미 소실: %+v", out)
			}
			raw, _ := json.Marshal(res)
			if strings.Contains(string(raw), "private-marker") {
				t.Fatal("허용하지 않은 시계열 메타 노출")
			}
		})
	}
}

func TestMonitoringStoragePartialAndPrivateFields(t *testing.T) {
	cs, logs := connect(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/clusters/c1/storage" || r.URL.RawQuery != "" {
			t.Errorf("저장량 계약 불일치: %s", r.URL.RequestURI())
		}
		io.WriteString(w, `{"status":"CRITICAL","partial":true,"brokerCount":1,"totalSize":2048,"failedBrokers":["2"],"skewSuppressed":true,"brokers":[{"broker":1,"size":2048,"state":"LOGDIR_ERROR","status":"CRITICAL","reasons":["private-marker"],"logDirs":[{"dir":"private-marker","error":"private-marker"}],"maxOffsetLag":500}],"leaders":{"status":"WARN","evaluated":false,"brokers":[]},"checkedAt":"2026-09-16T00:00:00Z"}`)
	})
	res := call(t, cs, "get_cluster_storage", map[string]any{"cluster_id": "c1"})
	out := decoded[ableops.ClusterStorageView](t, res)
	if out.Status != "partial" || out.Data.TotalSize != 2048 || out.Data.Status != "CRITICAL" || !out.Data.SkewSuppressed || out.Data.Brokers[0].MaxOffsetLag != 500 {
		t.Fatalf("저장량 부분 실패·단위·판정 소실: %+v", out)
	}
	raw, _ := json.Marshal(res)
	if strings.Contains(string(raw), "private-marker") || strings.Contains(logs.String(), "private-marker") {
		t.Fatal("LogDir 경로·오류 원문 노출")
	}
}

func TestMonitoringConfigAuditPartial(t *testing.T) {
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/clusters/c1/config-audit" || r.URL.RawQuery != "" {
			t.Errorf("설정 진단 계약 불일치: %s", r.URL.RequestURI())
		}
		io.WriteString(w, `{"clusterId":"c1","status":"WARN","total":2,"unavailable":1,"truncated":true,"topics":[{"topic":"orders","status":"WARN","findings":[{"code":"RETENTION_UNLIMITED","status":"WARN","target":"retention.ms","actual":"private-marker","message":"private-marker","exempted":true}]},{"topic":"shipping","status":"UNAVAILABLE","findings":[]}],"limits":{"maxRetentionMs":86400000},"note":"private-marker","generatedAt":"2026-09-16T00:00:00Z"}`)
	})
	res := call(t, cs, "get_cluster_config_audit", map[string]any{"cluster_id": "c1", "limit": 1})
	out := decoded[ableops.ConfigAuditReport](t, res)
	if out.Status != "partial" || !out.Truncated || out.Data.Total != 2 || out.Data.Unavailable != 1 || len(out.Data.Topics) != 1 || !out.Data.Topics[0].Findings[0].Exempted {
		t.Fatalf("정책 진단·백엔드 상한·부분 실패 소실: %+v", out)
	}
	raw, _ := json.Marshal(res)
	if strings.Contains(string(raw), "private-marker") {
		t.Fatal("설정 actual·진단 오류 원문 노출")
	}
}

func TestMonitoringReassignmentsEstimation(t *testing.T) {
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/clusters/c1/reassignments" || r.URL.RawQuery != "" {
			t.Errorf("재배치 계약 불일치: %s", r.URL.RequestURI())
		}
		io.WriteString(w, `{"status":"WARN","total":1,"estimated":true,"items":[{"topic":"orders","partition":0,"replicas":[1,2],"addingReplicas":[3,4],"removingReplicas":[1],"syncedAdding":1,"progressPct":50,"observedSince":"2026-09-16T00:00:00Z","observedMinutes":45,"estimated":true,"status":"WARN"}],"thresholds":{"warnMinutes":30},"checkedAt":"2026-09-16T00:45:00Z"}`)
	})
	out := decoded[ableops.PartitionReassignmentView](t, call(t, cs, "get_partition_reassignments", map[string]any{"cluster_id": "c1", "limit": 1}))
	if out.Status != "ok" || !out.Truncated || !out.Data.Estimated || out.Data.Items[0].ProgressPct != 50 || out.Data.Items[0].ObservedMinutes != 45 || len(out.Data.Items[0].AddingReplicas) != 1 {
		t.Fatalf("재배치 추정치·복제본 출력 상한 소실: %+v", out)
	}
}

func TestMonitoringBodyAndHTTPFailures(t *testing.T) {
	for _, tc := range []struct {
		name, body, code string
		httpStatus       int
	}{
		{"get_consumer_lag_overview", `{"clusterId":"c1","status":"OK","alertsStatus":"UNAVAILABLE","reasons":["private-marker"],"groups":[],"topics":[],"targets":[]}`, "backend_unavailable", 200},
		{"get_cluster_storage", `{"status":"FORBIDDEN","reasons":["private-marker"],"brokerCount":0}`, "access_denied", 200},
		{"get_cluster_config_audit", `{"clusterId":"c1","status":"UNAVAILABLE","note":"private-marker","topics":[]}`, "backend_unavailable", 200},
		{"get_partition_reassignments", `{"status":"UNAVAILABLE","reasons":["private-marker"],"items":[]}`, "backend_unavailable", 200},
		{"get_cluster_storage", `{"error":"private-marker"}`, "access_denied", 403},
		{"get_consumer_lag_overview", `{"clusterId":"other","status":"OK","alertsStatus":"OK"}`, "invalid_response", 200},
		{"get_consumer_lag_policy", `{"clusterId":"other","scope":"CLUSTER"}`, "invalid_response", 200},
	} {
		t.Run(tc.name+tc.code, func(t *testing.T) {
			cs, logs := connect(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.httpStatus); io.WriteString(w, tc.body) })
			res := call(t, cs, tc.name, map[string]any{"cluster_id": "c1"})
			out := decoded[map[string]any](t, res)
			if out.Status == "ok" || len(out.Errors) == 0 || out.Errors[0].Code != tc.code {
				t.Fatalf("HTTP/본문 실패 은폐: %+v", out)
			}
			raw, _ := json.Marshal(res)
			if strings.Contains(string(raw), "private-marker") || strings.Contains(logs.String(), "private-marker") {
				t.Fatal("내부 오류 원문 노출")
			}
		})
	}
}

func TestMonitoringOutputByteLimit(t *testing.T) {
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		items := make([]map[string]any, 50)
		for i := range items {
			items[i] = map[string]any{"topic": strings.Repeat("합성", 1200), "status": "WARN", "findings": []any{}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"clusterId": "c1", "status": "WARN", "total": len(items), "topics": items})
	})
	res := call(t, cs, "get_cluster_config_audit", map[string]any{"cluster_id": "c1"})
	out := decoded[ableops.ConfigAuditReport](t, res)
	raw, _ := json.Marshal(res)
	if len(raw) > tools.MaxResultBytes || !out.Truncated || out.Data == nil || out.Data.Total != 50 || len(out.Data.Topics) >= 50 {
		t.Fatalf("응답 바이트 제한 또는 원래 집계 범위가 유지되지 않았습니다: bytes=%d status=%s", len(raw), out.Status)
	}
}
