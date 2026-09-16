package tools_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

const summaryOperationsFixture = `{"openTotal":2,"attentionUrgent":1,"attentionReview":1,"syntheticExcluded":3,"monitoredClusters":1,"generatedAt":"2026-09-16T00:00:00Z","scope":{"all":false,"clusterCount":1,"totalClusters":2,"clusterId":"c1"},"collection":{"enabled":true,"intervalSec":30}}`
const attentionOperationsFixture = `{"clusterId":"c1","profile":"STANDARD","items":[{"eventCode":"CLUSTER_UNREACHABLE","profileApplied":"STANDARD","effective":{"defaultAttention":"URGENT","minimumAttention":"REVIEW","trendEnabled":false,"minDurationSec":30},"default":{"defaultAttention":"REVIEW"},"overridden":true,"clusterOverride":{"eventCode":"CLUSTER_UNREACHABLE","clusterId":"c1","trendEnabled":false}}],"total":1}`

func TestEventOperationsDiscoveryAndStrictSchemas(t *testing.T) {
	var count atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, _ *http.Request) { count.Add(1); io.WriteString(w, `{}`) })
	listed, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, tool := range listed.Tools {
		found[tool.Name] = true
	}
	for _, name := range []string{"get_event_summary", "list_operational_issues", "get_event_rule", "get_attention_policy", "list_maintenance_windows"} {
		if !found[name] {
			t.Fatalf("이벤트 도구 발견 실패: %s", name)
		}
		for _, args := range []map[string]any{{}, {"cluster_id": " "}, {"cluster_id": "c1", "unexpected": true}} {
			if !call(t, cs, name, args).IsError {
				t.Errorf("입력 경계 누락: %s", name)
			}
		}
	}
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{"get_event_summary", map[string]any{"cluster_id": " c1"}},
		{"list_operational_issues", map[string]any{"cluster_id": "c1", "sort": "SQL"}},
		{"list_operational_issues", map[string]any{"cluster_id": "c1", "page_size": 101}},
		{"get_event_rule", map[string]any{"cluster_id": "c1", "event_code": "CLUSTER_UNREACHABLE", "include_profile": true}},
		{"get_attention_policy", map[string]any{"cluster_id": "c1", "event_code": "../events"}},
		{"list_maintenance_windows", map[string]any{"cluster_id": "c1", "limit": 0}},
		{"list_cluster_events", map[string]any{"cluster_id": "c1", "attention": []string{"UNKNOWN"}}},
		{"list_cluster_events", map[string]any{"cluster_id": "c1", "sort": "SQL"}},
	} {
		if !call(t, cs, tc.name, tc.args).IsError {
			t.Errorf("스키마 잘못된 입력 허용: %s", tc.name)
		}
	}
	if count.Load() != 0 {
		t.Fatalf("잘못된 입력이 백엔드에 도달: %d", count.Load())
	}
}

func TestEventSummaryDeniedZeroAndDisabledCollection(t *testing.T) {
	for _, tc := range []struct {
		body, status, code string
		isError            bool
	}{
		{summaryOperationsFixture, "ok", "", false},
		{strings.Replace(summaryOperationsFixture, `"all":false`, `"all":false,"clusterDenied":true`, 1), "error", "access_denied", true},
		{strings.Replace(summaryOperationsFixture, `"enabled":true`, `"enabled":false`, 1), "partial", "collection_disabled", false},
	} {
		cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/events/summary" || r.URL.Query().Get("clusterId") != "c1" {
				t.Error("요약 계약 불일치")
			}
			io.WriteString(w, tc.body)
		})
		result := call(t, cs, "get_event_summary", map[string]any{"cluster_id": "c1"})
		out := decoded[ableops.EventSummary](t, result)
		if out.Status != tc.status || result.IsError != tc.isError || (tc.code != "" && (len(out.Errors) == 0 || out.Errors[0].Code != tc.code)) {
			t.Fatalf("권한·미수집 0건 의미 손실: %+v", out)
		}
		if tc.isError && out.Data != nil {
			t.Fatal("거부된 집계 노출")
		}
	}
}

func TestOperationalIssuesFiltersScopeAndPagination(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/api/events/summary" {
			io.WriteString(w, summaryOperationsFixture)
			return
		}
		q := r.URL.Query()
		if r.URL.Path != "/api/operational-issues" || q.Get("clusterId") != "c1" || q.Get("page") != "2" || q.Get("pageSize") != "1" || q.Get("status") != "OPEN" || q.Get("attention") != "URGENT" || q.Get("group") != "CLUSTER_DOWN" || q.Get("sort") != "-attention" || q.Get("search") != "합성" || q.Get("excludeSynthetic") != "true" {
			t.Errorf("Issue 필터 계약 불일치: %s", r.URL.RequestURI())
		}
		io.WriteString(w, `{"items":[{"id":"IS-1","clusterId":"c1","status":"OPEN","attentionLevel":"URGENT","dataMode":"REAL","demo":false,"activeMemberCount":4,"impact":{"password":"hidden-marker"},"resolutionReason":"hidden-marker"}],"total":3,"page":2,"pageSize":1}`)
	})
	result := call(t, cs, "list_operational_issues", map[string]any{"cluster_id": "c1", "page": 2, "page_size": 1, "status": []string{"OPEN"}, "attention": []string{"URGENT"}, "group": "CLUSTER_DOWN", "sort": "-attention", "search": "합성", "exclude_synthetic": true})
	out := decoded[tools.OperationalIssuesData](t, result)
	if out.Status != "ok" || !out.Truncated || !out.Data.HasMore || out.Data.Returned != 1 || out.Data.Total != 3 || calls.Load() != 2 || out.Data.ScopeCheckedAt == "" {
		t.Fatalf("한 페이지·권한 확인 경계: %+v calls=%d", out, calls.Load())
	}
	raw, _ := json.Marshal(result)
	if strings.Contains(string(raw), "hidden-marker") {
		t.Fatal("Issue 임의 payload 노출")
	}
}

func TestOperationalIssuesDeniedStopsAndEmptyRemainsUncertain(t *testing.T) {
	for _, denied := range []bool{false, true} {
		var calls atomic.Int32
		cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if r.URL.Path == "/api/events/summary" {
				body := summaryOperationsFixture
				if denied {
					body = strings.Replace(body, `"all":false`, `"all":false,"clusterDenied":true`, 1)
				}
				io.WriteString(w, body)
				return
			}
			io.WriteString(w, `{"items":[],"total":0,"page":1,"pageSize":50}`)
		})
		out := decoded[tools.OperationalIssuesData](t, call(t, cs, "list_operational_issues", map[string]any{"cluster_id": "c1"}))
		if denied {
			if out.Status != "error" || out.Errors[0].Code != "access_denied" || calls.Load() != 1 {
				t.Fatalf("거부 후 목록 호출: %+v", out)
			}
		} else if out.Status != "partial" || out.Errors[0].Code != "empty_scope_unconfirmed" || calls.Load() != 2 {
			t.Fatalf("모호한 0건 정상 처리: %+v", out)
		}
	}
}

func TestEventPolicySafeFieldsAndOptionalFailure(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/api/event-rules":
			io.WriteString(w, `{"clusterId":"c1","items":[{"eventCode":"CLUSTER_UNREACHABLE","sourceImplemented":true,"effective":{"enabled":false,"severity":"CRITICAL","minDurationSec":60},"default":{"enabled":true},"clusterOverride":{"eventCode":"CLUSTER_UNREACHABLE","clusterId":"c1","enabled":false,"runbookUrl":"hidden-marker"},"deepLink":"hidden-marker"}],"total":1}`)
		case "/api/event-attention-overrides":
			io.WriteString(w, attentionOperationsFixture)
		case "/api/clusters/c1/attention-profile":
			w.WriteHeader(403)
		default:
			t.Errorf("정책 변경 또는 미지원 호출: %s", r.URL.Path)
		}
	})
	result := call(t, cs, "get_event_rule", map[string]any{"cluster_id": "c1", "event_code": "CLUSTER_UNREACHABLE"})
	rule := decoded[ableops.EventRule](t, result)
	if rule.Status != "partial" || rule.Errors[0].Code != "backend_store_status_unavailable" || rule.Data.Effective.Enabled || rule.Data.Effective.MinDurationSec != 60 || rule.Data.ClusterOverride.Enabled == nil {
		t.Fatalf("정책 상속·명시 설정·불확실성 손실: %+v", rule)
	}
	raw, _ := json.Marshal(result)
	if strings.Contains(string(raw), "hidden-marker") {
		t.Fatal("정책 비공개 필드 노출")
	}
	attention := decoded[tools.AttentionPolicyData](t, call(t, cs, "get_attention_policy", map[string]any{"cluster_id": "c1", "event_code": "CLUSTER_UNREACHABLE", "include_profile": true}))
	if attention.Status != "partial" || attention.Data.ProfileStatus != "error" || attention.Data.Rule.Effective.DefaultAttention != "URGENT" || len(attention.Errors) != 2 || attention.Errors[1].Code != "access_denied" || calls.Load() != 3 {
		t.Fatalf("프로필 부가조회 부분 실패: %+v calls=%d", attention, calls.Load())
	}
}

func TestMaintenanceWindowsLimitsGlobalAndStoreUncertainty(t *testing.T) {
	var count atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		q := r.URL.Query()
		if r.URL.Path != "/api/event-maintenance-windows" || len(q) != 2 || q.Get("clusterId") != "c1" || q.Get("activeOnly") != "true" {
			t.Error("미지원 유지보수 페이지·limit 인자 전송")
		}
		io.WriteString(w, `{"items":[{"id":"MW-1","clusterId":"","enabled":true,"active":true,"reason":"hidden-marker"},{"id":"MW-2","clusterId":"c1","enabled":true,"active":true}],"total":2}`)
	})
	result := call(t, cs, "list_maintenance_windows", map[string]any{"cluster_id": "c1", "active_only": true, "limit": 1})
	out := decoded[tools.ListData[ableops.MaintenanceWindow]](t, result)
	if out.Status != "partial" || out.Errors[0].Code != "backend_store_status_unavailable" || !out.Truncated || out.Data.Returned != 1 || out.Data.Received != 2 || out.Data.Items[0].ClusterID != "" || count.Load() != 1 {
		t.Fatalf("전체 적용·출력 제한·저장소 불확실성: %+v", out)
	}
	raw, _ := json.Marshal(result)
	if strings.Contains(string(raw), "hidden-marker") {
		t.Fatal("유지보수 사유 원문 노출")
	}
}

func TestExtendedClusterEventFilters(t *testing.T) {
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		for key, want := range map[string]string{"attention": "URGENT", "eventCode": "CLUSTER_UNREACHABLE", "resourceType": "CLUSTER", "resourceId": "c1", "module": "Kafka", "assignedTo": "operator-a", "sort": "-attention", "excludeSynthetic": "true"} {
			if q.Get(key) != want {
				t.Errorf("이벤트 %s 필터: %s", key, q.Get(key))
			}
		}
		if r.URL.Path != "/api/clusters/c1/events" {
			t.Error("전역 이벤트 API로 우회")
		}
		io.WriteString(w, `{"items":[],"total":0,"page":1,"pageSize":50}`)
	})
	out := decoded[tools.EventData](t, call(t, cs, "list_cluster_events", map[string]any{"cluster_id": "c1", "attention": []string{"URGENT"}, "event_code": []string{"CLUSTER_UNREACHABLE"}, "resource_type": "CLUSTER", "resource_id": "c1", "module": "Kafka", "assigned_to": "operator-a", "sort": "-attention", "exclude_synthetic": true}))
	if out.Status != "ok" {
		t.Fatalf("이벤트 필터 확장 회귀: %+v", out)
	}
}

func TestUnevaluatedAttentionFilterForEventsAndIssues(t *testing.T) {
	var count atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		if r.URL.Path == "/api/events/summary" {
			io.WriteString(w, summaryOperationsFixture)
			return
		}
		if r.URL.Query().Get("attention") != "UNEVALUATED" {
			t.Errorf("미평가 주의도 필터 유실: %s", r.URL.RequestURI())
		}
		switch r.URL.Path {
		case "/api/clusters/c1/events":
			io.WriteString(w, `{"items":[{"id":"EV-1","clusterId":"c1","attentionLevel":"UNEVALUATED"}],"total":1,"page":1,"pageSize":50}`)
		case "/api/operational-issues":
			io.WriteString(w, `{"items":[{"id":"IS-1","clusterId":"c1","status":"OPEN","attentionLevel":"UNEVALUATED"}],"total":1,"page":1,"pageSize":50}`)
		default:
			t.Errorf("미지원 조회: %s", r.URL.Path)
		}
	})
	args := map[string]any{"cluster_id": "c1", "attention": []string{"UNEVALUATED"}}
	events := decoded[tools.EventData](t, call(t, cs, "list_cluster_events", args))
	issues := decoded[tools.OperationalIssuesData](t, call(t, cs, "list_operational_issues", args))
	if events.Status != "ok" || events.Data.Items[0].AttentionLevel != "UNEVALUATED" || issues.Status != "ok" || issues.Data.Items[0].AttentionLevel != "UNEVALUATED" || count.Load() != 3 {
		t.Fatalf("미평가 상태·필터 의미 손실: events=%+v issues=%+v calls=%d", events, issues, count.Load())
	}
}
