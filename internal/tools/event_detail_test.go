package tools_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

const eventDetailFixture = `{"id":"EV-1","clusterId":"c1","eventCode":"CRITICAL_CONSUMER_LAG","status":"OPEN","severity":"HIGH","attentionLevel":"REVIEW","dataMode":"REAL","lastObservedAt":"2026-09-16T00:00:00Z","recommendedAction":"담당 파티션 확인","evidence":{"group":"billing","totalLag":450,"password":"hidden-marker"}}`

func TestEventDetailSelectionLimitsAndSafeFields(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/api/events/EV-1":
			io.WriteString(w, eventDetailFixture)
		case "/api/events/EV-1/occurrences":
			if r.URL.Query().Get("limit") != "3" {
				t.Errorf("발생 이력 limit+1 누락: %s", r.URL.RawQuery)
			}
			io.WriteString(w, `{"items":[{"id":3,"eventId":"EV-1","isFiring":true},{"id":2,"eventId":"EV-1","isFiring":true},{"id":1,"eventId":"EV-1","isFiring":false}],"total":3}`)
		case "/api/event-playbooks/CRITICAL_CONSUMER_LAG":
			io.WriteString(w, `{"code":"CRITICAL_CONSUMER_LAG","title":"지연 점검","steps":[{"title":"권장 점검","command":"hidden-marker"}]}`)
		default:
			t.Errorf("허용하지 않은 조회: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	base := decoded[tools.EventDetailData](t, call(t, cs, "get_event_detail", map[string]any{"cluster_id": "c1", "event_id": "EV-1"}))
	if base.Status != "ok" || calls.Load() != 1 || base.Data.Components.Occurrences != "not_requested" || base.Data.Event.Severity != "HIGH" || base.Data.Event.AttentionLevel != "REVIEW" {
		t.Fatalf("기본 상세/선택 경계: %+v calls=%d", base, calls.Load())
	}
	result := call(t, cs, "get_event_detail", map[string]any{"cluster_id": "c1", "event_id": "EV-1", "include": []string{"occurrences", "issue", "playbook"}, "limit": 2})
	out := decoded[tools.EventDetailData](t, result)
	if out.Status != "partial" || result.IsError || !out.Truncated || out.Data.Occurrences.Returned != 2 || out.Data.Occurrences.Received != 3 || !out.Data.Occurrences.HasMore || calls.Load() != 4 || out.Data.Issue != nil || out.Data.Components.Issue != "unsupported" || len(out.Data.Playbook.Steps) != 1 || len(out.Errors) != 1 || out.Errors[0].Component != "issue" || out.Errors[0].Code != "unsupported" {
		t.Fatalf("선택 조회/호출 상한: %+v calls=%d", out, calls.Load())
	}
	raw, _ := json.Marshal(result)
	if strings.Contains(string(raw), "hidden-marker") {
		t.Fatal("민감 evidence/Issue/명령 payload 노출")
	}
}

func TestEventDetailRequiredFailureStopsHistory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		code   string
	}{
		{"다른 클러스터", 200, strings.ReplaceAll(eventDetailFixture, `"c1"`, `"c2"`), "invalid_response"},
		{"업무 실패", 200, `{"status":"FAILED","error":"hidden-marker"}`, "invalid_response"},
		{"권한 거부", 403, `{}`, "access_denied"},
		{"없는 이벤트", 404, `{}`, "not_found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var count atomic.Int32
			cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			result := call(t, cs, "get_event_detail", map[string]any{"cluster_id": "c1", "event_id": "EV-1", "include": []string{"occurrences", "actions", "issue", "playbook"}})
			out := decoded[tools.EventDetailData](t, result)
			if !result.IsError || out.Status != "error" || out.Data != nil || out.Errors[0].Code != tc.code || count.Load() != 1 {
				t.Fatalf("필수 실패 뒤 후속 조회/상세 노출: %+v count=%d", out, count.Load())
			}
		})
	}
}

func TestEventDetailOptionalFailureEmptyAndUnsupported(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/api/events/EV-1":
			io.WriteString(w, eventDetailFixture)
		case "/api/events/EV-1/occurrences":
			io.WriteString(w, `{"items":[],"total":0}`)
		case "/api/event-playbooks/CRITICAL_CONSUMER_LAG":
			w.WriteHeader(http.StatusForbidden)
		default:
			t.Errorf("제한 없는 조치 이력 또는 Issue 관계 호출: %s", r.URL.Path)
		}
	})
	result := call(t, cs, "get_event_detail", map[string]any{"cluster_id": "c1", "event_id": "EV-1", "include": []string{"occurrences", "actions", "issue", "playbook"}})
	out := decoded[tools.EventDetailData](t, result)
	if result.IsError || out.Status != "partial" || out.Data.Event.ID != "EV-1" || out.Data.Components.Occurrences != "empty" || out.Data.Components.Issue != "unsupported" || out.Data.Components.Actions != "unsupported" || out.Data.Components.Playbook != "error" || len(out.Errors) != 3 || calls.Load() != 3 {
		t.Fatalf("선택 조회 의미 손실: %+v calls=%d", out, calls.Load())
	}
}

func TestEventDetailIssueSelectionDoesNotCallUnboundedEndpoint(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/events/EV-1" {
			t.Errorf("제한 없는 Issue API 호출: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		io.WriteString(w, eventDetailFixture)
	})
	result := call(t, cs, "get_event_detail", map[string]any{"cluster_id": "c1", "event_id": "EV-1", "include": []string{"issue"}})
	out := decoded[tools.EventDetailData](t, result)
	if result.IsError || out.Status != "partial" || out.Data.Event.ID != "EV-1" || out.Data.Issue != nil || out.Data.Components.Issue != "unsupported" || len(out.Errors) != 1 || out.Errors[0].Component != "issue" || out.Errors[0].Code != "unsupported" || calls.Load() != 1 {
		t.Fatalf("Issue 미지원·기본 상세 보존·호출 상한: %+v calls=%d", out, calls.Load())
	}
}

func TestEventDetailStrictInput(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); io.WriteString(w, eventDetailFixture) })
	for _, args := range []map[string]any{
		{"cluster_id": "c1"},
		{"cluster_id": "c1", "event_id": " "},
		{"cluster_id": " ", "event_id": "EV-1"},
		{"cluster_id": "c1", "event_id": "EV-1", "include": []string{"payload"}},
		{"cluster_id": "c1", "event_id": "EV-1", "include": []string{"issue", "issue"}},
		{"cluster_id": "c1", "event_id": "EV-1", "limit": 101},
		{"cluster_id": "c1", "event_id": "EV-1", "limit": 0},
		{"cluster_id": "c1", "event_id": "EV-1", "unexpected": true},
	} {
		if !call(t, cs, "get_event_detail", args).IsError {
			t.Errorf("잘못된 입력 허용: %+v", args)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("잘못된 입력으로 백엔드 호출: %d", calls.Load())
	}
}

func TestEventDetailLargeHistoryDoesNotPaginate(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/api/events/EV-1" {
			io.WriteString(w, eventDetailFixture)
			return
		}
		if r.URL.Path != "/api/events/EV-1/occurrences" || r.URL.Query().Get("limit") != "101" {
			t.Errorf("제한 없는 이력 조회: %s", r.URL.RequestURI())
		}
		items := make([]map[string]any, 101)
		for i := range items {
			items[i] = map[string]any{"id": 101 - i, "eventId": "EV-1", "isFiring": true}
		}
		jsonReply(t, w, map[string]any{"items": items, "total": len(items)})
	})
	out := decoded[tools.EventDetailData](t, call(t, cs, "get_event_detail", map[string]any{"cluster_id": "c1", "event_id": "EV-1", "include": []string{"occurrences"}, "limit": 100}))
	if out.Status != "ok" || !out.Truncated || !out.Data.Occurrences.HasMore || out.Data.Occurrences.Returned != 100 || calls.Load() != 2 {
		t.Fatalf("많은 발생 이력의 제한·호출 수: %+v calls=%d", out, calls.Load())
	}
}
