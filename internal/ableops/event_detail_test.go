package ableops

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestEventDetailContractAndAllowlist(t *testing.T) {
	c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/events/EV-1" || r.URL.RawQuery != "" {
			t.Errorf("상세 경로: %s", r.URL.RequestURI())
		}
		io.WriteString(w, `{"id":"EV-1","clusterId":"c1","eventCode":"CRITICAL_CONSUMER_LAG","status":"ACKNOWLEDGED","severity":"HIGH","attentionLevel":"URGENT","recommendedAction":"담당 파티션 확인","acknowledgedAt":"2026-09-16T00:00:00Z","evidence":{"group":"billing","totalLag":0,"monitored":false,"password":"hidden-marker","reason":"hidden-marker","nested":{"body":"hidden-marker"}},"runbookUrl":"hidden-marker","resolutionReason":"hidden-marker"}`)
	})
	out, err := c.EventDetail(context.Background(), "c1", "EV-1")
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "ACKNOWLEDGED" || out.Severity != "HIGH" || out.AttentionLevel != "URGENT" || out.RecommendedAction == "" || out.AcknowledgedAt == nil || out.Evidence.TotalLag == nil || *out.Evidence.TotalLag != 0 || out.Evidence.Monitored == nil || *out.Evidence.Monitored {
		t.Fatalf("상태·발생 근거의 의미 손실: %+v", out)
	}
	b, _ := json.Marshal(out)
	if strings.Contains(string(b), "hidden-marker") {
		t.Fatal("허용하지 않은 payload 노출")
	}
}

func TestEventDetailRejectsUnsafeRequiredResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		code   string
	}{
		{"다른 클러스터", 200, `{"id":"EV-1","clusterId":"c2","eventCode":"CODE","status":"OPEN"}`, "invalid_response"},
		{"다른 객체", 200, `{"id":"EV-2","clusterId":"c1","eventCode":"CODE","status":"OPEN"}`, "invalid_response"},
		{"본문 업무 실패", 200, `{"status":"FAILED","error":"hidden-marker"}`, "invalid_response"},
		{"빈 응답", 200, `{}`, "invalid_response"},
		{"권한 거부", 403, `{"error":"hidden-marker"}`, "access_denied"},
		{"객체 없음", 404, `{"error":"hidden-marker"}`, "not_found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := apiTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tc.status); io.WriteString(w, tc.body) })
			out, err := c.EventDetail(context.Background(), "c1", "EV-1")
			var apiErr *Error
			if !errors.As(err, &apiErr) || apiErr.Code != tc.code || out.ID != "" {
				t.Fatalf("잘못된 응답 처리: %+v, %v", out, err)
			}
		})
	}
}

func TestEventOccurrencesContractAndObjectBoundary(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/events/EV-1/occurrences" || r.URL.Query().Get("limit") != "3" || len(r.URL.Query()) != 1 {
				t.Errorf("발생 이력 경로·제한: %s", r.URL.RequestURI())
			}
			id := "EV-1"
			if mismatch {
				id = "EV-2"
			}
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"eventId": id, "observedAt": "2026-09-16T00:00:00Z", "isFiring": false, "evidence": map[string]any{"totalLag": 0, "body": "hidden-marker"}}}, "total": 1})
		})
		out, err := c.EventOccurrences(context.Background(), "EV-1", 3)
		if mismatch {
			if err == nil || out.Items != nil {
				t.Fatal("다른 이벤트의 이력 허용")
			}
			continue
		}
		if err != nil || len(out.Items) != 1 || out.Items[0].IsFiring || out.Items[0].Evidence.TotalLag == nil {
			t.Fatalf("발생 이력 의미 손실: %+v %v", out, err)
		}
		b, _ := json.Marshal(out)
		if strings.Contains(string(b), "hidden-marker") {
			t.Fatal("이력 민감 필드 노출")
		}
	}
}

func TestEventIssueEmptyAndClusterBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		code   string
	}{
		{"관계 없음", 204, "", ""},
		{"관계 있음", 200, `{"issue":{"id":"IS-1","clusterId":"c1","status":"OPEN"},"members":[{"password":"hidden-marker"}],"actions":[{"detail":"hidden-marker"}]}`, ""},
		{"다른 클러스터", 200, `{"issue":{"id":"IS-1","clusterId":"c2","status":"OPEN"}}`, "invalid_response"},
		{"거부", 403, `{}`, "access_denied"},
		{"대상 없음", 404, `{}`, "not_found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/events/EV-1/issue" || r.URL.Query().Get("limit") != "1" {
					t.Errorf("Issue 경로: %s", r.URL.RequestURI())
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			out, err := c.EventIssue(context.Background(), "c1", "EV-1")
			if tc.code != "" {
				var apiErr *Error
				if !errors.As(err, &apiErr) || apiErr.Code != tc.code || out != nil {
					t.Fatalf("Issue 실패 처리: %+v %v", out, err)
				}
			} else if err != nil || (tc.status == 204) != (out == nil) {
				t.Fatalf("빈 관계와 실패 구분: %+v %v", out, err)
			}
			b, _ := json.Marshal(out)
			if strings.Contains(string(b), "hidden-marker") {
				t.Fatal("미공개 Issue payload 노출")
			}
		})
	}
}

func TestEventDetailTimeoutAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		})
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		want := "timeout"
		if canceled {
			cancel()
			want = "canceled"
		}
		out, err := c.EventDetail(ctx, "c1", "EV-1")
		cancel()
		var apiErr *Error
		if !errors.As(err, &apiErr) || apiErr.Code != want || out.ID != "" {
			t.Fatalf("이벤트 시간 제한/취소: %+v %v", out, err)
		}
	}
}

func TestEventOccurrencesRejectsIgnoredLimit(t *testing.T) {
	c := apiTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"items":[{"eventId":"EV-1"},{"eventId":"EV-1"}],"total":2}`)
	})
	if _, err := c.EventOccurrences(context.Background(), "EV-1", 1); err == nil {
		t.Fatal("백엔드가 제한을 무시한 응답 허용")
	}
}
