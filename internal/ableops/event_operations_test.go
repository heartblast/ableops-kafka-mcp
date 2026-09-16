package ableops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

const eventSummaryFixture = `{"openTotal":3,"attentionUrgent":1,"attentionReview":2,"syntheticExcluded":4,"monitoredClusters":1,"generatedAt":"2026-09-16T00:00:00Z","scope":{"all":false,"clusterCount":1,"totalClusters":2,"clusterId":"c1","clusterIds":["hidden-marker"]},"collection":{"enabled":true,"intervalSec":30}}`

func TestEventSummaryScopeFailuresAndSafeFields(t *testing.T) {
	for _, tc := range []struct{ name, body, code string }{
		{"정상", eventSummaryFixture, ""},
		{"권한 거부 0건", strings.Replace(eventSummaryFixture, `"all":false`, `"all":false,"clusterDenied":true`, 1), "access_denied"},
		{"권한 범위 없음", strings.Replace(eventSummaryFixture, `"clusterCount":1`, `"clusterCount":0`, 1), "access_denied"},
		{"미등록", strings.Replace(eventSummaryFixture, `"monitoredClusters":1`, `"monitoredClusters":0`, 1), "not_found"},
		{"다른 클러스터", strings.Replace(eventSummaryFixture, `"c1"`, `"c2"`, 1), "invalid_response"},
		{"음수", strings.Replace(eventSummaryFixture, `"openTotal":3`, `"openTotal":-1`, 1), "invalid_response"},
		{"본문 실패", `{"status":"FAILED","error":"hidden-marker"}`, "invalid_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/events/summary" || r.URL.Query().Get("clusterId") != "c1" || len(r.URL.Query()) != 1 {
					t.Error("요약 경로·클러스터 필터 불일치")
				}
				io.WriteString(w, tc.body)
			})
			out, err := c.GetEventSummary(context.Background(), "c1")
			if tc.code != "" {
				requireErrorCode(t, err, tc.code)
				if out.Scope != nil {
					t.Fatal("실패 집계 노출")
				}
				return
			}
			if err != nil || out.OpenTotal != 3 || out.AttentionUrgent != 1 || out.SyntheticExcluded != 4 {
				t.Fatalf("집계 의미 손실: %+v %v", out, err)
			}
			raw, _ := json.Marshal(out)
			if strings.Contains(string(raw), "hidden-marker") {
				t.Fatal("다른 클러스터 목록 노출")
			}
		})
	}
}

func TestEventRuleOverrideBoundaryAndFalse(t *testing.T) {
	base := `{"clusterId":"c1","total":1,"items":[{"eventCode":"CLUSTER_UNREACHABLE","effective":{"enabled":false,"severity":"CRITICAL"},"default":{"enabled":true},"globalOverride":{"eventCode":"CLUSTER_UNREACHABLE","clusterId":"","enabled":false,"minDurationSec":0,"runbookUrl":"hidden-marker"},"clusterOverride":{"eventCode":"CLUSTER_UNREACHABLE","clusterId":"c1","notifyEnabled":false},"runbookUrl":"hidden-marker"}]}`
	for _, mismatch := range []bool{false, true} {
		c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/event-rules" || r.URL.Query().Get("clusterId") != "c1" {
				t.Error("규칙 API 계약 불일치")
			}
			body := base
			if mismatch {
				body = strings.Replace(body, `"clusterId":"c1","notifyEnabled"`, `"clusterId":"c2","notifyEnabled"`, 1)
			}
			io.WriteString(w, body)
		})
		out, err := c.GetEventRule(context.Background(), "c1", "CLUSTER_UNREACHABLE")
		if mismatch {
			requireErrorCode(t, err, "invalid_response")
			continue
		}
		if err != nil || out.Effective.Enabled || out.GlobalOverride.Enabled == nil || *out.GlobalOverride.Enabled || out.GlobalOverride.MinDurationSec == nil || *out.GlobalOverride.MinDurationSec != 0 || out.ClusterOverride.NotifyEnabled == nil {
			t.Fatalf("명시적 false·0과 상속 손실: %+v %v", out, err)
		}
		raw, _ := json.Marshal(out)
		if strings.Contains(string(raw), "hidden-marker") {
			t.Fatal("규칙 URL 노출")
		}
	}
}

func TestEventPolicyAndMaintenanceObjectBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		call       func(*Client) error
	}{
		{"주의도 클러스터", `{"clusterId":"c2","profile":"STANDARD","items":[],"total":0}`, func(c *Client) error {
			_, err := c.GetAttentionRule(context.Background(), "c1", "CLUSTER_UNREACHABLE")
			return err
		}},
		{"주의도 조정 소속", `{"clusterId":"c1","profile":"STANDARD","items":[{"eventCode":"CLUSTER_UNREACHABLE","profileApplied":"STANDARD","effective":{},"default":{},"globalOverride":{"eventCode":"CLUSTER_UNREACHABLE","clusterId":"c2"}}],"total":1}`, func(c *Client) error {
			_, err := c.GetAttentionRule(context.Background(), "c1", "CLUSTER_UNREACHABLE")
			return err
		}},
		{"프로필 소속", `{"clusterId":"c2","profile":"STANDARD"}`, func(c *Client) error { _, err := c.GetAttentionProfile(context.Background(), "c1"); return err }},
		{"유지보수 소속", `{"items":[{"id":"m1","clusterId":"c2"}],"total":1}`, func(c *Client) error {
			_, err := c.ListMaintenanceWindows(context.Background(), "c1", false)
			return err
		}},
		{"Issue 소속", `{"items":[{"id":"is1","clusterId":"c2","status":"OPEN"}],"total":1,"page":1,"pageSize":2}`, func(c *Client) error {
			_, err := c.ListOperationalIssues(context.Background(), "c1", IssueFilters{Page: 1, PageSize: 2})
			return err
		}},
		{"Issue 업무 실패", `{"items":[{"id":"is1","clusterId":"c1","status":"FAILED"}],"total":1,"page":1,"pageSize":2}`, func(c *Client) error {
			_, err := c.ListOperationalIssues(context.Background(), "c1", IssueFilters{Page: 1, PageSize: 2})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := apiTestClient(t, func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, tc.body) })
			requireErrorCode(t, tc.call(c), "invalid_response")
		})
	}
}

func TestEventQueriesUseRequestCredentialsConcurrently(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer synthetic-event-")
		if id != "A" && id != "B" {
			t.Error("요청 사용자 자격증명 유실")
			w.WriteHeader(401)
			return
		}
		if r.URL.Query().Get("clusterId") != id {
			w.WriteHeader(403)
			return
		}
		fmt.Fprintf(w, `{"monitoredClusters":1,"generatedAt":"2026-09-16T00:00:00Z","scope":{"all":false,"clusterCount":1,"clusterId":%q},"collection":{"enabled":true}}`, id)
	}))
	t.Cleanup(backend.Close)
	u, _ := url.Parse(backend.URL)
	c, err := NewClient(config.Config{BaseURL: u, Timeout: time.Second, AllowHTTP: true, RequireRequestCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	var wg sync.WaitGroup
	for _, id := range []string{"A", "B"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := requestctx.WithPrincipal(context.Background(), requestctx.Principal{UserID: id, BackendToken: "synthetic-event-" + id})
			out, err := c.GetEventSummary(ctx, id)
			if err != nil || out.Scope.ClusterID != id {
				t.Error("사용자별 이벤트 범위 혼합")
			}
			_, err = c.GetEventSummary(ctx, "denied")
			requireErrorCode(t, err, "access_denied")
		}()
	}
	wg.Wait()
}
