package ableops

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

const securityIdentityFixture = `{"principal":"User:reader","identityKind":"SERVICE","identityKindSource":"inferred","status":"ACTIVE","lifecycle":{"stage":"REVIEW_DUE","severity":"WARN","reasons":[]},"credential":{"has":true,"supported":true,"locked":true,"items":[{"username":"reader","mechanism":"SCRAM-SHA-512","iterations":8192,"locked":true,"password":"excluded-secret"}]},"entitlement":{"aclCount":1},"findings":[{"code":"REVIEW_OVERDUE","severity":"WARN","attention":"REVIEW","message":"excluded-secret"}],"severity":"WARN","grants":[{"detail":"excluded-secret"}],"recentAudit":[{"before":"excluded-secret","after":"excluded-secret"}],"reachableTopics":{"items":["excluded-secret"]},"linkedFilebeat":[{"config":"excluded-secret"}],"purpose":"excluded-secret"}`

func TestSecurityRESTContracts(t *testing.T) {
	tests := []struct {
		name, path, body string
		call             func(context.Context, *Client) (any, error)
	}{
		{"identities", "/api/clusters/c1/identities", `{"clusterId":"c1","items":[` + securityIdentityFixture + `],"summary":{"total":7}}`, func(ctx context.Context, c *Client) (any, error) {
			return c.ListIdentities(ctx, "c1", IdentityFilters{})
		}},
		{"identity", "/api/clusters/c1/identities/" + url.PathEscape("User:reader"), `{"clusterId":"c1","items":` + securityIdentityFixture + `}`, func(ctx context.Context, c *Client) (any, error) { return c.IdentityDetail(ctx, "c1", "User:reader") }},
		{"acls", "/api/clusters/c1/acls", `{"clusterId":"c1","syncedAt":"2026-09-16T00:00:00Z","items":[{"principal":"User:reader","resourceType":"Topic","resourceName":"orders","operation":"Read","password":"excluded-secret","hasScram":true}]}`, func(ctx context.Context, c *Client) (any, error) { return c.ListACLs(ctx, "c1") }},
		{"principal-acls", "/api/clusters/c1/principals/" + url.PathEscape("User:reader") + "/acls", `{"clusterId":"c1","items":[{"principal":"User:reader","resourceType":"Topic","resourceName":"orders","operation":"Read"}]}`, func(ctx context.Context, c *Client) (any, error) {
			return c.ListPrincipalACLs(ctx, "c1", "User:reader")
		}},
		{"acl-risk", "/api/clusters/c1/acl-risk", `{"clusterId":"c1","status":"FORBIDDEN","findings":[],"principals":[],"generatedAt":"2026-09-16T00:00:00Z","note":"excluded-secret"}`, func(ctx context.Context, c *Client) (any, error) { return c.ACLRisk(ctx, "c1") }},
		{"scram-audit", "/api/clusters/c1/scram-audit", `{"clusterId":"c1","status":"UNAVAILABLE","supported":true,"findings":[],"unresolved":2,"generatedAt":"2026-09-16T00:00:00Z","note":"excluded-secret","authentication":{"password":"excluded-secret"}}`, func(ctx context.Context, c *Client) (any, error) { return c.ScramAudit(ctx, "c1") }},
		{"requests", "/api/requests", `[{"requestId":"r1","clusterId":"c1","status":"APPROVED","payload":{"topicName":"orders","password":"excluded-secret"},"summary":"excluded-secret","history":[{"comment":"excluded-secret"}]}]`, func(ctx context.Context, c *Client) (any, error) { return c.ListRequests(ctx, "c1") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.EscapedPath() != tc.path {
					t.Errorf("계약 경로 불일치: %s %s", r.Method, r.URL.EscapedPath())
				}
				if tc.name == "requests" {
					if r.URL.Query().Get("cluster") != "c1" || len(r.URL.Query()) != 1 {
						t.Error("객체 권한 목록의 cluster 필터 누락")
					}
				} else if r.URL.RawQuery != "" {
					t.Error("미지원 필터가 전송됨")
				}
				io.WriteString(w, tc.body)
			})
			out, err := tc.call(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(out)
			if strings.Contains(string(encoded), "excluded-secret") {
				t.Fatal("비밀/감사/클러스터 없는 Grant 노출")
			}
		})
	}
}

func TestIdentityActualFilters(t *testing.T) {
	c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		want := url.Values{"q": {"주문 + reader"}, "kind": {"SERVICE"}, "stage": {"REVIEW_DUE"}, "department": {"개발"}, "environment": {"prod"}, "severity": {"WARN"}, "finding": {"REVIEW_OVERDUE"}}
		if r.URL.Query().Encode() != want.Encode() {
			t.Errorf("실제 필터 불일치: %v", r.URL.Query())
		}
		io.WriteString(w, `{"clusterId":"c1","items":[],"summary":{"total":10}}`)
	})
	out, err := c.ListIdentities(context.Background(), "c1", IdentityFilters{Query: "주문 + reader", Kind: "SERVICE", Stage: "REVIEW_DUE", Department: "개발", Environment: "prod", Severity: "WARN", Finding: "REVIEW_OVERDUE"})
	if err != nil || out.Summary.Total != 10 {
		t.Fatalf("필터/전체 집계 계약: %+v %v", out, err)
	}
}

func TestSecurityFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		call       func(*Client) error
	}{
		{"잘못된 계정 범위", `{"clusterId":"other","items":[]}`, func(c *Client) error {
			_, e := c.ListIdentities(context.Background(), "c1", IdentityFilters{})
			return e
		}},
		{"누락된 계정 목록", `{"clusterId":"c1"}`, func(c *Client) error {
			_, e := c.ListIdentities(context.Background(), "c1", IdentityFilters{})
			return e
		}},
		{"다른 계정 상세", `{"clusterId":"c1","items":` + securityIdentityFixture + `}`, func(c *Client) error { _, e := c.IdentityDetail(context.Background(), "c1", "User:other"); return e }},
		{"다른 계정 ACL", `{"clusterId":"c1","items":[{"principal":"User:other"}]}`, func(c *Client) error {
			_, e := c.ListPrincipalACLs(context.Background(), "c1", "User:reader")
			return e
		}},
		{"다른 클러스터 신청", `[{"clusterId":"other","requestId":"r1","status":"APPLIED"}]`, func(c *Client) error { _, e := c.ListRequests(context.Background(), "c1"); return e }},
		{"빈 클러스터 신청", `[{"clusterId":"","requestId":"r1","status":"APPLIED"}]`, func(c *Client) error { _, e := c.ListRequests(context.Background(), "c1"); return e }},
		{"미정의 신청 상태", `[{"clusterId":"c1","requestId":"r1","status":"DONE"}]`, func(c *Client) error { _, e := c.ListRequests(context.Background(), "c1"); return e }},
		{"미정의 진단", `{"clusterId":"c1","status":"UNKNOWN","findings":[],"generatedAt":"2026-09-16T00:00:00Z"}`, func(c *Client) error { _, e := c.ACLRisk(context.Background(), "c1"); return e }},
		{"미정의 하위 진단", `{"clusterId":"c1","status":"OK","findings":[{"code":"A","status":"UNKNOWN"}],"generatedAt":"2026-09-16T00:00:00Z"}`, func(c *Client) error { _, e := c.ScramAudit(context.Background(), "c1"); return e }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) })
			requireErrorCode(t, tc.call(c), "invalid_response")
		})
	}
}

func TestSecurityNoDefaultOrRetry(t *testing.T) {
	calls := 0
	c := apiTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "excluded-secret", http.StatusForbidden)
	})
	checks := []func() error{
		func() error { _, e := c.ListIdentities(context.Background(), "", IdentityFilters{}); return e },
		func() error { _, e := c.IdentityDetail(context.Background(), "c1", ""); return e },
		func() error { _, e := c.ListACLs(context.Background(), " "); return e },
		func() error { _, e := c.ListPrincipalACLs(context.Background(), "c1", ""); return e },
		func() error { _, e := c.ACLRisk(context.Background(), ""); return e },
		func() error { _, e := c.ScramAudit(context.Background(), ""); return e },
		func() error { _, e := c.ListRequests(context.Background(), ""); return e },
	}
	for _, check := range checks {
		requireErrorCode(t, check(), "invalid_request")
	}
	if calls != 0 {
		t.Fatal("필수 입력 없이 API 호출")
	}
	_, err := c.ListRequests(context.Background(), "c1")
	requireErrorCode(t, err, "access_denied")
	if calls != 1 {
		t.Fatal("거부 후 재시도 또는 우회")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.ScramAudit(ctx, "c1")
	requireErrorCode(t, err, "canceled")
	if calls != 1 {
		t.Fatal("취소 후 API 호출")
	}
}
