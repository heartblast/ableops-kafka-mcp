package ableops

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

func TestDataOperationsSamplePolicyAndProjection(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "GET" || r.URL.Path != "/api/clusters/c1/topics/orders/messages" || r.URL.Query().Get("limit") != "20" {
			t.Error("허용하지 않은 샘플 호출")
		}
		io.WriteString(w, `[{"partition":2,"offset":9007199254740993,"timestamp":"2026-09-16T00:00:00Z","key":"private-key","value":"private-body","headers":{"password":"private-header"},"truncated":true}]`)
	}))
	defer backend.Close()
	c := mockClient(t, backend)
	_, err := c.SampleTopicMessages(context.Background(), "c1", "orders", 20)
	requireErrorCode(t, err, "feature_disabled")
	c.sampleEnabled = true
	c.sampleTopics = map[string]bool{"c1/orders": true}
	_, err = c.SampleTopicMessages(context.Background(), "c2", "orders", 20)
	requireErrorCode(t, err, "topic_not_allowed")
	_, err = c.SampleTopicMessages(context.Background(), "c1", "orders", 101)
	requireErrorCode(t, err, "invalid_request")
	if calls.Load() != 0 {
		t.Fatal("정책 거부가 백엔드까지 전달됨")
	}
	items, err := c.SampleTopicMessages(context.Background(), "c1", "orders", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Offset != 9007199254740993 || !items[0].Truncated {
		t.Fatal("위치 정밀도 또는 잘림 누락")
	}
	raw, _ := json.Marshal(items)
	if strings.Contains(string(raw), "private-") {
		t.Fatal("본문·키·헤더 노출")
	}
}

func TestDataOperationsSampleBoundsAndCancellation(t *testing.T) {
	for _, tc := range []struct{ name, body, code string }{
		{"oversized", `[{"value":"` + strings.Repeat("x", MaxSampleResponseBytes) + `"}]`, "response_too_large"},
		{"invalid", `[{"partition":-1,"offset":2,"timestamp":"2026-09-16T00:00:00Z"}]`, "invalid_response"},
		{"business_failure", `{"status":"FORBIDDEN"}`, "invalid_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) }))
			defer backend.Close()
			c := mockClient(t, backend)
			c.sampleEnabled = true
			c.sampleTopics = map[string]bool{"c1/orders": true}
			_, err := c.SampleTopicMessages(context.Background(), "c1", "orders", 20)
			requireErrorCode(t, err, tc.code)
		})
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("취소한 호출이 전송됨") }))
	defer backend.Close()
	c := mockClient(t, backend)
	c.sampleEnabled = true
	c.sampleTopics = map[string]bool{"c1/orders": true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.SampleTopicMessages(ctx, "c1", "orders", 20)
	requireErrorCode(t, err, "canceled")
}

func TestDataOperationsPreviewFixedRoutesAndCredentials(t *testing.T) {
	var paths []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Authorization") != "Bearer delegated-synthetic-session" {
			t.Error("미리보기 메서드·격리 인증 불일치")
		}
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "password") || strings.Contains(string(body), "samples") {
			t.Error("허용하지 않은 미리보기 입력")
		}
		switch r.URL.Path {
		case "/api/clusters/c1/acls/plan":
			io.WriteString(w, `{"plan":{"generated":[],"toCreate":[],"duplicates":[],"descriptions":["private-value"]},"policy":{"passed":false,"riskLevel":"HIGH","violations":[{"code":"ACL_BLOCK","severity":"BLOCK","message":"private-value"}]}}`)
		case "/api/clusters/c1/flink-acl-plan":
			io.WriteString(w, `{"required":[],"held":[],"missing":[],"aclKnown":false,"policyResult":{"passed":true,"riskLevel":"LOW","violations":[]}}`)
		case "/api/flink/ddl-preview":
			var in map[string]any
			if json.Unmarshal(body, &in) != nil || in["clusterId"] != "c1" || len(in["metadata"].([]any)) != 0 {
				t.Error("DDL 명시적 클러스터·metadata 계약 불일치")
			}
			json.NewEncoder(w).Encode(map[string]any{"tableName": "src_orders", "sourceDdl": "CREATE TABLE `src_orders` (\n  `id` STRING\n) WITH (\n  'properties.sasl.jaas.config' = 'private-value'\n);", "validation": map[string]any{"status": "static_warn", "checks": []any{map[string]any{"message": "private-value"}}}})
		default:
			t.Error("미리보기 외 경로 호출")
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	c := mockClient(t, backend)
	ctx := requestctx.WithPrincipal(context.Background(), requestctx.Principal{BackendToken: "delegated-synthetic-session", MCPToken: "synthetic-mcp-token"})
	plan, err := c.PreviewACLPlan(ctx, "c1", ACLPlanInput{Principal: "User:synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Policy.Passed || plan.Policy.RiskLevel != "HIGH" {
		t.Fatal("업무 실패 판정 유실")
	}
	flink, err := c.PreviewFlinkACLPlan(ctx, "c1", FlinkACLPlanInput{Principal: "User:synthetic"})
	if err != nil || flink.ACLKnown == nil || *flink.ACLKnown {
		t.Fatalf("ACL 미관측 상태: %v", err)
	}
	ddl, err := c.PreviewFlinkDDL(ctx, "c1", "orders", "src_orders", []DDLColumn{{Name: "id", Type: "STRING"}})
	if err != nil {
		t.Fatal(err)
	}
	if ddl.ValidationStatus != "static_warn" || strings.Contains(ddl.ColumnDDL, "WITH") {
		t.Fatal("검증 상태 또는 인증 옵션 제거 실패")
	}
	for _, out := range []any{plan, flink, ddl} {
		raw, _ := json.Marshal(out)
		if strings.Contains(string(raw), "private-value") {
			t.Fatal("시크릿 또는 검증 원문 노출")
		}
	}
	if len(paths) != 3 {
		t.Fatal("호출 상한 위반")
	}
}

func TestDataOperationsUnsafeResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		call       func(*Client) error
	}{
		{"wrong_backup_cluster", `{"items":[{"id":"b1","clusterId":"c2","status":"COMPLETE"}],"total":1,"limit":50,"offset":0}`, func(c *Client) error {
			_, err := c.ResourceBackups(context.Background(), "c1", BackupFilters{Limit: 50})
			return err
		}},
		{"wrong_principal", `{"plan":{"generated":[{"principal":"User:other"}],"toCreate":[],"duplicates":[]},"policy":{"riskLevel":"LOW"}}`, func(c *Client) error {
			_, err := c.PreviewACLPlan(context.Background(), "c1", ACLPlanInput{Principal: "User:synthetic"})
			return err
		}},
		{"missing_acl_observation", `{"required":[],"held":[],"missing":[],"policyResult":{"riskLevel":"LOW"}}`, func(c *Client) error {
			_, err := c.PreviewFlinkACLPlan(context.Background(), "c1", FlinkACLPlanInput{Principal: "User:synthetic"})
			return err
		}},
		{"unexpected_ddl", `{"tableName":"src_orders","sourceDdl":"CREATE TABLE x (password STRING)","validation":{"status":"static_ok"}}`, func(c *Client) error {
			_, err := c.PreviewFlinkDDL(context.Background(), "c1", "orders", "src_orders", []DDLColumn{{Name: "id", Type: "STRING"}})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) }))
			defer backend.Close()
			requireErrorCode(t, tc.call(mockClient(t, backend)), "invalid_response")
		})
	}
}

func TestDataOperationsBackupFilters(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/api/clusters/c1/resource-backups" || q.Get("status") != "PARTIAL" || q.Get("q") != "synthetic" || q.Get("limit") != "5" || q.Get("offset") != "10" || q.Get("from") != "2026-09-01" || q.Get("to") != "2026-09-16" {
			t.Error("백업 필터 계약 불일치")
		}
		io.WriteString(w, `{"items":[{"id":"b1","clusterId":"c1","status":"PARTIAL","failureCount":2,"failReason":"private-value","scope":{"password":"private-value"}}],"total":12,"limit":5,"offset":10}`)
	}))
	defer backend.Close()
	out, err := mockClient(t, backend).ResourceBackups(context.Background(), "c1", BackupFilters{Status: "PARTIAL", Search: "synthetic", Limit: 5, Offset: 10, From: "2026-09-01", To: "2026-09-16"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Items[0].Status != "PARTIAL" || out.Items[0].FailureCount != 2 {
		t.Fatal("백업 부분 실패 유실")
	}
	raw, _ := json.Marshal(out)
	if strings.Contains(string(raw), "private-value") {
		t.Fatal("백업 본문 노출")
	}
}
