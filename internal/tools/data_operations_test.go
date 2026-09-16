package tools_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/mcpserver"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func connectDataOperations(t *testing.T, enabled bool, handler http.HandlerFunc) *mcp.ClientSession {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+sessionToken {
			t.Error("인증 헤더 누락")
		}
		handler(w, r)
	}))
	t.Cleanup(backend.Close)
	u, _ := url.Parse(backend.URL)
	c, err := ableops.NewClient(config.Config{BaseURL: u, Token: sessionToken, AllowHTTP: true, Timeout: time.Second, SampleEnabled: enabled, SampleTopics: []string{"c1/orders"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	server := mcpserver.New(c, nil)
	st, ct := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "synthetic-data-test", Version: "1"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

func TestDataOperationsSDK(t *testing.T) {
	var count atomic.Int32
	cs := connectDataOperations(t, true, func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		switch r.URL.Path {
		case "/api/clusters/c1/resource-backups":
			if r.Method != "GET" || r.URL.Query().Get("limit") != "1" {
				t.Error("백업 조회 인자 불일치")
			}
			io.WriteString(w, `{"items":[{"id":"b1","clusterId":"c1","status":"FAILED","failureCount":1}],"total":2,"limit":1,"offset":0}`)
		case "/api/clusters/c1/topics/orders/messages":
			if r.Method != "GET" || r.URL.Query().Get("limit") != "20" {
				t.Error("샘플 기본 제한 불일치")
			}
			io.WriteString(w, `[{"partition":0,"offset":42,"timestamp":"2026-09-16T00:00:00Z","key":"private-key","value":"private-body","headers":{"secret":"private-header"}}]`)
		case "/api/clusters/c1/acls/plan":
			if r.Method != "POST" {
				t.Error("ACL 미리보기 메서드 불일치")
			}
			io.WriteString(w, `{"plan":{"generated":[],"toCreate":[],"duplicates":[]},"policy":{"passed":false,"riskLevel":"HIGH","violations":[]}}`)
		case "/api/clusters/c1/flink-acl-plan":
			if r.Method != "POST" {
				t.Error("Flink ACL 미리보기 메서드 불일치")
			}
			io.WriteString(w, `{"required":[],"held":[],"missing":[],"aclKnown":false,"policyResult":{"passed":false,"riskLevel":"HIGH","violations":[]}}`)
		case "/api/flink/ddl-preview":
			if r.Method != "POST" {
				t.Error("DDL 미리보기 메서드 불일치")
			}
			json.NewEncoder(w).Encode(map[string]any{"tableName": "src_orders", "sourceDdl": "CREATE TABLE `src_orders` (\n  `id` STRING\n) WITH (\n 'password'='private-option'\n);", "validation": map[string]any{"status": "static_error"}})
		default:
			t.Errorf("허용하지 않은 변경·조회 경로: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	backup := decoded[tools.ResourceBackupsData](t, call(t, cs, "list_resource_backups", map[string]any{"cluster_id": "c1", "limit": 1}))
	if backup.Status != "partial" || !backup.Truncated || !backup.Data.HasMore || backup.Data.Items[0].Status != "FAILED" {
		t.Fatal("페이지 또는 백업 실패 의미 유실")
	}
	sampleResult := call(t, cs, "sample_topic_messages", map[string]any{"cluster_id": "c1", "topic_name": "orders"})
	sample := decoded[tools.SampleMessagesData](t, sampleResult)
	if sample.Status != "partial" || sample.Data.DataMode != "unknown" || sample.Data.ContentPolicy != "metadata_only" || sample.Data.Items[0].Offset != 42 {
		t.Fatal("샘플 정책·출처 유실")
	}
	acl := decoded[ableops.ACLPlan](t, call(t, cs, "preview_acl_plan", map[string]any{"cluster_id": "c1", "template_key": "consumer", "principal": "User:synthetic", "topic": "orders", "hosts": []string{"127.0.0.1"}}))
	if acl.Status != "partial" || acl.Data.Policy.Passed {
		t.Fatal("정책 실패·관측 불확실성 유실")
	}
	flink := decoded[ableops.FlinkACLPlan](t, call(t, cs, "preview_flink_acl_plan", map[string]any{"cluster_id": "c1", "principal": "User:synthetic", "source_topic": "orders"}))
	if flink.Status != "partial" || *flink.Data.ACLKnown {
		t.Fatal("ACL 관측 불명 유실")
	}
	ddlResult := call(t, cs, "preview_flink_ddl", map[string]any{"cluster_id": "c1", "topic_name": "orders", "table_name": "src_orders", "columns": []any{map[string]any{"name": "id", "type": "STRING"}}})
	ddl := decoded[ableops.DDLPreview](t, ddlResult)
	if ddl.Status != "partial" || !ddl.Truncated || ddl.Data.ValidationStatus != "static_error" || ddl.Errors[0].BackendStatus != "static_error" {
		t.Fatal("DDL 검증 실패·생략 유실")
	}
	for _, result := range []*mcp.CallToolResult{sampleResult, ddlResult} {
		raw, _ := json.Marshal(result)
		if strings.Contains(string(raw), "private-") {
			t.Fatal("민감정보가 SDK에 노출됨")
		}
	}
	if count.Load() != 5 {
		t.Fatal("도구당 1회 호출 제한 위반")
	}
}

func TestDataOperationsSDKDeniedBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	cs := connectDataOperations(t, false, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, `[]`) })
	disabled := decoded[tools.SampleMessagesData](t, call(t, cs, "sample_topic_messages", map[string]any{"cluster_id": "c1", "topic_name": "orders"}))
	if disabled.Status != "error" || disabled.Errors[0].Code != "feature_disabled" {
		t.Fatal("기본 비활성화 실패")
	}
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{"sample_topic_messages", map[string]any{"cluster_id": "c1", "topic_name": "orders", "limit": 101}},
		{"sample_topic_messages", map[string]any{"cluster_id": "c1", "topic_name": "orders", "include_value": true}},
		{"preview_acl_plan", map[string]any{"cluster_id": "c1", "template_key": "unknown", "principal": "User:synthetic", "topic": "orders", "hosts": []string{"*"}}},
		{"preview_flink_acl_plan", map[string]any{"cluster_id": "c1", "principal": "User:synthetic"}},
		{"preview_flink_ddl", map[string]any{"cluster_id": "c1", "topic_name": "orders", "table_name": "src_orders", "columns": []any{map[string]any{"name": "id", "type": "STRING; DROP TABLE x"}}}},
		{"preview_flink_ddl", map[string]any{"cluster_id": "c1", "topic_name": "orders", "table_name": "src_orders", "columns": []any{map[string]any{"name": "id", "type": "STRING"}}, "sql": "SELECT 1"}},
		{"list_resource_backups", map[string]any{"cluster_id": "c1", "offset": 10001}},
	} {
		if !call(t, cs, tc.name, tc.args).IsError {
			t.Errorf("잘못된 입력 허용: %s", tc.name)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("검증 실패 후 백엔드 호출")
	}
}

func TestDataOperationsSDKEmptySampleIsUncertain(t *testing.T) {
	cs := connectDataOperations(t, true, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "null") })
	out := decoded[tools.SampleMessagesData](t, call(t, cs, "sample_topic_messages", map[string]any{"cluster_id": "c1", "topic_name": "orders"}))
	if out.Status != "partial" || out.Data == nil || out.Data.Items == nil || out.Data.Returned != 0 || out.Data.DataMode != "unknown" {
		t.Fatal("백엔드 빈 표본을 조회 오류나 확인된 정상으로 처리함")
	}
}

func TestDataOperationsSDKAnnotationsAndDeferredTools(t *testing.T) {
	cs := connectDataOperations(t, false, func(w http.ResponseWriter, r *http.Request) { t.Fatal("도구 발견이 백엔드를 호출") })
	list, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	deferred := map[string]bool{"search_topic_catalog": true, "list_governance_findings": true, "list_flink_jobs": true, "get_flink_job": true, "list_pipeline_projects": true, "get_pipeline_project": true, "inspect_es_target": true, "get_restore_plan": true}
	for _, tool := range list.Tools {
		if deferred[tool.Name] {
			t.Errorf("보류 도구 등록: %s", tool.Name)
		}
		switch tool.Name {
		case "sample_topic_messages", "preview_acl_plan", "preview_flink_acl_plan":
			if tool.Annotations.ReadOnlyHint || tool.Annotations.IdempotentHint || tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint {
				t.Errorf("실제 부작용과 annotation 불일치: %s", tool.Name)
			}
		}
	}
}
