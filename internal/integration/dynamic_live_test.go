package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/dynamic"
	"github.com/heartblast/ableops-kafka-mcp/internal/mcpserver"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestDynamicBackendLive는 실행 중인 AbleOps Backend의 /openapi.json으로 Dynamic 경로를 확인한다.
// 계약 조회·ETag·304·Registry·tools/list·tools/call·인증 전달·원문 JSON 반환을 본다.
// 응답 원문·토큰·실제 식별자는 출력하지 않는다. 테스트 픽스처를 실연동으로 대신하지 않는다.
func TestDynamicBackendLive(t *testing.T) {
	if os.Getenv("ABLEOPS_VERIFY_BACKEND") != "1" {
		t.Skip("실연동 미선택: ABLEOPS_VERIFY_BACKEND=1로 명시적으로 실행하세요")
	}
	cfg, err := config.LoadFrom(func(key string) string {
		value := os.Getenv(key)
		if key == "ABLEOPS_API_TOKEN" && value == "" {
			return invalidSession
		}
		return value
	})
	if err != nil {
		t.Fatal("실연동 설정 오류: 기존 ABLEOPS 환경변수 형식을 확인하세요")
	}
	api, err := ableops.NewClient(cfg)
	if err != nil {
		t.Fatal("실연동 REST 클라이언트 설정 오류")
	}
	t.Cleanup(api.CloseIdleConnections)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var etag string
	t.Run("contract_etag_304", func(t *testing.T) {
		doc, err := api.FetchOpenAPI(ctx, "")
		if err != nil {
			t.Fatalf("계약 조회 실패: code=%s", dynamic.ErrorCode(err))
		}
		contract, err := openapi.Parse(doc.Body)
		if err != nil {
			t.Fatalf("계약 검증 실패: code=%s", dynamic.ErrorCode(err))
		}
		etag = doc.ETag
		t.Logf("discovered=%d mcp_enabled=%d issues=%d etag_present=%t", contract.Discovered, contract.MCPDeclared, len(contract.Issues), etag != "")
		// 정책표와 실제 계약의 차이는 operationId로만 보고한다(공개 계약 식별자).
		policy := dynamic.ExposurePolicy()
		for _, op := range contract.MCPOperations() {
			rule, ok := policy[op.ID]
			switch {
			case !ok:
				t.Logf("unclassified_operation=%s", op.ID)
			case rule.Exposure == dynamic.ExposureSafe && dynamic.ClassifyExposure(op).Exposure != dynamic.ExposureSafe:
				// SAFE 검토 당시와 경로·파라미터가 달라 노출하지 않는 Operation이다.
				t.Logf("safe_policy_drift=%s", op.ID)
			}
		}
		if etag == "" {
			t.Fatal("upstream이 ETag를 제공하지 않았습니다")
		}
		again, err := api.FetchOpenAPI(ctx, etag)
		if err != nil || !again.NotModified {
			t.Fatalf("If-None-Match에 304가 아닙니다: code=%s", dynamic.ErrorCode(err))
		}
	})

	var safe []string
	for id, rule := range dynamic.ExposurePolicy() {
		if rule.Exposure == dynamic.ExposureSafe {
			safe = append(safe, id)
		}
	}
	runtime := dynamic.NewRuntime(api, slog.New(slog.NewJSONHandler(io.Discard, nil)), openapi.NewLoader(api), dynamic.Options{
		StaticToolNames: tools.StaticNames(),
		Operations:      safe,
	})
	server := mcpserver.New(api, slog.New(slog.NewJSONHandler(io.Discard, nil)), mcpserver.WithRuntime(runtime))

	t.Run("runtime_refresh", func(t *testing.T) {
		first := runtime.Refresh(ctx)
		if first.Outcome != dynamic.RefreshUpdated {
			t.Fatalf("최초 적재 outcome=%s code=%s", first.Outcome, first.Code)
		}
		stats := first.Registry.Stats()
		t.Logf("registrable=%d exposed=%d shadow=%d blocked=%d skipped=%d", stats.Registrable, stats.Exposed, stats.Shadow, stats.Blocked, stats.Skipped)
		if second := runtime.Refresh(ctx); second.Outcome != dynamic.RefreshNotModified {
			t.Fatalf("재조회 outcome=%s code=%s", second.Outcome, second.Code)
		}
	})

	st, ct := mcp.NewInMemoryTransports()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal("실연동 MCP 서버 연결 실패")
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "explicit-dynamic-verification", Version: "1"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal("공식 SDK 초기화 실패")
	}
	t.Cleanup(func() { _ = cs.Close() })

	t.Run("tools_list", func(t *testing.T) {
		listed, err := cs.ListTools(ctx, nil)
		if err != nil {
			t.Fatal("tools/list 실패")
		}
		dynamicCount := 0
		for _, tool := range listed.Tools {
			if tool.Meta["ableops/openapi"] != nil {
				dynamicCount++
			}
		}
		static := len(tools.StaticNames())
		t.Logf("tools=%d static=%d dynamic=%d published=%v", len(listed.Tools), static, dynamicCount, runtime.Published())
		if len(listed.Tools) != static+dynamicCount || dynamicCount != len(runtime.Published()) {
			t.Error("Static 도구와 게시한 Dynamic 도구 수가 tools/list와 다릅니다")
		}
	})

	cluster := strings.TrimSpace(os.Getenv("ABLEOPS_VERIFY_CLUSTER_ID"))
	topic := strings.TrimSpace(os.Getenv("ABLEOPS_VERIFY_TOPIC_NAME"))
	summaryArgs := map[string]any{}
	if cluster != "" {
		summaryArgs["clusterId"] = cluster
	}
	authenticated := os.Getenv("ABLEOPS_API_TOKEN") != ""
	calls := []struct {
		name string
		args map[string]any
		// public은 upstream이 미인증 공개로 선언한 Operation이다. 무효 토큰의 결과를 단정하지 않는다.
		public bool
		skip   string
	}{
		{name: "get_branding", args: map[string]any{}, public: true},
		{name: "get_event_summary", args: summaryArgs},
		{name: "get_topic_partitions", args: map[string]any{"id": cluster, "name": topic}},
	}
	if cluster == "" || topic == "" {
		calls[2].skip = "대상 미제공: ABLEOPS_VERIFY_CLUSTER_ID·ABLEOPS_VERIFY_TOPIC_NAME"
	}
	for _, call := range calls {
		t.Run("call_"+call.name, func(t *testing.T) {
			if call.skip != "" {
				t.Skip(call.skip)
			}
			out := invokeDynamic(t, ctx, cs, call.name, call.args, cfg.Token)
			switch {
			case !authenticated && !call.public:
				// 합성 무효 토큰도 Backend까지 전달되어 인증 실패로 돌아와야 한다.
				if out.Error == nil || out.Error.Code != "authentication_required" {
					t.Error("무효 토큰 호출이 인증 실패로 끝나지 않았습니다")
				}
			case out.Error != nil:
				t.Logf("result_code=%s http_status=%d", out.Error.Code, out.Error.HTTPStatus)
				if authenticated && call.public {
					t.Error("공개 Operation 호출이 실패했습니다")
				}
			default:
				if out.HTTPStatus != http.StatusOK || !json.Valid(out.Body) {
					t.Error("HTTP 200 원문 JSON이 아닙니다")
				}
			}
		})
	}
}

func invokeDynamic(t *testing.T, ctx context.Context, cs *mcp.ClientSession, name string, args map[string]any, token string) dynamic.Result {
	t.Helper()
	result, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil || result == nil || len(result.Content) != 1 {
		t.Fatal("공식 SDK Dynamic 도구 호출 실패; 원문은 생략합니다")
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok || strings.Contains(text.Text, token) {
		t.Fatal("Dynamic 결과 텍스트 또는 토큰 비노출 검사 실패")
	}
	var out dynamic.Result
	if json.Unmarshal([]byte(text.Text), &out) != nil || out.OperationID == "" {
		t.Fatal("Dynamic 결과 봉투 불일치")
	}
	if result.IsError != (out.Error != nil) {
		t.Error("MCP 오류 플래그와 결과 봉투 불일치")
	}
	t.Logf("operation=%s http_status=%d body_bytes=%d", out.OperationID, out.HTTPStatus, out.BodyBytes)
	return out
}
