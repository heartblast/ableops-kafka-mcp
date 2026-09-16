// Package integration_test는 명시적으로 선택한 경우에만 실제 백엔드를 조회한다.
package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/mcpserver"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const invalidSession = "synthetic-invalid-backend-session-for-explicit-verification"

// TestBackendLive는 실제 값과 응답을 출력하지 않고 시나리오와 안전한 결과만 남긴다.
// 무효 합성 토큰은 실제 만료 토큰 검증을 대체하지 않는다.
func TestBackendLive(t *testing.T) {
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
	probeBackend(t, cfg)

	t.Run("invalid_synthetic_token", func(t *testing.T) {
		invalid := cfg
		invalid.Token = invalidSession
		cs := connectBackend(t, invalid)
		out := invoke[tools.ListData[ableops.Cluster]](t, cs, "list_clusters", nil, invalidSession)
		requireFailure(t, out, "authentication_required", http.StatusUnauthorized)
	})
	t.Run("expired_real_token", func(t *testing.T) {
		token := os.Getenv("ABLEOPS_VERIFY_EXPIRED_TOKEN")
		if token == "" {
			t.Skip("실제 만료 토큰 미제공: ABLEOPS_VERIFY_EXPIRED_TOKEN")
		}
		if token == invalidSession || token == os.Getenv("ABLEOPS_API_TOKEN") {
			t.Fatal("만료 검증에는 현재 토큰이나 합성 무효 토큰과 다른 실제 만료 토큰이 필요합니다")
		}
		expired := cfg
		expired.Token = token
		cs := connectBackend(t, expired)
		out := invoke[tools.ListData[ableops.Cluster]](t, cs, "list_clusters", nil, token)
		requireFailure(t, out, "authentication_required", http.StatusUnauthorized)
	})
	t.Run("authenticated_tools", func(t *testing.T) {
		if os.Getenv("ABLEOPS_API_TOKEN") == "" {
			t.Skip("실제 사용자 토큰 미제공: ABLEOPS_API_TOKEN; 조회 도구 및 권한 시나리오 미검증")
		}
		cs := connectBackend(t, cfg)
		t.Run("tool_inventory", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
			defer cancel()
			inventory, err := cs.ListTools(ctx, nil)
			if err != nil || inventory == nil || len(inventory.Tools) != 33 {
				t.Fatal("공식 SDK 도구 목록 조회 실패")
			}
			for _, tool := range inventory.Tools {
				if tool.Annotations == nil || tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint {
					t.Fatal("비파괴 도구 속성 누락")
				}
			}
		})
		t.Run("list_clusters", func(t *testing.T) {
			out := invoke[tools.ListData[ableops.Cluster]](t, cs, "list_clusters", map[string]any{"limit": 10}, cfg.Token)
			if out.Status != "ok" || out.Data == nil {
				t.Fatal("제공한 사용자 토큰으로 클러스터 목록 조회에 성공하지 못했습니다")
			}
			checkList(t, len(out.Data.Items), out.Data.Returned, out.Data.Received, out.Truncated)
			t.Logf("empty=%t", len(out.Data.Items) == 0)
		})
		cluster := strings.TrimSpace(os.Getenv("ABLEOPS_VERIFY_CLUSTER_ID"))
		t.Run("analysis_tools", func(t *testing.T) { verifyAnalysisTools(t, cs, cfg.Token, cluster) })
		args := map[string]any{"cluster_id": cluster, "limit": 10}
		t.Run("get_cluster_health", func(t *testing.T) {
			requireCluster(t, cluster)
			out := invoke[tools.HealthData](t, cs, "get_cluster_health", args, cfg.Token)
			checkScope(t, out.ClusterID, cluster)
			if out.Data == nil {
				return
			}
			if connectivity := out.Data.Connectivity; connectivity != nil {
				checkScope(t, connectivity.ClusterID, cluster)
				if (!connectivity.Reachable || connectivity.Error != "") && out.Status == "ok" {
					t.Error("HTTP 200 연결 실패가 정상 결과로 변경되었습니다")
				}
			}
			if partitions := out.Data.Partitions; partitions != nil {
				checkObservationTime(t, partitions.CheckedAt)
				if (partitions.Status == "FORBIDDEN" || partitions.Status == "UNAVAILABLE" || partitions.Forbidden > 0 || partitions.Unavailable > 0) && out.Status == "ok" {
					t.Error("파티션 업무 실패가 정상 결과로 변경되었습니다")
				}
				if partitions.Truncated && !out.Truncated {
					t.Error("백엔드 잘림 표시가 누락되었습니다")
				}
			}
		})
		t.Run("list_topics", func(t *testing.T) {
			requireCluster(t, cluster)
			out := invoke[tools.SnapshotData[ableops.Topic]](t, cs, "list_topics", args, cfg.Token)
			checkSnapshot(t, out, cluster)
		})
		t.Run("list_consumer_groups", func(t *testing.T) {
			requireCluster(t, cluster)
			out := invoke[tools.SnapshotData[ableops.ConsumerGroup]](t, cs, "list_consumer_groups", args, cfg.Token)
			checkSnapshot(t, out, cluster)
		})
		t.Run("get_consumer_group_lag", func(t *testing.T) {
			requireCluster(t, cluster)
			group := strings.TrimSpace(os.Getenv("ABLEOPS_VERIFY_GROUP_NAME"))
			if group == "" {
				t.Skip("실제 그룹 미제공: ABLEOPS_VERIFY_GROUP_NAME")
			}
			out := invoke[ableops.ConsumerGroupLagView](t, cs, "get_consumer_group_lag", map[string]any{"cluster_id": cluster, "group_name": group, "limit": 10}, cfg.Token)
			checkScope(t, out.ClusterID, cluster)
			if out.Data != nil {
				checkScope(t, out.Data.Group, group)
				checkObservationTime(t, out.Data.CheckedAt)
				if (!out.Data.Found || out.Data.Status == "FORBIDDEN" || out.Data.Status == "UNAVAILABLE" || out.Data.ErrorPartitions > 0) && out.Status == "ok" {
					t.Error("Lag 업무 실패가 정상 결과로 변경되었습니다")
				}
				t.Logf("empty=%t", len(out.Data.Partitions) == 0)
			}
		})
		t.Run("list_cluster_events", func(t *testing.T) {
			requireCluster(t, cluster)
			out := invoke[tools.EventData](t, cs, "list_cluster_events", map[string]any{"cluster_id": cluster, "page": 1, "page_size": 10}, cfg.Token)
			checkScope(t, out.ClusterID, cluster)
			if out.Data == nil {
				return
			}
			if out.Data.Page != 1 || out.Data.PageSize != 10 || out.Data.Returned != len(out.Data.Items) {
				t.Error("이벤트 페이지 계약 불일치")
			}
			for _, event := range out.Data.Items {
				checkScope(t, event.ClusterID, cluster)
			}
			t.Logf("empty=%t", len(out.Data.Items) == 0)
		})
		t.Run("denied_cluster", func(t *testing.T) {
			denied := optionalTarget(t, "ABLEOPS_VERIFY_DENIED_CLUSTER_ID")
			out := invoke[tools.SnapshotData[ableops.Topic]](t, cs, "list_topics", map[string]any{"cluster_id": denied}, cfg.Token)
			checkScope(t, out.ClusterID, denied)
			requireFailure(t, out, "access_denied", http.StatusForbidden)
		})
		t.Run("missing_cluster", func(t *testing.T) {
			missing := optionalTarget(t, "ABLEOPS_VERIFY_MISSING_CLUSTER_ID")
			out := invoke[tools.HealthData](t, cs, "get_cluster_health", map[string]any{"cluster_id": missing}, cfg.Token)
			checkScope(t, out.ClusterID, missing)
			// 로컬 계약은 미등록 ID에 일관된 404를 보장하지 않는다.
			if out.Status == "ok" {
				t.Error("없는 클러스터의 조회 실패를 확인하지 못했습니다")
			}
		})
		t.Run("missing_group", func(t *testing.T) {
			requireCluster(t, cluster)
			missing := optionalTarget(t, "ABLEOPS_VERIFY_MISSING_GROUP_NAME")
			out := invoke[ableops.ConsumerGroupLagView](t, cs, "get_consumer_group_lag", map[string]any{"cluster_id": cluster, "group_name": missing}, cfg.Token)
			checkScope(t, out.ClusterID, cluster)
			requireFailure(t, out, "not_found", http.StatusOK)
			if out.Data == nil || out.Data.Found || out.Data.Code != "NOT_FOUND" {
				t.Error("없는 그룹의 HTTP 200 업무 실패가 보존되지 않았습니다")
			}
		})
	})
}

func probeBackend(t *testing.T, cfg config.Config) {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	transport.ResponseHeaderTimeout = cfg.Timeout
	transport.MaxResponseHeaderBytes = 64 << 10
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			t.Fatal("상태 확인용 CA 파일 읽기 실패")
		}
		pool, _ := x509.SystemCertPool()
		if pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			t.Fatal("상태 확인용 CA 형식 오류")
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, path := range []string{"/healthz", "/readyz"} {
		t.Run(path[1:], func(t *testing.T) {
			u := *cfg.BaseURL
			u.Path = path
			response, err := client.Get(u.String())
			if err != nil {
				t.Fatal("백엔드 상태 확인 연결 실패")
			}
			_ = response.Body.Close()
			t.Logf("http_status=%d", response.StatusCode)
			if response.StatusCode != http.StatusOK {
				t.Error("백엔드 상태 경로가 HTTP 200을 반환하지 않았습니다")
			}
		})
	}
}

func connectBackend(t *testing.T, cfg config.Config) *mcp.ClientSession {
	t.Helper()
	api, err := ableops.NewClient(cfg)
	if err != nil {
		t.Fatal("실연동 REST 클라이언트 설정 오류")
	}
	t.Cleanup(api.CloseIdleConnections)
	// 실제 클러스터 식별자도 검증 로그에 남기지 않는다.
	server := mcpserver.New(api, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	st, ct := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal("실연동 MCP 서버 연결 실패")
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "explicit-backend-verification", Version: "1"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal("공식 SDK 초기화 실패")
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func invoke[T any](t *testing.T, cs *mcp.ClientSession, name string, args map[string]any, token string) tools.Envelope[T] {
	t.Helper()
	if args == nil {
		args = map[string]any{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.MaxTimeout+5*time.Second)
	defer cancel()
	result, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil || result == nil {
		t.Fatal("공식 SDK 도구 호출 실패; 원문은 민감정보 보호를 위해 생략합니다")
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil || strings.Contains(string(raw), token) {
		t.Fatal("MCP 결과 인코딩 또는 토큰 비노출 검사 실패")
	}
	var out tools.Envelope[T]
	if json.Unmarshal(raw, &out) != nil {
		t.Fatal("공개 DTO와 MCP 결과 구조 불일치")
	}
	if out.Status != "ok" && out.Status != "partial" && out.Status != "error" {
		t.Fatal("MCP 결과 상태 계약 불일치")
	}
	if result.IsError != (out.Status == "error") {
		t.Error("MCP 오류 플래그와 업무 결과 상태 불일치")
	}
	checkObservationTime(t, out.QueriedAt)
	if len(result.Content) != 1 {
		t.Fatal("MCP 호환 텍스트 개수 불일치")
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok || strings.Contains(text.Text, token) {
		t.Fatal("MCP 호환 텍스트 또는 토큰 비노출 검사 실패")
	}
	var textValue, structuredValue any
	if json.Unmarshal([]byte(text.Text), &textValue) != nil || json.Unmarshal(raw, &structuredValue) != nil || !reflect.DeepEqual(textValue, structuredValue) {
		t.Error("MCP 텍스트와 구조화 결과 불일치")
	}
	t.Logf("status=%s truncated=%t", out.Status, out.Truncated)
	for _, failure := range out.Errors {
		switch failure.Code {
		case "authentication_required", "access_denied", "not_found", "rate_limited", "backend_unavailable", "timeout", "canceled", "response_too_large", "redirect_blocked", "backend_error", "metadata_unavailable", "partial_failure", "backend_partial_failure", "unsupported", "output_too_large", "call_limit_exceeded":
			t.Logf("result_code=%s http_status=%d", failure.Code, failure.HTTPStatus)
		default:
			t.Error("DTO/입력 계약 또는 미등록 결과 코드 오류; 원문은 생략합니다")
		}
	}
	return out
}

func checkScope(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Error("요청 대상과 결과 대상 불일치; 실제 식별자는 생략합니다")
	}
}

func checkObservationTime(t *testing.T, value string) {
	t.Helper()
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		t.Error("관측 또는 조회 시각 형식 오류; 실제 값은 생략합니다")
	}
}

func checkList(t *testing.T, length, returned, received int, truncated bool) {
	t.Helper()
	if length != returned || returned > received || (returned < received && !truncated) {
		t.Error("목록 수와 잘림 표시 계약 불일치")
	}
}

func checkSnapshot[T any](t *testing.T, out tools.Envelope[tools.SnapshotData[T]], cluster string) {
	t.Helper()
	checkScope(t, out.ClusterID, cluster)
	if out.Data == nil {
		return
	}
	checkList(t, len(out.Data.Items), out.Data.Returned, out.Data.Received, out.Truncated)
	if out.Data.SyncedAt == nil {
		if out.Status != "partial" || len(out.Limitations) == 0 {
			t.Error("동기화 시각 없는 스냅샷의 불완전성이 보존되지 않았습니다")
		}
	} else {
		checkObservationTime(t, *out.Data.SyncedAt)
	}
	t.Logf("empty=%t snapshot_time_present=%t", len(out.Data.Items) == 0, out.Data.SyncedAt != nil)
}

func requireCluster(t *testing.T, cluster string) {
	t.Helper()
	if cluster == "" {
		t.Skip("실제 대상 클러스터 미제공: ABLEOPS_VERIFY_CLUSTER_ID")
	}
}

func optionalTarget(t *testing.T, key string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		t.Skip("시나리오 대상 미제공: " + key)
	}
	return value
}

func requireFailure[T any](t *testing.T, out tools.Envelope[T], code string, status int) {
	t.Helper()
	if out.Status != "error" {
		t.Error("예상한 실패 상태를 반환하지 않았습니다")
	}
	for _, failure := range out.Errors {
		if failure.Code == code && failure.HTTPStatus == status {
			return
		}
	}
	t.Error("예상한 안전한 오류 분류/HTTP 상태를 반환하지 않았습니다")
}
