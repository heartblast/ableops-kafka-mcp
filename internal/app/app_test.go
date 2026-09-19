package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/localauth"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

// backendStub은 세션 확인과 계약 조회만 응답하는 최소 Backend다. 실제 Kafka·DB에는 닿지 않는다.
func backendStub(t *testing.T, spec []byte) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/me":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"id":"synthetic-user","username":"synthetic-user"}`)
		case "/openapi.json":
			if spec == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(spec)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func serverConfig(t *testing.T, backendURL string) config.ServerConfig {
	t.Helper()
	base, err := url.Parse(backendURL)
	if err != nil {
		t.Fatal(err)
	}
	return config.ServerConfig{
		Backend:   config.Config{BaseURL: base, AllowHTTP: true, Timeout: time.Second, RequireRequestCredentials: true},
		Transport: "stdio",
	}
}

// authStore는 HTTP 전송이 요구하는 로컬 인증 저장소를 만들고 발급된 MCP 토큰을 돌려준다.
func authStore(t *testing.T, backendURL string) (string, string) {
	t.Helper()
	cfg := serverConfig(t, backendURL)
	runtime, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	dir := t.TempDir()
	storePath := filepath.Join(dir, "local.auth-store.json")
	out := filepath.Join(dir, "client.mcp-token")
	verify := func(ctx context.Context, token string) (string, error) {
		p, _ := requestctx.FromContext(ctx)
		p.BackendToken = token
		return runtime.Client().CurrentUserID(requestctx.WithPrincipal(ctx, p))
	}
	if _, err := localauth.Enroll(context.Background(), storePath, "synthetic-client", "synthetic-delegated", time.Hour, out, verify); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return storePath, strings.TrimSpace(string(raw))
}

// stdio 조립은 HTTP 인증 경계를 만들지 않으며 Close는 여러 번 불러도 안전해야 한다.
func TestNewStdioRuntime(t *testing.T) {
	backend := backendStub(t, nil)
	runtime, err := New(serverConfig(t, backend.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Server() == nil {
		t.Fatal("MCP 서버가 조립되지 않았습니다")
	}
	if runtime.Dynamic() != nil {
		t.Fatal("Dynamic 설정이 꺼져 있으면 런타임을 만들지 않아야 합니다")
	}
	if runtime.HTTPOptions().Authenticate != nil {
		t.Fatal("stdio 전송은 HTTP 인증기를 조립하지 않아야 합니다")
	}
	// Start는 Dynamic이 없으면 아무것도 하지 않고, Close는 멱등이어야 한다.
	runtime.Start(context.Background())
	runtime.Close()
	runtime.Close()
}

// Extension 진입점은 listener 없이 Handler만 받아 자체 수신 계층에 붙일 수 있어야 한다.
func TestNewHTTPHandlerWithoutListener(t *testing.T) {
	backend := backendStub(t, nil)
	storePath, token := authStore(t, backend.URL)
	cfg := serverConfig(t, backend.URL)
	cfg.Transport, cfg.AuthStore = "http", storePath
	runtime, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handler, err := runtime.NewHTTPHandler()
	if err != nil {
		t.Fatal(err)
	}
	health := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8081/healthz", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, health)
	if recorder.Code != http.StatusOK {
		t.Fatalf("healthz 상태 = %d", recorder.Code)
	}
	// 인증 경계는 그대로다 — 토큰 없는 /mcp 는 401 이다.
	anonymous := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8081/mcp", strings.NewReader("{}"))
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, anonymous)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("미인증 /mcp 상태 = %d", recorder.Code)
	}
	// 위임 발급을 켜지 않았으므로 서버간 경로는 존재하지 않는다(404).
	internal := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8081/internal/delegations", strings.NewReader("{}"))
	internal.Header.Set("Authorization", "Bearer "+token)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, internal)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("내부 발급 경로 상태 = %d", recorder.Code)
	}
}

// 조립 실패는 원문 대신 코드로 구분할 수 있어야 진입점이 안전하게 로그를 남긴다.
func TestNewReportsAuthStoreFailure(t *testing.T) {
	backend := backendStub(t, nil)
	cfg := serverConfig(t, backend.URL)
	cfg.Transport = "http"
	_, err := New(cfg, nil)
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != CodeAuthConfiguration {
		t.Fatalf("오류 = %v", err)
	}
	if failure.Detail != "" {
		t.Fatal("인증 설정 오류는 원문을 담지 않아야 합니다")
	}
}

func TestNewReportsBackendClientFailure(t *testing.T) {
	cfg := config.ServerConfig{Transport: "stdio"}
	_, err := New(cfg, nil)
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != CodeBackendClient {
		t.Fatalf("오류 = %v", err)
	}
}

// Dynamic lifecycle 은 외부 호출자가 제어한다 — Start 가 최초 적재를 끝내고 Close 가 주기 갱신을 멈춘다.
func TestStartLoadsDynamicContract(t *testing.T) {
	spec, err := os.ReadFile(filepath.Join("..", "openapi", "testdata", "ableops-openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	backend := backendStub(t, spec)
	cfg := serverConfig(t, backend.URL)
	cfg.Dynamic = config.DynamicConfig{Enabled: true, RefreshInterval: time.Hour}
	runtime, err := New(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Dynamic() == nil {
		t.Fatal("Dynamic 런타임이 조립되지 않았습니다")
	}
	if runtime.Dynamic().Current() != nil {
		t.Fatal("Start 전에는 계약을 조회하지 않아야 합니다")
	}
	runtime.Start(context.Background())
	if runtime.Dynamic().Current() == nil {
		t.Fatal("Start 는 전송 시작 전에 최초 적재를 끝내야 합니다")
	}
	// Promotion 된 도구는 Static 이름으로 남아 Published 가 비어 있을 수 있다.
	// 계약 적재 자체는 Registry 의 선택 목록으로 확인한다.
	if len(runtime.Dynamic().Current().Selected()) == 0 {
		t.Fatal("적재한 계약에서 선택된 도구가 만들어지지 않았습니다")
	}
	// Close 는 주기 갱신 goroutine 이 끝날 때까지 기다린다.
	runtime.Close()
	runtime.Close()
}
