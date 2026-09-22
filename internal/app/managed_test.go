package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/config"
)

// managedSecret은 mcpserver 의 서버간 비밀 하한(32바이트)을 넘는 테스트용 값이다.
const managedSecret = "0123456789abcdef0123456789abcdef0123456789abcdef"

// managedConfig는 Managed 모드가 조립하는 것과 같은 모양의 설정이다.
// (Extension 진입점이 만드는 값은 internal/extension 이 따로 검증한다.)
func managedConfig(t *testing.T, backendURL string) config.ServerConfig {
	t.Helper()
	cfg := serverConfig(t, backendURL)
	cfg.Transport = "http"
	cfg.HTTPAddress = "127.0.0.1:0"
	cfg.WebDelegation = config.WebDelegationConfig{Enabled: true, TTL: time.Minute}
	return cfg
}

// Managed 조립은 로컬 인증 저장소 없이 성공해야 한다 — AuthStore 경로가 비어 있어도 조립된다.
func TestNewWithOptionsManagedWithoutLocalAuthStore(t *testing.T) {
	backend := backendStub(t, nil)
	cfg := managedConfig(t, backend.URL)
	if cfg.AuthStore != "" {
		t.Fatal("Managed 설정에는 로컬 인증 저장소 경로가 없어야 합니다")
	}
	// 같은 설정을 standalone 으로 조립하면 로컬 인증 저장소가 없어 실패한다 —
	// 두 모드가 실제로 다른 경계를 조립한다는 뜻이다.
	if _, err := New(cfg, nil); err == nil {
		t.Fatal("standalone 조립은 로컬 인증 저장소 없이 성공하면 안 됩니다")
	}
	runtime, err := NewWithOptions(cfg, nil, Options{Managed: true, InternalSecret: managedSecret})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if runtime.HTTPOptions().Authenticate == nil {
		t.Fatal("Managed 모드도 요청별 인증기를 조립해야 합니다")
	}
	if len(runtime.HTTPOptions().AllowedOrigins) != 0 {
		t.Fatal("Managed 모드는 브라우저 Origin 을 허용하지 않아야 합니다")
	}
}

// Managed `/mcp` 는 위임 토큰만 받는다. 발급 → 호출 경로가 살아 있어야 한다.
func TestManagedDelegationBoundary(t *testing.T) {
	backend := backendStub(t, nil)
	runtime, err := NewWithOptions(managedConfig(t, backend.URL), nil, Options{Managed: true, InternalSecret: managedSecret})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handler, err := runtime.NewHTTPHandler()
	if err != nil {
		t.Fatal(err)
	}

	// 토큰 없는 /mcp 는 401 이다.
	code, _ := serve(handler, request(http.MethodPost, "/mcp", "{}", nil))
	if code != http.StatusUnauthorized {
		t.Fatalf("미인증 /mcp 상태 = %d", code)
	}
	// 위임 형식이 아닌 토큰도 401 이다(로컬 토큰 저장소로 새지 않는다).
	code, _ = serve(handler, request(http.MethodPost, "/mcp", "{}", map[string]string{"Authorization": "Bearer local-token-value"}))
	if code != http.StatusUnauthorized {
		t.Fatalf("로컬 형식 토큰 /mcp 상태 = %d", code)
	}
	// 비밀 없는 발급 요청은 401 이다.
	code, _ = serve(handler, request(http.MethodPost, "/internal/delegations", `{"backendToken":"synthetic-session"}`, nil))
	if code != http.StatusUnauthorized {
		t.Fatalf("비밀 없는 발급 상태 = %d", code)
	}

	code, body := serve(handler, request(http.MethodPost, "/internal/delegations", `{"backendToken":"synthetic-session"}`, map[string]string{
		"Content-Type":              "application/json",
		"X-AbleOps-Internal-Secret": managedSecret,
	}))
	if code != http.StatusCreated {
		t.Fatalf("발급 상태 = %d, 본문 = %s", code, body)
	}
	var grant struct {
		Token  string `json:"token"`
		UserID string `json:"userId"`
	}
	if err := json.Unmarshal([]byte(body), &grant); err != nil {
		t.Fatal(err)
	}
	if grant.UserID != "synthetic-user" || grant.Token == "" {
		t.Fatalf("발급 결과 = %+v", grant)
	}
	if strings.Contains(body, "synthetic-session") {
		t.Fatal("발급 응답에 Backend 세션이 되비치면 안 됩니다")
	}

	// 발급받은 위임으로는 /mcp 가 인증을 통과한다(401 이 아니다).
	code, _ = serve(handler, request(http.MethodPost, "/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, map[string]string{
		"Authorization": "Bearer " + grant.Token,
		"Content-Type":  "application/json",
		"Accept":        "application/json, text/event-stream",
	}))
	if code == http.StatusUnauthorized {
		t.Fatal("발급된 위임 토큰이 /mcp 인증을 통과하지 못했습니다")
	}
}

// Managed 조립은 계약을 만족하지 못하면 코드로 구분되는 실패를 돌려준다.
func TestManagedConfigurationFailures(t *testing.T) {
	backend := backendStub(t, nil)
	for _, tc := range []struct {
		name string
		cfg  func(config.ServerConfig) config.ServerConfig
		opts Options
	}{
		{
			name: "stdio 전송",
			cfg:  func(c config.ServerConfig) config.ServerConfig { c.Transport = "stdio"; return c },
			opts: Options{Managed: true, InternalSecret: managedSecret},
		},
		{
			name: "위임 발급 없음",
			cfg: func(c config.ServerConfig) config.ServerConfig {
				c.WebDelegation = config.WebDelegationConfig{}
				return c
			},
			opts: Options{Managed: true, InternalSecret: managedSecret},
		},
		{
			name: "내부 비밀 없음",
			cfg:  func(c config.ServerConfig) config.ServerConfig { return c },
			opts: Options{Managed: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewWithOptions(tc.cfg(managedConfig(t, backend.URL)), nil, tc.opts)
			var failure *Error
			if !errors.As(err, &failure) || failure.Code != CodeManagedConfiguration {
				t.Fatalf("오류 = %v", err)
			}
			if failure.Detail != "" {
				t.Fatal("Managed 설정 오류는 원문을 담지 않아야 합니다")
			}
		})
	}
}

// Options 제로값은 기존 New 와 같아야 한다 — standalone 조립 의미가 바뀌지 않았다는 확인이다.
func TestNewWithOptionsZeroMatchesNew(t *testing.T) {
	backend := backendStub(t, nil)
	cfg := serverConfig(t, backend.URL)
	cfg.Transport = "http"
	// 로컬 인증 저장소가 없으므로 두 경로 모두 같은 코드로 실패해야 한다.
	_, zeroErr := NewWithOptions(cfg, nil, Options{})
	_, newErr := New(cfg, nil)
	var zero, plain *Error
	if !errors.As(zeroErr, &zero) || !errors.As(newErr, &plain) || zero.Code != plain.Code {
		t.Fatalf("zero = %v, new = %v", zeroErr, newErr)
	}
}

func request(method, path, body string, headers map[string]string) *http.Request {
	r := httptest.NewRequest(method, "http://127.0.0.1:8081"+path, strings.NewReader(body))
	// loopback 피어로 맞춘다(httptest 기본값은 외부 대역이라 프록시 경유 방어에 걸린다).
	r.RemoteAddr = "127.0.0.1:54321"
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func serve(handler http.Handler, r *http.Request) (int, string) {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, r.WithContext(context.Background()))
	return recorder.Code, recorder.Body.String()
}
