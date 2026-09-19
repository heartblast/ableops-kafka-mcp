package extension

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
	extv1 "github.com/heartblast/ableops-sdk/extension/v1"
	"github.com/heartblast/ableops-sdk/extension/v1/extserver"
	"github.com/heartblast/ableops-sdk/extension/v1/testkit"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	syntheticCallToken = "synthetic-call-token"
	syntheticSession   = "synthetic-backend-session"
)

// backendStub은 세션 확인과 계약 조회만 응답하는 최소 AbleOps Backend 다.
// 실제 Kafka·DB 에는 닿지 않으며, 사용자 ID 는 테스트가 바꿀 수 있다(세션 교체·권한 회수 재현).
type backendStub struct {
	*httptest.Server
	userID atomic.Value // string
	spec   []byte
}

func newBackendStub(t *testing.T, spec []byte) *backendStub {
	t.Helper()
	stub := &backendStub{spec: spec}
	stub.userID.Store("synthetic-user")
	stub.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/me":
			if r.Header.Get("Authorization") != "Bearer "+syntheticSession {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			id, _ := stub.userID.Load().(string)
			fmt.Fprintf(w, `{"id":%q,"username":%q}`, id, id)
		case "/openapi.json":
			if stub.spec == nil {
				// Dynamic 최초 적재 실패를 재현한다 — Extension 은 Static 도구로 기동해야 한다.
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(stub.spec)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(stub.Close)
	return stub
}

// hostURL은 Core 가 Extension 에 넘기는 Host API 주소 모양이다.
func (b *backendStub) hostURL() string { return b.URL + "/api/extensions/_host" }

// runExtension은 SDK 런타임(extserver)으로 Extension 프로세스와 **같은 경로**를 띄우고
// 실제 loopback 주소를 돌려준다. listener 는 extserver 가 여는 하나뿐이다.
func runExtension(t *testing.T, backend *backendStub) string {
	t.Helper()
	t.Setenv(extserver.EnvExtensionID, ID)
	t.Setenv(extserver.EnvCallToken, syntheticCallToken)
	t.Setenv(extserver.EnvHostURL, backend.hostURL())
	t.Setenv(extserver.EnvHostToken, "synthetic-host-token")

	reader, writer := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- extserver.RunContext(ctx, New(), extserver.WithStdout(writer), extserver.WithLogWriter(io.Discard))
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Extension 이 종료 신호 후 10초 안에 멈추지 않았습니다")
		}
	})

	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		t.Fatalf("기동 핸드셰이크를 읽지 못했습니다: %v", err)
	}
	// 핸드셰이크 이후 stdout 은 쓰이지 않지만, 파이프가 막히지 않도록 비워 둔다.
	go func() { _, _ = io.Copy(io.Discard, reader) }()

	payload, ok := strings.CutPrefix(strings.TrimSpace(line), extserver.ReadyPrefix)
	if !ok {
		t.Fatalf("기동 핸드셰이크 형식이 아닙니다: %q", line)
	}
	var ready extserver.ReadyMessage
	if err := json.Unmarshal([]byte(payload), &ready); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ready.Addr, "127.0.0.1:") {
		t.Fatalf("리스닝 주소가 loopback 이 아닙니다: %q", ready.Addr)
	}
	return "http://" + ready.Addr
}

// call은 Extension 에 요청 1건을 보낸다. headers 로 호출 토큰·인증을 그대로 통제한다.
func call(t *testing.T, method, url, body string, headers map[string]string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(payload)
}

// issueDelegation은 Core 내부 호출자와 같은 방식으로 단기 위임을 발급받는다.
func issueDelegation(t *testing.T, base string, extra map[string]string) (int, string) {
	t.Helper()
	headers := map[string]string{
		extserver.HeaderCallToken: syntheticCallToken,
		"Content-Type":            "application/json",
	}
	for k, v := range extra {
		headers[k] = v
	}
	return call(t, http.MethodPost, base+delegationPath, fmt.Sprintf(`{"backendToken":%q}`, syntheticSession), headers)
}

// 호출 토큰이 없거나 틀리면 어떤 경로도 열리지 않는다 — Core↔Extension 인증이 1차 방어선이다.
func TestCallTokenIsTheFirstBoundary(t *testing.T) {
	base := runExtension(t, newBackendStub(t, nil))
	for _, tc := range []struct {
		name   string
		method string
		path   string
		token  string
	}{
		{name: "토큰 없는 MCP", method: http.MethodPost, path: mcpPath},
		{name: "잘못된 토큰의 MCP", method: http.MethodPost, path: mcpPath, token: "wrong-call-token"},
		{name: "토큰 없는 위임 발급", method: http.MethodPost, path: delegationPath},
		{name: "잘못된 토큰의 위임 발급", method: http.MethodPost, path: delegationPath, token: "wrong-call-token"},
		{name: "토큰 없는 헬스", method: http.MethodGet, path: "/health"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := map[string]string{}
			if tc.token != "" {
				headers[extserver.HeaderCallToken] = tc.token
			}
			code, body := call(t, tc.method, base+tc.path, "{}", headers)
			if code != http.StatusUnauthorized {
				t.Fatalf("상태 = %d, 본문 = %s", code, body)
			}
			if strings.Contains(body, syntheticCallToken) {
				t.Fatal("응답에 호출 토큰이 되비쳤습니다")
			}
		})
	}
}

// 외부가 내부 공유 비밀 헤더를 직접 넣어도 신뢰하지 않는다.
// 호출 토큰만 맞으면 발급되고, 틀린 비밀을 보내도 그 값이 판정에 쓰이지 않는다.
func TestInternalSecretFromOutsideIsNotTrusted(t *testing.T) {
	base := runExtension(t, newBackendStub(t, nil))

	// ① 비밀 헤더 없이 — 호출 토큰만으로 발급된다(Managed 모드는 별도 비밀 설정이 없다).
	code, body := issueDelegation(t, base, nil)
	if code != http.StatusCreated {
		t.Fatalf("비밀 없는 발급 상태 = %d, 본문 = %s", code, body)
	}
	// ② 공격자가 고른 비밀을 넣어도 결과가 같다 — 외부 값은 지워지고 프로세스 비밀이 주입된다.
	code, body = issueDelegation(t, base, map[string]string{internalSecretHeader: strings.Repeat("a", 64)})
	if code != http.StatusCreated {
		t.Fatalf("외부 비밀 주입 시 상태 = %d, 본문 = %s", code, body)
	}
	if strings.Contains(body, syntheticSession) || strings.Contains(strings.Repeat("a", 64), body) {
		t.Fatal("응답에 자격증명이 되비쳤습니다")
	}
}

// 발급 → `/mcp` 경로가 살아 있고, 사용자별 권한 검사가 유지되어야 한다.
func TestDelegationBoundaryAndUserMismatch(t *testing.T) {
	backend := newBackendStub(t, nil)
	base := runExtension(t, backend)

	// 위임 없는 /mcp 는 401 이다(호출 토큰만으로는 MCP 를 쓸 수 없다).
	code, _ := call(t, http.MethodPost, base+mcpPath, "{}", map[string]string{
		extserver.HeaderCallToken: syntheticCallToken,
		"Content-Type":            "application/json",
	})
	if code != http.StatusUnauthorized {
		t.Fatalf("위임 없는 /mcp 상태 = %d", code)
	}

	code, body := issueDelegation(t, base, nil)
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

	session := mcpSession(t, base, grant.Token)
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("위임 토큰으로 tools/list 실패: %v", err)
	}
	// Dynamic 최초 적재는 실패했지만(openapi 404) Static 도구로 기동해야 한다.
	if len(tools.Tools) == 0 {
		t.Fatal("Dynamic 적재 실패 시에도 Static 도구는 노출되어야 합니다")
	}

	// 같은 Backend 세션이 다른 사용자로 해석되면 위임을 폐기하고 거부한다.
	backend.userID.Store("other-user")
	if _, err := session.ListTools(context.Background(), nil); err == nil {
		t.Fatal("사용자 ID 가 바뀐 세션의 위임이 계속 통했습니다")
	}
}

// mcpSession은 Core 내부 호출자와 같은 헤더(호출 토큰 + 위임 Bearer)로 MCP 에 붙는다.
func mcpSession(t *testing.T, base, delegation string) *mcp.ClientSession {
	t.Helper()
	client := &http.Client{
		Transport: managedRoundTripper{delegation: delegation},
		Timeout:   10 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "managed-extension-test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: base + mcpPath, HTTPClient: client}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

type managedRoundTripper struct{ delegation string }

func (rt managedRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	clone := r.Clone(r.Context())
	clone.Header.Set(extserver.HeaderCallToken, syntheticCallToken)
	clone.Header.Set("Authorization", "Bearer "+rt.delegation)
	return http.DefaultTransport.RoundTrip(clone)
}

// Health 는 외부 호출 없이 빠르게 상태를 돌려주고, 기동 후에는 정상이어야 한다.
func TestHealthReportsRuntimeState(t *testing.T) {
	backend := newBackendStub(t, nil)
	base := runExtension(t, backend)
	code, body := call(t, http.MethodGet, base+"/health", "", map[string]string{
		extserver.HeaderCallToken: syntheticCallToken,
	})
	if code != http.StatusOK {
		t.Fatalf("헬스 상태 = %d, 본문 = %s", code, body)
	}
	var health extv1.Health
	if err := json.Unmarshal([]byte(body), &health); err != nil {
		t.Fatal(err)
	}
	if !health.OK || health.CheckedAt.IsZero() {
		t.Fatalf("health = %+v", health)
	}
	// 시작 전 인스턴스는 사유와 함께 비정상이어야 한다.
	before := New().Health(context.Background())
	if before.OK || strings.TrimSpace(before.Message) == "" {
		t.Fatalf("시작 전 health = %+v", before)
	}
}

// 생애주기(Start·Stop 멱등·Health)는 SDK 계약 검사기를 그대로 통과해야 한다.
func TestLifecycleSatisfiesContract(t *testing.T) {
	backend := newBackendStub(t, nil)
	t.Setenv(extserver.EnvExtensionID, ID)
	t.Setenv(extserver.EnvCallToken, syntheticCallToken)
	t.Setenv(extserver.EnvHostURL, backend.hostURL())
	t.Setenv(extserver.EnvHostToken, "synthetic-host-token")

	ext := New()
	host := testkit.NewHost(ID).Context()
	if issues := testkit.CheckExtension(ext, host); len(issues) > 0 {
		t.Fatalf("Extension 계약 위반: %s", strings.Join(issues, " / "))
	}
	// 정지 후에는 MCP 경로가 준비되지 않았음을 알린다(핸들러는 남아 있어도 Runtime 은 없다).
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:0"+mcpPath, strings.NewReader("{}"))
	ext.Routes()[0].Handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("정지 후 /mcp 상태 = %d", recorder.Code)
	}
}

// 다른 Extension 의 환경으로 이 바이너리가 뜨면 기동을 거부해야 한다.
func TestStartRejectsExtensionIDMismatch(t *testing.T) {
	backend := newBackendStub(t, nil)
	t.Setenv(extserver.EnvExtensionID, "some-other-extension")
	t.Setenv(extserver.EnvCallToken, syntheticCallToken)
	t.Setenv(extserver.EnvHostURL, backend.hostURL())
	t.Setenv(extserver.EnvHostToken, "synthetic-host-token")

	err := New().Start(context.Background(), testkit.NewHost(ID).Context())
	if err == nil {
		t.Fatal("확장 ID 가 다른 환경에서 기동했습니다")
	}
	if !strings.Contains(err.Error(), extserver.EnvExtensionID) {
		t.Fatalf("오류가 원인을 알려주지 않습니다: %v", err)
	}
}

// Core 환경변수 없이 직접 실행하면 안내와 함께 거부되어야 한다.
func TestStartRejectsDirectRun(t *testing.T) {
	for _, key := range []string{extserver.EnvExtensionID, extserver.EnvCallToken, extserver.EnvHostURL, extserver.EnvHostToken} {
		t.Setenv(key, "")
	}
	err := New().Start(context.Background(), testkit.NewHost(ID).Context())
	if err == nil || !strings.Contains(err.Error(), extserver.DirectRunMessage) {
		t.Fatalf("직접 실행 오류 = %v", err)
	}
}

// Dynamic 최초 적재 성패와 무관하게 Static 도구 전체가 노출되어야 한다.
//
// 계약을 읽으면 Dynamic 도구는 대부분 같은 이름의 Static 도구로 승격되므로 목록 크기는 같다 —
// 여기서 보는 것은 "적재가 실패해도 도구가 줄지 않는다"는 것이고, 그것이 Static fallback 계약이다.
func TestStaticToolsSurviveDynamicRefreshFailure(t *testing.T) {
	spec, err := os.ReadFile(filepath.Join("..", "openapi", "testdata", "ableops-openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	names := func(t *testing.T, spec []byte) map[string]bool {
		t.Helper()
		backend := newBackendStub(t, spec)
		base := runExtension(t, backend)
		code, body := issueDelegation(t, base, nil)
		if code != http.StatusCreated {
			t.Fatalf("발급 상태 = %d, 본문 = %s", code, body)
		}
		var grant struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal([]byte(body), &grant); err != nil {
			t.Fatal(err)
		}
		tools, err := mcpSession(t, base, grant.Token).ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		out := make(map[string]bool, len(tools.Tools))
		for _, tool := range tools.Tools {
			out[tool.Name] = true
		}
		return out
	}
	var failed, loaded map[string]bool
	t.Run("계약 조회 실패", func(t *testing.T) { failed = names(t, nil) })
	t.Run("계약 조회 성공", func(t *testing.T) { loaded = names(t, spec) })

	if len(failed) != len(tools.StaticNames()) {
		t.Fatalf("계약 실패 시 도구 수 = %d, Static 도구 수 = %d", len(failed), len(tools.StaticNames()))
	}
	for _, name := range tools.StaticNames() {
		if !failed[name] {
			t.Fatalf("계약 조회 실패 시 Static 도구 %q 가 사라졌습니다", name)
		}
		if !loaded[name] {
			t.Fatalf("계약 조회 성공 시 Static 도구 %q 가 사라졌습니다", name)
		}
	}
}
