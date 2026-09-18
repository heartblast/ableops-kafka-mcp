package mcpserver

// Web 세션 → 내부 발급 → `/mcp` → Backend REST 까지 **실제 전송을 통과시키는** 통합 시험이다.
// 가짜는 AbleOps Backend 하나뿐이고, 위임 저장소·HTTP 핸들러·MCP SDK 는 실제 구현을 쓴다.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/localauth"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
	"github.com/heartblast/ableops-kafka-mcp/internal/webdelegation"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 시험용 서버간 공유 비밀(32바이트 이상). 실제 비밀이 아니다.
const syntheticInternalSecret = "synthetic-internal-secret-0123456789"

// webSessionToken은 AbleOps Web 로그인 세션 토큰을 흉내 낸다(kadmin 의 Bearer 세션에 대응).
func webSessionToken(id string) string { return "synthetic-web-session-" + id }

// delegationFixture는 Backend·위임 저장소·HTTP 핸들러를 한 벌로 세운다.
type delegationFixture struct {
	server        *httptest.Server
	delegations   *webdelegation.Store
	localTokens   map[string]string
	logs          *lockedLogBuffer
	loggedOut     *atomic.Bool
	backendCalled *atomic.Int32
}

func newDelegationFixture(t *testing.T) *delegationFixture {
	t.Helper()
	var loggedOut atomic.Bool
	var backendCalled atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalled.Add(1)
		bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		id := ""
		switch bearer {
		case webSessionToken("a"):
			id = "a"
		case webSessionToken("b"):
			id = "b"
		case "synthetic-local-session-c":
			id = "c"
		default:
			// 위임 토큰이 Backend 로 새는 경로가 있으면 여기서 드러난다.
			if strings.HasPrefix(bearer, webdelegation.TokenPrefix) || strings.HasPrefix(bearer, "ableops_mcp_") {
				t.Error("MCP 토큰이 Backend 로 전달됐다")
			}
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if loggedOut.Load() && id == "b" {
			w.WriteHeader(http.StatusUnauthorized) // 로그아웃된 세션
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/me":
			fmt.Fprintf(w, `{"id":%q}`, id)
		case "/api/clusters":
			fmt.Fprintf(w, `[{"id":%q,"name":%q}]`, id, id)
		case "/api/clusters/" + id + "/topics":
			fmt.Fprintf(w, `{"clusterId":%q,"syncedAt":null,"items":[]}`, id)
		default:
			w.WriteHeader(http.StatusForbidden) // 남의 클러스터
		}
	}))
	t.Cleanup(backend.Close)

	base, _ := url.Parse(backend.URL)
	rest, err := ableops.NewClient(config.Config{BaseURL: base, AllowHTTP: true, Timeout: 2 * time.Second, RequireRequestCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rest.CloseIdleConnections)
	verify := func(ctx context.Context, token string) (string, error) {
		p, _ := requestctx.FromContext(ctx)
		p.BackendToken = token
		return rest.CurrentUserID(requestctx.WithPrincipal(ctx, p))
	}

	// 기존 localauth 경로도 같은 핸들러에 살려 둔다 — 위임은 교체가 아니라 추가다.
	dir := t.TempDir()
	storePath := filepath.Join(dir, "local.auth-store.json")
	tokenOut := filepath.Join(dir, "c.mcp-token")
	if _, err := localauth.Enroll(context.Background(), storePath, "c", "synthetic-local-session-c", time.Hour, tokenOut, verify); err != nil {
		t.Fatal(err)
	}
	rawToken, err := os.ReadFile(tokenOut)
	if err != nil {
		t.Fatal(err)
	}
	local, err := localauth.NewStore(storePath, verify)
	if err != nil {
		t.Fatal(err)
	}
	delegations, err := webdelegation.NewStore(verify, webdelegation.DefaultTTL)
	if err != nil {
		t.Fatal(err)
	}

	logs := &lockedLogBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	handler, err := NewHTTPHandler(New(rest, logger), HTTPOptions{
		Authenticate: func(ctx context.Context, token string) (requestctx.Principal, error) {
			if webdelegation.ValidToken(token) {
				return delegations.Authenticate(ctx, token)
			}
			return local.Authenticate(ctx, token)
		},
		InternalSecret: syntheticInternalSecret,
		IssueDelegation: func(ctx context.Context, backendToken string) (DelegationGrant, error) {
			grant, err := delegations.Issue(ctx, backendToken)
			return DelegationGrant{Token: grant.Token, UserID: grant.UserID, ExpiresAt: grant.ExpiresAt}, err
		},
		AllowedOrigins: []string{"http://localhost:5173"},
		Logger:         logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &delegationFixture{
		server:        server,
		delegations:   delegations,
		localTokens:   map[string]string{"c": strings.TrimSpace(string(rawToken))},
		logs:          logs,
		loggedOut:     &loggedOut,
		backendCalled: &backendCalled,
	}
}

type issuedDelegation struct {
	Token     string `json:"token"`
	UserID    string `json:"userId"`
	ExpiresAt string `json:"expiresAt"`
}

// issue는 서버간 발급 엔드포인트를 호출한다. header 로 요청 헤더를 덧붙일 수 있다.
func (f *delegationFixture) issue(t *testing.T, body string, header http.Header) (*http.Response, issuedDelegation) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, f.server.URL+internalDelegationPath, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(internalSecretHeader, syntheticInternalSecret)
	for name, values := range header {
		request.Header.Del(name)
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	var decoded issuedDelegation
	if response.StatusCode == http.StatusCreated {
		if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
			t.Fatalf("발급 응답 해석 실패: %v", err)
		}
	}
	return response, decoded
}

func (f *delegationFixture) issueFor(t *testing.T, id string) issuedDelegation {
	t.Helper()
	response, grant := f.issue(t, fmt.Sprintf(`{"backendToken":%q}`, webSessionToken(id)), nil)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("%s 위임 발급 실패: %d", id, response.StatusCode)
	}
	return grant
}

// 완료 기준 그대로의 E2E: Web 세션 A/B → 위임 → /mcp → Backend 권한 결과.
func TestWebDelegationEndToEndUserIsolation(t *testing.T) {
	f := newDelegationFixture(t)

	grants := map[string]issuedDelegation{}
	for _, id := range []string{"a", "b"} {
		grant := f.issueFor(t, id)
		if grant.UserID != id {
			t.Fatalf("발급 사용자 불일치: got %q want %q", grant.UserID, id)
		}
		if grant.Token == webSessionToken(id) || !strings.HasPrefix(grant.Token, webdelegation.TokenPrefix) {
			t.Fatal("위임 토큰이 Backend 세션 토큰과 구분되지 않는다")
		}
		if grant.ExpiresAt == "" {
			t.Fatal("만료 시각 메타데이터 누락")
		}
		grants[id] = grant
	}
	if grants["a"].Token == grants["b"].Token {
		t.Fatal("두 사용자에게 같은 위임 토큰이 발급됐다")
	}

	sessions := map[string]*mcp.ClientSession{
		"a": sdkHTTPSession(t, f.server, grants["a"].Token),
		"b": sdkHTTPSession(t, f.server, grants["b"].Token),
	}
	// 동시 호출에서도 A/B 결과가 섞이지 않아야 한다.
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for range 10 {
				result, err := sessions[id].CallTool(context.Background(), &mcp.CallToolParams{Name: "list_clusters"})
				if err != nil || result.IsError {
					t.Error("클러스터 조회 실패")
					return
				}
				raw, _ := json.Marshal(result.StructuredContent)
				var decoded struct {
					Data struct {
						Items []struct {
							ID string `json:"id"`
						} `json:"items"`
					} `json:"data"`
				}
				if json.Unmarshal(raw, &decoded) != nil || len(decoded.Data.Items) != 1 || decoded.Data.Items[0].ID != id {
					t.Error("사용자별 결과 혼합")
				}
			}
		}(id)
	}
	wg.Wait()

	// A 의 위임으로 B 의 자원에 닿지 못한다(교차 사용 차단).
	for _, tc := range []struct{ caller, cluster, want string }{
		{"a", "a", `"status":"partial"`},
		{"a", "b", `"code":"access_denied"`},
		{"b", "a", `"code":"access_denied"`},
		{"b", "b", `"status":"partial"`},
	} {
		result, err := sessions[tc.caller].CallTool(context.Background(), &mcp.CallToolParams{
			Name: "list_topics", Arguments: map[string]any{"cluster_id": tc.cluster}})
		if err != nil {
			t.Fatalf("%s→%s 호출 실패: %v", tc.caller, tc.cluster, err)
		}
		raw, _ := json.Marshal(result.StructuredContent)
		if !strings.Contains(string(raw), tc.want) {
			t.Fatalf("%s→%s 경계 위반: %s", tc.caller, tc.cluster, string(raw))
		}
	}

	// 기존 localauth 클라이언트도 같은 핸들러에서 계속 동작한다(회귀 방지).
	localSession := sdkHTTPSession(t, f.server, f.localTokens["c"])
	if _, err := localSession.ListTools(context.Background(), nil); err != nil {
		t.Fatalf("기존 localauth 경로 회귀: %v", err)
	}

	// Logout → 다음 MCP 요청 실패 + 위임 자동 제거. A 는 영향받지 않는다.
	f.loggedOut.Store(true)
	if _, err := sessions["b"].ListTools(context.Background(), nil); err == nil {
		t.Fatal("로그아웃 후에도 기존 위임이 사용됐다")
	}
	if _, err := sessions["a"].ListTools(context.Background(), nil); err != nil {
		t.Fatalf("다른 사용자의 로그아웃이 영향을 줬다: %v", err)
	}
	if f.delegations.Len() != 1 {
		t.Fatalf("무효해진 위임이 저장소에 남았다: %d", f.delegations.Len())
	}

	// 자격증명은 어떤 형태로도 로그에 남지 않는다.
	for _, secret := range []string{grants["a"].Token, grants["b"].Token, webSessionToken("a"), webSessionToken("b"), syntheticInternalSecret, f.localTokens["c"]} {
		if strings.Contains(f.logs.String(), secret) {
			t.Fatal("자격증명이 로그에 노출됐다")
		}
	}
}

// 내부 발급 엔드포인트의 접근 통제. 실패 응답에 입력이 되비치지 않는 것까지 본다.
func TestInternalDelegationEndpointAccessControl(t *testing.T) {
	f := newDelegationFixture(t)
	valid := fmt.Sprintf(`{"backendToken":%q}`, webSessionToken("a"))

	for _, tc := range []struct {
		name   string
		body   string
		header http.Header
		status int
	}{
		{"정상", valid, nil, http.StatusCreated},
		{"공유 비밀 불일치", valid, http.Header{internalSecretHeader: {"wrong-secret-0123456789012345678"}}, http.StatusUnauthorized},
		{"공유 비밀 누락", valid, http.Header{internalSecretHeader: nil}, http.StatusUnauthorized},
		{"공유 비밀 중복 전달", valid, http.Header{internalSecretHeader: {syntheticInternalSecret, syntheticInternalSecret}}, http.StatusUnauthorized},
		{"브라우저 Origin", valid, http.Header{"Origin": {"http://localhost:5173"}}, http.StatusForbidden},
		{"브라우저 Sec-Fetch-Site", valid, http.Header{"Sec-Fetch-Site": {"same-origin"}}, http.StatusForbidden},
		{"브라우저 Cookie", valid, http.Header{"Cookie": {"session=x"}}, http.StatusForbidden},
		{"브라우저 Referer", valid, http.Header{"Referer": {"http://localhost:5173/"}}, http.StatusForbidden},
		{"알 수 없는 필드", `{"backendToken":"x","extra":1}`, nil, http.StatusBadRequest},
		{"빈 본문", `{}`, nil, http.StatusBadRequest},
		{"Backend 세션 무효", `{"backendToken":"synthetic-unknown-session"}`, nil, http.StatusUnauthorized},
		{"위임 토큰 되먹임", fmt.Sprintf(`{"backendToken":%q}`, webdelegation.TokenPrefix+"AAAA"), nil, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, _ := f.issue(t, tc.body, tc.header)
			if response.StatusCode != tc.status {
				t.Fatalf("상태 코드: got %d want %d", response.StatusCode, tc.status)
			}
			// 어떤 실패에도 CORS 헤더를 붙이지 않는다 — 붙는 순간 브라우저가 응답을 읽을 수 있다.
			if response.Header.Get("Access-Control-Allow-Origin") != "" {
				t.Fatal("내부 엔드포인트가 CORS 응답을 냈다")
			}
		})
	}

	// 브라우저 preflight 로도 열리지 않는다.
	for _, method := range []string{http.MethodGet, http.MethodOptions, http.MethodDelete} {
		request, _ := http.NewRequest(method, f.server.URL+internalDelegationPath, nil)
		request.Header.Set(internalSecretHeader, syntheticInternalSecret)
		response, err := f.server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s 가 거부되지 않았다: %d", method, response.StatusCode)
		}
	}

	// 알 수 없는 내부 경로는 /mcp 규칙으로 새지 않고 404 다.
	request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/internal/other", strings.NewReader(valid))
	request.Header.Set(internalSecretHeader, syntheticInternalSecret)
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("알 수 없는 내부 경로: %d", response.StatusCode)
	}
}

// 배선하지 않은 설치에서는 경로 자체가 존재하지 않아야 한다.
func TestInternalDelegationDisabledByDefault(t *testing.T) {
	handler, err := NewHTTPHandler(New(nil, nil), HTTPOptions{
		Authenticate: func(context.Context, string) (requestctx.Principal, error) {
			return requestctx.Principal{}, &localauth.Error{Code: "authentication_required"}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+internalDelegationPath, strings.NewReader(`{"backendToken":"x"}`))
	request.Header.Set(internalSecretHeader, syntheticInternalSecret)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("비활성 설치에서 내부 경로가 응답했다: %d", response.StatusCode)
	}
}

// 반쪽 배선은 기동 시점에 거부한다.
func TestInternalDelegationRequiresBothSecretAndIssuer(t *testing.T) {
	authenticate := func(context.Context, string) (requestctx.Principal, error) {
		return requestctx.Principal{}, &localauth.Error{Code: "authentication_required"}
	}
	issue := func(context.Context, string) (DelegationGrant, error) { return DelegationGrant{}, nil }
	for _, tc := range []struct {
		name   string
		opts   HTTPOptions
		wantOK bool
	}{
		{"비밀만", HTTPOptions{Authenticate: authenticate, InternalSecret: syntheticInternalSecret}, false},
		{"발급기만", HTTPOptions{Authenticate: authenticate, IssueDelegation: issue}, false},
		{"짧은 비밀", HTTPOptions{Authenticate: authenticate, InternalSecret: "short", IssueDelegation: issue}, false},
		{"정상", HTTPOptions{Authenticate: authenticate, InternalSecret: syntheticInternalSecret, IssueDelegation: issue}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewHTTPHandler(New(nil, nil), tc.opts)
			if (err == nil) != tc.wantOK {
				t.Fatalf("배선 검증: %v", err)
			}
		})
	}
}

// 위임 토큰을 도구 인자에 실어 보내는 우회는 기존 가드가 막아야 한다.
func TestDelegationTokenRejectedInToolArguments(t *testing.T) {
	f := newDelegationFixture(t)
	grant := f.issueFor(t, "a")
	session := sdkHTTPSession(t, f.server, grant.Token)
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "list_topics", Arguments: map[string]any{"cluster_id": grant.Token}}); err == nil {
		t.Fatal("도구 인자에 실린 위임 토큰이 통과했다")
	}
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "list_topics", Arguments: map[string]any{"cluster_id": webSessionToken("a")}}); err == nil {
		t.Fatal("도구 인자에 실린 Backend 세션 토큰이 통과했다")
	}
}
