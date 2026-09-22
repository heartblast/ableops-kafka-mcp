package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const syntheticMCPToken = "synthetic-mcp-token-a"

// loopbackPeerAddr는 loopback 피어를 흉내 낸다. httptest.NewRequest 의 기본 RemoteAddr 는
// 문서용 외부 대역이라 프록시 경유 방어에 걸린다.
const loopbackPeerAddr = "127.0.0.1:54321"
const syntheticBackendToken = "synthetic-backend-token-a"

func httpTestOptions() HTTPOptions {
	return HTTPOptions{Authenticate: func(ctx context.Context, token string) (requestctx.Principal, error) {
		if token != syntheticMCPToken && token != "synthetic-mcp-token-b" {
			return requestctx.Principal{}, errors.New("synthetic-private-auth-error")
		}
		user := "user-a"
		if token != syntheticMCPToken {
			user = "user-b"
		}
		return requestctx.Principal{UserID: user, ClientID: "test-client", BackendToken: syntheticBackendToken}, nil
	}}
}

func emptyMCPServer() *mcp.Server {
	return mcp.NewServer(&mcp.Implementation{Name: "http-test", Version: "1"}, nil)
}

func postRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8081/mcp", strings.NewReader(body))
	// 피어는 loopback 이어야 한다 — httptest 기본값(192.0.2.1)은 외부 피어로 거부된다.
	r.RemoteAddr = loopbackPeerAddr
	r.Header.Set("Authorization", "Bearer "+syntheticMCPToken)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	r.Header.Set("MCP-Protocol-Version", "2025-11-25")
	return r
}

const pingPayload = `{"jsonrpc":"2.0","id":1,"method":"ping"}`
const toolPayload = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wait","arguments":{}}}`

func TestHTTPAuthenticationOriginAndLimits(t *testing.T) {
	var logs bytes.Buffer
	opts := httpTestOptions()
	opts.AllowedOrigins = []string{"http://localhost:3000"}
	opts.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	handler, err := NewHTTPHandler(emptyMCPServer(), opts)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		modify func(*http.Request)
		status int
	}{
		{"인증_없음", func(r *http.Request) { r.Header.Del("Authorization") }, 401},
		{"무효_토큰", func(r *http.Request) { r.Header.Set("Authorization", "Bearer invalid-token") }, 401},
		{"중복_인증", func(r *http.Request) { r.Header.Add("Authorization", "Bearer duplicate") }, 401},
		{"Origin_차단", func(r *http.Request) { r.Header.Set("Origin", "http://evil.example") }, 403},
		{"Origin_null", func(r *http.Request) { r.Header.Set("Origin", "null") }, 403},
		{"Origin_빈값", func(r *http.Request) { r.Header.Set("Origin", "") }, 403},
		{"Origin_중복", func(r *http.Request) {
			r.Header.Add("Origin", "http://localhost:3000")
			r.Header.Add("Origin", "http://localhost:3000")
		}, 403},
		{"Host_차단", func(r *http.Request) { r.Host = "evil.example:8081" }, 403},
		{"인증된_GET", func(r *http.Request) { r.Method = http.MethodGet }, 405},
		{"인증없는_GET", func(r *http.Request) { r.Method = http.MethodGet; r.Header.Del("Authorization") }, 401},
		{"DELETE_차단", func(r *http.Request) { r.Method = http.MethodDelete }, 405},
		{"없는_경로", func(r *http.Request) { r.URL.Path = "/unknown" }, 404},
		{"일반_요청", func(r *http.Request) {}, 200},
		{"허용_Origin", func(r *http.Request) { r.Header.Set("Origin", "http://localhost:3000") }, 200},
		{"무상태_세션_무시", func(r *http.Request) { r.Header.Set("Mcp-Session-Id", "other-user-session") }, 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := postRequest(pingPayload)
			tc.modify(r)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
			}
			if w.Header().Get("X-Request-ID") == "" || w.Header().Get("Mcp-Session-Id") != "" {
				t.Fatal("요청 ID 누락 또는 세션 발급")
			}
			if strings.Contains(w.Body.String(), syntheticMCPToken) || strings.Contains(w.Body.String(), "synthetic-private-auth-error") {
				t.Fatal("응답에 민감정보 포함")
			}
		})
	}
	for _, path := range []string{"/healthz", "/readyz"} {
		r := httptest.NewRequest(http.MethodGet, "http://localhost"+path, nil)
		r.RemoteAddr = loopbackPeerAddr
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 200 || w.Body.String() != "ok\n" {
			t.Fatal("상태 조회 실패")
		}
	}
	r := httptest.NewRequest(http.MethodOptions, "http://localhost/mcp", nil)
	r.RemoteAddr = loopbackPeerAddr
	r.Header.Set("Origin", "http://localhost:3000")
	r.Header.Set("Access-Control-Request-Method", "POST")
	r.Header.Set("Access-Control-Request-Headers", "authorization, content-type, mcp-protocol-version")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 204 || w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatal("허용된 preflight 실패")
	}
	r.Header.Set("Access-Control-Request-Headers", "X-User")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("임의 preflight 헤더 허용")
	}
	for _, chunked := range []bool{false, true} {
		r = postRequest(strings.Repeat("x", 64<<10+1))
		if chunked {
			r.ContentLength = -1
			r.TransferEncoding = []string{"chunked"}
		}
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 413 {
			t.Fatalf("크기 제한 실패: %d", w.Code)
		}
	}
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"` + syntheticMCPToken + `"}`,
		`{"jsonrpc":"2.0","id":"synthetic\u002dbackend-token-a","method":"ping"}`,
		`{"jsonrpc":"2.0","id":1,"method":"ping","params":{"synthetic\u002dmcp-token-a":true}}`,
	} {
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, postRequest(body))
		if w.Code != 400 || strings.Contains(w.Body.String(), "synthetic") {
			t.Fatal("자격증명 입력 반사 방지 실패")
		}
	}
	for _, private := range []string{syntheticMCPToken, syntheticBackendToken, "synthetic-private-auth-error", "invalid-token", "other-user-session", pingPayload} {
		if strings.Contains(logs.String(), private) {
			t.Fatal("HTTP 로그에 민감정보 포함")
		}
	}
}

func TestHTTPOptionsRejectPublicBindingAndUnsafeOrigins(t *testing.T) {
	for _, address := range []string{"0.0.0.0:8081", "[::]:8081", "192.0.2.1:8081", "localhost:8081", "127.0.0.1", "127.0.0.1:-1"} {
		opts := httpTestOptions()
		opts.Address = address
		if _, err := NewHTTPHandler(emptyMCPServer(), opts); err == nil {
			t.Errorf("허용되지 않는 수신 주소 통과: %s", address)
		}
	}
	for _, origin := range []string{"*", "null", "https://example.test/", "https://example.test/a", "https://user@example.test", "https://*.example.test", "https://example.test?", "https://example.test#"} {
		opts := httpTestOptions()
		opts.AllowedOrigins = []string{origin}
		if _, err := NewHTTPHandler(emptyMCPServer(), opts); err == nil {
			t.Errorf("허용되지 않는 Origin 통과: %s", origin)
		}
	}
	for _, address := range []string{"127.0.0.1:0", "[::1]:8081"} {
		opts := httpTestOptions()
		opts.Address = address
		if _, err := NewHTTPHandler(emptyMCPServer(), opts); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NewHTTPHandler(emptyMCPServer(), HTTPOptions{}); err == nil {
		t.Fatal("인증 없는 HTTP 서버 허용")
	}
}

type bearerRoundTripper struct {
	next  http.RoundTripper
	token string
	calls *atomic.Int32
}

func (rt bearerRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+rt.token)
	if rt.calls != nil {
		rt.calls.Add(1)
	}
	return rt.next.RoundTrip(r)
}

func sdkHTTPSession(t *testing.T, server *httptest.Server, token string) *mcp.ClientSession {
	t.Helper()
	client := &http.Client{Transport: bearerRoundTripper{next: server.Client().Transport, token: token}, Timeout: 5 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "http-sdk-test", Version: "1"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: client}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestSDKHTTPInitializationAndRepresentativeTools(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+syntheticBackendToken || r.Header.Get("X-Request-ID") == "" {
			t.Error("요청별 위임 토큰 또는 추적 ID 누락")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/clusters":
			io.WriteString(w, `[{"id":"c1","name":"합성"}]`)
		case "/api/clusters/c1/health":
			io.WriteString(w, `{"clusterId":"c1","reachable":true,"cluster":{"clusterId":"c1"}}`)
		case "/api/clusters/c1/partition-health":
			io.WriteString(w, `{"status":"OK","checkedAt":"2026-09-16T00:00:00Z"}`)
		case "/api/clusters/c1/topics", "/api/clusters/c1/consumer-groups":
			io.WriteString(w, `{"clusterId":"c1","syncedAt":"2026-09-16T00:00:00Z","items":[]}`)
		case "/api/clusters/c1/consumer-groups/g1/lag":
			io.WriteString(w, `{"group":"g1","found":true,"status":"OK","partitions":[],"checkedAt":"2026-09-16T00:00:00Z"}`)
		case "/api/clusters/c1/events":
			io.WriteString(w, `{"items":[],"total":0,"page":1,"pageSize":50}`)
		case "/api/clusters/c1/topics/orders":
			io.WriteString(w, `{"name":"orders","partitions":3,"replicationFactor":3}`)
		case "/api/clusters/c1/consumer-groups/g1/members":
			io.WriteString(w, `[]`)
		case "/api/events/e1":
			io.WriteString(w, `{"id":"e1","clusterId":"c1","status":"OPEN","eventCode":"SYNTHETIC_EVENT","severity":"WARN","attentionLevel":"UNEVALUATED"}`)
		case "/api/clusters/c1/asset-graph":
			io.WriteString(w, `{"clusterId":"c1","center":"TOPIC:orders","view":"full","depth":1,"nodes":[{"id":"TOPIC:orders","type":"TOPIC","label":"orders"}],"edges":[],"syncedAt":"2026-09-16T00:00:00Z"}`)
		case "/api/requests/r1":
			io.WriteString(w, `{"requestId":"r1","clusterId":"c1","type":"TOPIC_CREATE","status":"APPROVED","payload":{"topicName":"orders"}}`)
		case "/api/requests":
			if r.URL.Query().Get("cluster") != "c1" {
				t.Error("신청 클러스터 필터 누락")
			}
			io.WriteString(w, `[]`)
		case "/api/clusters/c1/resource-backups":
			io.WriteString(w, `{"items":[],"total":0,"limit":50,"offset":0}`)
		case "/api/clusters/c1/acls/plan":
			if r.Method != http.MethodPost {
				t.Error("미리보기 POST 누락")
			}
			io.WriteString(w, `{"plan":{"generated":[],"toCreate":[],"duplicates":[]},"policy":{"passed":true,"riskLevel":"LOW","violations":[]}}`)
		case "/api/flink/ddl-preview":
			if r.Method != http.MethodPost {
				t.Error("미리보기 POST 누락")
			}
			json.NewEncoder(w).Encode(map[string]any{"tableName": "src_orders", "sourceDdl": "CREATE TABLE `src_orders` (\n  `id` STRING\n) WITH (\n 'connector'='kafka'\n);", "validation": map[string]any{"status": "static_ok"}})
		default:
			t.Error("예상하지 않은 Backend 경로")
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	base, _ := url.Parse(backend.URL)
	api, err := ableops.NewClient(config.Config{BaseURL: base, RequireRequestCredentials: true, Timeout: time.Second, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	defer api.CloseIdleConnections()
	var authCalls atomic.Int32
	opts := httpTestOptions()
	auth := opts.Authenticate
	opts.Authenticate = func(ctx context.Context, token string) (requestctx.Principal, error) {
		authCalls.Add(1)
		p, ok := requestctx.FromContext(ctx)
		if !ok || p.RequestID == "" {
			t.Error("인증 Backend 요청 추적 ID 누락")
		}
		return auth(ctx, token)
	}
	handler, err := NewHTTPHandler(New(api, slog.New(slog.DiscardHandler)), opts)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	session := sdkHTTPSession(t, server, syntheticMCPToken)
	list, err := session.ListTools(context.Background(), nil)
	if err != nil || len(list.Tools) != 33 {
		t.Fatalf("도구 목록 오류: %v", err)
	}
	for _, tool := range list.Tools {
		// 전체 스키마 발견과 기존 도구 회귀, 신규 GET/POST 대표 호출을 HTTP로 검증한다.
		switch tool.Name {
		case "list_clusters", "get_cluster_health", "list_topics", "list_consumer_groups", "get_consumer_group_lag", "list_cluster_events", "get_topic_detail", "get_consumer_group_members", "get_event_detail", "get_asset_impact", "get_request_status", "list_requests", "list_resource_backups", "preview_acl_plan", "preview_flink_ddl":
		default:
			continue
		}
		args := map[string]any{"cluster_id": "c1"}
		if tool.Name == "list_clusters" {
			args = map[string]any{}
		}
		if tool.Name == "get_consumer_group_lag" || tool.Name == "get_consumer_group_members" {
			args["group_name"] = "g1"
		}
		switch tool.Name {
		case "get_topic_detail":
			args["topic_name"] = "orders"
		case "get_event_detail":
			args["event_id"] = "e1"
		case "get_asset_impact":
			args["asset_type"], args["asset_key"] = "TOPIC", "orders"
		case "get_request_status":
			args["request_id"] = "r1"
		case "preview_acl_plan":
			args["template_key"], args["principal"], args["topic"], args["hosts"] = "consumer", "User:synthetic", "orders", []string{"127.0.0.1"}
		case "preview_flink_ddl":
			args["topic_name"], args["table_name"], args["columns"] = "orders", "src_orders", []any{map[string]any{"name": "id", "type": "STRING"}}
		}
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool.Name, Arguments: args})
		if err != nil {
			t.Fatalf("%s SDK 호출 실패: %v", tool.Name, err)
		}
		if result.IsError || len(result.Content) == 0 {
			t.Fatalf("%s 호출 실패: %+v", tool.Name, result)
		}
		raw, _ := json.Marshal(result)
		if strings.Contains(string(raw), syntheticMCPToken) || strings.Contains(string(raw), syntheticBackendToken) {
			t.Fatal("MCP 출력에 토큰 포함")
		}
	}
	if calls.Load() != 16 || authCalls.Load() < 17 {
		t.Fatalf("Backend=%d 인증=%d", calls.Load(), authCalls.Load())
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func blockingMCPServer(entered chan<- struct{}, canceled chan<- struct{}) *mcp.Server {
	server := emptyMCPServer()
	server.AddTool(&mcp.Tool{Name: "wait", Description: "합성 취소 시험", InputSchema: map[string]any{"type": "object"}}, func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		entered <- struct{}{}
		<-ctx.Done()
		canceled <- struct{}{}
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "취소됨"}}}, nil
	})
	return server
}

func awaitSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

func TestHTTPConcurrencyPerUserAndGlobal(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(map[bool]string{true: "전체", false: "사용자별"}[global], func(t *testing.T) {
			entered, canceled := make(chan struct{}, 2), make(chan struct{}, 2)
			opts := httpTestOptions()
			opts.MaxConcurrentPerUser = 1
			if global {
				opts.MaxConcurrent = 1
			}
			handler, err := NewHTTPHandler(blockingMCPServer(entered, canceled), opts)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				handler.ServeHTTP(httptest.NewRecorder(), postRequest(toolPayload).WithContext(ctx))
			}()
			awaitSignal(t, entered, "진행 중 요청 시작 실패")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, postRequest(pingPayload))
			if w.Code != 429 || w.Header().Get("Retry-After") == "" {
				t.Fatal("동시 호출 제한 실패")
			}
			r := postRequest(pingPayload)
			r.Header.Set("Authorization", "Bearer synthetic-mcp-token-b")
			w = httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			want := 200
			if global {
				want = 429
			}
			if w.Code != want {
				t.Fatalf("다른 사용자 제한 오류: %d", w.Code)
			}
			cancel()
			awaitSignal(t, canceled, "기존 프로토콜의 요청 취소 누락")
			awaitSignal(t, done, "취소 요청 종료 실패")
			w = httptest.NewRecorder()
			handler.ServeHTTP(w, postRequest(pingPayload))
			if w.Code != 200 {
				t.Fatal("취소 후 제한 슬롯 누수")
			}
		})
	}
}

func TestHTTPRequestDeadlineAndProcessShutdown(t *testing.T) {
	t.Run("처리_시간", func(t *testing.T) {
		entered, canceled := make(chan struct{}, 1), make(chan struct{}, 1)
		opts := httpTestOptions()
		opts.RequestTimeout = 30 * time.Millisecond
		handler, err := NewHTTPHandler(blockingMCPServer(entered, canceled), opts)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { defer close(done); handler.ServeHTTP(httptest.NewRecorder(), postRequest(toolPayload)) }()
		awaitSignal(t, entered, "도구 시작 실패")
		awaitSignal(t, canceled, "처리 시간 제한 누락")
		awaitSignal(t, done, "시간 초과 후 종료 실패")
	})
	t.Run("프로세스_종료", func(t *testing.T) {
		entered, canceled := make(chan struct{}, 1), make(chan struct{}, 1)
		opts, err := httpTestOptions().normalized()
		if err != nil {
			t.Fatal(err)
		}
		handler, err := NewHTTPHandler(blockingMCPServer(entered, canceled), opts)
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- serveHTTP(ctx, listener, handler, opts) }()
		r := postRequest(toolPayload)
		r.URL, _ = url.Parse("http://" + listener.Addr().String() + "/mcp")
		r.RequestURI = ""
		requestDone := make(chan struct{})
		go func() {
			defer close(requestDone)
			response, err := http.DefaultClient.Do(r)
			if err == nil {
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
			}
		}()
		awaitSignal(t, entered, "HTTP 도구 시작 실패")
		cancel()
		awaitSignal(t, canceled, "프로세스 종료시 도구 취소 누락")
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("정상 종료 실패: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("정상 종료 지연")
		}
		awaitSignal(t, requestDone, "HTTP 연결 종료 실패")
	})
}

type syntheticAuthError string

func (e syntheticAuthError) Error() string    { return "synthetic-private-auth-error" }
func (e syntheticAuthError) AuthCode() string { return string(e) }

func TestHTTPAuthErrorsAndPerRequestRevalidation(t *testing.T) {
	for _, tc := range []struct {
		code   string
		status int
	}{
		{"authentication_required", 401},
		{"backend_authentication_required", 401},
		{"access_denied", 403},
		{"backend_unavailable", 503},
		{"authentication_unavailable", 503},
		{"timeout", 504},
		{"canceled", 408},
		{"synthetic-private-code", 401},
	} {
		t.Run(tc.code, func(t *testing.T) {
			opts := httpTestOptions()
			opts.Authenticate = func(context.Context, string) (requestctx.Principal, error) {
				return requestctx.Principal{}, syntheticAuthError(tc.code)
			}
			handler, err := NewHTTPHandler(emptyMCPServer(), opts)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, postRequest(pingPayload))
			if w.Code != tc.status || strings.Contains(w.Body.String(), "synthetic-private") {
				t.Fatal("인증 실패 분류 또는 정보 보호 실패")
			}
		})
	}
	var revoked atomic.Bool
	opts := httpTestOptions()
	authenticate := opts.Authenticate
	opts.Authenticate = func(ctx context.Context, token string) (requestctx.Principal, error) {
		if revoked.Load() {
			return requestctx.Principal{}, syntheticAuthError("authentication_required")
		}
		return authenticate(ctx, token)
	}
	handler, err := NewHTTPHandler(emptyMCPServer(), opts)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	session := sdkHTTPSession(t, server, syntheticMCPToken)
	if _, err := session.ListTools(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	revoked.Store(true)
	if _, err := session.ListTools(context.Background(), nil); err == nil {
		t.Fatal("초기화 후 인증 폐기를 우회했습니다")
	}
}

type deadlineHTTPWriter struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (w *deadlineHTTPWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func TestHTTPWriteDeadlineMovesWithStreamProgress(t *testing.T) {
	underlying := &deadlineHTTPWriter{ResponseRecorder: httptest.NewRecorder()}
	w := &safeHTTPWriter{ResponseWriter: underlying, writeIdleTimeout: 10 * time.Second}
	if _, err := w.Write([]byte("합성 스트림")); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	if len(underlying.deadlines) != 2 || underlying.deadlines[1].Before(underlying.deadlines[0]) || time.Until(underlying.deadlines[1]) < 9*time.Second {
		t.Fatal("전송 진행에 따른 쓰기 제한 갱신 실패")
	}
}
