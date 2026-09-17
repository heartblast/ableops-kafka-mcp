package dynamic

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const executorToken = "synthetic-dynamic-session-token"

type recordedRequest struct {
	method  string
	escaped string
	query   url.Values
	raw     string
	auth    string
}

type recordingBackend struct {
	mu       sync.Mutex
	requests []recordedRequest
	handler  http.HandlerFunc
}

func (b *recordingBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	b.requests = append(b.requests, recordedRequest{method: r.Method, escaped: r.URL.EscapedPath(), query: r.URL.Query(), raw: r.URL.RawQuery, auth: r.Header.Get("Authorization")})
	b.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	b.handler(w, r)
}

func (b *recordingBackend) calls() []recordedRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]recordedRequest(nil), b.requests...)
}

func newClient(t *testing.T, handler http.HandlerFunc, cfg config.Config) (*ableops.Client, *recordingBackend) {
	t.Helper()
	backend := &recordingBackend{handler: handler}
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	cfg.BaseURL, _ = url.Parse(server.URL)
	cfg.AllowHTTP = true
	if cfg.Timeout == 0 {
		cfg.Timeout = 2 * time.Second
	}
	client, err := ableops.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	return client, backend
}

var executorOperations = []string{"getTopic", "listEvents", "getEventIssue", "listDefaultClusterTopics", "getConsumerGroupLag", "getAssetImpact", "getEvent"}

// connectDynamic은 Static 도구 없이 Dynamic 도구만 등록한 서버에 공식 SDK 클라이언트를 연결한다.
func connectDynamic(t *testing.T, handler http.HandlerFunc) (*mcp.ClientSession, *recordingBackend, *bytes.Buffer) {
	t.Helper()
	client, backend := newClient(t, handler, config.Config{Token: executorToken})
	// 실행기 검증이므로 노출 게이트와 무관하게 모두 등록한다.
	registry, err := Build(fixtureContract(t), Options{Operations: executorOperations, Exposure: exposeAll})
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	server := mcp.NewServer(&mcp.Implementation{Name: "dynamic-test", Version: "0"}, nil)
	registered := Register(server, client, slog.New(slog.NewJSONHandler(&logs, nil)), registry.Selected(), nil)
	if len(registered) != len(executorOperations) {
		t.Fatalf("registered=%v", registered)
	}
	st, ct := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "dynamic-test-client", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, backend, &logs
}

func callTool(t *testing.T, cs *mcp.ClientSession, name string, args map[string]any) (*mcp.CallToolResult, Result) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: 프로토콜 오류 %v", name, err)
	}
	var out Result
	if res.StructuredContent != nil {
		// SDK 클라이언트는 structuredContent를 any(float64)로 해석하므로 큰 정수가 변한다.
		// 원문 비교는 같은 JSON을 담은 text로 하고, 두 표현이 의미상 같은지만 확인한다.
		text, ok := res.Content[0].(*mcp.TextContent)
		if !ok || !json.Valid([]byte(text.Text)) {
			t.Fatal("호환용 JSON 텍스트 누락")
		}
		if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
			t.Fatal(err)
		}
		var structured, fromText any
		raw, _ := json.Marshal(res.StructuredContent)
		_ = json.Unmarshal(raw, &structured)
		_ = json.Unmarshal([]byte(text.Text), &fromText)
		if !reflect.DeepEqual(structured, fromText) {
			t.Fatalf("structuredContent와 text 불일치: %s / %s", raw, text.Text)
		}
		if _, err := time.Parse(time.RFC3339Nano, out.QueriedAt); err != nil || out.Notes == nil {
			t.Fatalf("봉투 필드 누락: %s", text.Text)
		}
	}
	encoded, _ := json.Marshal(res)
	if bytes.Contains(encoded, []byte(executorToken)) {
		t.Fatal("결과에 토큰 노출")
	}
	return res, out
}

func splitEscaped(t *testing.T, escaped string) []string {
	t.Helper()
	var out []string
	for _, part := range strings.Split(strings.TrimPrefix(escaped, "/"), "/") {
		value, err := url.PathUnescape(part)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, value)
	}
	return out
}

func TestExecutorPathBindingEscapesEachSegment(t *testing.T) {
	cs, backend, logs := connectDynamic(t, func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"name":"x"}`)
	})
	cases := []struct {
		tool string
		args map[string]any
		want []string
	}{
		{"get_topic", map[string]any{"id": "prod 01", "name": "orders/v1?x=%2F#frag"}, []string{"api", "clusters", "prod 01", "topics", "orders/v1?x=%2F#frag"}},
		{"get_topic", map[string]any{"id": "c1", "name": ".."}, []string{"api", "clusters", "c1", "topics", ".."}},
		{"get_consumer_group_lag", map[string]any{"id": "c1", "name": "그룹;a,b"}, []string{"api", "clusters", "c1", "consumer-groups", "그룹;a,b", "lag"}},
		{"get_event", map[string]any{"id": "evt:1/2"}, []string{"api", "events", "evt:1/2"}},
	}
	for _, tc := range cases {
		res, out := callTool(t, cs, tc.tool, tc.args)
		if tc.args["name"] == ".." {
			// ".." 조각은 경로 탐색이 되므로 Client가 호출 전에 거부한다.
			if !res.IsError || out.Error == nil || out.Error.Code != "invalid_request" {
				t.Fatalf("..: %+v", out)
			}
			continue
		}
		if res.IsError || out.HTTPStatus != 200 {
			t.Fatalf("%s: %+v", tc.tool, out)
		}
	}
	calls := backend.calls()
	if len(calls) != 3 {
		t.Fatalf("calls=%d", len(calls))
	}
	for i, want := range [][]string{cases[0].want, cases[2].want, cases[3].want} {
		if got := splitEscaped(t, calls[i].escaped); !reflect.DeepEqual(got, want) {
			t.Errorf("segments=%q want %q (escaped=%s)", got, want, calls[i].escaped)
		}
		if calls[i].raw != "" || calls[i].method != http.MethodGet || calls[i].auth != "Bearer "+executorToken {
			t.Errorf("call=%+v", calls[i])
		}
	}
	// principal 형식 query 값도 인코딩한다.
	callTool(t, cs, "get_asset_impact", map[string]any{"id": "c1", "type": "PRINCIPAL", "key": "User:svc a&b=c"})
	last := backend.calls()[3]
	if last.query.Get("key") != "User:svc a&b=c" || last.query.Get("type") != "PRINCIPAL" || len(last.query) != 2 {
		t.Fatalf("query=%v raw=%s", last.query, last.raw)
	}
	if strings.Contains(logs.String(), "orders/v1") || strings.Contains(logs.String(), executorToken) {
		t.Fatal("로그에 인자 원문 또는 토큰 노출")
	}
	if !strings.Contains(logs.String(), `"operation_id":"getTopic"`) || !strings.Contains(logs.String(), `"cluster_id":"prod 01"`) {
		t.Fatalf("로그 필드 누락: %s", logs.String())
	}
}

func TestExecutorQueryEncodingSendsOnlyGivenArguments(t *testing.T) {
	cs, backend, _ := connectDynamic(t, func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"items":[],"total":0,"page":1,"pageSize":20}`)
	})
	_, out := callTool(t, cs, "list_events", map[string]any{})
	if out.Error != nil {
		t.Fatalf("out=%+v", out)
	}
	// 스키마 default(page=1, pageSize=20, sort, excludeSynthetic)는 사용자가 주지 않았으므로 보내지 않는다.
	if first := backend.calls()[0]; first.escaped != "/api/events" || first.raw != "" {
		t.Fatalf("first=%+v", first)
	}
	_, out = callTool(t, cs, "list_events", map[string]any{
		"clusterId":        "c1",
		"status":           []any{"OPEN", "ACKNOWLEDGED"},
		"severity":         []any{"HIGH"},
		"category":         []any{},
		"search":           "a b&c=d/한글",
		"excludeSynthetic": true,
		"page":             2,
		"pageSize":         200,
	})
	if out.Error != nil {
		t.Fatalf("out=%+v", out)
	}
	second := backend.calls()[1]
	want := url.Values{
		"clusterId":        {"c1"},
		"status":           {"OPEN", "ACKNOWLEDGED"},
		"severity":         {"HIGH"},
		"search":           {"a b&c=d/한글"},
		"excludeSynthetic": {"true"},
		"page":             {"2"},
		"pageSize":         {"200"},
	}
	if !reflect.DeepEqual(second.query, want) {
		t.Fatalf("query=%v raw=%s", second.query, second.raw)
	}
	// SDK가 스키마로 거부하는 인자는 백엔드를 호출하지 않는다.
	for _, args := range []map[string]any{
		{"status": []any{"CLOSED"}},
		{"page": 0},
		{"pageSize": 1.5},
		{"unknown": "x"},
		{"excludeSynthetic": "true"},
	} {
		res, _ := callTool(t, cs, "list_events", args)
		if !res.IsError {
			t.Errorf("잘못된 인자 허용: %v", args)
		}
	}
	if len(backend.calls()) != 2 {
		t.Fatalf("잘못된 인자로 백엔드 호출: %d", len(backend.calls()))
	}
}

func TestExecutorPreservesBackendJSON(t *testing.T) {
	const body = `{"clusterId":"c1","status":"UNAVAILABLE","partial":true,"source":"live","isSynthetic":false,"error":"broker 1 unreachable","warnings":[],"failedBrokers":null,"offset":9007199254740993,"items":null}`
	cs, _, _ := connectDynamic(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/topics" {
			io.WriteString(w, "null")
			return
		}
		io.WriteString(w, body)
	})
	res, out := callTool(t, cs, "get_topic", map[string]any{"id": "c1", "name": "orders"})
	if res.IsError || out.Error != nil || out.HTTPStatus != 200 || out.BodyBytes != len(body) {
		t.Fatalf("out=%+v", out)
	}
	if string(out.Body) != body {
		t.Fatalf("원문 변형\n got=%s\nwant=%s", out.Body, body)
	}
	if !reflect.DeepEqual(out.Notes, []string{noteTransport, noteRawBody}) {
		t.Fatalf("notes=%v", out.Notes)
	}
	// 봉투에 성공·정상 판정 필드를 만들지 않는다.
	var envelope map[string]any
	raw, _ := json.Marshal(res.StructuredContent)
	_ = json.Unmarshal(raw, &envelope)
	for _, key := range []string{"success", "ok", "status", "healthy"} {
		if _, exists := envelope[key]; exists {
			t.Fatalf("임의 판정 필드 %s 추가", key)
		}
	}
	// 최상위 null은 빈 목록이나 오류로 바꾸지 않는다.
	res, out = callTool(t, cs, "list_default_cluster_topics", nil)
	if res.IsError || out.Error != nil || string(out.Body) != "null" {
		t.Fatalf("null 보존 실패: %+v", out)
	}
	if text := res.Content[0].(*mcp.TextContent).Text; !strings.Contains(text, `"body":null`) {
		t.Fatalf("null body 키 누락: %s", text)
	}
}

func TestExecutorHTTPErrors(t *testing.T) {
	statuses := map[string]int{"s401": 401, "s403": 403, "s404": 404, "s500": 500, "s400": 400, "s429": 429, "s503": 503}
	cs, _, _ := connectDynamic(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/api/clusters/c1/topics/")
		http.Error(w, `{"error":"backend-secret detail"}`, statuses[name])
	})
	want := map[string]string{"s401": "authentication_required", "s403": "access_denied", "s404": "not_found", "s500": "backend_unavailable", "s400": "backend_error", "s429": "rate_limited", "s503": "backend_unavailable"}
	for name, code := range want {
		res, out := callTool(t, cs, "get_topic", map[string]any{"id": "c1", "name": name})
		if !res.IsError || out.Error == nil || out.Error.Code != code || out.Error.HTTPStatus != statuses[name] || out.HTTPStatus != statuses[name] || out.Body != nil {
			t.Errorf("%s: %+v", name, out)
		}
		encoded, _ := json.Marshal(res)
		if bytes.Contains(encoded, []byte("backend-secret")) {
			t.Errorf("%s: 오류 본문 원문 노출", name)
		}
	}
}

func TestExecutorTimeoutAndNoContent(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	client, backend := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/issue"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/slow"):
			select {
			case <-release:
			case <-r.Context().Done():
			}
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}, config.Config{Token: executorToken, Timeout: time.Second})
	registry, _ := Build(fixtureContract(t), Options{Operations: executorOperations})
	executor := NewExecutor(client, nil)
	issue, _ := registry.Lookup("get_event_issue")
	out := executor.Call(context.Background(), issue.Tool, json.RawMessage(`{"id":"e1"}`))
	if out.Error != nil || !out.NoContent || out.HTTPStatus != 204 || out.Body != nil || out.Notes[1] != noteNoContent {
		t.Fatalf("204: %+v", out)
	}
	if res := finish(out); res.IsError {
		t.Fatalf("204 결과가 오류: %+v", res)
	}
	// 계약이 204를 선언하지 않은 Operation의 204는 원문 없는 성공으로 해석하지 않는다.
	topic, _ := registry.Lookup("get_topic")
	out = executor.Call(context.Background(), topic.Tool, json.RawMessage(`{"id":"c1","name":"t"}`))
	if out.Error == nil || out.Error.Code != "invalid_response" {
		t.Fatalf("undeclared 204: %+v", out)
	}
	started := time.Now()
	out = executor.Call(context.Background(), topic.Tool, json.RawMessage(`{"id":"c1","name":"slow"}`))
	if out.Error == nil || out.Error.Code != "timeout" || time.Since(started) > 5*time.Second {
		t.Fatalf("timeout: %+v", out)
	}
	if len(backend.calls()) != 3 {
		t.Fatalf("calls=%d", len(backend.calls()))
	}
}

func TestExecutorRefusesUnsafeCalls(t *testing.T) {
	client, backend := newClient(t, func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, `{}`) }, config.Config{Token: executorToken})
	executor := NewExecutor(client, nil)
	response := map[string]openapi.Response{"200": {MediaTypes: []string{"application/json"}}}
	for _, op := range []openapi.Operation{
		{ID: "createTopic", Method: http.MethodPost, Path: "/api/topics", MCPEnabled: true, Responses: response},
		{ID: "deleteTopic", Method: http.MethodDelete, Path: "/api/topics", MCPEnabled: true, Responses: response},
		{ID: "getHidden", Method: http.MethodGet, Path: "/api/hidden", MCPEnabled: false, Responses: response},
	} {
		// Compile을 우회해 만든 도구도 실행 경계에서 거부한다.
		out := executor.Call(context.Background(), &Tool{Name: "x", Operation: op}, nil)
		if out.Error == nil || out.Error.Code != "unsupported_method" {
			t.Fatalf("%s: %+v", op.ID, out)
		}
	}
	registry, _ := Build(fixtureContract(t), Options{Operations: executorOperations})
	topic, _ := registry.Lookup("get_topic")
	for _, raw := range []string{`{"id":"   ","name":"t"}`, `{"id":"c1"}`, `{"id":"c1","name":"t","extra":1}`, `{"id":1,"name":"t"}`, `[]`, `{"id":"c1","name":"a\nb"}`} {
		out := executor.Call(context.Background(), topic.Tool, json.RawMessage(raw))
		if out.Error == nil || out.Error.Code != "invalid_request" {
			t.Fatalf("%s: %+v", raw, out)
		}
	}
	if len(backend.calls()) != 0 {
		t.Fatalf("거부해야 할 호출이 백엔드에 도달: %+v", backend.calls())
	}
}

func TestExecutorOutputLimit(t *testing.T) {
	large := `{"items":["` + strings.Repeat("x", 100<<10) + `"]}`
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, large) }, config.Config{Token: executorToken})
	registry, _ := Build(fixtureContract(t), Options{Operations: executorOperations})
	topic, _ := registry.Lookup("get_topic")
	out := NewExecutor(client, nil).Call(context.Background(), topic.Tool, json.RawMessage(`{"id":"c1","name":"t"}`))
	res := finish(out)
	if !res.IsError || out.Error == nil || out.Error.Code != "output_too_large" || out.Body != nil || out.BodyBytes != len(large) || out.HTTPStatus != 200 {
		t.Fatalf("out=%+v", out)
	}
	raw, _ := json.Marshal(res)
	if len(raw) > 128<<10 || bytes.Contains(raw, []byte("xxxx")) {
		t.Fatalf("부분 본문이 남음: %d", len(raw))
	}
}

func TestExecutorDelegatedCredentials(t *testing.T) {
	const delegated = "synthetic-delegated-user-token"
	client, backend := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/clusters/c1/topics/leak" {
			io.WriteString(w, `{"echo":"`+delegated+`"}`)
			return
		}
		io.WriteString(w, `{"name":"t"}`)
	}, config.Config{RequireRequestCredentials: true})
	registry, _ := Build(fixtureContract(t), Options{Operations: executorOperations})
	topic, _ := registry.Lookup("get_topic")
	executor := NewExecutor(client, nil)
	// HTTP 모드에서 요청 자격증명이 없으면 호출하지 않는다.
	out := executor.Call(context.Background(), topic.Tool, json.RawMessage(`{"id":"c1","name":"t"}`))
	if out.Error == nil || out.Error.Code != "authentication_required" || len(backend.calls()) != 0 {
		t.Fatalf("out=%+v", out)
	}
	ctx := requestctx.WithPrincipal(context.Background(), requestctx.Principal{UserID: "u1", BackendToken: delegated, RequestID: "req-1"})
	out = executor.Call(ctx, topic.Tool, json.RawMessage(`{"id":"c1","name":"t"}`))
	if out.Error != nil || backend.calls()[0].auth != "Bearer "+delegated {
		t.Fatalf("out=%+v calls=%+v", out, backend.calls())
	}
	// 응답에 위임 토큰이 섞이면 본문을 전달하지 않는다.
	out = executor.Call(ctx, topic.Tool, json.RawMessage(`{"id":"c1","name":"leak"}`))
	raw, _ := json.Marshal(finish(out))
	if out.Error == nil || out.Error.Code != "invalid_response" || bytes.Contains(raw, []byte(delegated)) {
		t.Fatalf("out=%+v", out)
	}
}
