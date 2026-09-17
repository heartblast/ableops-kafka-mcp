package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/dynamic"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const e2eToken = "synthetic-e2e-session-token"

// 합성 백엔드 응답. 민감 성격 필드(username·tlsKeyFile·configs·evidence)를 일부러 넣어
// Static DTO와 Dynamic 원문 전달의 공개 범위 차이를 드러낸다.
var e2eResponses = map[string]string{
	"/api/clusters": `[{"id":"c1","name":"개발","environment":"dev","mode":"franz","kafkaVersion":"4.2.1","kraftMode":true,"isActive":true,"isDefault":true,` +
		`"username":"synthetic-sasl-user","tlsKeyFile":"/synthetic/tls/key.pem","hasPassword":true}]`,
	"/api/clusters/c1/topics": `{"clusterId":"c1","clusterName":"개발","environment":"dev","syncedAt":null,"items":[` +
		`{"name":"orders","partitions":3,"replicationFactor":3,"cleanupPolicy":"delete","retentionMs":604800000,"owner":"team-a","configs":{"min.insync.replicas":"2"}}]}`,
	"/api/clusters/c1/topics/orders": `{"name":"orders","partitions":3,"replicationFactor":3,"cleanupPolicy":"delete","retentionMs":604800000,"owner":"team-a","configs":{"min.insync.replicas":"2"}}`,
	"/api/clusters/c1/consumer-groups": `{"clusterId":"c1","clusterName":"개발","environment":"dev","syncedAt":"2026-09-17T00:00:00Z","items":[` +
		`{"name":"g1","state":"Stable","members":2,"totalLag":9007199254740993,"topicLag":{"orders":9007199254740993}}]}`,
	"/api/events": `{"items":[{"id":"e1","clusterId":"c1","eventCode":"CRITICAL_CONSUMER_LAG","status":"OPEN","severity":"HIGH","dataMode":"LIVE","evidence":{"lag":10}}],"total":1,"page":1,"pageSize":20}`,
	// 노출 안전 게이트가 SAFE로 분류한 Operation의 응답.
	"/api/clusters/c1/topics/orders/partitions": `{"topic":"orders","partitions":[{"partition":0,"leader":1,"replicas":[1,2,3],"isr":[1,2]}],"health":{"status":"OK","reasons":[]},"logEndHint":9007199254740993}`,
	"/api/events/summary":                       `{"open":1,"scope":{"all":false,"clusterIds":["c1"],"clusterDenied":false},"collection":{"enabled":true},"lastEventAt":null}`,
	"/api/branding":                             `{"productName":"AbleOps","demo":false}`,
}

// safeSelection은 SAFE 3개와 SHADOW·BLOCKED 하나씩을 고른 선택이다. SAFE 중 getEventSummary 는
// v0.2.0 Static 도구와 이름이 같아 shadow 이므로 노출되는 것은 2개다.
var safeSelection = []string{"getTopicPartitions", "getEventSummary", "getBranding", "getTopic", "listClusters"}

// staticCount는 Static 도구 수다. v0.2.0에서 33개이며 도구가 늘어도 테스트가 따라간다.
func staticCount() int { return len(tools.StaticNames()) }

type e2eRequest struct {
	path  string
	query string
	auth  string
}

type e2eBackend struct {
	mu        sync.Mutex
	requests  []e2eRequest
	spec      []byte
	specCode  int
	specETag  string
	specCalls int
	baseURL   *url.URL
}

// setSpec은 계약 응답을 바꾼다. 같은 ETag의 조건부 요청에는 304로 답한다.
func (b *e2eBackend) setSpec(code int, spec []byte, etag string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.specCode, b.spec, b.specETag = code, spec, etag
}

func (b *e2eBackend) currentSpec() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spec
}

func (b *e2eBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path == "/openapi.json" {
		b.specCalls++
		if r.Header.Get("Authorization") != "" {
			http.Error(w, "계약 조회에 인증 헤더", http.StatusBadRequest)
			return
		}
		if b.specCode == http.StatusOK && r.Header.Get("If-None-Match") == b.specETag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", b.specETag)
		w.WriteHeader(b.specCode)
		w.Write(b.spec)
		return
	}
	b.requests = append(b.requests, e2eRequest{path: r.URL.EscapedPath(), query: r.URL.RawQuery, auth: r.Header.Get("Authorization")})
	body, ok := e2eResponses[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, body)
}

func (b *e2eBackend) take() []e2eRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.requests
	b.requests = nil
	return out
}

func newE2E(t *testing.T, specCode int, spec []byte) (*ableops.Client, *e2eBackend) {
	t.Helper()
	if spec == nil {
		var err error
		spec, err = os.ReadFile("../openapi/testdata/ableops-openapi.json")
		if err != nil {
			t.Fatal(err)
		}
	}
	backend := &e2eBackend{spec: spec, specCode: specCode, specETag: `"e2e-v1"`}
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	backend.baseURL = base
	client, err := ableops.NewClient(config.Config{BaseURL: base, Token: e2eToken, Timeout: 2 * time.Second, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	return client, backend
}

func session(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	st, ct := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "e2e", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func toolMap(t *testing.T, cs *mcp.ClientSession) map[string]*mcp.Tool {
	t.Helper()
	listed, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*mcp.Tool{}
	for _, tool := range listed.Tools {
		out[tool.Name] = tool
	}
	return out
}

func invokeText(t *testing.T, cs *mcp.ClientSession, name string, args map[string]any) (*mcp.CallToolResult, string) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	text := res.Content[0].(*mcp.TextContent).Text
	encoded, _ := json.Marshal(res)
	if bytes.Contains(encoded, []byte(e2eToken)) {
		t.Fatalf("%s: 결과에 토큰 노출", name)
	}
	return res, text
}

func dynamicResult(t *testing.T, text string) dynamic.Result {
	t.Helper()
	var out dynamic.Result
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func staticResult[T any](t *testing.T, text string) tools.Envelope[T] {
	t.Helper()
	var out tools.Envelope[T]
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// loadRegistry는 기본 파일럿 선택(operations 생략)으로 계약을 적재한다.
func loadRegistry(t *testing.T, client *ableops.Client) *dynamic.Registry {
	t.Helper()
	return loadSelected(t, client, nil)
}

func loadSelected(t *testing.T, client *ableops.Client, operations []string) *dynamic.Registry {
	t.Helper()
	registry, err := dynamic.Load(context.Background(), openapi.NewLoader(client), dynamic.Options{StaticToolNames: tools.StaticNames(), Operations: operations})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

// listChanged는 받은 tools/list_changed 수다. wake는 깨우기 신호일 뿐이며 가득 차도 핸들러를
// 막지 않는다(막히면 세션 종료가 핸들러를 기다리며 멈춘다).
type listChanged struct {
	count atomic.Int32
	wake  chan struct{}
}

// expectChange는 base 이후 알림을 기다리고, 디바운스(10ms)보다 충분히 긴 시간 뒤 증가분이
// 1 이상 limit(SDK 변경 호출 수) 이하인지 확인한 뒤 누적 수를 돌려준다.
func (c *listChanged) expectChange(t *testing.T, base, limit int32) int32 {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for c.count.Load() <= base {
		select {
		case <-c.wake:
		case <-deadline:
			t.Fatal("tools/list_changed 알림 없음")
		}
	}
	time.Sleep(200 * time.Millisecond)
	total := c.count.Load()
	if got := total - base; got < 1 || got > limit {
		t.Fatalf("tools/list_changed 증가분=%d, 기대 1~%d", got, limit)
	}
	return total
}

// expectNone은 늦게 온 알림까지 기다린 뒤에도 누적 수가 base 그대로인지 확인한다.
func (c *listChanged) expectNone(t *testing.T, base int32) {
	t.Helper()
	time.Sleep(200 * time.Millisecond)
	if got := c.count.Load(); got != base {
		t.Fatalf("알림 수=%d, 기대=%d", got, base)
	}
}

// notifyingSession은 tools/list_changed를 세는 공식 SDK 클라이언트를 연결한다(SDK 기본 최신 프로토콜).
// 구독 요청은 응답을 기다리지 않으므로 서버의 구독 확인을 받은 뒤 반환한다.
func notifyingSession(t *testing.T, server *mcp.Server) (*mcp.ClientSession, *listChanged) {
	t.Helper()
	changed := &listChanged{wake: make(chan struct{}, 1)}
	acked := make(chan struct{}, 1)
	client := mcp.NewClient(&mcp.Implementation{Name: "e2e-notify", Version: "0"}, &mcp.ClientOptions{
		ToolListChangedHandler: func(context.Context, *mcp.ToolListChangedRequest) {
			changed.count.Add(1)
			select {
			case changed.wake <- struct{}{}:
			default:
			}
		},
	})
	client.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
			if method == "notifications/subscriptions/acknowledged" {
				select {
				case acked <- struct{}{}:
				default:
				}
			}
			return next(ctx, method, request)
		}
	})
	st, ct := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	select {
	case <-acked:
	case <-time.After(5 * time.Second):
		t.Fatal("tools/list_changed 구독 확인 없음")
	}
	return cs, changed
}

func definitionsJSON(t *testing.T, listed map[string]*mcp.Tool, names []string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, name := range names {
		raw, err := json.Marshal(listed[name])
		if err != nil {
			t.Fatal(err)
		}
		out[name] = string(raw)
	}
	return out
}

// editFixture는 upstream 픽스처의 paths를 고친 계약 본문을 만든다. 숫자 표기는 보존한다.
func editFixture(t *testing.T, edit func(paths map[string]any)) []byte {
	t.Helper()
	raw, err := os.ReadFile("../openapi/testdata/ableops-openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var doc map[string]any
	if err := decoder.Decode(&doc); err != nil {
		t.Fatal(err)
	}
	edit(doc["paths"].(map[string]any))
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDynamicDisabledKeepsStaticServer(t *testing.T) {
	client, backend := newE2E(t, http.StatusOK, nil)
	for _, server := range []*mcp.Server{New(client, nil), New(client, nil, WithDynamic(nil))} {
		cs := session(t, server)
		listed := toolMap(t, cs)
		if len(listed) != staticCount() {
			t.Fatalf("tools=%d", len(listed))
		}
		if cs.InitializeResult().Instructions != staticInstructions {
			t.Fatal("비활성 서버에 동적 도구 안내가 붙음")
		}
	}
	if backend.specCalls != 0 {
		t.Fatal("비활성 상태에서 계약을 조회함")
	}
}

// OpenAPI → Loader → Compiler → 노출 안전 게이트 → Registry → tools/list → tools/call → REST → MCP Result
func TestDynamicEndToEnd(t *testing.T) {
	client, backend := newE2E(t, http.StatusOK, nil)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	// 1차 기본 파일럿은 모두 SHADOW 또는 BLOCKED라 기본 서버에 추가되는 도구가 없다.
	pilot := loadRegistry(t, client)
	if stats := pilot.Stats(); pilot.ETag() != `"e2e-v1"` || stats.Exposed != 0 || stats.Shadow != 4 || stats.Blocked != 1 {
		t.Fatalf("etag=%s stats=%+v", pilot.ETag(), stats)
	}
	if listed := toolMap(t, session(t, New(client, nil, WithDynamic(pilot)))); len(listed) != staticCount() {
		t.Fatalf("pilot tools=%d", len(listed))
	}

	registry := loadSelected(t, client, safeSelection)
	if stats := registry.Stats(); stats.Exposed != 2 || stats.Shadow != 2 || stats.Blocked != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	dynamic.LogSummary(logger, registry)

	staticOnly := toolMap(t, session(t, New(client, nil)))
	server := New(client, logger, WithDynamic(registry))
	cs := session(t, server)
	if !strings.Contains(cs.InitializeResult().Instructions, "OpenAPI 기반 동적 도구") {
		t.Fatal("동적 도구 안내 누락")
	}
	listed := toolMap(t, cs)
	if len(listed) != staticCount()+2 {
		t.Fatalf("tools=%d", len(listed))
	}
	// Static 도구 정의는 한 글자도 바뀌지 않는다.
	for name, want := range staticOnly {
		got, ok := listed[name]
		wantJSON, _ := json.Marshal(want)
		gotJSON, _ := json.Marshal(got)
		if !ok || !bytes.Equal(wantJSON, gotJSON) {
			t.Fatalf("Static 도구 %s 정의가 바뀜", name)
		}
	}
	for _, name := range []string{"get_topic_partitions", "get_branding"} {
		tool := listed[name]
		if tool == nil || tool.Annotations == nil || !tool.Annotations.ReadOnlyHint || tool.OutputSchema == nil || tool.Meta["ableops/openapi"] == nil {
			t.Fatalf("동적 도구 %s 정의 누락: %+v", name, tool)
		}
	}
	// SHADOW(get_topic)와 BLOCKED(listClusters)는 노출되지 않고, Static 이름과 겹치는 SAFE
	// (get_event_summary)도 Static 정의 그대로 남는다.
	if listed["get_topic"] != nil || listed["list_clusters"].Meta["ableops/openapi"] != nil || listed["get_event_summary"].Meta["ableops/openapi"] != nil {
		t.Fatal("SAFE가 아니거나 Static과 겹치는 Dynamic 도구가 노출됨")
	}

	for _, tc := range []struct {
		name string
		args map[string]any
		op   string
		body string
	}{
		{"get_topic_partitions", map[string]any{"id": "c1", "name": "orders"}, "getTopicPartitions", e2eResponses["/api/clusters/c1/topics/orders/partitions"]},
		{"get_branding", map[string]any{}, "getBranding", e2eResponses["/api/branding"]},
	} {
		res, text := invokeText(t, cs, tc.name, tc.args)
		out := dynamicResult(t, text)
		// body는 원문 그대로다(큰 정수·null 포함).
		if res.IsError || out.OperationID != tc.op || out.HTTPStatus != 200 || string(out.Body) != tc.body {
			t.Fatalf("%s=%s", tc.name, text)
		}
	}
	// 기존 Static 도구도 같은 서버에서 그대로 동작한다.
	res, text := invokeText(t, cs, "list_clusters", map[string]any{})
	if res.IsError || !strings.Contains(text, `"backend_cluster_registry"`) {
		t.Fatalf("static list_clusters=%s", text)
	}
	// 도구 인자로 전달한 토큰은 동적 도구에서도 차단된다.
	res, _ = invokeText(t, cs, "get_topic_partitions", map[string]any{"id": "c1", "name": e2eToken})
	if !res.IsError {
		t.Fatal("인자 토큰 허용")
	}
	requests := backend.take()
	want := []e2eRequest{
		{path: "/api/clusters/c1/topics/orders/partitions", auth: "Bearer " + e2eToken},
		{path: "/api/branding", auth: "Bearer " + e2eToken},
		{path: "/api/clusters", auth: "Bearer " + e2eToken},
	}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("requests=%+v", requests)
	}
	if backend.specCalls != 2 {
		t.Fatalf("계약 조회 횟수=%d", backend.specCalls)
	}
	for _, fragment := range []string{`"msg":"동적 도구 계약 적재"`, `"registrable_get":28`, `"static_conflicts":11`, `"blocked":1`, `"msg":"동적 도구 조회 완료"`, `"operation_id":"getTopicPartitions"`, `"code":"exposure_BLOCKED"`, `"code":"static_tool_conflict"`} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("로그 누락 %s: %s", fragment, logs.String())
		}
	}
	if strings.Contains(logs.String(), e2eToken) || strings.Contains(logs.String(), "synthetic-sasl-user") || strings.Contains(logs.String(), "9007199254740993") {
		t.Fatal("로그에 비밀 또는 응답 원문 노출")
	}
}

// 종료 기준(서버 수준): Static 11개와 함께 Registry A 서비스 → ETag 변경 → 새 계약 검증 →
// Registry B → 원자적 교체 → Diff → tools/list_changed 1회. 잘못된 계약은 A를 유지한다.
func TestDynamicRuntimeHotReloadWithStaticTools(t *testing.T) {
	client, backend := newE2E(t, http.StatusOK, nil)
	var logs lockedLogBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	runtime := dynamic.NewRuntime(client, logger, openapi.NewLoader(client), dynamic.Options{
		StaticToolNames: tools.StaticNames(),
		Operations:      []string{"getTopicPartitions", "getEventSummary", "getBranding"},
	})
	server := New(client, logger, WithRuntime(runtime))
	ctx := context.Background()
	if report := runtime.Refresh(ctx); report.Outcome != dynamic.RefreshUpdated {
		t.Fatalf("initial=%+v", report)
	}
	cs, changed := notifyingSession(t, server)
	if !strings.Contains(cs.InitializeResult().Instructions, "OpenAPI 기반 동적 도구") {
		t.Fatal("런타임 서버 안내 누락")
	}
	before := toolMap(t, cs)
	staticNames := tools.StaticNames()
	staticBefore := definitionsJSON(t, before, staticNames)
	if len(before) != staticCount()+2 {
		t.Fatalf("Registry A tools=%d", len(before))
	}

	// 304와 잘못된 계약: Registry A 유지, 알림 없음, 기존 도구 정상 호출.
	registryA := runtime.Current()
	if report := runtime.Refresh(ctx); report.Outcome != dynamic.RefreshNotModified || report.Registry != registryA || runtime.Current() != registryA {
		t.Fatalf("304=%+v", report)
	}
	backend.setSpec(http.StatusOK, []byte(`{"openapi":`), `"e2e-bad"`)
	if report := runtime.Refresh(ctx); report.Outcome != dynamic.RefreshFailed || report.Code != "invalid_json" || runtime.Current() != registryA {
		t.Fatalf("bad=%+v", report)
	}
	if got := toolMap(t, cs); len(got) != staticCount()+2 {
		t.Fatalf("잘못된 계약 후 tools=%d", len(got))
	}
	if res, _ := invokeText(t, cs, "get_topic_partitions", map[string]any{"id": "c1", "name": "orders"}); res.IsError {
		t.Fatal("잘못된 계약 후 기존 Dynamic 도구 호출 실패")
	}
	if res, _ := invokeText(t, cs, "list_clusters", map[string]any{}); res.IsError {
		t.Fatal("잘못된 계약 후 Static 도구 호출 실패")
	}
	changed.expectNone(t, 0)

	// Contract B: getBranding 회수(x-mcp-enabled=false), getTopicPartitions 설명 변경.
	backend.setSpec(http.StatusOK, editFixture(t, func(paths map[string]any) {
		paths["/api/branding"].(map[string]any)["get"].(map[string]any)["x-mcp-enabled"] = false
		get := paths["/api/clusters/{id}/topics/{name}/partitions"].(map[string]any)["get"].(map[string]any)
		get["summary"] = get["summary"].(string) + " (개정)"
	}), `"e2e-v2"`)
	report := runtime.Refresh(ctx)
	if report.Outcome != dynamic.RefreshUpdated || !reflect.DeepEqual(report.Diff.Removed, []string{"get_branding"}) ||
		!reflect.DeepEqual(report.Diff.Changed, []string{"get_topic_partitions"}) ||
		len(report.Diff.Unchanged) != 0 || len(report.Diff.Added) != 0 {
		t.Fatalf("B=%+v", report)
	}
	// 변경 1(AddTool)과 삭제 1(RemoveTools)은 SDK 디바운스로 보통 한 번에 묶인다(상한 2).
	seen := changed.expectChange(t, 0, 2)
	after := toolMap(t, cs)
	if len(after) != staticCount()+1 || after["get_branding"] != nil || !strings.Contains(after["get_topic_partitions"].Description, "(개정)") {
		t.Fatalf("Registry B tools=%d", len(after))
	}
	if !reflect.DeepEqual(definitionsJSON(t, after, staticNames), staticBefore) {
		t.Fatal("교체 중 Static 도구 정의가 바뀜")
	}
	if res, _ := invokeText(t, cs, "list_topics", map[string]any{"cluster_id": "c1"}); res.IsError {
		t.Fatal("교체 후 Static 도구 호출 실패")
	}
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "get_branding", Arguments: map[string]any{}}); err == nil {
		t.Fatal("회수된 도구가 호출됨")
	}
	// ETag만 바뀐 같은 계약은 알림이 없다.
	backend.setSpec(http.StatusOK, backend.currentSpec(), `"e2e-v2b"`)
	if report := runtime.Refresh(ctx); report.Outcome != dynamic.RefreshUnchanged || !report.ETagChanged {
		t.Fatalf("etag only=%+v", report)
	}
	changed.expectNone(t, seen)
	for _, fragment := range []string{`"msg":"동적 도구 계약 갱신 실패: 마지막 정상 도구 목록을 유지합니다"`, `"code":"invalid_json"`, `"outcome":"updated"`, `"outcome":"unchanged"`} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("로그 누락 %s: %s", fragment, logs.String())
		}
	}
	if strings.Contains(logs.String(), e2eToken) {
		t.Fatal("로그에 토큰 노출")
	}
}

// HTTP(stateless)는 알림을 받지 못하는 클라이언트가 있으므로 다음 tools/list에서 반영을 확인한다.
// 요청 처리 중 교체가 일어나도 응답은 깨지지 않는다.
func TestDynamicRuntimeOverHTTP(t *testing.T) {
	_, backend := newE2E(t, http.StatusOK, nil)
	rest, err := ableops.NewClient(config.Config{BaseURL: backend.baseURL, AllowHTTP: true, Timeout: 2 * time.Second, RequireRequestCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rest.CloseIdleConnections()
	runtime := dynamic.NewRuntime(rest, nil, openapi.NewLoader(rest), dynamic.Options{StaticToolNames: tools.StaticNames(), Operations: []string{"getTopicPartitions", "getBranding"}})
	handler, err := NewHTTPHandler(New(rest, nil, WithRuntime(runtime)), HTTPOptions{
		Authenticate: func(context.Context, string) (requestctx.Principal, error) {
			return requestctx.Principal{UserID: "user-a", ClientID: "test", BackendToken: e2eToken}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report := runtime.Refresh(context.Background()); report.Outcome != dynamic.RefreshUpdated {
		t.Fatalf("initial=%+v", report)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	cs := sdkHTTPSession(t, server, "synthetic-mcp-a")
	if listed := toolMap(t, cs); len(listed) != staticCount()+2 || listed["get_branding"] == nil {
		t.Fatalf("A tools=%d", len(listed))
	}
	backend.setSpec(http.StatusOK, editFixture(t, func(paths map[string]any) {
		paths["/api/branding"].(map[string]any)["get"].(map[string]any)["x-mcp-enabled"] = false
	}), `"e2e-http-v2"`)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 20 {
			res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "get_topic_partitions", Arguments: map[string]any{"id": "c1", "name": "orders"}})
			if err != nil || res.IsError {
				t.Errorf("교체 중 HTTP 호출 실패: %v", err)
				return
			}
		}
	}()
	if report := runtime.Refresh(context.Background()); report.Outcome != dynamic.RefreshUpdated {
		t.Errorf("B=%+v", report)
	}
	wg.Wait()
	if listed := toolMap(t, cs); len(listed) != staticCount()+1 || listed["get_branding"] != nil {
		t.Fatalf("B tools=%d", len(listed))
	}
}

// 같은 백엔드에 대해 Static 도구와 shadow Dynamic 도구를 나란히 호출해 차이를 기록한다.
// 차이는 Dynamic 쪽에서 맞추지 않는다. 아래 단언은 현재 차이 자체를 고정한다.
func TestDynamicShadowComparison(t *testing.T) {
	client, backend := newE2E(t, http.StatusOK, nil)
	registry := loadRegistry(t, client)
	staticCS := session(t, New(client, nil, WithDynamic(registry)))
	shadowCS := session(t, NewDynamicComparison(client, nil, registry))

	shadowTools := toolMap(t, shadowCS)
	names := make([]string, 0, len(shadowTools))
	for name := range shadowTools {
		names = append(names, name)
	}
	sort.Strings(names)
	// 비교용 서버도 BLOCKED(listClusters)는 등록하지 않는다.
	if !reflect.DeepEqual(names, []string{"get_topic", "list_consumer_groups", "list_events", "list_topics"}) {
		t.Fatalf("shadow tools=%v", names)
	}
	// 같은 이름이라도 입력 계약이 다르다: Static은 cluster_id, Dynamic은 OpenAPI 이름(id)을 쓴다.
	staticTools := toolMap(t, staticCS)
	staticInput, _ := json.Marshal(staticTools["list_topics"].InputSchema)
	shadowInput, _ := json.Marshal(shadowTools["list_topics"].InputSchema)
	if !strings.Contains(string(staticInput), `"cluster_id"`) || !strings.Contains(string(shadowInput), `"id"`) || strings.Contains(string(shadowInput), "cluster_id") {
		t.Fatalf("입력 계약 static=%s shadow=%s", staticInput, shadowInput)
	}

	// BLOCKED 근거: Dynamic 원문 전달이었다면 인증 설정이 LLM에 전달된다. 실행기로 직접 확인만 하고
	// 어떤 서버에도 등록하지 않는다.
	t.Run("list_clusters_is_blocked", func(t *testing.T) {
		entry, ok := registry.Lookup("list_clusters")
		if !ok || entry.Placement != dynamic.PlacementBlocked || entry.Exposure != dynamic.ExposureBlocked || !entry.StaticConflict {
			t.Fatalf("entry=%+v", entry)
		}
		_, staticText := invokeText(t, staticCS, "list_clusters", map[string]any{})
		raw := dynamic.NewExecutor(client, nil).Call(context.Background(), entry.Tool, nil)
		requireSameRequests(t, backend.take())
		st := staticResult[tools.ListData[ableops.Cluster]](t, staticText)
		var body []map[string]any
		if err := json.Unmarshal(raw.Body, &body); err != nil {
			t.Fatal(err)
		}
		if st.Status != "ok" || len(st.Data.Items) != 1 || len(body) != 1 || st.Data.Items[0].ID != body[0]["id"] {
			t.Fatalf("static=%s raw=%s", staticText, raw.Body)
		}
		if strings.Contains(staticText, "synthetic-sasl-user") || strings.Contains(staticText, "tlsKeyFile") {
			t.Fatal("Static 결과에 인증 설정 노출")
		}
		if !strings.Contains(string(raw.Body), "synthetic-sasl-user") || !strings.Contains(string(raw.Body), "/synthetic/tls/key.pem") {
			t.Fatal("원문 공개 범위가 바뀜: BLOCKED 판단 근거와 문서를 함께 갱신하세요")
		}
	})

	t.Run("list_topics", func(t *testing.T) {
		_, staticText := invokeText(t, staticCS, "list_topics", map[string]any{"cluster_id": "c1"})
		_, shadowText := invokeText(t, shadowCS, "list_topics", map[string]any{"id": "c1"})
		requireSameRequests(t, backend.take())
		st := staticResult[tools.SnapshotData[ableops.Topic]](t, staticText)
		shadow := dynamicResult(t, shadowText)
		var body struct {
			SyncedAt *string          `json:"syncedAt"`
			Items    []map[string]any `json:"items"`
		}
		if err := json.Unmarshal(shadow.Body, &body); err != nil {
			t.Fatal(err)
		}
		if len(st.Data.Items) != 1 || len(body.Items) != 1 || st.Data.Items[0].Name != body.Items[0]["name"] {
			t.Fatalf("static=%s shadow=%s", staticText, shadowText)
		}
		// 차이: Static은 syncedAt=null을 partial과 제한 설명으로 해석한다. Dynamic은 판정하지 않고 null을 보존한다.
		if st.Status != "partial" || len(st.Limitations) < 2 {
			t.Fatalf("static 판정 변경: %s", staticText)
		}
		if shadow.Error != nil || body.SyncedAt != nil || !strings.Contains(string(shadow.Body), `"syncedAt":null`) {
			t.Fatalf("shadow=%s", shadowText)
		}
		// 차이: Static은 토픽 configs를 목록에서 버린다.
		if strings.Contains(staticText, "min.insync.replicas") || !strings.Contains(shadowText, "min.insync.replicas") {
			t.Fatal("configs 공개 범위 차이가 바뀜")
		}
	})

	t.Run("list_consumer_groups", func(t *testing.T) {
		_, staticText := invokeText(t, staticCS, "list_consumer_groups", map[string]any{"cluster_id": "c1"})
		_, shadowText := invokeText(t, shadowCS, "list_consumer_groups", map[string]any{"id": "c1"})
		requireSameRequests(t, backend.take())
		st := staticResult[tools.SnapshotData[ableops.ConsumerGroup]](t, staticText)
		shadow := dynamicResult(t, shadowText)
		if st.Status != "ok" || len(st.Data.Items) != 1 || st.Data.Items[0].Name != "g1" {
			t.Fatalf("static=%s", staticText)
		}
		// Dynamic은 2^53을 넘는 Lag를 원문 그대로 전달한다.
		if !strings.Contains(string(shadow.Body), `"totalLag":9007199254740993`) || !strings.Contains(string(shadow.Body), `"orders":9007199254740993`) {
			t.Fatalf("Dynamic 정밀도 손실: %s", shadow.Body)
		}
		// 차이(기존 Static 결함): SDK 제네릭 AddTool이 출력 스키마 default를 적용하며 결과를
		// float64로 해석했다가 다시 직렬화하므로 2^53을 넘는 int64가 반올림된다.
		// Static 쪽을 고치면 이 단언과 docs/api-mapping.md의 차이 기록을 함께 갱신한다.
		if st.Data.Items[0].TotalLag != 9007199254740992 || !strings.Contains(staticText, `"totalLag":9007199254740992`) {
			t.Fatalf("Static 정밀도 동작 변경: %d %s", st.Data.Items[0].TotalLag, staticText)
		}
	})

	// SHADOW 도구(get_topic·list_events)는 기본 서버에 없고 비교용 서버에서만 원문으로 실행된다.
	t.Run("shadow_tools_run_only_on_comparison_server", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			args  map[string]any
			path  string
			query string
		}{
			{"get_topic", map[string]any{"id": "c1", "name": "orders"}, "/api/clusters/c1/topics/orders", ""},
			{"list_events", map[string]any{"clusterId": "c1"}, "/api/events", "clusterId=c1"},
		} {
			if staticTools[tc.name] != nil {
				t.Fatalf("%s가 기본 서버에 노출됨", tc.name)
			}
			res, text := invokeText(t, shadowCS, tc.name, tc.args)
			out := dynamicResult(t, text)
			if res.IsError || out.HTTPStatus != 200 || string(out.Body) != e2eResponses[tc.path] {
				t.Fatalf("%s=%s", tc.name, text)
			}
			if requests := backend.take(); len(requests) != 1 || requests[0].path != tc.path || requests[0].query != tc.query || requests[0].auth != "Bearer "+e2eToken {
				t.Fatalf("%s requests=%+v", tc.name, requests)
			}
		}
	})
}

// requireSameRequests는 두 구현이 같은 REST 요청을 같은 인증으로 보냈는지 확인한다.
func requireSameRequests(t *testing.T, requests []e2eRequest) {
	t.Helper()
	if len(requests) != 2 || requests[0] != requests[1] || requests[0].auth != "Bearer "+e2eToken {
		t.Fatalf("requests=%+v", requests)
	}
}

func TestDynamicStartupFailureKeepsStaticTools(t *testing.T) {
	for name, tc := range map[string]struct {
		code int
		spec []byte
		want string
	}{
		"계약 서버 오류": {http.StatusInternalServerError, []byte("down"), "backend_unavailable"},
		"계약 검증 실패": {http.StatusOK, []byte(`{"openapi":"3.1.0","servers":[{"url":"https://evil.example.test"}],"paths":{"/api/x":{}}}`), "unsupported_servers"},
		"JSON 오류":  {http.StatusOK, []byte(`<html>`), "invalid_json"},
	} {
		t.Run(name, func(t *testing.T) {
			client, backend := newE2E(t, tc.code, tc.spec)
			registry, err := dynamic.Load(context.Background(), openapi.NewLoader(client), dynamic.Options{StaticToolNames: tools.StaticNames()})
			var upstream *ableops.Error
			var contract *openapi.ContractError
			code := ""
			switch {
			case errors.As(err, &upstream):
				code = upstream.Code
			case errors.As(err, &contract):
				code = contract.Code
			}
			if registry != nil || code != tc.want {
				t.Fatalf("registry=%v err=%v", registry, err)
			}
			cs := session(t, New(client, nil, WithDynamic(registry)))
			if listed := toolMap(t, cs); len(listed) != staticCount() {
				t.Fatalf("tools=%d", len(listed))
			}
			if res, _ := invokeText(t, cs, "list_clusters", map[string]any{}); res.IsError {
				t.Fatal("Static 도구 실패")
			}
			if requests := backend.take(); len(requests) != 1 || requests[0].path != "/api/clusters" {
				t.Fatalf("requests=%+v", requests)
			}
		})
	}
}

// HTTP 전송에서는 공용 토큰 없이 계약을 적재하고, 동적 도구도 요청별 위임 토큰으로만 호출한다.
func TestDynamicHTTPUsesDelegatedCredentials(t *testing.T) {
	spec, err := os.ReadFile("../openapi/testdata/ableops-openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/openapi.json" {
			if r.Header.Get("Authorization") != "" {
				t.Error("계약 조회에 인증 헤더")
			}
			w.Write(spec)
			return
		}
		user := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer synthetic-delegated-")
		if r.Header.Get("X-Request-ID") == "" {
			t.Error("추적 ID 누락")
		}
		// 사용자 a만 클러스터 a의 토픽을 볼 수 있다.
		if user != "a" || r.URL.Path != "/api/clusters/a/topics/t1/partitions" {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
		io.WriteString(w, `{"topic":"t1","owner":"owner-a"}`)
	}))
	defer backend.Close()
	base, _ := url.Parse(backend.URL)
	rest, err := ableops.NewClient(config.Config{BaseURL: base, AllowHTTP: true, Timeout: 2 * time.Second, RequireRequestCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rest.CloseIdleConnections()
	registry, err := dynamic.Load(context.Background(), openapi.NewLoader(rest), dynamic.Options{StaticToolNames: tools.StaticNames(), Operations: []string{"getTopicPartitions"}})
	if err != nil {
		t.Fatal(err)
	}
	var logs lockedLogBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler, err := NewHTTPHandler(New(rest, logger, WithDynamic(registry)), HTTPOptions{
		Logger: logger,
		Authenticate: func(_ context.Context, token string) (requestctx.Principal, error) {
			user := strings.TrimPrefix(token, "synthetic-mcp-")
			if user != "a" && user != "b" {
				return requestctx.Principal{}, errors.New("unknown")
			}
			return requestctx.Principal{UserID: "user-" + user, ClientID: "test", BackendToken: "synthetic-delegated-" + user}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	sessions := map[string]*mcp.ClientSession{"a": sdkHTTPSession(t, server, "synthetic-mcp-a"), "b": sdkHTTPSession(t, server, "synthetic-mcp-b")}
	if listed := toolMap(t, sessions["a"]); len(listed) != staticCount()+1 || listed["get_topic_partitions"] == nil {
		t.Fatalf("tools=%d", len(listed))
	}
	var wg sync.WaitGroup
	for user, want := range map[string]string{"a": `"body":{"topic":"t1","owner":"owner-a"}`, "b": `"code":"access_denied"`} {
		wg.Add(1)
		go func(user, want string) {
			defer wg.Done()
			for range 5 {
				res, err := sessions[user].CallTool(context.Background(), &mcp.CallToolParams{Name: "get_topic_partitions", Arguments: map[string]any{"id": "a", "name": "t1"}})
				if err != nil {
					t.Error(err)
					return
				}
				text := res.Content[0].(*mcp.TextContent).Text
				if !strings.Contains(text, want) || (user == "b") != res.IsError {
					t.Errorf("user %s: %s", user, text)
					return
				}
			}
		}(user, want)
	}
	wg.Wait()
	for _, secret := range []string{"synthetic-delegated-a", "synthetic-delegated-b", "synthetic-mcp-a", "owner-a"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("로그에 %s 노출", secret)
		}
	}
	if !strings.Contains(logs.String(), `"user_id":"user-b"`) || !strings.Contains(logs.String(), `"result_code":"access_denied"`) {
		t.Fatalf("사용자별 로그 누락: %s", logs.String())
	}
}

func TestDynamicRegisterNeverReplacesStaticTool(t *testing.T) {
	client, _ := newE2E(t, http.StatusOK, nil)
	registry := loadRegistry(t, client)
	server := New(client, nil)
	static := tools.StaticNames()
	// 배치 계산이 잘못되어 Static과 같은 이름의 도구가 넘어와도 등록 경계에서 거부한다.
	var conflicts []*dynamic.Tool
	for _, entry := range registry.Entries() {
		if entry.StaticConflict {
			conflicts = append(conflicts, entry.Tool)
		}
	}
	if len(conflicts) != 11 {
		t.Fatalf("conflicts=%d", len(conflicts))
	}
	if registered := dynamic.Register(server, client, nil, conflicts, static); len(registered) != 0 {
		t.Fatalf("registered=%v", registered)
	}
	listed := toolMap(t, session(t, server))
	if len(listed) != staticCount() || listed["list_topics"].Meta["ableops/openapi"] != nil {
		t.Fatal("Static 도구가 교체됨")
	}
}
