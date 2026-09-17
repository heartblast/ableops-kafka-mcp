package dynamic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const runtimeToken = "synthetic-runtime-session-token"

// exposeAll은 합성 계약의 Operation을 모두 SAFE로 본다. 게이트가 아니라 교체 동작을 검증할 때 쓴다.
func exposeAll(openapi.Operation) ExposureRule {
	return ExposureRule{Exposure: ExposureSafe, Reason: "테스트"}
}

// runtimeSpec은 GET Operation 목록으로 합성 계약을 만든다. 값은 "operationId|경로|설명" 형식이다.
func runtimeSpec(ops ...string) string {
	var paths []string
	for _, op := range ops {
		parts := strings.SplitN(op, "|", 3)
		paths = append(paths, pathItem(parts[0], parts[1], parts[2], ""))
	}
	return specOf(paths...)
}

// pathItem은 GET 하나를 가진 경로 항목이다. params는 parameters 배열 JSON(없으면 빈 값)이다.
func pathItem(id, path, summary, params string) string {
	extra := ""
	if params != "" {
		extra = `"parameters":` + params + `,`
	}
	return fmt.Sprintf(`%q:{"get":{"operationId":%q,"summary":%q,"x-mcp-enabled":true,%s%s}}`, path, id, summary, extra, jsonOK)
}

func specOf(paths ...string) string {
	return `{"openapi":"3.1.0","servers":[{"url":"/"}],"paths":{` + strings.Join(paths, ",") + `}}`
}

var (
	specA = runtimeSpec("getAlpha|/api/alpha|알파 조회", "getBeta|/api/beta|베타 조회")
	// specB: beta 삭제, alpha 설명 변경, gamma 추가.
	specB = runtimeSpec("getAlpha|/api/alpha|알파 조회(개정)", "getGamma|/api/gamma|감마 조회")
)

// runtimeBackend는 계약 문서와 업무 GET을 제공한다. 계약은 ETag가 같으면 304로 답한다.
type runtimeBackend struct {
	mu          sync.Mutex
	status      int
	spec        string
	etag        string
	delay       time.Duration
	fetches     int
	conditional []string
	business    atomic.Int64
	callDelay   time.Duration
	// queries는 업무 GET의 원본 query 문자열이다(도착 순).
	queries []string
}

func (b *runtimeBackend) lastQuery() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.queries) == 0 {
		return ""
	}
	return b.queries[len(b.queries)-1]
}

func (b *runtimeBackend) set(status int, spec, etag string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status, b.spec, b.etag, b.delay = status, spec, etag, 0
}

func (b *runtimeBackend) stall(delay time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.delay = delay
}

func (b *runtimeBackend) snapshot() (int, []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.fetches, append([]string(nil), b.conditional...)
}

func (b *runtimeBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/openapi.json" {
		b.business.Add(1)
		b.mu.Lock()
		delay := b.callDelay
		b.queries = append(b.queries, r.URL.RawQuery)
		b.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		if r.URL.Path == "/api/empty" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"path":"`+r.URL.Path+`","big":9007199254740993}`)
		return
	}
	b.mu.Lock()
	b.fetches++
	match := r.Header.Get("If-None-Match")
	b.conditional = append(b.conditional, match)
	status, spec, etag, delay := b.status, b.spec, b.etag, b.delay
	b.mu.Unlock()
	if r.Header.Get("Authorization") != "" {
		http.Error(w, "unexpected auth", http.StatusBadRequest)
		return
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if status == http.StatusOK && match != "" && match == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	w.WriteHeader(status)
	io.WriteString(w, spec)
}

type runtimeHarness struct {
	backend *runtimeBackend
	client  *ableops.Client
	runtime *Runtime
	server  *mcp.Server
	logs    *lockedBuffer
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newRuntimeHarness는 넉넉한 REST 제한시간으로 하네스를 만든다. 부하가 큰 CI(-race)에서도
// 동시 호출·계약 조회가 제한시간에 걸리지 않게 한다.
func newRuntimeHarness(t *testing.T, opts Options) *runtimeHarness {
	t.Helper()
	return newRuntimeHarnessTimeout(t, opts, 30*time.Second)
}

// newRuntimeHarnessTimeout은 timeout 실패를 재현할 때처럼 REST 제한시간을 직접 정한다.
func newRuntimeHarnessTimeout(t *testing.T, opts Options, timeout time.Duration) *runtimeHarness {
	t.Helper()
	backend := &runtimeBackend{status: http.StatusOK, spec: specA, etag: `"A"`}
	httpServer := httptest.NewServer(backend)
	t.Cleanup(httpServer.Close)
	base, _ := url.Parse(httpServer.URL)
	client, err := ableops.NewClient(config.Config{BaseURL: base, Token: runtimeToken, Timeout: timeout, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	if opts.Exposure == nil {
		opts.Exposure = exposeAll
	}
	if opts.Operations == nil {
		opts.Operations = []string{"getAlpha", "getBeta", "getGamma", "getDelta"}
	}
	logs := &lockedBuffer{}
	rt := NewRuntime(client, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})), openapi.NewLoader(client), opts)
	// 운영 서버는 Static 도구가 항상 있어 tools capability가 광고된다. 도구 0개로 시작하는 이
	// 하네스도 같은 조건을 만들어야 최신 프로토콜 클라이언트가 list_changed를 구독한다.
	server := mcp.NewServer(&mcp.Implementation{Name: "runtime-test", Version: "0"}, &mcp.ServerOptions{
		Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{ListChanged: true}},
	})
	if err := rt.Bind(server, opts.StaticToolNames); err != nil {
		t.Fatal(err)
	}
	return &runtimeHarness{backend: backend, client: client, runtime: rt, server: server, logs: logs}
}

// notifications는 클라이언트가 받은 tools/list_changed 수를 센다. ch는 깨우기 신호일 뿐이며
// 가득 차도 핸들러를 막지 않는다(막히면 세션 종료가 핸들러를 기다리며 멈춘다).
type notifications struct {
	count atomic.Int32
	ch    chan struct{}
}

// settleWindow는 SDK 디바운스(10ms)보다 충분히 긴, 늦게 온 알림을 모으는 시간이다.
const settleWindow = 150 * time.Millisecond

// expectChange는 base 이후 알림을 기다리고, 늦게 온 알림까지 모은 증가분이 1 이상 limit 이하인지
// 확인한 뒤 누적 수를 돌려준다. SDK는 AddTool·RemoveTools 호출마다 알림을 예약하고 10ms 안의
// 연속 호출만 한 번으로 묶으므로, 정상 조건은 1회이고 부하 시 상한은 SDK 변경 호출 수다.
func (n *notifications) expectChange(t *testing.T, base, limit int32) int32 {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for n.count.Load() <= base {
		select {
		case <-n.ch:
		case <-deadline:
			t.Fatal("tools/list_changed 알림을 받지 못했습니다")
		}
	}
	time.Sleep(settleWindow)
	total := n.count.Load()
	if got := total - base; got < 1 || got > limit {
		t.Fatalf("알림 증가분=%d, 기대 1~%d", got, limit)
	}
	return total
}

// expectNone은 늦게 온 알림을 기다린 뒤에도 누적 수가 base 그대로인지 확인한다.
func (n *notifications) expectNone(t *testing.T, base int32) {
	t.Helper()
	time.Sleep(settleWindow)
	if got := n.count.Load(); got != base {
		t.Fatalf("알림 수=%d, 기대=%d", got, base)
	}
}

// sdkChangeCalls는 Diff를 반영할 때 부르는 SDK 변경 호출 수(알림 상한)다.
func sdkChangeCalls(d Diff) int32 {
	calls := int32(len(d.Added) + len(d.Changed))
	if len(d.Removed) > 0 {
		calls++
	}
	return calls
}

// connect는 알림을 받는 공식 SDK 클라이언트를 연결한다. protocol이 비면 SDK 기본(최신) 버전이다.
// 최신 프로토콜은 ToolListChangedHandler가 있으면 subscriptions/listen으로 알림을 구독한다.
func connect(t *testing.T, server *mcp.Server, protocol string) (*mcp.ClientSession, *notifications) {
	t.Helper()
	n := &notifications{ch: make(chan struct{}, 64)}
	st, ct := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "runtime-client", Version: "0"}, &mcp.ClientOptions{
		ToolListChangedHandler: func(context.Context, *mcp.ToolListChangedRequest) {
			n.count.Add(1)
			select {
			case n.ch <- struct{}{}:
			default:
			}
		},
	})
	// 최신 프로토콜의 구독 요청은 응답을 기다리지 않으므로, 서버 등록을 뜻하는 ack를 받은 뒤에 반환한다.
	acked := make(chan struct{}, 1)
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
	var opts *mcp.ClientSessionOptions
	if protocol != "" {
		opts = &mcp.ClientSessionOptions{ProtocolVersion: protocol}
	}
	cs, err := client.Connect(ctx, ct, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	if caps := cs.InitializeResult().Capabilities; protocol == "" && caps != nil && caps.Tools != nil && caps.Tools.ListChanged {
		select {
		case <-acked:
		case <-time.After(5 * time.Second):
			t.Fatal("subscriptions/listen 확인 응답을 받지 못했습니다")
		}
	}
	return cs, n
}

func listed(t *testing.T, cs *mcp.ClientSession) map[string]*mcp.Tool {
	t.Helper()
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*mcp.Tool{}
	for _, tool := range res.Tools {
		out[tool.Name] = tool
	}
	return out
}

func toolNames(tools map[string]*mcp.Tool) []string {
	out := make([]string, 0, len(tools))
	for name := range tools {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func requireReport(t *testing.T, got RefreshReport, outcome RefreshOutcome, code string) {
	t.Helper()
	if got.Outcome != outcome || got.Code != code {
		t.Fatalf("report=%+v, 기대 outcome=%s code=%s", got, outcome, code)
	}
}

// 종료 기준: Registry A 서비스 → ETag 변경 → 새 계약 → 검증 → Registry B → 원자적 교체 → Diff → list_changed.
// 잘못된 계약은 검증에서 실패하고 Registry A를 유지하며 기존 도구를 계속 호출할 수 있다.
func TestRuntimeHotReloadLifecycle(t *testing.T) {
	for _, protocol := range []string{"", "2025-11-25"} {
		name := protocol
		if name == "" {
			name = "latest_subscriptions_listen"
		}
		t.Run(name, func(t *testing.T) {
			// timeout 실패를 1초 안에 재현하려고 짧은 REST 제한시간을 쓴다.
			h := newRuntimeHarnessTimeout(t, Options{}, time.Second)
			ctx := context.Background()
			initial := h.runtime.Refresh(ctx)
			requireReport(t, initial, RefreshUpdated, "")
			if !reflect.DeepEqual(initial.Diff.Added, []string{"get_alpha", "get_beta"}) || !initial.ETagChanged {
				t.Fatalf("initial=%+v", initial)
			}
			registryA := h.runtime.Current()
			cs, n := connect(t, h.server, protocol)
			if got := toolNames(listed(t, cs)); !reflect.DeepEqual(got, []string{"get_alpha", "get_beta"}) {
				t.Fatalf("Registry A tools=%v", got)
			}

			// 304: Registry A를 그대로 유지(다시 만들지 않음), 알림 없음.
			notModified := h.runtime.Refresh(ctx)
			requireReport(t, notModified, RefreshNotModified, "")
			if notModified.Registry != registryA || h.runtime.Current() != registryA {
				t.Fatal("304에서 Registry가 바뀜")
			}
			// ETag만 변경: 도구 정의가 같으므로 Registry는 새 값이지만 알림 없음.
			h.backend.set(http.StatusOK, specA, `"A2"`)
			etagOnly := h.runtime.Refresh(ctx)
			requireReport(t, etagOnly, RefreshUnchanged, "")
			if !etagOnly.ETagChanged || etagOnly.Diff.ToolSetChanged() || h.runtime.Current().ETag() != `"A2"` {
				t.Fatalf("etag only=%+v", etagOnly)
			}
			registryA = h.runtime.Current()

			// 실패 계열: 모두 Registry A 유지, 알림 없음, 다음 조회도 A의 ETag로 확인한다.
			failures := []struct {
				name   string
				status int
				spec   string
				delay  time.Duration
				code   string
			}{
				{"500", http.StatusInternalServerError, "down", 0, "backend_unavailable"},
				{"timeout", http.StatusOK, specB, 5 * time.Second, "timeout"},
				{"invalid JSON", http.StatusOK, "<html>", 0, "invalid_json"},
				{"invalid contract", http.StatusOK, strings.Replace(specB, `"url":"/"`, `"url":"https://evil.example.test"`, 1), 0, "unsupported_servers"},
				{"empty paths", http.StatusOK, `{"openapi":"3.1.0","servers":[{"url":"/"}],"paths":{}}`, 0, "missing_paths"},
			}
			for _, f := range failures {
				h.backend.set(f.status, f.spec, `"BAD-`+f.name+`"`)
				h.backend.stall(f.delay)
				report := h.runtime.Refresh(ctx)
				requireReport(t, report, RefreshFailed, f.code)
				if h.runtime.Current() != registryA || report.Registry != registryA {
					t.Fatalf("%s: Last Known Good가 바뀜", f.name)
				}
				if got := toolNames(listed(t, cs)); !reflect.DeepEqual(got, []string{"get_alpha", "get_beta"}) {
					t.Fatalf("%s: tools=%v", f.name, got)
				}
				// 기존 도구는 계속 정상 호출된다.
				res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "get_beta", Arguments: map[string]any{}})
				if err != nil || res.IsError {
					t.Fatalf("%s: 기존 도구 호출 실패 %v %+v", f.name, err, res)
				}
			}
			// 빌드 실패도 같은 규칙이다. 실패한 계약의 ETag를 보관하지 않는다.
			h.backend.set(http.StatusOK, specB, `"B"`)
			h.runtime.build = func(*openapi.Contract, Options) (*Registry, error) {
				return nil, compileError("synthetic_build_failure", "합성 빌드 실패")
			}
			requireReport(t, h.runtime.Refresh(ctx), RefreshFailed, "synthetic_build_failure")
			h.runtime.build = Build
			if h.runtime.Current() != registryA {
				t.Fatal("빌드 실패 후 Registry가 바뀜")
			}
			_, conditional := h.backend.snapshot()
			for _, sent := range conditional[len(conditional)-len(failures)-1:] {
				if sent != `"A2"` {
					t.Fatalf("실패 뒤 조건부 헤더=%v", conditional)
				}
			}
			n.expectNone(t, 0)

			// 정상 갱신: beta 삭제, alpha 정의 변경, gamma 추가 → 원자적 교체 → 알림.
			updated := h.runtime.Refresh(ctx)
			requireReport(t, updated, RefreshUpdated, "")
			want := Diff{Added: []string{"get_gamma"}, Removed: []string{"get_beta"}, Changed: []string{"get_alpha"}}
			if !reflect.DeepEqual(updated.Diff.Added, want.Added) || !reflect.DeepEqual(updated.Diff.Removed, want.Removed) ||
				!reflect.DeepEqual(updated.Diff.Changed, want.Changed) || len(updated.Diff.Unchanged) != 0 {
				t.Fatalf("diff=%+v", updated.Diff)
			}
			seen := n.expectChange(t, 0, sdkChangeCalls(updated.Diff))
			tools := listed(t, cs)
			if got := toolNames(tools); !reflect.DeepEqual(got, []string{"get_alpha", "get_gamma"}) {
				t.Fatalf("Registry B tools=%v", got)
			}
			if !strings.Contains(tools["get_alpha"].Description, "알파 조회(개정)") {
				t.Fatalf("alpha 정의가 교체되지 않음: %s", tools["get_alpha"].Description)
			}
			if !reflect.DeepEqual(h.runtime.Published(), []string{"get_alpha", "get_gamma"}) {
				t.Fatalf("published=%v", h.runtime.Published())
			}
			// 삭제된 도구는 더 이상 호출할 수 없고, 새 도구는 호출된다.
			if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "get_beta", Arguments: map[string]any{}}); err == nil {
				t.Fatal("삭제된 도구가 호출됨")
			}
			res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "get_gamma", Arguments: map[string]any{}})
			if err != nil || res.IsError || !strings.Contains(res.Content[0].(*mcp.TextContent).Text, `"big":9007199254740993`) {
				t.Fatalf("새 도구 호출 %v %+v", err, res)
			}
			// 같은 계약 재조회는 304이며 Registry B를 유지하고 알림이 늘지 않는다.
			again := h.runtime.Refresh(ctx)
			requireReport(t, again, RefreshNotModified, "")
			if again.Registry != updated.Registry || h.runtime.Current() != updated.Registry {
				t.Fatal("304에서 Registry B가 바뀜")
			}
			n.expectNone(t, seen)

			logs := h.logs.String()
			for _, fragment := range []string{`"msg":"동적 도구 계약 갱신 실패: 마지막 정상 도구 목록을 유지합니다"`, `"outcome":"updated"`, `"removed":["get_beta"]`, `"msg":"동적 도구 계약 변경 없음"`} {
				if !strings.Contains(logs, fragment) {
					t.Fatalf("로그 누락 %s: %s", fragment, logs)
				}
			}
			if strings.Contains(logs, runtimeToken) || strings.Contains(logs, "evil.example.test") || strings.Contains(logs, "9007199254740993") {
				t.Fatal("로그에 비밀·원문 노출")
			}
		})
	}
}

// 기동 시 적재에 실패해도 Static만으로 서비스하고, 다음 갱신이 성공하면 도구를 추가하고 알린다.
func TestRuntimeRecoversAfterInitialFailure(t *testing.T) {
	h := newRuntimeHarness(t, Options{})
	h.backend.set(http.StatusServiceUnavailable, "down", "")
	ctx := context.Background()
	report := h.runtime.Refresh(ctx)
	requireReport(t, report, RefreshFailed, "backend_unavailable")
	if report.Registry != nil || h.runtime.Current() != nil {
		t.Fatal("실패한 적재가 Registry를 만듦")
	}
	cs, n := connect(t, h.server, "")
	if got := listed(t, cs); len(got) != 0 {
		t.Fatalf("tools=%v", toolNames(got))
	}
	if !strings.Contains(h.logs.String(), "다음 갱신 주기에 다시 시도합니다") {
		t.Fatal(h.logs.String())
	}
	h.backend.set(http.StatusOK, specA, "")
	recovered := h.runtime.Refresh(ctx)
	requireReport(t, recovered, RefreshUpdated, "")
	seen := n.expectChange(t, 0, sdkChangeCalls(recovered.Diff))
	if got := toolNames(listed(t, cs)); !reflect.DeepEqual(got, []string{"get_alpha", "get_beta"}) {
		t.Fatalf("tools=%v", got)
	}
	// ETag가 없는 계약은 매번 전체를 받지만 정의가 같으면 알리지 않는다.
	requireReport(t, h.runtime.Refresh(ctx), RefreshUnchanged, "")
	n.expectNone(t, seen)
	if _, conditional := h.backend.snapshot(); conditional[len(conditional)-1] != "" {
		t.Fatalf("ETag 없이 조건부 요청: %v", conditional)
	}
}

// 일부 Operation이 지원되지 않으면 그 Operation만 제외(skip + warning)하고 나머지는 유지한다.
func TestRuntimeUnsupportedOperationIsolated(t *testing.T) {
	h := newRuntimeHarness(t, Options{})
	ctx := context.Background()
	requireReport(t, h.runtime.Refresh(ctx), RefreshUpdated, "")
	cs, n := connect(t, h.server, "")
	broken := specOf(
		pathItem("getAlpha", "/api/alpha", "알파 조회", ""),
		pathItem("getBeta", "/api/beta", "베타 조회", `[{"name":"x","in":"header","schema":{"type":"string"}}]`),
		pathItem("getDelta", "/api/delta", "델타 조회", `[{"name":"f","in":"query","schema":{"type":"object"}}]`),
	)
	h.backend.set(http.StatusOK, broken, `"A-broken"`)
	report := h.runtime.Refresh(ctx)
	requireReport(t, report, RefreshUpdated, "")
	// beta는 새 계약에서 실행할 수 없는 형태가 되었으므로 옛 정의로 남기지 않고 제거한다.
	if !reflect.DeepEqual(report.Diff.Removed, []string{"get_beta"}) || len(report.Diff.Added) != 0 || !reflect.DeepEqual(report.Diff.Unchanged, []string{"get_alpha"}) {
		t.Fatalf("diff=%+v", report.Diff)
	}
	n.expectChange(t, 0, sdkChangeCalls(report.Diff))
	if got := toolNames(listed(t, cs)); !reflect.DeepEqual(got, []string{"get_alpha"}) {
		t.Fatalf("tools=%v", got)
	}
	codes := map[string]string{}
	for _, issue := range report.Registry.Skipped() {
		codes[issue.OperationID] = issue.Code
	}
	if codes["getBeta"] != openapi.IssueUnsupportedParameter || codes["getDelta"] != openapi.IssueUnsupportedSchema {
		t.Fatalf("skipped=%v", codes)
	}
	for _, fragment := range []string{`"msg":"동적 도구 제외"`, `"operation_id":"getBeta"`, `"operation_id":"getDelta"`} {
		if !strings.Contains(h.logs.String(), fragment) {
			t.Fatalf("경고 누락 %s", fragment)
		}
	}
}

// upstream이 x-mcp-enabled를 모두 끈 계약은 회수로 보고 Dynamic 도구를 모두 제거한다.
func TestRuntimeHonorsUpstreamRevocation(t *testing.T) {
	h := newRuntimeHarness(t, Options{})
	ctx := context.Background()
	requireReport(t, h.runtime.Refresh(ctx), RefreshUpdated, "")
	cs, n := connect(t, h.server, "")
	h.backend.set(http.StatusOK, strings.ReplaceAll(specA, `"x-mcp-enabled":true`, `"x-mcp-enabled":false`), `"revoked"`)
	report := h.runtime.Refresh(ctx)
	requireReport(t, report, RefreshUpdated, "")
	if !reflect.DeepEqual(report.Diff.Removed, []string{"get_alpha", "get_beta"}) {
		t.Fatalf("diff=%+v", report.Diff)
	}
	// 삭제만 있으면 RemoveTools 한 번이므로 알림도 정확히 한 번이다.
	n.expectChange(t, 0, sdkChangeCalls(report.Diff))
	if got := listed(t, cs); len(got) != 0 {
		t.Fatalf("회수 후 tools=%v", toolNames(got))
	}
}

// 정의는 같고 실행 바인딩만 바뀌면 알림 없이 실행 대상만 바꾼다. 실제 REST 호출이 새 바인딩을
// 따르는지(query 표기, 선언된 204 처리)로 확인한다.
func TestRuntimeRebindsWithoutNotification(t *testing.T) {
	h := newRuntimeHarness(t, Options{Operations: []string{"getAlpha", "getEmpty"}})
	contract := func(explode, declare204 bool) string {
		empty := `"responses":{"200":{"description":"ok","content":{"application/json":{"schema":{"type":"object"}}}}}`
		if declare204 {
			empty = `"responses":{"200":{"description":"ok","content":{"application/json":{"schema":{"type":"object"}}}},"204":{"description":"없음"}}`
		}
		return specOf(
			pathItem("getAlpha", "/api/alpha", "알파 조회", fmt.Sprintf(`[{"name":"s","in":"query","explode":%t,"schema":{"type":"array","items":{"type":"string"}}}]`, explode)),
			fmt.Sprintf(`"/api/empty":{"get":{"operationId":"getEmpty","summary":"빈 조회","x-mcp-enabled":true,%s}}`, empty),
		)
	}
	call := func(cs *mcp.ClientSession, name string, args map[string]any) Result {
		t.Helper()
		res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var out Result
		if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &out); err != nil || res.IsError != (out.Error != nil) {
			t.Fatalf("%s: %+v", name, res)
		}
		return out
	}
	h.backend.set(http.StatusOK, contract(true, false), `"E1"`)
	ctx := context.Background()
	requireReport(t, h.runtime.Refresh(ctx), RefreshUpdated, "")
	cs, n := connect(t, h.server, "")
	pair := map[string]any{"s": []any{"a", "b"}}
	if out := call(cs, "get_alpha", pair); out.Error != nil || h.backend.lastQuery() != "s=a&s=b" {
		t.Fatalf("explode=true query=%q out=%+v", h.backend.lastQuery(), out)
	}
	// 계약이 204를 선언하지 않았으므로 204 응답은 응답 결함이다.
	if out := call(cs, "get_empty", nil); out.Error == nil || out.Error.Code != "invalid_response" {
		t.Fatalf("204 미선언=%+v", out)
	}

	h.backend.set(http.StatusOK, contract(false, true), `"E2"`)
	report := h.runtime.Refresh(ctx)
	requireReport(t, report, RefreshUnchanged, "")
	if !reflect.DeepEqual(report.Diff.Rebound, []string{"get_alpha", "get_empty"}) || report.Diff.ToolSetChanged() {
		t.Fatalf("diff=%+v", report.Diff)
	}
	// 같은 도구 정의로 호출해도 새 바인딩(콤마 목록, 선언된 204)을 따른다.
	if out := call(cs, "get_alpha", pair); out.Error != nil || h.backend.lastQuery() != "s=a%2Cb" {
		t.Fatalf("explode=false query=%q out=%+v", h.backend.lastQuery(), out)
	}
	if out := call(cs, "get_empty", nil); out.Error != nil || !out.NoContent || out.HTTPStatus != http.StatusNoContent {
		t.Fatalf("204 선언=%+v", out)
	}
	n.expectNone(t, 0)
	entry, _ := h.runtime.Current().Lookup("get_alpha")
	if entry.Tool.Operation.Parameters[0].Explode {
		t.Fatal("현재 Registry가 새 바인딩이 아님")
	}
}

// Registry 교체 중에도 tools/list는 교체 전·후의 완전한 목록만 보이고, 진행 중 호출은
// panic 없이 끝나며, 교체와 무관한 도구 호출은 실패하지 않는다. -race 대상이다.
func TestRuntimeConcurrentSwap(t *testing.T) {
	h := newRuntimeHarness(t, Options{})
	h.backend.mu.Lock()
	h.backend.callDelay = 2 * time.Millisecond
	h.backend.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	requireReport(t, h.runtime.Refresh(ctx), RefreshUpdated, "")
	cs, _ := connect(t, h.server, "")
	legacy, _ := connect(t, h.server, "2025-11-25")

	setA := []string{"get_alpha", "get_beta"}
	setB := []string{"get_alpha", "get_gamma"}
	var wg sync.WaitGroup
	var failures sync.Map
	fail := func(key string, value any) { failures.LoadOrStore(key, fmt.Sprint(value)) }
	done := make(chan struct{})

	for _, session := range []*mcp.ClientSession{cs, legacy} {
		for range 2 {
			wg.Add(1)
			go func(session *mcp.ClientSession) {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}
					res, err := session.ListTools(ctx, nil)
					if err != nil {
						fail("list", err)
						return
					}
					names := make([]string, 0, len(res.Tools))
					var alpha string
					for _, tool := range res.Tools {
						names = append(names, tool.Name)
						if tool.Name == "get_alpha" {
							alpha = tool.Description
						}
					}
					sort.Strings(names)
					// 부분 목록이나 A·B가 섞인 정의가 보이면 안 된다.
					switch {
					case reflect.DeepEqual(names, setA) && !strings.Contains(alpha, "(개정)"):
					case reflect.DeepEqual(names, setB) && strings.Contains(alpha, "(개정)"):
					default:
						fail("partial", fmt.Sprintf("%v %q", names, alpha))
						return
					}
				}
			}(session)
		}
		for _, name := range []string{"get_alpha", "get_beta", "get_gamma"} {
			wg.Add(1)
			go func(session *mcp.ClientSession, name string) {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}
					res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: map[string]any{}})
					switch {
					case err != nil && name == "get_alpha":
						// alpha는 A·B 모두에 있으므로 교체 중에도 사라지면 안 된다.
						fail("alpha", err)
						return
					case err != nil && !strings.Contains(err.Error(), "unknown tool"):
						fail("call "+name, err)
						return
					case err == nil && res.IsError:
						fail("result "+name, res.Content)
						return
					}
				}
			}(session, name)
		}
	}

	for i := range 40 {
		if i%2 == 0 {
			h.backend.set(http.StatusOK, specB, fmt.Sprintf(`"B%d"`, i))
		} else {
			h.backend.set(http.StatusOK, specA, fmt.Sprintf(`"A%d"`, i))
		}
		if report := h.runtime.Refresh(ctx); report.Outcome != RefreshUpdated {
			fail("refresh", report)
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(done)
	wg.Wait()
	failures.Range(func(key, value any) bool {
		t.Errorf("%s: %s", key, value)
		return true
	})
	if h.backend.business.Load() == 0 {
		t.Fatal("동시 호출이 실행되지 않음")
	}
}

// Run은 주기마다 조회하고 ctx가 끝나면 멈춘다. loader가 없으면 아무것도 조회하지 않는다.
func TestRuntimeRunPollsUntilCanceled(t *testing.T) {
	h := newRuntimeHarness(t, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		h.runtime.Run(ctx, 10*time.Millisecond)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if fetches, _ := h.backend.snapshot(); fetches >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("주기 조회가 일어나지 않음")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("취소 후 Run이 끝나지 않음")
	}
	after, _ := h.backend.snapshot()
	time.Sleep(50 * time.Millisecond)
	if fetches, _ := h.backend.snapshot(); fetches != after {
		t.Fatal("취소 후에도 조회함")
	}
	// 첫 조회만 200이고 이후는 304다.
	_, conditional := h.backend.snapshot()
	if conditional[0] != "" || conditional[1] != `"A"` {
		t.Fatalf("conditional=%v", conditional)
	}

	idle := NewRuntime(h.client, nil, nil, Options{})
	done := make(chan struct{})
	go func() { idle.Run(context.Background(), time.Millisecond); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loader 없는 Run이 반환하지 않음")
	}
	requireReport(t, idle.Refresh(context.Background()), RefreshFailed, "refresh_unavailable")
	// 주기가 0 이하이면 조회 없이 반환한다.
	fetches, _ := h.backend.snapshot()
	h.runtime.Run(context.Background(), 0)
	if now, _ := h.backend.snapshot(); now != fetches {
		t.Fatal("주기 0에서 조회함")
	}
}

// Static 도구 이름은 Registry 배치와 무관하게 Runtime 등록 경계에서 다시 거부한다.
func TestRuntimeNeverReplacesStaticTool(t *testing.T) {
	h := newRuntimeHarness(t, Options{StaticToolNames: nil})
	server := mcp.NewServer(&mcp.Implementation{Name: "static", Version: "0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "get_alpha", Description: "static alpha"}, func(context.Context, *mcp.CallToolRequest, map[string]any) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{}, nil, nil
	})
	rt := NewRuntime(h.client, nil, openapi.NewLoader(h.client), Options{Exposure: exposeAll, Operations: []string{"getAlpha", "getBeta"}})
	if err := rt.Bind(server, []string{"get_alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Bind(server, nil); err == nil {
		t.Fatal("두 번째 Bind 허용")
	}
	requireReport(t, rt.Refresh(context.Background()), RefreshUpdated, "")
	cs, _ := connect(t, server, "")
	tools := listed(t, cs)
	if tools["get_alpha"].Description != "static alpha" || tools["get_beta"] == nil || !reflect.DeepEqual(rt.Published(), []string{"get_beta"}) {
		t.Fatalf("tools=%v published=%v", toolNames(tools), rt.Published())
	}
}

// Bind 전에 Install한 Registry는 Bind에서 등록하고, 빈 선택은 기본 목록으로 바뀌지 않는다.
func TestRuntimeInstallBeforeBindAndEmptySelection(t *testing.T) {
	contract, err := openapi.Parse([]byte(specA))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := Build(contract, Options{Exposure: exposeAll, Operations: []string{"getAlpha"}})
	if err != nil {
		t.Fatal(err)
	}
	rt := NewRuntime(nil, nil, nil, Options{})
	if diff := rt.Install(registry); !reflect.DeepEqual(diff.Added, []string{"get_alpha"}) {
		t.Fatalf("diff=%+v", diff)
	}
	if diff := rt.Install(nil); diff.ToolSetChanged() || rt.Current() != registry {
		t.Fatal("nil Install이 상태를 바꿈")
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "install", Version: "0"}, nil)
	if err := rt.Bind(server, nil); err != nil {
		t.Fatal(err)
	}
	if err := NewRuntime(nil, nil, nil, Options{}).Bind(nil, nil); err == nil {
		t.Fatal("nil 서버 허용")
	}
	cs, _ := connect(t, server, "")
	if got := toolNames(listed(t, cs)); !reflect.DeepEqual(got, []string{"get_alpha"}) {
		t.Fatalf("tools=%v", got)
	}

	h := newRuntimeHarness(t, Options{Operations: []string{}})
	report := h.runtime.Refresh(context.Background())
	requireReport(t, report, RefreshUnchanged, "")
	if report.Registry.Stats().Selected != 0 || len(h.runtime.Published()) != 0 {
		t.Fatalf("빈 선택이 기본 목록으로 바뀜: %+v", report.Registry.Stats())
	}
}

func TestDiffTools(t *testing.T) {
	compile := func(spec string) []*Tool {
		t.Helper()
		contract, err := openapi.Parse([]byte(spec))
		if err != nil {
			t.Fatal(err)
		}
		registry, err := Build(contract, Options{Exposure: exposeAll, Operations: []string{"getAlpha", "getBeta", "getGamma", "getHTTPItem", "getHttpItem"}})
		if err != nil {
			t.Fatal(err)
		}
		return registry.Exposed()
	}
	a := compile(specA)
	same := compile(specA)
	if d := DiffTools(a, same); d.ToolSetChanged() || !reflect.DeepEqual(d.Unchanged, []string{"get_alpha", "get_beta"}) || len(d.Rebound) != 0 {
		t.Fatalf("same=%+v", d)
	}
	// 도구 정의에 쓰이지 않는 계약 부분(info·x-mcp-note·응답 설명)만 바뀌면 변경이 아니다.
	cosmetic := strings.Replace(specA, `"openapi":"3.1.0"`, `"openapi":"3.1.0","info":{"title":"새 제목","version":"9.9.9"}`, 1)
	cosmetic = strings.Replace(cosmetic, `"x-mcp-enabled":true`, `"x-mcp-enabled":true,"x-mcp-note":"새 메모"`, 1)
	cosmetic = strings.Replace(cosmetic, `"description":"ok"`, `"description":"바뀐 응답 설명"`, 1)
	if d := DiffTools(a, compile(cosmetic)); d.ToolSetChanged() {
		t.Fatalf("cosmetic=%+v", d)
	}
	cases := map[string]struct {
		spec string
		want Diff
	}{
		"설명": {runtimeSpec("getAlpha|/api/alpha|알파 조회!", "getBeta|/api/beta|베타 조회"), Diff{Changed: []string{"get_alpha"}}},
		"경로": {runtimeSpec("getAlpha|/api/alpha2|알파 조회", "getBeta|/api/beta|베타 조회"), Diff{Changed: []string{"get_alpha"}}},
		"입력 스키마": {strings.Replace(specA, `"operationId":"getAlpha"`, `"operationId":"getAlpha","parameters":[{"name":"q","in":"query","schema":{"type":"string"}}]`, 1),
			Diff{Changed: []string{"get_alpha"}}},
		"필수 여부": {strings.Replace(specA, `"operationId":"getAlpha"`, `"operationId":"getAlpha","parameters":[{"name":"q","in":"query","required":true,"schema":{"type":"string"}}]`, 1),
			Diff{Changed: []string{"get_alpha"}}},
		"추가·삭제": {runtimeSpec("getAlpha|/api/alpha|알파 조회", "getGamma|/api/gamma|감마 조회"), Diff{Added: []string{"get_gamma"}, Removed: []string{"get_beta"}}},
	}
	for name, tc := range cases {
		d := DiffTools(a, compile(tc.spec))
		if !reflect.DeepEqual(d.Added, tc.want.Added) || !reflect.DeepEqual(d.Removed, tc.want.Removed) || !reflect.DeepEqual(d.Changed, tc.want.Changed) {
			t.Fatalf("%s: diff=%+v", name, d)
		}
	}
	// 도구 이름은 같고 operationId만 다르면(getHTTPItem → getHttpItem, 둘 다 get_http_item)
	// 경로·설명이 같아도 Changed다. operationId는 정의의 메타와 출처 문구에 들어 있다.
	before := compile(runtimeSpec("getHTTPItem|/api/item|항목 조회"))
	after := compile(runtimeSpec("getHttpItem|/api/item|항목 조회"))
	if d := DiffTools(before, after); !reflect.DeepEqual(d.Changed, []string{"get_http_item"}) || len(d.Added)+len(d.Removed) != 0 {
		t.Fatalf("operationId=%+v", d)
	}
	if d := DiffTools(nil, nil); d.ToolSetChanged() {
		t.Fatal("빈 비교가 변경")
	}
}

func TestErrorCode(t *testing.T) {
	for err, want := range map[error]string{
		&ableops.Error{Code: "timeout"}:               "timeout",
		compileError("x_code", "m"):                   "x_code",
		errors.New("plain"):                           "contract_unavailable",
		fmt.Errorf("wrap: %w", compileError("y", "")): "y",
	} {
		if got := ErrorCode(err); got != want {
			t.Fatalf("%v: %s", err, got)
		}
	}
	if _, err := openapi.Parse([]byte("{")); ErrorCode(err) != "invalid_json" {
		t.Fatal(ErrorCode(err))
	}
}
