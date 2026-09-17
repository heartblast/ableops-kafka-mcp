package dynamic

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// methodListTools는 SDK가 도구 목록 요청에 쓰는 JSON-RPC 메서드다(SDK 내부 상수와 같은 값).
const methodListTools = "tools/list"

// RefreshOutcome은 계약 갱신 한 번의 결과다.
type RefreshOutcome string

const (
	// RefreshUpdated는 기본 서버의 Dynamic 도구 정의가 바뀌어 교체하고 알림을 보냈다.
	RefreshUpdated RefreshOutcome = "updated"
	// RefreshUnchanged는 새 계약(200)을 받았지만 노출 도구 정의가 같아 알림을 보내지 않았다.
	// ETag만 바뀐 경우와 설명 외 부분만 바뀐 경우가 여기에 속한다.
	RefreshUnchanged RefreshOutcome = "unchanged"
	// RefreshNotModified는 304다. 아무것도 바꾸지 않았다.
	RefreshNotModified RefreshOutcome = "not_modified"
	// RefreshFailed는 전송·검증 실패다. 마지막 정상 도구 목록(Last Known Good)을 유지한다.
	RefreshFailed RefreshOutcome = "failed"
)

// Diff는 기본 서버에 노출한 Dynamic 도구 집합의 변화다. 각 목록은 이름 순이다.
//
// Changed는 tools/list로 보이는 정의(이름·설명·입출력 스키마·주석·operationId·메서드·경로)가
// 달라진 도구다. ETag나 계약의 다른 부분만 바뀌면 Changed가 아니다.
type Diff struct {
	Added     []string `json:"added"`
	Removed   []string `json:"removed"`
	Changed   []string `json:"changed"`
	Unchanged []string `json:"unchanged"`
	// Rebound는 Unchanged 중 정의는 같고 실행 바인딩(explode·선언된 204)만 바뀐 도구다.
	// 클라이언트가 볼 변화가 없으므로 알림 대상이 아니다.
	Rebound []string `json:"rebound"`
}

// ToolSetChanged는 클라이언트에 tools/list_changed를 알려야 하는 변화인지다.
func (d Diff) ToolSetChanged() bool {
	return len(d.Added)+len(d.Removed)+len(d.Changed) > 0
}

// DiffTools는 두 노출 도구 집합의 차이를 계산한다. 같은 이름이 여러 번 있으면 마지막 값을 쓴다.
func DiffTools(before, after []*Tool) Diff {
	old := map[string]*Tool{}
	for _, tool := range before {
		old[tool.Name] = tool
	}
	next := map[string]*Tool{}
	for _, tool := range after {
		next[tool.Name] = tool
	}
	return diffTools(old, next)
}

func diffTools(before, after map[string]*Tool) Diff {
	var d Diff
	for name, old := range before {
		tool, ok := after[name]
		switch {
		case !ok:
			d.Removed = append(d.Removed, name)
		case old.definitionKey != tool.definitionKey:
			d.Changed = append(d.Changed, name)
		default:
			d.Unchanged = append(d.Unchanged, name)
			if old.bindingKey != tool.bindingKey {
				d.Rebound = append(d.Rebound, name)
			}
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			d.Added = append(d.Added, name)
		}
	}
	for _, list := range []*[]string{&d.Added, &d.Removed, &d.Changed, &d.Unchanged, &d.Rebound} {
		sort.Strings(*list)
	}
	return d
}

// RefreshReport는 Refresh 한 번의 결과다.
type RefreshReport struct {
	Outcome RefreshOutcome
	// Code는 실패 코드다(ableops·openapi 오류 코드). 성공이면 빈 값이다.
	Code string
	Diff Diff
	// ETagChanged는 새 계약의 ETag가 이전 Registry와 다른지다.
	ETagChanged bool
	// Registry는 이 호출이 끝난 뒤 서비스 중인 Registry다. 한 번도 적재하지 못했으면 nil이다.
	Registry *Registry
}

// Runtime은 실행 중 OpenAPI 계약 변경을 기본 서버의 Dynamic 도구 목록에 반영한다.
//
//	Timer → Loader(If-None-Match) → 304: 그대로
//	                               → 200: Parse → Build(불변 Registry) → 교체 → Diff → list_changed
//	                               → 실패: Last Known Good 유지, 알림 없음
//
// 교체 방식: Registry는 불변 스냅샷이고 현재 값은 atomic.Pointer로 읽는다. SDK에는 여러 도구를
// 한 번에 바꾸는 API가 없으므로, tools/list 요청과 교체 구간을 RWMutex로 직렬화해 도구가 일부만
// 바뀐 목록이 보이지 않게 한다. tools/call은 잠그지 않는다. SDK는 도구별로 입력 스키마와 핸들러를
// 함께 교체하고, 핸들러는 시작 시점의 도구로 끝까지 실행한다.
type Runtime struct {
	loader   *openapi.Loader
	executor *Executor
	logger   *slog.Logger
	opts     Options
	// build는 새 계약으로 Registry를 만든다. 운영에서는 Build이며 테스트가 실패를 주입한다.
	build func(*openapi.Contract, Options) (*Registry, error)

	// listGate는 tools/list(읽기)와 SDK 도구 집합 교체(쓰기)를 직렬화한다.
	listGate sync.RWMutex

	// mu는 Bind·Install·Refresh를 직렬화한다. 아래 필드는 mu 아래에서만 바꾼다.
	mu        sync.Mutex
	server    *mcp.Server
	reserved  map[string]bool
	published map[string]*toolSlot

	current atomic.Pointer[Registry]
}

// NewRuntime은 서버에 연결되지 않은 Runtime을 만든다. loader가 nil이면 Install로 받은
// Registry만 제공하고 갱신하지 않는다. 서버 연결은 mcpserver.New가 Bind로 한다.
func NewRuntime(client *ableops.Client, logger *slog.Logger, loader *openapi.Loader, opts Options) *Runtime {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	reserved := map[string]bool{}
	for _, name := range opts.StaticToolNames {
		reserved[name] = true
	}
	opts.StaticToolNames = append([]string(nil), opts.StaticToolNames...)
	// nil(기본 목록)과 빈 슬라이스(선택 없음)는 의미가 다르다. 빈 슬라이스를 nil로 바꾸지 않게 복사한다.
	if opts.Operations != nil {
		opts.Operations = append([]string{}, opts.Operations...)
	}
	return &Runtime{
		loader:    loader,
		build:     Build,
		executor:  NewExecutor(client, logger),
		logger:    logger,
		opts:      opts,
		reserved:  reserved,
		published: map[string]*toolSlot{},
	}
}

// Bind는 Static 도구를 등록한 서버에 Runtime을 연결한다. static은 서버에 실제로 등록된
// Static 도구 이름이며 Dynamic 도구가 절대 덮어쓰지 않는다. tools/list를 교체와 직렬화하는
// 미들웨어를 설치하고, 이미 적재한 Registry가 있으면 그 노출 도구를 등록한다.
func (r *Runtime) Bind(server *mcp.Server, static []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if server == nil {
		return errors.New("MCP 서버가 없습니다")
	}
	if r.server != nil {
		return errors.New("동적 도구 런타임은 서버 하나에만 연결합니다")
	}
	for _, name := range static {
		r.reserved[name] = true
	}
	r.server = server
	server.AddReceivingMiddleware(r.listMiddleware)
	if current := r.current.Load(); current != nil {
		r.apply(current)
	}
	return nil
}

func (r *Runtime) listMiddleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
		if method != methodListTools {
			return next(ctx, method, request)
		}
		r.listGate.RLock()
		defer r.listGate.RUnlock()
		return next(ctx, method, request)
	}
}

// Install은 이미 만든 Registry로 교체한다. 계약 조회 없이 고정 Registry를 제공하는 경로다.
func (r *Runtime) Install(registry *Registry) Diff {
	r.mu.Lock()
	defer r.mu.Unlock()
	if registry == nil {
		return Diff{}
	}
	return r.apply(registry)
}

// Current는 서비스 중인 Registry다. 한 번도 적재하지 못했으면 nil이다.
func (r *Runtime) Current() *Registry { return r.current.Load() }

// Published는 기본 서버에 등록된 Dynamic 도구 이름이다(이름 순).
func (r *Runtime) Published() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.published))
	for name := range r.published {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Refresh는 계약을 한 번 조회하고, 새 계약이 검증을 통과했을 때만 도구 목록을 교체한다.
// 어떤 실패도 현재 도구를 지우지 않는다.
func (r *Runtime) Refresh(ctx context.Context) RefreshReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := r.current.Load()
	if r.loader == nil {
		return RefreshReport{Outcome: RefreshFailed, Code: "refresh_unavailable", Registry: previous}
	}
	var next *Registry
	result, err := r.loader.LoadValidated(ctx, func(contract *openapi.Contract) error {
		built, err := r.build(contract, r.opts)
		if err != nil {
			return err
		}
		next = built
		return nil
	})
	if err != nil {
		code := ErrorCode(err)
		if previous == nil {
			r.logger.Warn("동적 도구 계약 적재 실패: Static 도구만 제공하며 다음 갱신 주기에 다시 시도합니다", "code", code)
		} else {
			r.logger.Warn("동적 도구 계약 갱신 실패: 마지막 정상 도구 목록을 유지합니다", "code", code, "etag_present", previous.ETag() != "")
		}
		return RefreshReport{Outcome: RefreshFailed, Code: code, Registry: previous}
	}
	if result.NotModified {
		r.logger.Debug("동적 도구 계약 변경 없음", "code", "not_modified")
		return RefreshReport{Outcome: RefreshNotModified, Registry: previous}
	}
	LogSummary(r.logger, next)
	diff := r.apply(next)
	report := RefreshReport{Outcome: RefreshUnchanged, Diff: diff, Registry: next}
	report.ETagChanged = previous == nil || previous.ETag() != next.ETag()
	if diff.ToolSetChanged() {
		report.Outcome = RefreshUpdated
	}
	// 도구 이름은 검증된 operationId에서 만든 식별자이므로 기록해도 된다.
	r.logger.Info("동적 도구 목록 갱신",
		"outcome", string(report.Outcome),
		"added", diff.Added,
		"removed", diff.Removed,
		"changed", diff.Changed,
		"rebound", diff.Rebound,
		"unchanged", len(diff.Unchanged),
		"etag_changed", report.ETagChanged,
		"initial", previous == nil)
	return report
}

// Run은 interval마다 Refresh를 호출하고 ctx가 끝나면 반환한다. 최초 적재는 호출자가
// Refresh로 먼저 끝낸다. loader가 없거나 interval이 0 이하이면 바로 반환한다.
//
// 주기는 이전 갱신이 끝난 시점부터 센다. 응답이 늦는 Backend에 조회를 쉬지 않고 이어 보내
// 공유 동시 호출 슬롯을 계속 차지하지 않게 한다.
func (r *Runtime) Run(ctx context.Context, interval time.Duration) {
	if r.loader == nil || interval <= 0 {
		return
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			r.Refresh(ctx)
			timer.Reset(interval)
		}
	}
}

// apply는 next를 서비스 Registry로 만들고 서버 도구 집합을 맞춘다. mu를 잡은 상태로 부른다.
//
// 정의가 같은 도구는 SDK를 건드리지 않고 실행 대상만 바꾼다(알림 없음). 추가·정의 변경은
// AddTool, 삭제는 RemoveTools 한 번으로 반영한다. SDK는 이 호출들에서 notifications/tools/list_changed를
// 보내며 연속 변경을 10ms 디바운스로 한 번에 묶는다. 교체 구간은 listGate 쓰기 잠금 안에 있어
// tools/list는 교체 전 또는 교체 후의 전체 목록만 본다.
func (r *Runtime) apply(next *Registry) Diff {
	desired := map[string]*Tool{}
	for _, tool := range next.Exposed() {
		if r.reserved[tool.Name] {
			// Registry가 이미 거르지만 Static 도구 덮어쓰기는 등록 경계에서 한 번 더 막는다.
			r.logger.Error("동적 도구 등록 거부", "tool", tool.Name, "operation_id", tool.Operation.ID, "code", "static_tool_conflict")
			continue
		}
		desired[tool.Name] = tool
	}
	if r.server == nil {
		// 서버에 연결하기 전에는 Registry만 보관한다. Bind가 이 값으로 등록한다.
		r.current.Store(next)
		return diffTools(nil, desired)
	}
	before := map[string]*Tool{}
	for name, slot := range r.published {
		before[name] = slot.tool.Load()
	}
	diff := diffTools(before, desired)
	for _, name := range diff.Unchanged {
		r.published[name].tool.Store(desired[name])
	}
	if !diff.ToolSetChanged() {
		r.current.Store(next)
		return diff
	}
	r.listGate.Lock()
	defer r.listGate.Unlock()
	for _, name := range append(append([]string(nil), diff.Added...), diff.Changed...) {
		slot := newSlot(desired[name])
		if err := add(r.server, r.executor, slot); err != nil {
			// precheck를 통과한 도구라 일어나지 않아야 한다. 기존 등록(있으면)을 그대로 둔다.
			r.logger.Error("동적 도구 등록 실패", "tool", name, "operation_id", desired[name].Operation.ID, "code", IssueRegisterFailed)
			continue
		}
		r.published[name] = slot
	}
	if len(diff.Removed) > 0 {
		r.server.RemoveTools(diff.Removed...)
		for _, name := range diff.Removed {
			delete(r.published, name)
		}
	}
	r.current.Store(next)
	return diff
}

// ErrorCode는 계약 적재 오류를 원문 없이 기록할 코드로 바꾼다.
func ErrorCode(err error) string {
	var upstream *ableops.Error
	var contract *openapi.ContractError
	var compile *CompileError
	switch {
	case errors.As(err, &upstream):
		return upstream.Code
	case errors.As(err, &contract):
		return contract.Code
	case errors.As(err, &compile):
		return compile.Code
	default:
		return "contract_unavailable"
	}
}
