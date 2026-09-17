package dynamic

import (
	"context"
	"errors"
	"sort"

	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// DefaultOperations는 1차 파일럿 조회 Operation이다. 설정에서 목록을 생략하면 이 값을 쓴다.
// 선택되더라도 노출 안전 게이트(exposure.go)가 SAFE로 분류한 것만 기본 서버에 노출된다.
// 이 목록은 설정 계약이므로 노출 범위를 넓히려고 바꾸지 않는다.
var DefaultOperations = []string{"listClusters", "listTopics", "getTopic", "listConsumerGroups", "listEvents"}

// Placement는 컴파일된 도구의 배치다.
type Placement string

const (
	// PlacementExposed는 기본 서버의 tools/list에 추가된다.
	PlacementExposed Placement = "exposed"
	// PlacementShadow는 기본 서버에 등록하지 않고 비교용 서버에서만 실행한다.
	// Static 도구와 이름이 같거나, 노출 안전 게이트가 SHADOW로 분류한 경우다.
	PlacementShadow Placement = "shadow"
	// PlacementBlocked는 선택했지만 노출 안전 게이트가 BLOCKED로 분류해 어떤 서버에도 등록하지 않는다.
	PlacementBlocked Placement = "blocked"
	// PlacementNotSelected는 컴파일은 되었지만 설정에서 선택하지 않았다.
	PlacementNotSelected Placement = "not_selected"
)

// IssueRegisterFailed는 SDK 등록 사전검사에 실패해 제외한 Operation이다.
const IssueRegisterFailed = "register_failed"

// Entry는 Registry의 도구 하나다.
type Entry struct {
	Tool           *Tool
	Placement      Placement
	StaticConflict bool
	// Exposure는 노출 안전 게이트의 분류다. Static 이름 충돌은 반영하지 않은 정책 값이다.
	Exposure Exposure
	// ExposureReason은 분류 근거(고정 문구)다.
	ExposureReason string
}

// Options는 Registry 구성 입력이다.
type Options struct {
	// StaticToolNames는 기존 Static 도구 이름이다. 겹치는 Dynamic 도구는 절대 노출하지 않는다.
	StaticToolNames []string
	// Operations는 선택할 operationId다. nil이면 DefaultOperations, 빈 슬라이스면 선택 없음이다.
	Operations []string
	// Exposure는 Operation의 노출 분류다. nil이면 내장 정책 ClassifyExposure를 쓴다.
	// 테스트에서 합성 계약을 분류하려고 바꾸는 용도이며 운영 경로는 nil을 쓴다.
	Exposure func(openapi.Operation) ExposureRule
}

// Stats는 적재 결과 요약이다. 로그와 보고에 쓴다.
type Stats struct {
	Discovered      int `json:"discovered"`
	MCPEnabled      int `json:"mcp_enabled"`
	Registrable     int `json:"registrable_get"`
	Selected        int `json:"selected"`
	Exposed         int `json:"exposed"`
	Shadow          int `json:"shadow"`
	Blocked         int `json:"blocked"`
	StaticConflicts int `json:"static_conflicts"`
	Skipped         int `json:"skipped"`
}

// Registry는 한 계약에서 만든 도구 집합이다. 생성 후 변경하지 않는다(불변 스냅샷).
// 계약 갱신은 새 Registry를 만들어 Runtime이 통째로 교체한다.
type Registry struct {
	etag    string
	entries []Entry
	byName  map[string]int
	skipped []openapi.Issue
	unknown []string
	stats   Stats
}

// Build는 계약의 x-mcp-enabled GET Operation을 컴파일하고 배치를 정한다.
// 일부 Operation이 실패해도 나머지로 Registry를 만든다.
func Build(contract *openapi.Contract, opts Options) (*Registry, error) {
	if contract == nil {
		return nil, errors.New("OpenAPI 계약이 없습니다")
	}
	selection := opts.Operations
	if selection == nil {
		selection = DefaultOperations
	}
	classify := opts.Exposure
	if classify == nil {
		classify = ClassifyExposure
	}
	selected := map[string]bool{}
	for _, id := range selection {
		selected[id] = true
	}
	reserved := map[string]bool{}
	for _, name := range opts.StaticToolNames {
		reserved[name] = true
	}
	r := &Registry{
		etag:    contract.ETag,
		byName:  map[string]int{},
		skipped: append([]openapi.Issue(nil), contract.Issues...),
		stats:   Stats{Discovered: contract.Discovered, MCPEnabled: contract.MCPDeclared},
	}

	var compiled []*Tool
	for _, op := range contract.MCPOperations() {
		tool, err := Compile(op)
		if err == nil {
			err = precheck(tool)
		}
		if err != nil {
			r.skip(op, err)
			continue
		}
		compiled = append(compiled, tool)
	}
	// operationId는 달라도 snake_case 이름이 겹치면 어느 쪽이 맞는지 추측하지 않고 모두 제외한다.
	names := map[string]int{}
	for _, tool := range compiled {
		names[tool.Name]++
	}
	sort.Slice(compiled, func(i, j int) bool { return compiled[i].Name < compiled[j].Name })
	known := map[string]bool{}
	for _, tool := range compiled {
		if names[tool.Name] > 1 {
			r.skip(tool.Operation, compileError(IssueDuplicateToolName, "다른 operationId와 도구 이름이 겹쳐 제외했습니다."))
			continue
		}
		known[tool.Operation.ID] = true
		rule := classify(tool.Operation)
		if !rule.Exposure.valid() {
			rule = ExposureRule{Exposure: ExposureBlocked, Reason: "노출 분류 값이 올바르지 않아 차단했습니다."}
		}
		entry := Entry{
			Tool:           tool,
			StaticConflict: reserved[tool.Name],
			Placement:      PlacementNotSelected,
			Exposure:       rule.Exposure,
			ExposureReason: rule.Reason,
		}
		if entry.StaticConflict {
			r.stats.StaticConflicts++
		}
		if selected[tool.Operation.ID] {
			r.stats.Selected++
			entry.Placement = placementFor(entry)
		}
		switch entry.Placement {
		case PlacementExposed:
			r.stats.Exposed++
		case PlacementShadow:
			r.stats.Shadow++
		case PlacementBlocked:
			r.stats.Blocked++
		}
		r.byName[tool.Name] = len(r.entries)
		r.entries = append(r.entries, entry)
	}
	r.stats.Registrable = len(r.entries)
	r.stats.Skipped = len(r.skipped)
	for id := range selected {
		if !known[id] {
			r.unknown = append(r.unknown, id)
		}
	}
	sort.Strings(r.unknown)
	return r, nil
}

// placementFor는 선택된 도구의 배치다. BLOCKED가 가장 우선하고, Static 이름 충돌과 SHADOW 분류는
// 기본 서버 노출을 막는다. SAFE이면서 충돌이 없을 때만 노출한다.
func placementFor(entry Entry) Placement {
	switch {
	case entry.Exposure == ExposureBlocked:
		return PlacementBlocked
	case entry.StaticConflict || entry.Exposure != ExposureSafe:
		return PlacementShadow
	default:
		return PlacementExposed
	}
}

// precheck는 SDK 등록에서 panic이 날 도구를 계약 적재 단계에서 제외한다. 실제 서버와 같은
// 등록 함수를 버리는 서버에 먼저 적용한다. 그래서 실행 중 교체가 중간에 실패해 도구 목록이
// 일부만 바뀌는 일이 없다.
func precheck(tool *Tool) error {
	server := mcp.NewServer(&mcp.Implementation{Name: "ableops-dynamic-precheck", Version: "0"}, nil)
	if err := add(server, NewExecutor(nil, nil), newSlot(tool)); err != nil {
		return compileError(IssueRegisterFailed, "MCP SDK 도구 등록 사전검사에 실패했습니다.")
	}
	return nil
}

func (r *Registry) skip(op openapi.Operation, err error) {
	issue := openapi.Issue{OperationID: op.ID, Method: op.Method, Path: op.Path, Code: "compile_failed", Message: err.Error()}
	var ce *CompileError
	if errors.As(err, &ce) {
		issue.Code, issue.Message = ce.Code, ce.Message
	}
	r.skipped = append(r.skipped, issue)
}

// ETag는 이 Registry를 만든 계약의 ETag다.
func (r *Registry) ETag() string { return r.etag }

// Stats는 적재 요약이다.
func (r *Registry) Stats() Stats { return r.stats }

// Entries는 등록 가능한 도구 전체다(이름 순).
func (r *Registry) Entries() []Entry { return append([]Entry(nil), r.entries...) }

// Skipped는 제외한 Operation과 계약 경고다.
func (r *Registry) Skipped() []openapi.Issue { return append([]openapi.Issue(nil), r.skipped...) }

// UnknownSelections는 선택했지만 등록 가능한 도구에 없는 operationId다.
func (r *Registry) UnknownSelections() []string { return append([]string(nil), r.unknown...) }

// Lookup은 도구 이름으로 항목을 찾는다.
func (r *Registry) Lookup(name string) (Entry, bool) {
	i, ok := r.byName[name]
	if !ok {
		return Entry{}, false
	}
	return r.entries[i], true
}

// Exposed는 기본 서버에 추가할 도구다.
func (r *Registry) Exposed() []*Tool { return r.filter(PlacementExposed) }

// Shadow는 기본 서버에 노출하지 않고 비교용으로만 실행하는 도구다.
func (r *Registry) Shadow() []*Tool { return r.filter(PlacementShadow) }

// Blocked는 선택했지만 노출 안전 게이트가 차단한 도구다. 어떤 서버에도 등록하지 않는다.
func (r *Registry) Blocked() []*Tool { return r.filter(PlacementBlocked) }

// Selected는 비교용 서버에 등록할 도구(exposed + shadow)다. BLOCKED는 포함하지 않는다.
func (r *Registry) Selected() []*Tool { return r.filter(PlacementExposed, PlacementShadow) }

func (r *Registry) filter(placements ...Placement) []*Tool {
	var out []*Tool
	for _, entry := range r.entries {
		for _, placement := range placements {
			if entry.Placement == placement {
				out = append(out, entry.Tool)
			}
		}
	}
	return out
}

// Load는 계약을 적재해 Registry를 만든다. 실패하면 Registry 없이 오류를 돌려준다.
// 실행 중 갱신은 Runtime.Refresh를 쓴다.
func Load(ctx context.Context, loader *openapi.Loader, opts Options) (*Registry, error) {
	var registry *Registry
	result, err := loader.LoadValidated(ctx, func(contract *openapi.Contract) error {
		built, err := Build(contract, opts)
		if err != nil {
			return err
		}
		registry = built
		return nil
	})
	if err != nil {
		return nil, err
	}
	if result.NotModified {
		// 같은 Loader를 재사용하면 304일 수 있다. 보관한 계약으로 다시 만든다.
		return Build(result.Contract, opts)
	}
	return registry, nil
}
