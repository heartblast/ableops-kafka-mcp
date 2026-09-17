package dynamic

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var (
	wantSafe    = []string{"getBranding", "getEventSummary", "getTopicPartitions"}
	wantBlocked = []string{"getAssetGraph", "getCluster", "getClusterHealth", "listClusters", "listDefaultClusterTopics", "getRequest", "listRequests"}
)

// upstream 계약의 x-mcp-enabled=true Operation은 모두 정책표로 분류되어야 한다. upstream이 Operation을
// 추가·삭제하면 이 테스트가 실패해 공개 범위를 다시 확인하게 한다.
func TestExposurePolicyCoversUpstreamContract(t *testing.T) {
	contract := fixtureContract(t)
	policy := ExposurePolicy()
	var ids []string
	for _, op := range contract.MCPOperations() {
		ids = append(ids, op.ID)
		rule, ok := policy[op.ID]
		if !ok {
			t.Errorf("정책표에 없는 upstream Operation: %s", op.ID)
		}
		// SAFE 고정 경로·파라미터가 현재 계약과 같아야 실제로 SAFE로 분류된다.
		if got := ClassifyExposure(op); got.Exposure != rule.Exposure {
			t.Errorf("%s: 정책 %s인데 분류 %s (%s)", op.ID, rule.Exposure, got.Exposure, got.Reason)
		}
	}
	if len(ids) != 28 || len(policy) != 28 {
		t.Fatalf("upstream=%d policy=%d", len(ids), len(policy))
	}
	byClass := map[Exposure][]string{}
	for id, rule := range policy {
		if !rule.Exposure.valid() || strings.TrimSpace(rule.Reason) == "" {
			t.Errorf("%s: 분류 또는 근거 누락 %+v", id, rule)
		}
		byClass[rule.Exposure] = append(byClass[rule.Exposure], id)
	}
	for _, list := range byClass {
		sort.Strings(list)
	}
	sortedSafe := append([]string(nil), wantSafe...)
	sortedBlocked := append([]string(nil), wantBlocked...)
	sort.Strings(sortedSafe)
	sort.Strings(sortedBlocked)
	if !reflect.DeepEqual(byClass[ExposureSafe], sortedSafe) || !reflect.DeepEqual(byClass[ExposureBlocked], sortedBlocked) || len(byClass[ExposureShadow]) != 18 {
		t.Fatalf("classes=%v", byClass)
	}
	// 요청서가 명시한 Operation은 SAFE가 아니다.
	for _, id := range []string{"getCluster", "getTopic", "getEvent", "listClusters", "listEvents"} {
		if policy[id].Exposure == ExposureSafe {
			t.Fatalf("%s가 SAFE", id)
		}
	}
	// 사본을 바꿔도 내장 정책은 바뀌지 않는다.
	policy["getTopic"] = ExposureRule{Exposure: ExposureSafe}
	if ClassifyExposure(openapi.Operation{ID: "getTopic"}).Exposure != ExposureShadow {
		t.Fatal("정책표 사본이 원본을 바꿈")
	}
}

func TestExposureGatePlacement(t *testing.T) {
	contract := fixtureContract(t)
	var all []string
	for _, op := range contract.MCPOperations() {
		all = append(all, op.ID)
	}
	registry, err := Build(contract, Options{StaticToolNames: tools.StaticNames(), Operations: all})
	if err != nil {
		t.Fatal(err)
	}
	stats := registry.Stats()
	// SAFE 3개 중 get_event_summary 는 v0.2.0 Static 도구와 이름이 같아 shadow 다.
	if stats.Selected != 28 || stats.Exposed != 2 || stats.Shadow != 19 || stats.Blocked != 7 || stats.StaticConflicts != 11 {
		t.Fatalf("stats=%+v", stats)
	}
	if got := names(registry.Exposed()); !reflect.DeepEqual(got, []string{"get_branding", "get_topic_partitions"}) {
		t.Fatalf("exposed=%v", got)
	}
	for _, tool := range registry.Selected() {
		if entry, _ := registry.Lookup(tool.Name); entry.Exposure == ExposureBlocked {
			t.Fatalf("BLOCKED가 비교용 목록에 포함: %s", tool.Name)
		}
	}
	// Static 이름 충돌은 SAFE여도 노출하지 않고, 잘못된 분류 값은 BLOCKED로 본다.
	synthetic := syntheticContract(t, `{
	  "/api/a": {"get": {"operationId":"getA","x-mcp-enabled":true,`+jsonOK+`}},
	  "/api/b": {"get": {"operationId":"getB","x-mcp-enabled":true,`+jsonOK+`}},
	  "/api/c": {"get": {"operationId":"getC","x-mcp-enabled":true,`+jsonOK+`}}
	}`)
	custom, _ := Build(synthetic, Options{
		StaticToolNames: []string{"get_a"},
		Operations:      []string{"getA", "getB", "getC"},
		Exposure: func(op openapi.Operation) ExposureRule {
			if op.ID == "getB" {
				return ExposureRule{Exposure: "MAYBE"}
			}
			return ExposureRule{Exposure: ExposureSafe}
		},
	})
	for name, want := range map[string]Placement{"get_a": PlacementShadow, "get_b": PlacementBlocked, "get_c": PlacementExposed} {
		if entry, _ := custom.Lookup(name); entry.Placement != want {
			t.Fatalf("%s: %+v", name, entry)
		}
	}
	// 정책표에 없는 Operation은 선택해도 SHADOW다.
	unclassified, _ := Build(synthetic, Options{Operations: []string{"getC"}})
	if entry, _ := unclassified.Lookup("get_c"); entry.Placement != PlacementShadow || entry.ExposureReason != reasonUnclassified {
		t.Fatalf("unclassified=%+v", entry)
	}

	// 기본 서버 런타임은 SAFE만 등록한다.
	rt := NewRuntime(nil, nil, nil, Options{})
	rt.Install(registry)
	server := mcp.NewServer(&mcp.Implementation{Name: "gate", Version: "0"}, nil)
	static := tools.Register(server, nil, nil)
	if err := rt.Bind(server, static); err != nil {
		t.Fatal(err)
	}
	if got := rt.Published(); !reflect.DeepEqual(got, []string{"get_branding", "get_topic_partitions"}) {
		t.Fatalf("published=%v", got)
	}
	cs, _ := connect(t, server, "")
	listedTools := listed(t, cs)
	if len(listedTools) != len(static)+2 {
		t.Fatalf("tools=%v", toolNames(listedTools))
	}
	for _, tool := range append(registry.Blocked(), registry.Shadow()...) {
		if got := listedTools[tool.Name]; got != nil && got.Meta["ableops/openapi"] != nil {
			t.Fatalf("SAFE가 아닌 Dynamic 도구가 노출됨: %s", tool.Name)
		}
	}
}

// SAFE는 검토 당시의 경로·파라미터 구성에 묶인다. 실행 중 계약이 바뀌어 달라지면 재검토 전까지 SHADOW다.
func TestExposureSafePolicyDrift(t *testing.T) {
	summary := func(params string) string {
		return `"/api/events/summary": {"get": {"operationId":"getEventSummary","summary":"요약","x-mcp-enabled":true,"parameters":` + params + `,` + jsonOK + `}}`
	}
	cases := map[string]struct {
		paths string
		id    string
		want  Exposure
	}{
		"그대로":         {`{` + summary(`[{"name":"clusterId","in":"query","schema":{"type":"string"}}]`) + `}`, "getEventSummary", ExposureSafe},
		"설명·제약 변경":    {`{` + summary(`[{"name":"clusterId","in":"query","description":"새 설명","schema":{"type":"string","maxLength":255}}]`) + `}`, "getEventSummary", ExposureSafe},
		"파라미터 추가":     {`{` + summary(`[{"name":"clusterId","in":"query","schema":{"type":"string"}},{"name":"includeRaw","in":"query","schema":{"type":"boolean"}}]`) + `}`, "getEventSummary", ExposureShadow},
		"파라미터 삭제":     {`{` + summary(`[]`) + `}`, "getEventSummary", ExposureShadow},
		"경로 변경":       {`{"/api/branding/v2": {"get": {"operationId":"getBranding","x-mcp-enabled":true,` + jsonOK + `}}}`, "getBranding", ExposureShadow},
		"경로 변수 이름 변경": {`{"/api/clusters/{cluster}/topics/{name}/partitions": {"get": {"operationId":"getTopicPartitions","x-mcp-enabled":true,"parameters":[{"name":"cluster","in":"path","required":true,"schema":{"type":"string"}},{"name":"name","in":"path","required":true,"schema":{"type":"string"}}],` + jsonOK + `}}}`, "getTopicPartitions", ExposureShadow},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			op := findOperation(t, syntheticContract(t, tc.paths), tc.id)
			got := ClassifyExposure(op)
			if got.Exposure != tc.want {
				t.Fatalf("got=%+v", got)
			}
			if tc.want == ExposureShadow && got.Reason != reasonPolicyDrift {
				t.Fatalf("reason=%s", got.Reason)
			}
		})
	}
	// Registry 배치에도 반영되어 기본 서버에 노출하지 않는다.
	registry, err := Build(syntheticContract(t, cases["파라미터 추가"].paths), Options{Operations: []string{"getEventSummary"}})
	if err != nil {
		t.Fatal(err)
	}
	if entry, _ := registry.Lookup("get_event_summary"); entry.Placement != PlacementShadow || entry.ExposureReason != reasonPolicyDrift {
		t.Fatalf("entry=%+v", entry)
	}
}

func TestLogSummaryExplainsHiddenSelections(t *testing.T) {
	registry, err := Build(fixtureContract(t), Options{
		StaticToolNames: tools.StaticNames(),
		Operations:      []string{"listClusters", "listTopics", "getTopic", "getEventSummary", "getTopicPartitions", "getAssetGraph"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	LogSummary(slog.New(slog.NewJSONHandler(&logs, nil)), registry)
	text := logs.String()
	for _, fragment := range []string{
		`"blocked":2`, `"exposed":1`, `"shadow":3`,
		`"tool":"list_clusters","operation_id":"listClusters","placement":"blocked","exposure":"BLOCKED","code":"exposure_BLOCKED"`,
		`"tool":"list_topics","operation_id":"listTopics","placement":"shadow","exposure":"SHADOW","code":"static_tool_conflict"`,
		`"tool":"get_topic","operation_id":"getTopic","placement":"shadow","exposure":"SHADOW","code":"exposure_SHADOW"`,
		// SAFE 분류여도 Static 이름과 겹치면 그 사유로 숨긴다.
		`"tool":"get_event_summary","operation_id":"getEventSummary","placement":"shadow","exposure":"SAFE","code":"static_tool_conflict"`,
		`"tool":"get_asset_graph"`,
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("로그 누락 %s: %s", fragment, text)
		}
	}
	if strings.Contains(text, `"tool":"get_topic_partitions"`) {
		t.Fatal("노출된 도구를 숨김 경고로 기록")
	}
}

// staticDefinitions는 Static 도구 정의를 공식 SDK 클라이언트로 받는다.
func staticDefinitions(t *testing.T) map[string]*mcp.Tool {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "static", Version: "0"}, nil)
	tools.Register(server, nil, nil)
	cs, _ := connect(t, server, "")
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
