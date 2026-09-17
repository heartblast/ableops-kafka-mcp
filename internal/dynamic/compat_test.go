package dynamic

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

// Static과 이름이 같은 8개 도구의 전환 가능 여부. 현재 COMPATIBLE은 없다.
func TestStaticDynamicCompatibility(t *testing.T) {
	statics := staticDefinitions(t)
	registry, err := Build(fixtureContract(t), Options{StaticToolNames: tools.StaticNames()})
	if err != nil {
		t.Fatal(err)
	}
	all := []CompatClass{CompatInputMismatch, CompatOutputMismatch, CompatSemanticMismatch, CompatSecurityMismatch}
	noSecurity := []CompatClass{CompatInputMismatch, CompatOutputMismatch, CompatSemanticMismatch}
	want := map[string][]CompatClass{
		"list_clusters":              all,
		"get_cluster_health":         all,
		"list_topics":                noSecurity,
		"list_consumer_groups":       noSecurity,
		"get_consumer_group_lag":     all,
		"get_consumer_group_members": all,
		"list_cluster_events":        all,
		"get_asset_impact":           noSecurity,
		// v0.2.0 Static 도구가 늘면서 생긴 이름 충돌 3개.
		"get_event_summary":         noSecurity,
		"get_consumer_lag_overview": all,
		"list_requests":             all,
	}
	got := map[string]Compatibility{}
	for _, entry := range registry.Entries() {
		if !entry.StaticConflict {
			continue
		}
		static := statics[entry.Tool.Name]
		if static == nil {
			t.Fatalf("Static 정의 없음: %s", entry.Tool.Name)
		}
		got[entry.Tool.Name] = AssessCompatibility(StaticTool{Name: static.Name, InputSchema: static.InputSchema, OutputSchema: static.OutputSchema}, entry)
	}
	if len(got) != len(want) {
		t.Fatalf("충돌 도구=%v", keys(got))
	}
	// 이름이 겹치는 Static 도구는 모두 동작 선언이 있어야 한다.
	if profiles := StaticProfiles(); !reflect.DeepEqual(keys(profiles), keys(want)) {
		t.Fatalf("선언=%v", keys(profiles))
	}
	for name, classes := range want {
		result := got[name]
		if !reflect.DeepEqual(result.Classes, classes) || result.Compatible() {
			t.Errorf("%s: classes=%v input=%v output=%v semantic=%v security=%v", name, result.Classes, result.Input, result.Output, result.Semantic, result.Security)
		}
	}
	// 기계 비교 결과의 대표 근거.
	requireContains(t, got["list_topics"].Input, "Static에만 있는 입력: cluster_id", "Dynamic에만 있는 입력: id", "Static에만 있는 입력: limit")
	requireContains(t, got["list_clusters"].Input, "Static에만 있는 입력: limit")
	requireContains(t, got["get_consumer_group_lag"].Input, "Static에만 있는 입력: group_name", "Dynamic에만 있는 입력: name")
	requireContains(t, got["list_cluster_events"].Input, "Dynamic에만 있는 입력: clusterId", "Static에만 있는 입력: page_size", "Dynamic에만 있는 입력: pageSize")
	requireContains(t, got["get_cluster_health"].Semantic, "호출 Operation 구성이 다릅니다: Static getClusterHealth+getClusterPartitionHealth, Dynamic getClusterHealth")
	requireContains(t, got["get_asset_impact"].Semantic, "호출 Operation 구성이 다릅니다: Static getAssetGraph+getAssetImpact, Dynamic getAssetImpact")
	requireContains(t, got["list_clusters"].Security, "노출 안전 게이트가 BLOCKED로 분류했습니다")
	for name, result := range got {
		if len(result.Output) == 0 || !strings.HasPrefix(result.Output[0], "Static 결과에만 있는 최상위 필드") {
			t.Errorf("%s: 결과 봉투 비교 누락 %v", name, result.Output)
		}
	}
}

func TestAssessCompatibilityCompatiblePath(t *testing.T) {
	contract := syntheticContract(t, `{"/api/items/{id}": {"get": {"operationId":"getItem","x-mcp-enabled":true,
	  "parameters":[{"name":"id","in":"path","required":true,"schema":{"type":"string"}}],`+jsonOK+`}}}`)
	registry, err := Build(contract, Options{Operations: []string{}, Exposure: exposeAll})
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := registry.Lookup("get_item")
	definition := entry.Tool.definition()
	same := StaticTool{Name: "get_item", InputSchema: definition.InputSchema, OutputSchema: definition.OutputSchema}

	// 선언이 없으면 의미 동등성을 추측하지 않는다.
	if result := AssessCompatibility(same, entry); !reflect.DeepEqual(result.Classes, []CompatClass{CompatSemanticMismatch}) {
		t.Fatalf("선언 없음=%+v", result)
	}
	staticProfiles["get_item"] = StaticProfile{Operations: []string{"getItem"}}
	t.Cleanup(func() { delete(staticProfiles, "get_item") })
	if result := AssessCompatibility(same, entry); !result.Compatible() || len(result.Input)+len(result.Output)+len(result.Semantic)+len(result.Security) != 0 {
		t.Fatalf("동등=%+v", result)
	}
	blocked := entry
	blocked.Exposure = ExposureBlocked
	if result := AssessCompatibility(same, blocked); !reflect.DeepEqual(result.Classes, []CompatClass{CompatSecurityMismatch}) {
		t.Fatalf("BLOCKED=%+v", result)
	}
	other := same
	other.OutputSchema = map[string]any{"type": "object", "properties": map[string]any{"status": map[string]any{}}}
	if result := AssessCompatibility(other, entry); !reflect.DeepEqual(result.Classes, []CompatClass{CompatOutputMismatch}) {
		t.Fatalf("출력 차이=%+v", result)
	}
}

func TestCompareInputSchemas(t *testing.T) {
	base := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id":    map[string]any{"type": "string", "minLength": 1, "description": "설명은 비교하지 않음"},
			"limit": map[string]any{"type": "integer", "maximum": 100},
		},
		"required":             []string{"id"},
		"additionalProperties": false,
	}
	clone := func(edit func(map[string]any)) map[string]any {
		out, _ := schemaObject(base)
		edit(out)
		return out
	}
	if diffs := CompareInputSchemas(base, clone(func(m map[string]any) {
		m["properties"].(map[string]any)["id"].(map[string]any)["description"] = "다른 설명"
		m["properties"].(map[string]any)["limit"].(map[string]any)["default"] = 50
	})); len(diffs) != 0 {
		t.Fatalf("설명·기본값 차이를 불일치로 봄: %v", diffs)
	}
	cases := map[string]struct {
		edit func(map[string]any)
		want string
	}{
		"이름": {func(m map[string]any) {
			props := m["properties"].(map[string]any)
			props["cluster_id"] = props["id"]
			delete(props, "id")
		}, "Static에만 있는 입력: id"},
		"필수":    {func(m map[string]any) { m["required"] = []any{} }, "필수 여부가 다른 입력: id"},
		"타입":    {func(m map[string]any) { m["properties"].(map[string]any)["limit"].(map[string]any)["type"] = "number" }, "제약이 다른 입력: limit.type"},
		"범위":    {func(m map[string]any) { m["properties"].(map[string]any)["limit"].(map[string]any)["maximum"] = 200 }, "제약이 다른 입력: limit.maximum"},
		"enum":  {func(m map[string]any) { m["properties"].(map[string]any)["id"].(map[string]any)["enum"] = []any{"a"} }, "제약이 다른 입력: id.enum"},
		"const": {func(m map[string]any) { m["properties"].(map[string]any)["id"].(map[string]any)["const"] = "a" }, "제약이 다른 입력: id.const"},
		"배수":    {func(m map[string]any) { m["properties"].(map[string]any)["limit"].(map[string]any)["multipleOf"] = 5 }, "제약이 다른 입력: limit.multipleOf"},
		"배타 하한": {func(m map[string]any) {
			m["properties"].(map[string]any)["limit"].(map[string]any)["exclusiveMinimum"] = 0
		}, "제약이 다른 입력: limit.exclusiveMinimum"},
		"추가 속성": {func(m map[string]any) { delete(m, "additionalProperties") }, "추가 입력 허용 여부가 다릅니다"},
	}
	for name, tc := range cases {
		if diffs := CompareInputSchemas(base, clone(tc.edit)); !contains(diffs, tc.want) {
			t.Errorf("%s: %v", name, diffs)
		}
	}
	if diffs := CompareInputSchemas(base, func() {}); len(diffs) != 1 {
		t.Fatalf("해석 불가=%v", diffs)
	}
	if diffs := compareTopLevel("출력", nil, base); len(diffs) != 1 || !strings.Contains(diffs[0], "Dynamic 결과에만") {
		t.Fatalf("nil 스키마=%v", diffs)
	}
}

func requireContains(t *testing.T, list []string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !contains(list, value) {
			t.Errorf("%q 없음: %v", value, list)
		}
	}
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
