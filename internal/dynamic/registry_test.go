package dynamic

import (
	"reflect"
	"sort"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

func names(list []*Tool) []string {
	var out []string
	for _, tool := range list {
		out = append(out, tool.Name)
	}
	sort.Strings(out)
	return out
}

func TestBuildUpstreamRegistryWithStaticTools(t *testing.T) {
	static := tools.StaticNames()
	if len(static) != 33 {
		t.Fatalf("static=%v", static)
	}
	registry, err := Build(fixtureContract(t), Options{StaticToolNames: static})
	if err != nil {
		t.Fatal(err)
	}
	// 1차 기본 파일럿 5개는 노출 안전 게이트에서 모두 SHADOW 또는 BLOCKED다.
	want := Stats{Discovered: 31, MCPEnabled: 28, Registrable: 28, Selected: 5, Exposed: 0, Shadow: 4, Blocked: 1, StaticConflicts: 11, Skipped: 0}
	if registry.Stats() != want {
		t.Fatalf("stats=%+v", registry.Stats())
	}
	if got := names(registry.Exposed()); len(got) != 0 {
		t.Fatalf("exposed=%v", got)
	}
	if got := names(registry.Shadow()); !reflect.DeepEqual(got, []string{"get_topic", "list_consumer_groups", "list_events", "list_topics"}) {
		t.Fatalf("shadow=%v", got)
	}
	// BLOCKED가 Static 이름 충돌보다 우선한다.
	if got := names(registry.Blocked()); !reflect.DeepEqual(got, []string{"list_clusters"}) {
		t.Fatalf("blocked=%v", got)
	}
	if got := names(registry.Selected()); !reflect.DeepEqual(got, []string{"get_topic", "list_consumer_groups", "list_events", "list_topics"}) || len(registry.UnknownSelections()) != 0 {
		t.Fatalf("selected=%v unknown=%v", got, registry.UnknownSelections())
	}
	var conflicts []string
	for _, entry := range registry.Entries() {
		if entry.StaticConflict {
			conflicts = append(conflicts, entry.Tool.Name)
			if entry.Placement == PlacementExposed {
				t.Fatalf("Static 이름 충돌 도구가 노출됨: %s", entry.Tool.Name)
			}
		}
	}
	wantConflicts := []string{"get_asset_impact", "get_cluster_health", "get_consumer_group_lag", "get_consumer_group_members", "get_consumer_lag_overview", "get_event_summary", "list_cluster_events", "list_clusters", "list_consumer_groups", "list_requests", "list_topics"}
	if !reflect.DeepEqual(conflicts, wantConflicts) {
		t.Fatalf("conflicts=%v", conflicts)
	}
	entry, ok := registry.Lookup("get_consumer_group_lag")
	if !ok || entry.Placement != PlacementNotSelected || entry.Tool.Operation.ID != "getConsumerGroupLag" {
		t.Fatalf("lookup=%+v", entry)
	}
	if _, ok := registry.Lookup("login"); ok {
		t.Fatal("x-mcp-enabled=false 도구가 등록됨")
	}
}

func TestBuildSelection(t *testing.T) {
	contract := fixtureContract(t)
	// 빈 목록은 선택 없음이다.
	none, _ := Build(contract, Options{Operations: []string{}})
	if none.Stats().Selected != 0 || len(none.Exposed()) != 0 {
		t.Fatalf("none=%+v", none.Stats())
	}
	// Static 이름이 없고 모두 SAFE로 보면 선택한 도구가 모두 노출된다(선택 로직만 확인).
	all, _ := Build(contract, Options{Exposure: exposeAll})
	if all.Stats().Exposed != 5 || all.Stats().StaticConflicts != 0 {
		t.Fatalf("all=%+v", all.Stats())
	}
	custom, _ := Build(contract, Options{
		StaticToolNames: []string{"get_event"},
		Operations:      []string{"getEvent", "getEventSummary", "login", "getClusterStorage", "noSuchOperation"},
	})
	if got := names(custom.Exposed()); !reflect.DeepEqual(got, []string{"get_event_summary"}) {
		t.Fatalf("exposed=%v", got)
	}
	if got := names(custom.Shadow()); !reflect.DeepEqual(got, []string{"get_event"}) {
		t.Fatalf("shadow=%v", got)
	}
	// x-mcp-enabled=false를 선택해도 등록하지 않고 알 수 없는 선택으로 보고한다.
	if got := custom.UnknownSelections(); !reflect.DeepEqual(got, []string{"getClusterStorage", "login", "noSuchOperation"}) {
		t.Fatalf("unknown=%v", got)
	}
	if _, err := Build(nil, Options{}); err == nil {
		t.Fatal("nil 계약 허용")
	}
}

func TestBuildIsolatesFailures(t *testing.T) {
	contract := syntheticContract(t, `{
	  "/api/one": {"get": {"operationId":"getHTTPItem","x-mcp-enabled":true,`+jsonOK+`}},
	  "/api/two": {"get": {"operationId":"getHttpItem","x-mcp-enabled":true,`+jsonOK+`}},
	  "/api/three": {"get": {"operationId":"getOther","x-mcp-enabled":true,`+jsonOK+`},
	                 "delete": {"operationId":"deleteOther","x-mcp-enabled":true,`+jsonOK+`}},
	  "/api/four": {"get": {"operationId":"brokenParam","x-mcp-enabled":true,"parameters":[{"name":"x","in":"header","schema":{"type":"string"}}],`+jsonOK+`}}
	}`)
	registry, err := Build(contract, Options{Operations: []string{"getHTTPItem", "getHttpItem", "getOther", "deleteOther"}, Exposure: exposeAll})
	if err != nil {
		t.Fatal(err)
	}
	if got := names(registry.Exposed()); !reflect.DeepEqual(got, []string{"get_other"}) {
		t.Fatalf("exposed=%v", got)
	}
	codes := map[string]string{}
	for _, issue := range registry.Skipped() {
		codes[issue.OperationID] = issue.Code
	}
	want := map[string]string{
		"getHTTPItem": IssueDuplicateToolName,
		"getHttpItem": IssueDuplicateToolName,
		"deleteOther": IssueWriteMethod,
		"brokenParam": openapi.IssueUnsupportedParameter,
	}
	if !reflect.DeepEqual(codes, want) {
		t.Fatalf("skipped=%v", codes)
	}
	stats := registry.Stats()
	if stats.MCPEnabled != 5 || stats.Registrable != 1 || stats.Skipped != 4 {
		t.Fatalf("stats=%+v", stats)
	}
}
