package dynamic

import (
	"context"
	"errors"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

// adapter는 계약을 적재하지 않은 런타임의 어댑터다. 계약을 넣지 않으면 인프라 사용 불가다.
func adapter(t *testing.T, registry *Registry) *Adapter {
	t.Helper()
	runtime := NewRuntime(nil, nil, nil, Options{StaticToolNames: tools.StaticNames()})
	if registry != nil {
		runtime.Install(registry)
	}
	return runtime.Adapter()
}

func TestAdapterUnavailableWithoutContract(t *testing.T) {
	_, err := adapter(t, nil).Fetch(context.Background(), "getEventSummary", map[string]any{"clusterId": "c1"})
	if !errors.Is(err, tools.ErrDynamicUnavailable) {
		t.Fatalf("err=%v", err)
	}
}

// TestAdapterRejectsUnknownAndBlockedOperations는 어댑터가 계약에 없는 operationId를 비슷한
// 이름으로 추측하지 않고, 노출 안전 게이트가 BLOCKED로 분류한 Operation도 실행하지 않음을 본다.
func TestAdapterRejectsUnknownAndBlockedOperations(t *testing.T) {
	registry, err := Build(fixtureContract(t), Options{StaticToolNames: tools.StaticNames()})
	if err != nil {
		t.Fatal(err)
	}
	a := adapter(t, registry)
	for _, id := range []string{"getEventSummaries", "listConsumerGroup", "listClusters"} {
		if _, err := a.Fetch(context.Background(), id, map[string]any{"id": "c1"}); !errors.Is(err, tools.ErrDynamicUnavailable) {
			t.Fatalf("%s: err=%v", id, err)
		}
	}
}

// TestAdapterFindsPromotedOperations는 Promotion 대상 두 Operation이 선택·노출 배치와 무관하게
// 계약에서 조회되고, 계약이 선언한 REST 경로를 쓰는지 본다.
func TestAdapterFindsPromotedOperations(t *testing.T) {
	registry, err := Build(fixtureContract(t), Options{StaticToolNames: tools.StaticNames(), Operations: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	for id, path := range map[string]string{
		tools.OperationEventSummary:      "/api/events/summary",
		tools.OperationListConsumerGroup: "/api/clusters/{id}/consumer-groups",
	} {
		entry, ok := registry.LookupOperation(id)
		if !ok {
			t.Fatalf("%s를 계약에서 찾지 못했습니다", id)
		}
		if entry.Placement != PlacementNotSelected {
			t.Fatalf("%s: 선택하지 않았는데 배치가 %s입니다", id, entry.Placement)
		}
		if entry.Tool.Operation.Path != path {
			t.Fatalf("%s: 경로가 %s입니다", id, entry.Tool.Operation.Path)
		}
	}
}
