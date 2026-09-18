package dynamic

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

// Adapter는 기존 MCP 도구가 내부 실행만 Dynamic으로 바꾸게 하는 Stable Adapter다.
//
//	기존 MCP Tool → Stable Adapter → operationId → Dynamic Registry → Generic REST Executor → REST
//
// 도구 이름·Input Schema·결과 계약은 Static 그대로이고, 바뀌는 것은 REST 경로를 계약에서
// 가져온다는 점뿐이다. 응답은 호출자가 기존 공개 범위로 투영하므로 원문을 그대로 내보내지 않는다.
// 같은 이름의 Dynamic 도구를 따로 등록하지 않으므로 중복 노출도 생기지 않는다.
type Adapter struct {
	runtime *Runtime
	client  *ableops.Client
}

// Adapter는 이 Runtime이 서비스 중인 계약으로 실행하는 Stable Adapter다.
// 계약을 아직 적재하지 못했거나 필요한 operationId가 없으면 호출자가 Static 경로를 쓴다.
func (r *Runtime) Adapter() *Adapter {
	return &Adapter{runtime: r, client: r.executor.client}
}

// unavailable은 Dynamic 인프라를 쓸 수 없다는 뜻이다. 백엔드 실패에는 쓰지 않는다.
func unavailable(reason string) error {
	return fmt.Errorf("%w: %s", tools.ErrDynamicUnavailable, reason)
}

// Fetch는 operationId로 계약을 찾아 REST GET 한 번을 실행하고 본문 원문을 돌려준다.
// REST 경로·쿼리는 계약의 것이며 이 파일에 하드코딩하지 않는다. 비슷한 이름의 Operation을
// 추측하지 않으며, 계약에 없으면 Dynamic Promotion을 쓰지 않는다.
func (a *Adapter) Fetch(ctx context.Context, operationID string, args map[string]any) (json.RawMessage, error) {
	registry := a.runtime.Current()
	if registry == nil {
		return nil, unavailable("계약을 아직 적재하지 못했습니다")
	}
	entry, ok := registry.LookupOperation(operationID)
	if !ok {
		return nil, unavailable("계약에 해당 operationId가 없습니다")
	}
	op := entry.Tool.Operation
	// BLOCKED는 노출 안전 게이트가 어떤 경로로도 실행하지 않기로 한 Operation이다.
	if entry.Exposure == ExposureBlocked || op.Method != http.MethodGet || !op.MCPEnabled {
		return nil, unavailable("계약이 이 Operation을 안전한 GET 실행 대상으로 두지 않습니다")
	}
	segments, query, err := bind(op, args)
	if err != nil {
		// 계약의 파라미터 구성이 어댑터가 아는 것과 달라졌다. 추측해 보내지 않는다.
		return nil, unavailable("어댑터 인자를 계약 파라미터로 바꿀 수 없습니다")
	}
	response, err := a.client.GetRaw(ctx, segments, query, false)
	if err != nil {
		// 백엔드 4xx/5xx/timeout이다. Static으로 재호출하지 않고 그대로 올린다.
		return nil, err
	}
	if response.Body == nil {
		return nil, ableops.PublicError("invalid_response", response.Status)
	}
	return response.Body, nil
}
