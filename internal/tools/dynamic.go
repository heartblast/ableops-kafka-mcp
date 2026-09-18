package tools

import (
	"context"
	"encoding/json"
	"errors"
)

// Promotion 대상 Operation. 기존 MCP 도구 이름·Input Schema는 그대로 두고 내부 실행만
// 이 operationId로 계약에서 REST 경로를 가져와 수행한다. 비슷한 이름을 추측하지 않는다.
const (
	OperationEventSummary      = "getEventSummary"
	OperationListConsumerGroup = "listConsumerGroups"
)

// ErrDynamicUnavailable은 Dynamic 실행 인프라를 쓸 수 없다는 뜻이다(계약 미적재, operationId 부재,
// 계약 파라미터와 어댑터 인자 불일치 등). 이때만 기존 Static 경로를 쓴다.
// 백엔드 4xx/5xx/timeout은 여기에 해당하지 않으며 Static으로 재호출하지 않는다.
var ErrDynamicUnavailable = errors.New("동적 도구 실행 경로를 사용할 수 없습니다")

// DynamicSource는 operationId로 계약의 REST GET 한 번을 실행하는 경로다.
// 구현은 internal/dynamic이며 이 패키지는 계약(operationId)만 안다. REST 경로는 알지 않는다.
type DynamicSource interface {
	// Fetch는 계약 파라미터 이름으로 준 인자를 REST 요청으로 바꿔 본문 원문을 돌려준다.
	// 인프라를 쓸 수 없으면 ErrDynamicUnavailable을, 백엔드 실패는 *ableops.Error를 돌려준다.
	Fetch(ctx context.Context, operationID string, args map[string]any) (json.RawMessage, error)
}

// Option은 도구 등록 선택 사항이다.
type Option func(*service)

// WithDynamic은 Promotion된 도구가 내부 실행에 쓸 Dynamic 실행 경로를 연결한다.
// nil이면 모든 도구가 기존 Static 경로를 그대로 쓴다.
func WithDynamic(source DynamicSource) Option {
	return func(s *service) { s.dynamic = source }
}

// fetch는 Dynamic 경로로 한 번 조회한다.
//
//	used=false  → Dynamic 인프라 사용 불가. 호출자가 기존 Static 경로를 쓴다.
//	used=true   → Dynamic 경로를 실제로 사용했다. err가 있으면 그대로 반환하고 Static으로 재호출하지 않는다.
func (s *service) fetch(ctx context.Context, operationID string, args map[string]any) (body json.RawMessage, used bool, err error) {
	if s.dynamic == nil {
		return nil, false, nil
	}
	body, err = s.dynamic.Fetch(ctx, operationID, args)
	if errors.Is(err, ErrDynamicUnavailable) {
		return nil, false, nil
	}
	return body, true, err
}
