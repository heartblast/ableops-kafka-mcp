package ableops

import (
	"context"
	"sync/atomic"
)

// MaxOperationRequests는 한 도구 실행의 하위 REST 호출 수에 적용하는 안전 상한이다.
// 개별 도구는 확인된 API 조합만 호출하며 이 상한까지 조회를 늘리지 않는다.
const MaxOperationRequests = 8

type operationKey struct{}

// BeginOperation은 여러 GET이 같은 전체 시간과 호출 예산을 공유하게 한다.
// 예산은 요청 context에만 속하며 사용자 간에 공유되지 않는다.
func (c *Client) BeginOperation(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	return context.WithValue(ctx, operationKey{}, new(atomic.Int32)), cancel
}

func reserveOperationRequest(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return requestError(ctx, err)
	}
	if count, ok := ctx.Value(operationKey{}).(*atomic.Int32); ok && count.Add(1) > MaxOperationRequests {
		return publicError("call_limit_exceeded", 0)
	}
	return nil
}
