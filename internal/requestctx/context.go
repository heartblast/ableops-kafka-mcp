// Package requestctx는 인증된 요청의 신원과 위임 자격증명을 요청 수명에 한정한다.
package requestctx

import "context"

// Principal의 자격증명 필드는 직렬화하거나 로그에 기록하지 않는다.
type Principal struct {
	UserID       string
	ClientID     string
	BackendToken string `json:"-"`
	MCPToken     string `json:"-"`
	RequestID    string
}

type principalKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
