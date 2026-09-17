package openapi

import (
	"context"
	"sync"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
)

// Fetcher는 계약 문서 전송을 담당한다. 운영에서는 *ableops.Client가 구현한다.
type Fetcher interface {
	FetchOpenAPI(ctx context.Context, etag string) (ableops.OpenAPIDocument, error)
}

// Loader는 마지막으로 검증에 성공한 계약(Last Known Good)과 그 ETag를 보관한다.
//
// 보관한 계약과 ETag는 호출자가 실제로 서비스 중인 도구 집합과 짝을 이룬다. 그래서 새 계약은
// 문서 검증과 호출자 검증(Registry 생성)을 모두 통과한 뒤에만 커밋한다. 호출자 검증에 실패한
// 계약의 ETag를 먼저 보관하면 다음 조회가 304를 받아 고쳐지지 않은 계약에 영구히 머문다.
type Loader struct {
	fetcher Fetcher

	mu      sync.Mutex
	current *Contract
}

// LoadResult는 조회 결과다. NotModified이면 Contract는 기존 계약 그대로다.
type LoadResult struct {
	Contract    *Contract
	NotModified bool
}

func NewLoader(fetcher Fetcher) *Loader { return &Loader{fetcher: fetcher} }

// Current는 마지막 정상 계약이다. 한 번도 성공하지 못했으면 nil이다.
func (l *Loader) Current() *Contract {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current
}

// Load는 호출자 검증 없이 LoadValidated를 실행한다.
func (l *Loader) Load(ctx context.Context) (LoadResult, error) {
	return l.LoadValidated(ctx, nil)
}

// LoadValidated는 계약을 조회한다. 기존 계약에 ETag가 있으면 If-None-Match를 보낸다.
//
//   - 304: 기존 계약을 그대로 돌려준다(NotModified). accept는 호출하지 않는다.
//   - 200 + 문서 검증 + accept 성공: 새 계약으로 교체하고 돌려준다.
//   - 전송 실패·문서 검증 실패·accept 실패: 오류와 함께 기존 계약(없으면 nil)을 돌려주며
//     계약과 ETag를 교체하지 않는다. 다음 조회는 기존 ETag로 다시 확인한다.
//
// accept는 잠금 안에서 호출되므로 Loader 메서드를 다시 부르면 안 된다.
// 동시 호출은 직렬화한다. 같은 ETag로 중복 조회하지 않게 하기 위함이다.
func (l *Loader) LoadValidated(ctx context.Context, accept func(*Contract) error) (LoadResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	etag := ""
	if l.current != nil {
		etag = l.current.ETag
	}
	doc, err := l.fetcher.FetchOpenAPI(ctx, etag)
	if err != nil {
		return LoadResult{Contract: l.current}, err
	}
	if doc.NotModified {
		if l.current == nil {
			return LoadResult{}, contractError("unexpected_not_modified", "보관한 계약 없이 304를 받았습니다.")
		}
		return LoadResult{Contract: l.current, NotModified: true}, nil
	}
	contract, err := Parse(doc.Body)
	if err != nil {
		return LoadResult{Contract: l.current}, err
	}
	contract.ETag = doc.ETag
	if accept != nil {
		if err := accept(contract); err != nil {
			return LoadResult{Contract: l.current}, err
		}
	}
	l.current = contract
	return LoadResult{Contract: contract}, nil
}
