package openapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
)

// contractServer는 교체 가능한 계약 본문과 ETag를 제공하고 받은 조건부 헤더를 기록한다.
type contractServer struct {
	mu          sync.Mutex
	status      int
	body        string
	etag        string
	conditional []string
}

func (s *contractServer) set(status int, body, etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.body, s.etag = status, body, etag
}

func (s *contractServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Header.Get("Authorization") != "" {
		http.Error(w, "unexpected auth", http.StatusBadRequest)
		return
	}
	match := r.Header.Get("If-None-Match")
	s.conditional = append(s.conditional, match)
	if s.status == http.StatusOK && match != "" && match == s.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if s.etag != "" {
		w.Header().Set("ETag", s.etag)
	}
	w.WriteHeader(s.status)
	io.WriteString(w, s.body)
}

func newLoader(t *testing.T, handler http.Handler) *Loader {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	client, err := ableops.NewClient(config.Config{BaseURL: base, Token: "synthetic-loader-token", Timeout: time.Second, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	return NewLoader(client)
}

func contractWith(id string) string {
	return string(specWith(`{"/api/x":{"get":{"operationId":"`+id+`","x-mcp-enabled":true,`+okResponses+`}}}`, ""))
}

func TestLoaderETagAndLastKnownGood(t *testing.T) {
	ctx := context.Background()
	backend := &contractServer{}
	backend.set(http.StatusOK, contractWith("getFirst"), `"v1"`)
	loader := newLoader(t, backend)

	first, err := loader.Load(ctx)
	if err != nil || first.NotModified || first.Contract.ETag != `"v1"` || first.Contract.Operations[0].ID != "getFirst" {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	// 304: 기존 Registry의 기반 계약을 그대로 유지한다.
	same, err := loader.Load(ctx)
	if err != nil || !same.NotModified || same.Contract != first.Contract {
		t.Fatalf("same=%+v err=%v", same, err)
	}
	// 200 + 새 ETag: 새 계약을 돌려준다.
	backend.set(http.StatusOK, contractWith("getSecond"), `"v2"`)
	second, err := loader.Load(ctx)
	if err != nil || second.NotModified || second.Contract.ETag != `"v2"` || second.Contract.Operations[0].ID != "getSecond" {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	// 새 계약 검증 실패: 오류와 함께 현재 정상 계약을 유지한다.
	backend.set(http.StatusOK, `{"openapi":"3.1.0","paths":{}}`, `"v3"`)
	broken, err := loader.Load(ctx)
	var ce *ContractError
	if !errors.As(err, &ce) || broken.Contract != second.Contract || loader.Current() != second.Contract {
		t.Fatalf("broken=%+v err=%v", broken, err)
	}
	backend.set(http.StatusOK, `not json`, `"v4"`)
	if _, err := loader.Load(ctx); !errors.As(err, &ce) || loader.Current() != second.Contract {
		t.Fatalf("invalid json err=%v", err)
	}
	// 전송 실패도 교체하지 않는다.
	backend.set(http.StatusInternalServerError, "backend-secret", "")
	down, err := loader.Load(ctx)
	var upstream *ableops.Error
	if !errors.As(err, &upstream) || upstream.Code != "backend_unavailable" || down.Contract != second.Contract {
		t.Fatalf("down=%+v err=%v", down, err)
	}
	// 복구 후 마지막 정상 계약의 ETag로 조건부 조회한다.
	backend.set(http.StatusOK, contractWith("getSecond"), `"v2"`)
	recovered, err := loader.Load(ctx)
	if err != nil || !recovered.NotModified || recovered.Contract != second.Contract {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
	want := []string{"", `"v1"`, `"v1"`, `"v2"`, `"v2"`, `"v2"`, `"v2"`}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.conditional) != len(want) {
		t.Fatalf("conditional=%q", backend.conditional)
	}
	for i := range want {
		if backend.conditional[i] != want[i] {
			t.Fatalf("conditional=%q want=%q", backend.conditional, want)
		}
	}
}

func TestLoaderWithoutETagAlwaysFetches(t *testing.T) {
	backend := &contractServer{}
	backend.set(http.StatusOK, contractWith("getA"), "")
	loader := newLoader(t, backend)
	for i := 0; i < 2; i++ {
		result, err := loader.Load(context.Background())
		if err != nil || result.NotModified || result.Contract.ETag != "" {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	if backend.conditional[0] != "" || backend.conditional[1] != "" {
		t.Fatalf("ETag 없는 계약에 조건부 요청: %q", backend.conditional)
	}
}

func TestLoaderInitialFailureHasNoContract(t *testing.T) {
	backend := &contractServer{}
	backend.set(http.StatusServiceUnavailable, "down", "")
	loader := newLoader(t, backend)
	result, err := loader.Load(context.Background())
	if err == nil || result.Contract != nil || loader.Current() != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

type fakeFetcher struct{ doc ableops.OpenAPIDocument }

func (f fakeFetcher) FetchOpenAPI(context.Context, string) (ableops.OpenAPIDocument, error) {
	return f.doc, nil
}

func TestLoaderRejectsNotModifiedWithoutContract(t *testing.T) {
	loader := NewLoader(fakeFetcher{doc: ableops.OpenAPIDocument{NotModified: true, ETag: `"x"`}})
	result, err := loader.Load(context.Background())
	var ce *ContractError
	if !errors.As(err, &ce) || ce.Code != "unexpected_not_modified" || result.Contract != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

// 호출자 검증(Registry 생성)에 실패한 계약은 ETag와 함께 커밋하지 않는다. 먼저 커밋하면 다음 조회가
// 304를 받아 고쳐지지 않은 계약에 머물고, 서비스 중인 도구와 보관한 계약이 어긋난다.
func TestLoaderValidatedCommitsOnlyAfterAccept(t *testing.T) {
	ctx := context.Background()
	backend := &contractServer{}
	backend.set(http.StatusOK, contractWith("getFirst"), `"v1"`)
	loader := newLoader(t, backend)
	rejected := errors.New("합성 거부")
	calls := 0
	reject := func(*Contract) error { calls++; return rejected }
	accept := func(*Contract) error { calls++; return nil }

	// 최초 거부: 계약이 없는 상태를 유지한다.
	result, err := loader.LoadValidated(ctx, reject)
	if !errors.Is(err, rejected) || result.Contract != nil || loader.Current() != nil || calls != 1 {
		t.Fatalf("initial reject=%+v err=%v calls=%d", result, err, calls)
	}
	first, err := loader.LoadValidated(ctx, accept)
	if err != nil || loader.Current() != first.Contract || first.Contract.ETag != `"v1"` {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	// 304에서는 호출자 검증을 다시 부르지 않는다.
	calls = 0
	if same, err := loader.LoadValidated(ctx, reject); err != nil || !same.NotModified || calls != 0 {
		t.Fatalf("304=%+v err=%v calls=%d", same, err, calls)
	}
	// 새 계약을 거부하면 기존 계약과 ETag를 유지한다.
	backend.set(http.StatusOK, contractWith("getSecond"), `"v2"`)
	denied, err := loader.LoadValidated(ctx, func(c *Contract) error {
		if c.ETag != `"v2"` || c.Operations[0].ID != "getSecond" {
			t.Errorf("검증 대상이 새 계약이 아님: %+v", c)
		}
		return rejected
	})
	if !errors.Is(err, rejected) || denied.Contract != first.Contract || loader.Current() != first.Contract {
		t.Fatalf("denied=%+v err=%v", denied, err)
	}
	// 다음 조회는 기존 ETag로 확인하므로 같은 새 계약을 다시 받아 검증한다.
	second, err := loader.LoadValidated(ctx, accept)
	if err != nil || second.NotModified || second.Contract.ETag != `"v2"` || loader.Current() != second.Contract {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	want := []string{"", "", `"v1"`, `"v1"`, `"v1"`}
	if len(backend.conditional) != len(want) {
		t.Fatalf("conditional=%q", backend.conditional)
	}
	for i := range want {
		if backend.conditional[i] != want[i] {
			t.Fatalf("conditional=%q want=%q", backend.conditional, want)
		}
	}
}
