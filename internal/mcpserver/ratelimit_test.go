package mcpserver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

// requestAs는 지정한 토큰과 피어로 /mcp 요청을 만든다.
func requestAs(token, peer string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8081/mcp", strings.NewReader(pingPayload))
	r.RemoteAddr = peer
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	r.Header.Set("MCP-Protocol-Version", "2025-11-25")
	return r
}

func statusOf(t *testing.T, handler http.Handler, r *http.Request) int {
	t.Helper()
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w.Code
}

// TestHTTPAuthFailureRateLimit은 짧은 시간 내 반복 인증 실패가 차단되는지 본다.
func TestHTTPAuthFailureRateLimit(t *testing.T) {
	opts := httpTestOptions()
	opts.MaxAuthFailuresPerPeerPerWindow = 3
	handler, err := NewHTTPHandler(emptyMCPServer(), opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if status := statusOf(t, handler, requestAs("synthetic-wrong-token", "127.0.0.1:40001")); status != 401 {
			t.Fatalf("%d번째 인증 실패 상태 = %d", i+1, status)
		}
	}
	// 예산을 소진하면 인증기를 부르기도 전에 429 다.
	if status := statusOf(t, handler, requestAs("synthetic-wrong-token", "127.0.0.1:40001")); status != 429 {
		t.Fatalf("반복 인증 실패 차단 안 됨: %d", status)
	}
	// ⚠ 정상 토큰도 같은 피어면 막힌다 — 자격증명 대입 중인 피어를 통과시키면 제한의 의미가 없다.
	if status := statusOf(t, handler, requestAs(syntheticMCPToken, "127.0.0.1:40001")); status != 429 {
		t.Fatalf("차단된 피어의 정상 요청 상태 = %d", status)
	}
	// 다른 피어는 영향받지 않는다.
	if status := statusOf(t, handler, requestAs(syntheticMCPToken, "127.0.0.2:40001")); status != 200 {
		t.Fatalf("다른 피어 정상 요청 상태 = %d", status)
	}
}

// TestHTTPUserRateLimitIsolationAndRecovery는 사용자별 반복 요청 제한, 사용자 간 독립성,
// 시간 경과에 따른 회복을 함께 본다.
func TestHTTPUserRateLimitIsolationAndRecovery(t *testing.T) {
	opts := httpTestOptions()
	opts.MaxRequestsPerUserPerWindow = 2
	opts.RateLimitWindow = time.Minute
	handler, err := NewHTTPHandler(emptyMCPServer(), opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if status := statusOf(t, handler, requestAs(syntheticMCPToken, loopbackPeerAddr)); status != 200 {
			t.Fatalf("user-a %d번째 요청 상태 = %d", i+1, status)
		}
	}
	if status := statusOf(t, handler, requestAs(syntheticMCPToken, loopbackPeerAddr)); status != 429 {
		t.Fatalf("user-a 반복 요청 제한 안 됨: %d", status)
	}
	// 사용자 간 독립성 — 같은 피어라도 user-b 는 자기 예산을 쓴다.
	if status := statusOf(t, handler, requestAs("synthetic-mcp-token-b", loopbackPeerAddr)); status != 200 {
		t.Fatalf("user-b 요청이 user-a 제한에 걸림: %d", status)
	}
}

// TestRateLimiterRecovery는 창이 지나면 제한이 풀리는지 고정 시계로 본다.
func TestRateLimiterRecovery(t *testing.T) {
	now := time.Now()
	limiter := newRateLimiter(2, time.Minute, 16)
	limiter.now = func() time.Time { return now }
	if !limiter.allow("k") || !limiter.allow("k") {
		t.Fatal("한도 안 요청이 거부됐다")
	}
	if limiter.allow("k") {
		t.Fatal("한도 초과가 통과했다")
	}
	// 절반이 지나면 한 개만 회복된다(창 전체를 기다리지 않는다).
	now = now.Add(30 * time.Second)
	if !limiter.allow("k") {
		t.Fatal("부분 회복 실패")
	}
	if limiter.allow("k") {
		t.Fatal("회복량보다 많이 통과했다")
	}
	// 창 전체가 지나면 완전히 회복된다.
	now = now.Add(time.Minute)
	if !limiter.allow("k") || !limiter.allow("k") {
		t.Fatal("전체 회복 실패")
	}
}

// TestRateLimiterEntryCap은 키가 무한히 늘지 않는지 본다(메모리 상한).
func TestRateLimiterEntryCap(t *testing.T) {
	now := time.Now()
	limiter := newRateLimiter(4, time.Minute, 8)
	limiter.now = func() time.Time { return now }
	for i := range 10000 {
		limiter.allow(fmt.Sprintf("key-%d", i))
		now = now.Add(time.Millisecond)
	}
	if size := limiter.size(); size > 8 {
		t.Fatalf("제한 상태 키 수 = %d (상한 8)", size)
	}
	// 상한에 닿아도 판정은 계속 동작해야 한다 — 새 키를 거부하는 방식이면 여기서 막힌다.
	if !limiter.allow("fresh-key") {
		t.Fatal("상한 도달 후 새 키가 거부됐다")
	}
}

// TestRateLimitDisabledAndValidation은 설정 경계를 본다.
func TestRateLimitDisabledAndValidation(t *testing.T) {
	if newRateLimiter(0, time.Minute, 8) != nil || newRateLimiter(3, 0, 8) != nil {
		t.Fatal("비활성 설정이 제한기를 만들었다")
	}
	// nil 제한기는 항상 통과한다(호출자가 분기하지 않아도 되도록).
	var disabled *rateLimiter
	if !disabled.allow("k") || !disabled.permitted("k") || disabled.size() != 0 {
		t.Fatal("비활성 제한기가 요청을 막았다")
	}
	disabled.penalize("k")
	for _, opts := range []HTTPOptions{
		{MaxRequestsPerUserPerWindow: -1},
		{MaxAuthFailuresPerPeerPerWindow: -1},
		{MaxRateLimitEntries: -1},
		{RateLimitWindow: -time.Second},
	} {
		opts.Authenticate = func(context.Context, string) (requestctx.Principal, error) {
			return requestctx.Principal{}, nil
		}
		if _, err := opts.normalized(); err == nil {
			t.Fatal("음수 빈도 제한 설정이 통과했다")
		}
	}
}

// TestPeerKey는 제한 키가 피어 주소에서만 나오는지 본다.
func TestPeerKey(t *testing.T) {
	for addr, want := range map[string]string{
		"127.0.0.1:54321": "127.0.0.1",
		"[::1]:54321":     "::1",
		"":                "unknown",
		"pipe":            "pipe",
	} {
		if got := peerKey(addr); got != want {
			t.Fatalf("peerKey(%q) = %q, want %q", addr, got, want)
		}
	}
}
