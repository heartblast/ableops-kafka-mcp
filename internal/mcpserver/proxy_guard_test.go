package mcpserver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHTTPForwardedHeadersDenied는 프록시 헤더를 이용한 우회 시도를 차단하는지 본다.
// 실제 공격 경로는 "운영자가 실수로 reverse proxy 를 127.0.0.1:8081 로 걸어 둔 상태"이며,
// 그때 서버가 보는 연결은 여전히 loopback 이므로 Host·Origin 검사로는 잡히지 않는다.
func TestHTTPForwardedHeadersDenied(t *testing.T) {
	handler, err := NewHTTPHandler(emptyMCPServer(), httpTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	for _, header := range []struct{ name, value string }{
		{"Forwarded", "for=203.0.113.10;host=mcp.example;proto=https"},
		{"X-Forwarded-For", "203.0.113.10"},
		{"X-Forwarded-Host", "mcp.example"},
		{"X-Forwarded-Proto", "https"},
		{"X-Forwarded-Port", "443"},
		{"X-Forwarded-Server", "proxy.example"},
		{"X-Original-Forwarded-For", "203.0.113.10"},
		{"X-Real-IP", "203.0.113.10"},
		{"X-Client-IP", "203.0.113.10"},
		{"X-Original-URL", "/mcp"},
		{"X-Rewrite-URL", "/mcp"},
		{"Via", "1.1 proxy.example"},
		{"Proxy-Connection", "keep-alive"},
	} {
		t.Run(header.name, func(t *testing.T) {
			// 자격증명이 올바른 요청이어도 거부돼야 한다 — 인증 성공 여부와 무관한 경계다.
			r := requestAs(syntheticMCPToken, loopbackPeerAddr)
			r.Header.Set(header.name, header.value)
			if status := statusOf(t, handler, r); status != http.StatusForbidden {
				t.Fatalf("%s 상태 = %d, want 403", header.name, status)
			}
		})
	}
	// 같은 방어가 헬스 체크에도 적용된다(프록시 뒤라는 사실 자체가 거부 사유다).
	health := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8081/healthz", nil)
	health.RemoteAddr = loopbackPeerAddr
	health.Header.Set("X-Forwarded-For", "203.0.113.10")
	if status := statusOf(t, handler, health); status != http.StatusForbidden {
		t.Fatalf("프록시 경유 healthz 상태 = %d, want 403", status)
	}
}

// TestHTTPNonLoopbackPeerDenied는 loopback 이 아닌 피어를 거부하는지 본다.
// 주소를 해석할 수 없는 전송(Managed 호스트의 in-process 라우팅 등)은 판단하지 않고 통과시킨다.
func TestHTTPNonLoopbackPeerDenied(t *testing.T) {
	handler, err := NewHTTPHandler(emptyMCPServer(), httpTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range []string{"203.0.113.10:5000", "192.0.2.1:1234", "[2001:db8::1]:5000"} {
		if status := statusOf(t, handler, requestAs(syntheticMCPToken, peer)); status != http.StatusForbidden {
			t.Fatalf("외부 피어 %s 상태 = %d, want 403", peer, status)
		}
	}
	// 기존 loopback 직접 호출은 그대로 동작한다.
	for _, peer := range []string{"127.0.0.1:54321", "[::1]:54321", "", "pipe"} {
		if status := statusOf(t, handler, requestAs(syntheticMCPToken, peer)); status != http.StatusOK {
			t.Fatalf("loopback 피어 %q 상태 = %d, want 200", peer, status)
		}
	}
}

// TestInternalDelegationForwardedDenied는 서버간 경로가 프록시 뒤에서 호출되는 경우를 막는지 본다.
// ⚠ 이 경로는 공유 비밀만으로 위임 토큰을 내주므로, 외부 노출은 곧 토큰 유출이다.
func TestInternalDelegationForwardedDenied(t *testing.T) {
	f := newDelegationFixture(t)
	for _, name := range []string{"X-Forwarded-For", "X-Forwarded-Host", "Forwarded", "Via"} {
		response, grant := f.issue(t, fmt.Sprintf(`{"backendToken":%q}`, webSessionToken("a")), http.Header{name: []string{"203.0.113.10"}})
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("%s 로 발급된 상태 = %d, want 403", name, response.StatusCode)
		}
		if grant.Token != "" {
			t.Fatal("거부된 요청이 위임 토큰을 돌려줬다")
		}
	}
	// 프록시 헤더가 없는 기존 서버간 호출은 그대로 성공한다.
	response, grant := f.issue(t, fmt.Sprintf(`{"backendToken":%q}`, webSessionToken("a")), nil)
	if response.StatusCode != http.StatusCreated || grant.Token == "" {
		t.Fatalf("정상 서버간 발급 상태 = %d", response.StatusCode)
	}
	// 감사 로그에는 거부 사실만 남고 헤더 값은 남지 않는다.
	if strings.Contains(f.logs.String(), "203.0.113.10") {
		t.Fatal("감사 로그에 프록시 헤더 값이 남았다")
	}
}
