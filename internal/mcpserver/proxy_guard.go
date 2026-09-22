package mcpserver

// Reverse Proxy 경유 노출 방어다.
//
// MCP HTTP 서버는 loopback 에서만 수신한다. 그런데 운영자가 실수로 nginx 같은 프록시를
// `proxy_pass http://127.0.0.1:8081` 로 걸면, 서버 입장에서는 **여전히 loopback 연결**이므로
// 기존 Host·Origin 검사만으로는 외부 노출을 알아채지 못한다.
//
// 그래서 fail-closed 로 간다. 프록시가 붙였을 법한 헤더가 하나라도 보이면 거부한다.
// ⚠ 이 헤더들을 "신뢰해서 실제 클라이언트 IP 를 복원"하는 방향으로 바꾸지 않는다.
// loopback 전용 서버에는 신뢰할 수 있는 프록시가 없으므로, 헤더의 존재 자체가 이상 신호다.
// 정상 경로(Claude Desktop, Codex, standalone HTTP, Managed Extension)는 이 헤더를 붙이지 않는다.

import (
	"net"
	"net/http"
	"strings"
)

// forwardedHeaderPrefixes / forwardedHeaders는 프록시 경유를 드러내는 헤더다.
// 접두사로도 잡는 이유는 `X-Forwarded-*` 변종이 제품마다 다르기 때문이다.
var forwardedHeaderPrefixes = []string{"X-Forwarded-", "X-Original-Forwarded-"}

var forwardedHeaders = map[string]bool{
	"Forwarded":           true,
	"X-Real-Ip":           true,
	"X-Original-Url":      true,
	"X-Original-Host":     true,
	"X-Rewrite-Url":       true,
	"X-Client-Ip":         true,
	"X-Host":              true,
	"Via":                 true,
	"Proxy-Connection":    true,
	"Proxy-Authorization": true,
}

// forwardedRequest는 요청이 프록시를 거쳤을 가능성을 보인다.
func forwardedRequest(header http.Header) bool {
	for name := range header {
		canonical := http.CanonicalHeaderKey(name)
		if forwardedHeaders[canonical] {
			return true
		}
		for _, prefix := range forwardedHeaderPrefixes {
			if strings.HasPrefix(canonical, prefix) {
				return true
			}
		}
	}
	return false
}

// loopbackPeer는 TCP 피어가 loopback 인지 본다.
// 주소를 해석할 수 없으면(빈 값, in-process 파이프, Managed 호스트의 비 TCP 전송) 판단하지
// 않고 통과시킨다 — 여기서 막아야 할 것은 "명백히 외부에서 온 연결"이고, 해석 불가를
// 거부로 바꾸면 기존 Managed/Extension 사용 방식이 깨진다.
func loopbackPeer(remoteAddr string) bool {
	if remoteAddr == "" {
		return true
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return true
	}
	return ip.IsLoopback()
}
