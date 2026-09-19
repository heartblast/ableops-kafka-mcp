package extension

// Core Host API 주소에서 Backend REST origin 을 만든다.
//
// Managed 모드에서 운영자는 ABLEOPS_BASE_URL 을 설정하지 않는다. Core 가 기동 시 넘긴
// HostURL(예: http://127.0.0.1:8080/api/extensions/_host)이 곧 "이 MCP 가 말을 걸어야 할
// AbleOps 서버"이므로, 그 URL 에서 **scheme 과 host 만** 취해 origin 을 만든다.

import (
	"errors"
	"net/url"
	"strings"
)

// backendOrigin은 Host API 베이스 URL 에서 Backend REST origin 을 뽑는다.
//
// 문자열 치환을 쓰지 않는다 — "/api/extensions/_host" 같은 접미사를 잘라내는 방식은 Core 가
// Host API 경로를 바꾸는 순간 조용히 엉뚱한 주소를 만든다. 구조를 파싱해 scheme+host 만 남긴다.
//
// 돌려주는 값은 loopback HTTP 여부(allowHTTP)를 함께 담는다. HTTP 는 loopback 에서만 허용하며,
// 그 외의 평문 HTTP 는 Backend 세션 토큰이 평문으로 흐른다는 뜻이므로 거부한다.
func backendOrigin(hostURL string) (origin string, allowHTTP bool, err error) {
	raw := strings.TrimSpace(hostURL)
	if raw == "" {
		return "", false, errors.New("Core Host API 주소가 비어 있습니다")
	}
	u, parseErr := url.Parse(raw)
	if parseErr != nil {
		return "", false, errors.New("Core Host API 주소를 해석할 수 없습니다")
	}
	// 자격증명·query·fragment 가 섞인 주소는 origin 이 아니다. 잘라내고 쓰는 대신 거부한다 —
	// 여기서 관대하게 굴면 Core 설정 오류가 MCP 의 Backend 주소 오류로 둔갑한다.
	if u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", false, errors.New("Core Host API 주소에 자격증명·query·fragment 를 쓸 수 없습니다")
	}
	host := u.Hostname()
	if u.Host == "" || host == "" {
		return "", false, errors.New("Core Host API 주소에 호스트가 없습니다")
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !loopbackHostname(host) {
			return "", false, errors.New("평문 HTTP Core Host API 주소는 loopback 에서만 허용합니다")
		}
		allowHTTP = true
	default:
		return "", false, errors.New("Core Host API 주소는 http 또는 https 여야 합니다")
	}
	// Path 를 버리는 것이 이 함수의 전부다. `(&url.URL{Scheme, Host}).String()` 은
	// "<scheme>://<host>" 만 만든다.
	return (&url.URL{Scheme: u.Scheme, Host: u.Host}).String(), allowHTTP, nil
}

// loopbackHostname은 config 패키지가 평문 HTTP 를 허용하는 호스트 집합과 **같은 판정**이다.
// 한쪽만 넓히면 여기서는 통과하고 설정 검증에서 거부되는 조합이 생긴다.
func loopbackHostname(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	return false
}
