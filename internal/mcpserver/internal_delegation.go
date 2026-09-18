package mcpserver

// Web/Chat Backend 전용 **서버간** 위임 발급 엔드포인트다(`POST /internal/delegations`).
//
// ⚠ 이 경로는 `/mcp` 와 성격이 정반대다. 한 핸들러에 얹되 규칙은 절대 공유하지 않는다.
//   - 브라우저가 직접 부를 수 없다 — Origin·Referer·Cookie·Sec-Fetch-* 가 하나라도 있으면 거부한다.
//   - CORS 대상이 아니다 — Access-Control-* 를 **절대** 내보내지 않으므로 http.go 의 Origin 허용
//     블록보다 **먼저** 분기해야 한다. 뒤로 밀면 허용 Origin 인 Web UI 가 이 경로에 붙을 수 있다.
//   - MCP Bearer 인증이 아니라 별도 공유 비밀(서버간 인증)로만 통과한다.
//   - 요청 본문·오류 응답에 자격증명을 되비추지 않는다.
//
// 공유 비밀이 없거나 발급기가 배선되지 않으면 이 경로는 **존재하지 않는다**(404).
// 503 으로 알리지 않는 이유: "기능은 있는데 꺼져 있다"는 사실 자체가 공격자에게 줄 정보다.

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// internalPathPrefix 아래는 전부 서버간 전용이다. 접두사로 잡아야 오타 경로가 /mcp 규칙으로 새지 않는다.
	internalPathPrefix = "/internal/"
	// internalDelegationPath는 유일한 내부 엔드포인트다.
	internalDelegationPath = "/internal/delegations"
	// internalSecretHeader는 서버간 공유 비밀을 싣는 헤더다. Authorization 을 재사용하지 않는 이유는
	// MCP Bearer 와 같은 헤더를 쓰면 한쪽 토큰을 다른 쪽에 잘못 보내는 사고가 조용히 통과하기 때문이다.
	internalSecretHeader = "X-AbleOps-Internal-Secret"
	// minInternalSecretBytes는 공유 비밀의 최소 길이다. 짧은 비밀은 loopback 이어도 무의미하다.
	minInternalSecretBytes = 32
	// maxInternalRequestBytes는 발급 요청 본문 상한이다. Backend 토큰 하나면 충분하다.
	maxInternalRequestBytes = 8 << 10
)

// DelegationGrant는 내부 발급 결과다.
// ⚠ Backend 토큰 필드를 추가하지 않는다 — 이 값은 그대로 HTTP 응답으로 직렬화된다.
type DelegationGrant struct {
	Token     string
	UserID    string
	ExpiresAt time.Time
}

// IssueDelegationFunc는 Backend 세션 자격증명을 단기 위임으로 바꾼다.
// 세션 검증은 구현체(webdelegation.Store)가 하며, 여기서는 전송과 접근 통제만 책임진다.
type IssueDelegationFunc func(ctx context.Context, backendToken string) (DelegationGrant, error)

// internalDelegationEnabled는 내부 엔드포인트 배선 여부다.
func (o HTTPOptions) internalDelegationEnabled() bool {
	return o.IssueDelegation != nil && o.InternalSecret != ""
}

// serveInternalDelegation은 서버간 발급 요청을 처리하고 감사 로그에 남길 사용자 ID를 돌려준다.
// 실패는 전부 fail 로 보고하며 어떤 경우에도 입력값을 응답에 넣지 않는다.
func serveInternalDelegation(w http.ResponseWriter, r *http.Request, opts HTTPOptions, fail func(int, string)) string {
	if !opts.internalDelegationEnabled() || r.URL.Path != internalDelegationPath {
		fail(http.StatusNotFound, "not_found")
		return ""
	}
	// 브라우저가 만든 요청은 형태만으로 걸러 낸다. 공유 비밀이 어딘가로 샜더라도
	// 브라우저 컨텍스트에서는 이 경로를 쓸 수 없어야 한다(토큰 탈취의 2차 경로 차단).
	for _, name := range []string{"Origin", "Referer", "Cookie", "Sec-Fetch-Site", "Sec-Fetch-Mode", "Sec-Fetch-Dest", "Sec-Fetch-User"} {
		if len(r.Header.Values(name)) > 0 {
			fail(http.StatusForbidden, "browser_request_denied")
			return ""
		}
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		fail(http.StatusMethodNotAllowed, "method_not_allowed")
		return ""
	}
	if !constantTimeEqual(r.Header.Get(internalSecretHeader), opts.InternalSecret) || len(r.Header.Values(internalSecretHeader)) != 1 {
		fail(http.StatusUnauthorized, "internal_authentication_required")
		return ""
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		if media, _, _ := strings.Cut(contentType, ";"); strings.TrimSpace(media) != "application/json" {
			fail(http.StatusUnsupportedMediaType, "unsupported_media_type")
			return ""
		}
	}
	body, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, maxInternalRequestBytes))
	_ = r.Body.Close()
	if readErr != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(readErr, &tooLarge) {
			fail(http.StatusRequestEntityTooLarge, "request_too_large")
			return ""
		}
		fail(http.StatusBadRequest, "invalid_request")
		return ""
	}
	var request struct {
		BackendToken string `json:"backendToken"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF || request.BackendToken == "" {
		fail(http.StatusBadRequest, "invalid_delegation_request")
		return ""
	}
	ctx, cancel := context.WithTimeout(r.Context(), opts.RequestTimeout)
	defer cancel()
	grant, err := opts.IssueDelegation(ctx, request.BackendToken)
	if err != nil {
		status, code := delegationFailure(err)
		fail(status, code)
		return ""
	}
	// 응답에는 위임 토큰과 최소 메타데이터만 싣는다. Backend 토큰은 절대 돌려주지 않는다.
	payload, marshalErr := json.Marshal(struct {
		Token     string `json:"token"`
		UserID    string `json:"userId"`
		ExpiresAt string `json:"expiresAt"`
	}{Token: grant.Token, UserID: grant.UserID, ExpiresAt: grant.ExpiresAt.UTC().Format(time.RFC3339)})
	if marshalErr != nil {
		fail(http.StatusInternalServerError, "delegation_unavailable")
		return ""
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(payload)
	return grant.UserID
}

// constantTimeEqual은 길이 차이로도 값이 새지 않도록 다이제스트를 비교한다.
func constantTimeEqual(got, want string) bool {
	if want == "" {
		return false
	}
	gotSum, wantSum := sha256.Sum256([]byte(got)), sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(gotSum[:], wantSum[:]) == 1
}

// delegationFailure는 발급 실패 코드를 HTTP 상태로 옮긴다.
// ⚠ 인증 실패(`authenticationFailure`)와 표를 합치지 않는다 — 여기서 401 은 "Backend 세션이
// 무효하다"는 뜻이고 `/mcp` 의 401 은 "MCP 토큰이 무효하다"는 뜻이라 호출자의 후속 조치가 다르다.
func delegationFailure(err error) (int, string) {
	var coded interface{ AuthCode() string }
	if errors.As(err, &coded) {
		switch code := coded.AuthCode(); code {
		case "invalid_delegation_request":
			return http.StatusBadRequest, code
		case "backend_authentication_required":
			return http.StatusUnauthorized, code
		case "access_denied":
			return http.StatusForbidden, code
		case "delegation_limit":
			return http.StatusTooManyRequests, code
		case "timeout":
			return http.StatusGatewayTimeout, code
		case "canceled":
			return http.StatusRequestTimeout, code
		case "backend_unavailable", "delegation_unavailable":
			return http.StatusServiceUnavailable, code
		}
	}
	return http.StatusServiceUnavailable, "delegation_unavailable"
}
