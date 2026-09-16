package ableops

// Error는 안전한 고정 설명만 제공한다. 백엔드 응답 본문, 네트워크 오류 원문,
// 요청 URL, 인증 정보는 포함하지 않는다.
type Error struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func publicError(code string, status int) *Error {
	message := "The backend request failed."
	switch code {
	case "authentication_required":
		message = "The Backend session is invalid or expired. Renew the delegated credential; for stdio restart with a new session token."
	case "access_denied":
		message = "The backend denied access to the requested resource."
	case "not_found":
		message = "The backend resource was not found."
	case "rate_limited":
		message = "The backend request limit was reached. Try again later."
	case "backend_unavailable":
		message = "The backend is unavailable or could not complete the request."
	case "timeout":
		message = "The backend request timed out."
	case "canceled":
		message = "The request was canceled."
	case "invalid_response":
		message = "The backend returned an invalid or unsafe response."
	case "response_too_large":
		message = "The backend response exceeded the transport size limit; no partial response was returned."
	case "redirect_blocked":
		message = "The backend returned a redirect, which is not allowed."
	case "invalid_request":
		message = "The request arguments are invalid."
	case "call_limit_exceeded":
		message = "도구 실행의 하위 API 호출 수 제한을 초과했습니다."
	case "unsupported":
		message = "현재 백엔드 API 계약으로 안전하게 조회할 수 없는 대상 또는 기능입니다."
	}
	return &Error{Code: code, Message: message, HTTPStatus: status}
}

func statusError(status int) *Error {
	switch {
	case status == 401:
		return publicError("authentication_required", status)
	case status == 403:
		return publicError("access_denied", status)
	case status == 404:
		return publicError("not_found", status)
	case status == 429:
		return publicError("rate_limited", status)
	case status >= 300 && status <= 399:
		return publicError("redirect_blocked", status)
	case status >= 500:
		return publicError("backend_unavailable", status)
	default:
		return publicError("backend_error", status)
	}
}
