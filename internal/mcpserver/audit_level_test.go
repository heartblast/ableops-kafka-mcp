package mcpserver

// 감사 로그의 **수준**이 실패 코드에 따라 갈리는지 고정한다.
//
// 경계를 두드린 흔적과 발급 포화를 정상 요청과 같은 Info 로 남기면 수만 건에 섞여 반복 시도를
// 알아볼 수 없다. 반대로 흔한 실패를 경고로 올리면 경고 자체가 무의미해진다. 두 성질을 함께 본다.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// auditRecord는 감사 로그 한 줄에서 수준과 코드만 뽑는다.
type auditRecord struct {
	Level string `json:"level"`
	Msg   string `json:"msg"`
	Code  string `json:"code"`
}

// auditRecords는 버퍼에 쌓인 감사 로그를 순서대로 돌려준다.
func auditRecords(t *testing.T, logs *lockedLogBuffer) []auditRecord {
	t.Helper()
	var out []auditRecord
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var record auditRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("감사 로그를 해석할 수 없다: %v (%s)", err, line)
		}
		if record.Msg == "HTTP 요청 완료" {
			out = append(out, record)
		}
	}
	return out
}

// levelFor는 code 가 기록된 마지막 감사 로그의 수준이다.
func levelFor(t *testing.T, logs *lockedLogBuffer, code string) string {
	t.Helper()
	level := ""
	for _, record := range auditRecords(t, logs) {
		if record.Code == code {
			level = record.Level
		}
	}
	if level == "" {
		t.Fatalf("code=%s 인 감사 로그가 없다: %s", code, logs.String())
	}
	return level
}

// 공유 비밀 대입 시도는 경고로 남아야 한다 — 정상 호출자는 이 코드를 낼 수 없다.
func TestInternalAuthFailureLogsAtWarn(t *testing.T) {
	fixture := newDelegationFixture(t)
	body := `{"backendToken":"` + webSessionToken("a") + `"}`
	response, _ := fixture.issue(t, body, http.Header{internalSecretHeader: []string{"wrong-secret-0123456789012345678901"}})
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("상태가 401 이 아니다: %d", response.StatusCode)
	}
	if level := levelFor(t, fixture.logs, "internal_authentication_required"); level != "WARN" {
		t.Fatalf("공유 비밀 불일치가 %s 로 남았다(WARN 이어야 한다)", level)
	}
}

// 브라우저 컨텍스트에서 서버간 경로를 부른 흔적도 경고로 남아야 한다.
func TestBrowserRequestDeniedLogsAtWarn(t *testing.T) {
	fixture := newDelegationFixture(t)
	body := `{"backendToken":"` + webSessionToken("a") + `"}`
	response, _ := fixture.issue(t, body, http.Header{"Cookie": []string{"session=x"}})
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("상태가 403 이 아니다: %d", response.StatusCode)
	}
	if level := levelFor(t, fixture.logs, "browser_request_denied"); level != "WARN" {
		t.Fatalf("브라우저 형태 요청이 %s 로 남았다(WARN 이어야 한다)", level)
	}
}

// 허용하지 않은 Origin 도 경고다 — CORS 우회 시도다.
func TestOriginDeniedLogsAtWarn(t *testing.T) {
	fixture := newDelegationFixture(t)
	request, err := http.NewRequest(http.MethodPost, fixture.server.URL+"/mcp", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", "https://evil.example.com")
	response, err := fixture.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("상태가 403 이 아니다: %d", response.StatusCode)
	}
	if level := levelFor(t, fixture.logs, "origin_denied"); level != "WARN" {
		t.Fatalf("거부된 Origin 이 %s 로 남았다(WARN 이어야 한다)", level)
	}
}

// 정상 요청과 **흔한** 실패는 Info 로 남아야 한다. 이것이 경고를 의미 있게 유지한다.
func TestRoutineOutcomesLogAtInfo(t *testing.T) {
	fixture := newDelegationFixture(t)
	// 만료·미등록 토큰은 정상 운영에서도 나온다.
	request, err := http.NewRequest(http.MethodPost, fixture.server.URL+"/mcp", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := fixture.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("상태가 401 이 아니다: %d", response.StatusCode)
	}
	if level := levelFor(t, fixture.logs, "authentication_required"); level != "INFO" {
		t.Fatalf("흔한 인증 실패가 %s 로 올라갔다(INFO 여야 한다)", level)
	}
	// 정상 발급도 Info 다.
	body := `{"backendToken":"` + webSessionToken("a") + `"}`
	issued, grant := fixture.issue(t, body, nil)
	if issued.StatusCode != http.StatusCreated || grant.Token == "" {
		t.Fatalf("정상 발급이 실패했다: %d", issued.StatusCode)
	}
	if level := levelFor(t, fixture.logs, "ok"); level != "INFO" {
		t.Fatalf("정상 요청이 %s 로 남았다(INFO 여야 한다)", level)
	}
}

// 경고 목록에 흔한 실패 코드가 섞여 들어가지 않도록 목록 자체를 고정한다.
//
// 이 시험이 막는 것은 "일단 다 경고로 올리자"는 변경이다. 경고가 흔해지면 운영자는 경고를 보지
// 않게 되고, 그 순간 이 로깅의 목적이 사라진다.
func TestSecurityFailureCodesExcludeRoutineFailures(t *testing.T) {
	for _, code := range []string{
		"ok",
		"authentication_required",         // 만료된 토큰 — 정상 운영에서 나온다
		"backend_authentication_required", // Backend 세션 만료
		"backend_unavailable",             // 일시적 장애
		"timeout", "canceled",             // 취소·시간 초과
		"method_not_allowed", "not_found", // 클라이언트 구현 차이
		"invalid_request", "request_too_large",
		"global_limit", "user_limit", // 동시성 제한 — 부하 신호이며 공격 신호가 아니다
		"protocol_error",
	} {
		if securityFailureCodes[code] {
			t.Fatalf("흔한 실패 코드가 경고 목록에 있다: %s", code)
		}
	}
	// 반대로 반드시 있어야 하는 코드도 고정한다.
	for _, code := range []string{
		"internal_authentication_required",
		"browser_request_denied",
		"credential_in_payload",
		"invalid_host",
		"origin_denied",
		"preflight_denied",
		"delegation_limit",
	} {
		if !securityFailureCodes[code] {
			t.Fatalf("경고로 남겨야 하는 코드가 빠졌다: %s", code)
		}
	}
}
