package extension

// Managed 모드 전용 Runtime 설정과 프로세스 내부 공유 비밀.

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/url"

	"github.com/heartblast/ableops-kafka-mcp/internal/config"
)

// managedAddress는 Managed 모드가 HTTPOptions 에 넣는 **placeholder** 수신 주소다.
//
// 이 주소로 bind 하지 않는다. 실제 listener 는 SDK 의 extserver 가 127.0.0.1:0 으로 **하나만**
// 연다. mcpserver 의 HTTPOptions 검증이 loopback 주소를 요구하므로 계약을 만족하는 값을 넣을
// 뿐이며, Runtime 은 listener 없는 Handler(NewHTTPHandler)만 만든다.
const managedAddress = "127.0.0.1:0"

// internalSecretBytes는 프로세스 내부 공유 비밀의 엔트로피다(hex 로 64자 = mcpserver 하한의 2배).
const internalSecretBytes = 32

// newInternalSecret은 이 프로세스에서만 쓰는 서버간 공유 비밀을 만든다.
//
// Managed 모드에서 운영자는 ABLEOPS_WEB_DELEGATION_SECRET 을 설정하지 않는다. `/internal/**`
// 의 1차 방어선은 Extension 호출 토큰(extserver)이고, 이 비밀은 그 뒤에서 기존 mcpserver 의
// 서버간 인증 계약을 **그대로 만족시키기 위한** 값이다.
//
// ⚠ 반환값은 시크릿이다. 로그·오류·Health·응답·Manifest 어디에도 넣지 않으며 디스크에도 쓰지 않는다.
func newInternalSecret() (string, error) {
	buf := make([]byte, internalSecretBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", errors.New("내부 공유 비밀을 생성하지 못했습니다")
	}
	return hex.EncodeToString(buf), nil
}

// managedServerConfig는 Core 가 준 Host API 주소로 Managed 모드 Runtime 설정을 만든다.
//
// standalone 의 설정 의미는 건드리지 않는다. 이 함수는 YAML 도 환경변수도 읽지 않으며,
// Managed 모드에서 유일하게 외부에서 오는 값은 hostURL 하나다.
func managedServerConfig(hostURL string) (config.ServerConfig, error) {
	origin, allowHTTP, err := backendOrigin(hostURL)
	if err != nil {
		return config.ServerConfig{}, err
	}
	baseURL, parseErr := url.Parse(origin)
	if parseErr != nil {
		return config.ServerConfig{}, errors.New("Backend REST origin 을 해석할 수 없습니다")
	}
	backend := config.Config{
		BaseURL: baseURL,
		// 공유 Backend 토큰을 갖지 않는다. 모든 REST 호출은 요청한 사용자의 세션으로 나간다.
		Token:                     "",
		RequireRequestCredentials: true,
		Timeout:                   config.DefaultTimeout,
		LogLevel:                  slog.LevelInfo,
		AllowHTTP:                 allowHTTP,
	}
	if err := backend.Validate(); err != nil {
		// 검증 오류 원문에는 주소가 섞여 들어오므로 그대로 쓰지 않고 분류만 한다.
		return config.ServerConfig{}, errors.New("Core Host API 주소로 만든 Backend 설정이 올바르지 않습니다")
	}
	return config.ServerConfig{
		Backend:     backend,
		Transport:   "http",
		HTTPAddress: managedAddress,
		// 브라우저가 Managed MCP 에 직접 붙는 경로는 없다(허용 Origin 없음).
		AllowedOrigins: nil,
		// 로컬 인증 저장소를 쓰지 않으므로 경로도 없다.
		AuthStore: "",
		Dynamic: config.DynamicConfig{
			Enabled:         true,
			RefreshInterval: config.DefaultDynamicRefreshInterval,
		},
		WebDelegation: config.WebDelegationConfig{
			Enabled: true,
			TTL:     config.DefaultWebDelegationTTL,
			// Secret 은 비워 둔다 — Managed 모드의 공유 비밀은 설정이 아니라 프로세스가 만든다.
		},
	}, nil
}
