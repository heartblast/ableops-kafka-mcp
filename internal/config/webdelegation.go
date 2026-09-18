package config

// Web/Chat Backend 전용 단기 위임(`/internal/delegations`) 설정이다.
//
// ⚠ 기본값은 **비활성**이다. 켜는 순간 "Backend 세션을 MCP 인증으로 바꿔 주는" 경로가 생기므로
// 운영자가 명시적으로 선택해야 한다. 켜고 비밀을 주지 않으면 기동을 거부한다 — 조용히 꺼진 채
// 뜨면 운영자는 켰다고 믿는데 Web 연동만 404 로 실패한다.
// ⚠ 공유 비밀은 **환경변수로만** 받는다. YAML 에는 비밀을 담을 키 자체를 두지 않는다.

import (
	"errors"
	"strings"
	"time"
)

const (
	envWebDelegation       = "ABLEOPS_WEB_DELEGATION"
	envWebDelegationTTL    = "ABLEOPS_WEB_DELEGATION_TTL"
	envWebDelegationSecret = "ABLEOPS_WEB_DELEGATION_SECRET"
)

const (
	// DefaultWebDelegationTTL은 위임 수명의 기본값이다(webdelegation.DefaultTTL 과 같은 값).
	//
	// ⚠ webdelegation 패키지를 import 해 상수를 공유하지 않는다 — ableops 가 config 를 읽으므로
	// config → webdelegation → ableops → config 순환이 된다. 대신 값이 갈라지면 저장소 생성이
	// 거부하도록 webdelegation.NewStore 가 같은 범위를 다시 검사한다.
	DefaultWebDelegationTTL = 10 * time.Minute
	// MinWebDelegationTTL은 발급 직후 만료되는 설정을 막는 하한이다.
	MinWebDelegationTTL = time.Minute
	// MaxWebDelegationTTL은 상한이다. 재발급이 싼 위임을 길게 살려 둘 이유가 없다.
	MaxWebDelegationTTL = 30 * time.Minute
	// minWebDelegationSecret은 서버간 공유 비밀의 최소 길이다(mcpserver 와 동일 기준).
	minWebDelegationSecret = 32
)

// WebDelegationConfig는 서버간 위임 발급 설정이다.
// ⚠ Secret 은 시크릿이다. 로그·오류 메시지·도구 결과에 넣지 않는다.
type WebDelegationConfig struct {
	Enabled bool
	TTL     time.Duration
	Secret  string
}

type webDelegationFile struct {
	enabled   string
	ttl       string
	secretEnv string
}

// loadWebDelegation은 비어 있지 않은 환경변수, YAML, 기본값 순으로 적용한다(다른 섹션과 동일 규칙).
func loadWebDelegation(file webDelegationFile, getenv func(string) string) (WebDelegationConfig, error) {
	cfg := WebDelegationConfig{TTL: DefaultWebDelegationTTL}
	enabled := strings.TrimSpace(getenv(envWebDelegation))
	if enabled == "" {
		enabled = file.enabled
	}
	switch enabled {
	case "", "false":
	case "true":
		cfg.Enabled = true
	default:
		return WebDelegationConfig{}, errors.New("ABLEOPS_WEB_DELEGATION must be true or false")
	}
	// 수명은 꺼져 있어도 검증한다. 설정 오류를 켜는 시점까지 숨기지 않는다(dynamic_tools 선례).
	ttl := strings.TrimSpace(getenv(envWebDelegationTTL))
	if ttl == "" {
		ttl = strings.TrimSpace(file.ttl)
	}
	if ttl != "" {
		value, err := time.ParseDuration(ttl)
		if err != nil || value < MinWebDelegationTTL || value > MaxWebDelegationTTL {
			return WebDelegationConfig{}, errors.New("ABLEOPS_WEB_DELEGATION_TTL must be a Go duration from 1m to 30m")
		}
		cfg.TTL = value
	}
	secretEnv := strings.TrimSpace(file.secretEnv)
	if secretEnv == "" {
		secretEnv = envWebDelegationSecret
	}
	cfg.Secret = strings.TrimSpace(getenv(secretEnv))
	if !cfg.Enabled {
		// 꺼져 있으면 비밀을 들고 다니지 않는다 — 읽은 값이 다른 경로로 새지 않게 즉시 버린다.
		cfg.Secret = ""
		return cfg, nil
	}
	// 오류에는 비밀은 물론 환경변수 **이름**도 넣지 않는다(설정 파일이 무엇을 가리키는지도 정보다).
	if len(cfg.Secret) < minWebDelegationSecret {
		return WebDelegationConfig{}, errors.New("서버간 위임 발급을 켜려면 32바이트 이상의 공유 비밀 환경변수가 필요합니다")
	}
	return cfg, nil
}

// validSecretEnvName은 위임 비밀 환경변수 이름이 기존 설정 이름과 충돌하지 않는지 본다.
func validSecretEnvName(value string) bool {
	if !validEnvName(value) {
		return false
	}
	for _, reserved := range []string{
		"ABLEOPS_BASE_URL", "ABLEOPS_ALLOW_HTTP", "ABLEOPS_REQUEST_TIMEOUT", "ABLEOPS_CA_FILE",
		"MCP_AUTH_STORE", "MCP_LOG_LEVEL", "ABLEOPS_API_TOKEN",
		"ABLEOPS_MESSAGE_SAMPLE_ENABLED", "ABLEOPS_MESSAGE_SAMPLE_TOPICS",
		envDynamicTools, envDynamicOperations, envDynamicRefreshInterval,
		envWebDelegation, envWebDelegationTTL,
	} {
		if strings.EqualFold(value, reserved) {
			return false
		}
	}
	return true
}
