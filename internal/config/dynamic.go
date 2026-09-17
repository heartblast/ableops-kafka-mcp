package config

import (
	"errors"
	"regexp"
	"strings"
	"time"
)

const (
	envDynamicTools           = "ABLEOPS_DYNAMIC_TOOLS"
	envDynamicOperations      = "ABLEOPS_DYNAMIC_OPERATIONS"
	envDynamicRefreshInterval = "ABLEOPS_DYNAMIC_REFRESH_INTERVAL"
	// maxDynamicOperations는 한 번에 선택할 수 있는 operationId 수의 상한이다.
	maxDynamicOperations = 64
)

const (
	// DefaultDynamicRefreshInterval은 실행 중 계약 갱신 주기의 기본값이다.
	DefaultDynamicRefreshInterval = 5 * time.Minute
	// MinDynamicRefreshInterval은 지나친 `/openapi.json` 폴링을 막는 하한이다.
	MinDynamicRefreshInterval = time.Minute
	// MaxDynamicRefreshInterval은 계약 변경이 무기한 반영되지 않는 설정을 막는 상한이다.
	MaxDynamicRefreshInterval = 24 * time.Hour
)

var dynamicOperationPattern = regexp.MustCompile(`^[a-z][A-Za-z0-9]{0,127}$`)

// DynamicConfig는 OpenAPI 기반 동적 도구 설정이다. 기본값은 비활성이다.
type DynamicConfig struct {
	// Enabled가 참이면 기동 시 `/openapi.json`을 읽어 동적 도구를 추가하고 주기적으로 갱신한다.
	Enabled bool
	// Operations는 선택한 operationId다. nil이면 기본 파일럿 목록, 빈 슬라이스면 선택 없음이다.
	Operations []string
	// RefreshInterval은 실행 중 계약 갱신 주기다. Enabled가 거짓이면 사용하지 않는다.
	RefreshInterval time.Duration
}

type dynamicFile struct {
	enabled         string
	operations      []string
	operationsSet   bool
	refreshInterval string
}

// loadDynamic은 비어 있지 않은 환경변수, YAML, 기본값 순으로 적용한다.
func loadDynamic(file dynamicFile, getenv func(string) string) (DynamicConfig, error) {
	cfg := DynamicConfig{RefreshInterval: DefaultDynamicRefreshInterval}
	enabled := strings.TrimSpace(getenv(envDynamicTools))
	if enabled == "" {
		enabled = file.enabled
	}
	switch enabled {
	case "", "false":
	case "true":
		cfg.Enabled = true
	default:
		return DynamicConfig{}, errors.New("ABLEOPS_DYNAMIC_TOOLS must be true or false")
	}
	if raw := strings.TrimSpace(getenv(envDynamicOperations)); raw != "" {
		cfg.Operations = []string{}
		for _, item := range strings.Split(raw, ",") {
			cfg.Operations = append(cfg.Operations, strings.TrimSpace(item))
		}
	} else if file.operationsSet {
		cfg.Operations = append([]string{}, file.operations...)
	}
	if len(cfg.Operations) > maxDynamicOperations {
		return DynamicConfig{}, errors.New("동적 도구 operationId는 64개 이하여야 합니다")
	}
	seen := map[string]bool{}
	for _, id := range cfg.Operations {
		// 오류에는 입력값을 넣지 않는다.
		if !dynamicOperationPattern.MatchString(id) {
			return DynamicConfig{}, errors.New("동적 도구 operationId는 lowerCamelCase 영숫자여야 합니다")
		}
		if seen[id] {
			return DynamicConfig{}, errors.New("동적 도구 operationId가 중복되었습니다")
		}
		seen[id] = true
	}
	// 갱신 주기는 Dynamic이 꺼져 있어도 검증한다. 설정 오류를 켜는 시점까지 숨기지 않는다.
	interval := strings.TrimSpace(getenv(envDynamicRefreshInterval))
	if interval == "" {
		interval = strings.TrimSpace(file.refreshInterval)
	}
	if interval != "" {
		value, err := time.ParseDuration(interval)
		if err != nil || value < MinDynamicRefreshInterval || value > MaxDynamicRefreshInterval {
			return DynamicConfig{}, errors.New("ABLEOPS_DYNAMIC_REFRESH_INTERVAL must be a Go duration from 1m to 24h")
		}
		cfg.RefreshInterval = value
	}
	return cfg, nil
}
