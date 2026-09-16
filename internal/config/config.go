// Package config는 stdio 개인 세션과 HTTP 요청별 인증에 필요한 설정을 읽는다.
package config

import (
	"errors"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultTimeout = 15 * time.Second
	MinTimeout     = time.Second
	MaxTimeout     = 120 * time.Second
)

// Config는 프로세스 전용 설정이다. 토큰을 로그나 도구 결과에 출력하면 안 된다.
type Config struct {
	BaseURL   *url.URL
	Token     string
	Timeout   time.Duration
	CAFile    string
	LogLevel  slog.Level
	AllowHTTP bool
	// RequireRequestCredentials는 HTTP 요청별 자격증명을 강제하며 공용 토큰을 금지한다.
	RequireRequestCredentials bool
}

// Load는 프로세스 환경변수만 읽으며 .env 파일을 자동으로 불러오지 않는다.
func Load() (Config, error) { return LoadFrom(os.Getenv) }

// LoadFrom은 프로세스 환경변수를 변경하지 않고 설정을 검증할 수 있게 한다.
func LoadFrom(getenv func(string) string) (Config, error) {
	return loadFrom(getenv, false)
}

// LoadHTTPFrom은 공용 Backend 토큰을 읽지 않는다. 각 HTTP 요청이 위임 토큰을 제공해야 한다.
func LoadHTTPFrom(getenv func(string) string) (Config, error) {
	return loadFrom(getenv, true)
}

func loadFrom(getenv func(string) string, requestCredentials bool) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("environment reader is required")
	}
	rawURL := strings.TrimSpace(getenv("ABLEOPS_BASE_URL"))
	if rawURL == "" {
		return Config{}, errors.New("ABLEOPS_BASE_URL is required")
	}
	if strings.ContainsAny(rawURL, "?#") {
		return Config{}, errors.New("ABLEOPS_BASE_URL must be an origin without a query or fragment")
	}
	baseURL, err := url.Parse(rawURL)
	if err != nil {
		return Config{}, errors.New("ABLEOPS_BASE_URL is not a valid origin URL")
	}
	cfg := Config{
		BaseURL:                   baseURL,
		RequireRequestCredentials: requestCredentials,
		Timeout:                   DefaultTimeout,
		CAFile:                    strings.TrimSpace(getenv("ABLEOPS_CA_FILE")),
		LogLevel:                  slog.LevelInfo,
	}
	if !requestCredentials {
		cfg.Token = getenv("ABLEOPS_API_TOKEN")
	}
	if raw := strings.TrimSpace(getenv("ABLEOPS_ALLOW_HTTP")); raw != "" {
		switch raw {
		case "true":
			cfg.AllowHTTP = true
		case "false":
		default:
			return Config{}, errors.New("ABLEOPS_ALLOW_HTTP must be true or false")
		}
	}
	if raw := strings.TrimSpace(getenv("ABLEOPS_REQUEST_TIMEOUT")); raw != "" {
		cfg.Timeout, err = time.ParseDuration(raw)
		if err != nil {
			return Config{}, errors.New("ABLEOPS_REQUEST_TIMEOUT must be a Go duration from 1s to 120s")
		}
	}
	if raw := strings.ToLower(strings.TrimSpace(getenv("MCP_LOG_LEVEL"))); raw != "" {
		switch raw {
		case "debug":
			cfg.LogLevel = slog.LevelDebug
		case "info":
			cfg.LogLevel = slog.LevelInfo
		case "warn":
			cfg.LogLevel = slog.LevelWarn
		case "error":
			cfg.LogLevel = slog.LevelError
		default:
			return Config{}, errors.New("MCP_LOG_LEVEL must be debug, info, warn, or error")
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	cfg.BaseURL.Path = ""
	return cfg, nil
}

// Validate는 Config를 직접 생성한 호출에도 동일한 설정 검증을 적용한다.
func (c Config) Validate() error {
	u := c.BaseURL
	if u == nil || u.Host == "" || u.Hostname() == "" || u.Opaque != "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || u.RawFragment != "" {
		return errors.New("ABLEOPS_BASE_URL must be an origin without credentials, path, query, or fragment")
	}
	if strings.ContainsAny(u.Host, " \\\t\r\n") || strings.HasSuffix(u.Host, ":") {
		return errors.New("ABLEOPS_BASE_URL has an invalid host or port")
	}
	if port := u.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return errors.New("ABLEOPS_BASE_URL port must be from 1 to 65535")
		}
	}
	switch u.Scheme {
	case "https":
	case "http":
		host := strings.ToLower(u.Hostname())
		if !c.AllowHTTP || (host != "localhost" && host != "127.0.0.1" && host != "::1") {
			return errors.New("HTTP requires ABLEOPS_ALLOW_HTTP=true and a localhost, 127.0.0.1, or ::1 host")
		}
	default:
		return errors.New("ABLEOPS_BASE_URL must use HTTPS (or explicitly enabled loopback HTTP)")
	}
	if c.RequireRequestCredentials && c.Token != "" {
		return errors.New("HTTP mode forbids a shared Backend token")
	}
	if !c.RequireRequestCredentials && c.Token == "" {
		return errors.New("ABLEOPS_API_TOKEN is required")
	}
	for _, ch := range c.Token {
		if ch < 0x21 || ch > 0x7e {
			return errors.New("ABLEOPS_API_TOKEN must contain only printable ASCII without whitespace")
		}
	}
	if c.Timeout < MinTimeout || c.Timeout > MaxTimeout {
		return errors.New("ABLEOPS_REQUEST_TIMEOUT must be from 1s to 120s")
	}
	return nil
}
