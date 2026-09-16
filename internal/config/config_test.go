package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsAndExplicitOptions(t *testing.T) {
	env := map[string]string{"ABLEOPS_BASE_URL": "https://ableops.example/", "ABLEOPS_API_TOKEN": "synthetic-session"}
	cfg, err := LoadFrom(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseURL.String() != "https://ableops.example" || cfg.Timeout != 15*time.Second || cfg.LogLevel != slog.LevelInfo || cfg.AllowHTTP {
		t.Fatalf("unexpected defaults: base=%s timeout=%v level=%v allow_http=%v", cfg.BaseURL, cfg.Timeout, cfg.LogLevel, cfg.AllowHTTP)
	}
	env["ABLEOPS_BASE_URL"] = "http://[::1]:8080"
	env["ABLEOPS_ALLOW_HTTP"] = "true"
	env["ABLEOPS_REQUEST_TIMEOUT"] = "120s"
	env["MCP_LOG_LEVEL"] = "DEBUG"
	env["ABLEOPS_CA_FILE"] = "C:\\certificates\\local-ca.pem"
	cfg, err = LoadFrom(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AllowHTTP || cfg.Timeout != 120*time.Second || cfg.LogLevel != slog.LevelDebug || cfg.CAFile != env["ABLEOPS_CA_FILE"] {
		t.Fatal("explicit configuration not applied")
	}
}

func TestInvalidConfigurationIsRejectedWithoutEchoingValues(t *testing.T) {
	for _, tc := range []struct{ name, key, value string }{
		{"missing_url", "ABLEOPS_BASE_URL", ""},
		{"missing_token", "ABLEOPS_API_TOKEN", ""},
		{"token_header_injection", "ABLEOPS_API_TOKEN", "sensitive\r\nHeader: value"},
		{"token_whitespace", "ABLEOPS_API_TOKEN", "sensitive session"},
		{"bad_url", "ABLEOPS_BASE_URL", "https://%sensitive"},
		{"no_scheme", "ABLEOPS_BASE_URL", "sensitive.example"},
		{"credentials", "ABLEOPS_BASE_URL", "https://sensitive:secret@host.example"},
		{"api_suffix", "ABLEOPS_BASE_URL", "https://host.example/api"},
		{"path_suffix", "ABLEOPS_BASE_URL", "https://host.example/sensitive"},
		{"query", "ABLEOPS_BASE_URL", "https://host.example?token=sensitive"},
		{"empty_query", "ABLEOPS_BASE_URL", "https://host.example?"},
		{"fragment", "ABLEOPS_BASE_URL", "https://host.example#sensitive"},
		{"empty_fragment", "ABLEOPS_BASE_URL", "https://host.example#"},
		{"port_range", "ABLEOPS_BASE_URL", "https://host.example:65536"},
		{"zero_port", "ABLEOPS_BASE_URL", "https://host.example:0"},
		{"blank_port", "ABLEOPS_BASE_URL", "https://host.example:"},
		{"ftp", "ABLEOPS_BASE_URL", "ftp://host.example"},
		{"loopback_opt_in", "ABLEOPS_BASE_URL", "http://127.0.0.1:8080"},
		{"bad_timeout", "ABLEOPS_REQUEST_TIMEOUT", "sensitive"},
		{"short_timeout", "ABLEOPS_REQUEST_TIMEOUT", "999ms"},
		{"long_timeout", "ABLEOPS_REQUEST_TIMEOUT", "121s"},
		{"zero_timeout", "ABLEOPS_REQUEST_TIMEOUT", "0"},
		{"bad_level", "MCP_LOG_LEVEL", "sensitive"},
		{"bad_opt_in", "ABLEOPS_ALLOW_HTTP", "sensitive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"ABLEOPS_BASE_URL": "https://host.example", "ABLEOPS_API_TOKEN": "synthetic-session"}
			env[tc.key] = tc.value
			_, err := LoadFrom(func(key string) string { return env[key] })
			if err == nil {
				t.Fatal("expected validation failure")
			}
			if strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "synthetic-session") {
				t.Fatal("configuration error disclosed a value")
			}
		})
	}
}

func TestDevelopmentHTTPOnlyAllowsExplicitLoopbackLiterals(t *testing.T) {
	for _, host := range []string{"localhost", "127.0.0.1", "[::1]", "127.0.0.2", "127.1", "0.0.0.0", "host.example", "localhost.example", "localhost.", "[::ffff:127.0.0.1]"} {
		t.Run(host, func(t *testing.T) {
			env := map[string]string{"ABLEOPS_BASE_URL": "http://" + host + ":8080", "ABLEOPS_API_TOKEN": "synthetic-session", "ABLEOPS_ALLOW_HTTP": "true"}
			_, err := LoadFrom(func(key string) string { return env[key] })
			wantOK := host == "localhost" || host == "127.0.0.1" || host == "[::1]"
			if (err == nil) != wantOK {
				t.Fatalf("loopback acceptance=%v, want=%v", err == nil, wantOK)
			}
		})
	}
}

func TestHTTPConfigurationNeverReadsSharedToken(t *testing.T) {
	cfg, err := LoadHTTPFrom(func(key string) string {
		if key == "ABLEOPS_API_TOKEN" {
			t.Fatal("HTTP 설정에서 공용 토큰을 읽었습니다")
		}
		if key == "ABLEOPS_BASE_URL" {
			return "https://backend.example"
		}
		return ""
	})
	if err != nil || !cfg.RequireRequestCredentials || cfg.Token != "" {
		t.Fatal("HTTP 인증 설정 실패")
	}
	cfg.Token = "synthetic-forbidden-shared-token"
	if cfg.Validate() == nil {
		t.Fatal("HTTP 공용 토큰 허용")
	}
}
