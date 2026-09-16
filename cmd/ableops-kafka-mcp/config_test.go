package main

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestServerCLIEnvironmentDefaults(t *testing.T) {
	env := map[string]string{
		"ABLEOPS_BASE_URL":  "https://synthetic-backend.invalid",
		"ABLEOPS_API_TOKEN": "synthetic-backend-token",
	}
	cfg, err := loadServerConfig(nil, func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Transport != "stdio" || cfg.HTTPAddress != "127.0.0.1:8081" || len(cfg.AllowedOrigins) != 0 || cfg.Backend.Token != env["ABLEOPS_API_TOKEN"] {
		t.Fatal("설정 파일을 지정하지 않은 실행의 기존 기본값이 변경되었습니다")
	}
	if cfg.Backend.RequireRequestCredentials {
		t.Fatal("stdio 실행에서 요청별 자격증명이 강제되었습니다")
	}
}

func TestServerCLIOnlyExplicitFlagsOverrideYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp-server-config.yaml")
	yaml := `version: 1
server:
  transport: http
  http_address: "127.0.0.1:18081"
  allowed_origins:
    - "https://synthetic-browser.invalid"
backend:
  base_url: "https://synthetic-backend.invalid"
auth:
  store_file: "synthetic-store.json"
`
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, transport, address string
		args                     []string
		origins                  []string
	}{
		{"파일 값 유지", "http", "127.0.0.1:18081", nil, []string{"https://synthetic-browser.invalid"}},
		{"주소와 Origin 변경", "http", "127.0.0.1:28081", []string{"--http-address=127.0.0.1:28081", "--allowed-origins=https://synthetic-a.invalid, https://synthetic-b.invalid"}, []string{"https://synthetic-a.invalid", "https://synthetic-b.invalid"}},
		{"빈 Origin 명시", "http", "127.0.0.1:18081", []string{"--allowed-origins="}, []string{}},
		{"기본값과 같은 전송 방식 명시", "stdio", "127.0.0.1:18081", []string{"--transport=stdio"}, []string{"https://synthetic-browser.invalid"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--config", path}, tt.args...)
			cfg, err := loadServerConfig(args, func(key string) string {
				if key == "ABLEOPS_API_TOKEN" {
					return "synthetic-session-token"
				}
				return ""
			})
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Transport != tt.transport || cfg.HTTPAddress != tt.address || !slices.Equal(cfg.AllowedOrigins, tt.origins) {
				t.Fatal("명시한 플래그와 YAML의 설정 우선순위가 올바르지 않습니다")
			}
			if tt.transport == "http" && cfg.AuthStore != filepath.Join(dir, "synthetic-store.json") {
				t.Fatal("YAML 인증 저장소 경로가 실행 설정에 전달되지 않았습니다")
			}
			if tt.transport == "stdio" && cfg.AuthStore != "" {
				t.Fatal("stdio 실행이 HTTP 인증 저장소를 읽었습니다")
			}
			if tt.transport == "http" && (cfg.Backend.Token != "" || !cfg.Backend.RequireRequestCredentials) {
				t.Fatal("HTTP 실행이 공용 백엔드 토큰을 사용합니다")
			}
			if tt.transport == "stdio" && (cfg.Backend.Token == "" || cfg.Backend.RequireRequestCredentials) {
				t.Fatal("전송 방식 플래그가 백엔드 인증 설정에 반영되지 않았습니다")
			}
		})
	}
}

func TestServerCLIRejectsInvalidArgumentsWithoutValues(t *testing.T) {
	for _, args := range [][]string{
		{"--config="}, {"--config=  "}, {"--config"}, {"--synthetic-secret"},
		{"--transport=synthetic-secret"}, {"synthetic-secret"},
	} {
		_, err := loadServerConfig(args, func(string) string { return "" })
		if !errors.Is(err, errArguments) {
			t.Fatal("잘못된 실행 인자가 허용되었습니다")
		}
		if strings.Contains(err.Error(), "synthetic-secret") {
			t.Fatal("입력값이 실행 인자 오류에 노출되었습니다")
		}
	}
}

func TestServerCLIConfigReadErrorDoesNotExposePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synthetic-secret-path.yaml")
	_, err := loadServerConfig([]string{"--config", path}, func(string) string { return "" })
	if err == nil || strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "synthetic-secret") {
		t.Fatal("설정 파일 읽기 오류가 누락되었거나 경로를 노출했습니다")
	}
}
