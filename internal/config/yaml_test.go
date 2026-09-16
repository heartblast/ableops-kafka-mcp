package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeYAMLConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mcp-server-config.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal("테스트 설정 파일 생성 실패")
	}
	return path
}

func configEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func TestLoadServerYAMLHTTP(t *testing.T) {
	path := writeYAMLConfig(t, `version: 1
server:
  transport: http
  http_address: '127.0.0.1:8181'
  allowed_origins: ['https://client.example.test']
backend:
  base_url: 'http://localhost:8080'
  allow_http: true
  request_timeout: '21s'
  ca_file: 'certs/backend.pem'
auth:
  store_file: 'auth/local.json'
  token_env: 'TEST_BACKEND_TOKEN'
logging:
  level: debug
`)
	reads := map[string]int{}
	cfg, err := LoadServer(path, func(key string) string {
		reads[key]++
		if key == "TEST_BACKEND_TOKEN" || key == "ABLEOPS_API_TOKEN" {
			t.Error("HTTP 서버가 백엔드 토큰 환경변수를 읽음")
		}
		return ""
	}, ServerOverrides{})
	if err != nil {
		t.Fatalf("HTTP 설정 읽기 실패: %v", err)
	}
	if cfg.Transport != "http" || cfg.HTTPAddress != "127.0.0.1:8181" || len(cfg.AllowedOrigins) != 1 || cfg.AllowedOrigins[0] != "https://client.example.test" {
		t.Error("서버 파일 설정 불일치")
	}
	if cfg.Backend.BaseURL.String() != "http://localhost:8080" || !cfg.Backend.AllowHTTP || cfg.Backend.Timeout != 21*time.Second || cfg.Backend.LogLevel != slog.LevelDebug {
		t.Error("백엔드 파일 설정 불일치")
	}
	if !cfg.Backend.RequireRequestCredentials || cfg.Backend.Token != "" {
		t.Error("HTTP 요청별 인증 경계 손상")
	}
	if cfg.Backend.CAFile != filepath.Join(filepath.Dir(path), "certs", "backend.pem") || cfg.AuthStore != filepath.Join(filepath.Dir(path), "auth", "local.json") {
		t.Error("파일 기준 상대 경로 변환 실패")
	}
}

func TestLoadServerPrecedence(t *testing.T) {
	path := writeYAMLConfig(t, `version: 1
server:
  transport: http
  http_address: '127.0.0.1:8181'
  allowed_origins: ['https://yaml.example.test']
backend:
  base_url: 'http://localhost:8080'
  allow_http: true
  request_timeout: '21s'
  ca_file: '${MISSING_CA}/ca.pem'
auth:
  store_file: '${MISSING_STORE}/store.json'
  token_env: 'CUSTOM_BACKEND_TOKEN'
logging:
  level: debug
`)
	values := map[string]string{
		"ABLEOPS_BASE_URL":        "https://env.example.test",
		"ABLEOPS_ALLOW_HTTP":      "false",
		"ABLEOPS_REQUEST_TIMEOUT": "9s",
		"ABLEOPS_CA_FILE":         "relative-env.pem",
		"MCP_AUTH_STORE":          "relative-env-store.json",
		"MCP_LOG_LEVEL":           "warn",
		"CUSTOM_BACKEND_TOKEN":    "synthetic-session",
		"ABLEOPS_API_TOKEN":       "ignored-synthetic-session",
	}
	transport, address := "stdio", "127.0.0.1:8282"
	origins := []string{}
	cfg, err := LoadServer(path, configEnv(values), ServerOverrides{Transport: &transport, HTTPAddress: &address, AllowedOrigins: &origins})
	if err != nil {
		t.Fatalf("우선순위 설정 실패: %v", err)
	}
	if cfg.Transport != transport || cfg.HTTPAddress != address || len(cfg.AllowedOrigins) != 0 {
		t.Error("명시한 실행 인자가 파일보다 우선하지 않음")
	}
	if cfg.Backend.BaseURL.String() != values["ABLEOPS_BASE_URL"] || cfg.Backend.AllowHTTP || cfg.Backend.Timeout != 9*time.Second || cfg.Backend.LogLevel != slog.LevelWarn {
		t.Error("환경변수가 파일보다 우선하지 않음")
	}
	if cfg.Backend.CAFile != values["ABLEOPS_CA_FILE"] || cfg.AuthStore != "" {
		t.Error("환경변수 상대 경로는 기존 작업 디렉터리 기준이어야 함")
	}
	if cfg.Backend.RequireRequestCredentials || cfg.Backend.Token != values["CUSTOM_BACKEND_TOKEN"] {
		t.Error("stdio 전환 후 지정한 환경변수의 토큰을 사용해야 함")
	}
}

func TestLoadServerEnvironmentCompatibility(t *testing.T) {
	values := map[string]string{"ABLEOPS_BASE_URL": "https://backend.example.test", "ABLEOPS_API_TOKEN": "synthetic-session"}
	cfg, err := LoadServer("", configEnv(values), ServerOverrides{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Transport != "stdio" || cfg.HTTPAddress != "127.0.0.1:8081" || cfg.Backend.Timeout != DefaultTimeout || cfg.AuthStore != "" {
		t.Error("파일 없는 기존 기본값 변경")
	}
	if cfg.Backend.Token != values["ABLEOPS_API_TOKEN"] {
		t.Error("기존 개인 세션 환경변수 미적용")
	}
	values["MCP_AUTH_STORE"] = "env-store.json"
	transport := "http"
	cfg, err = LoadServer("", func(key string) string {
		if key == "ABLEOPS_API_TOKEN" {
			t.Fatal("HTTP 환경 호환 모드에서 공용 토큰 조회")
		}
		return values[key]
	}, ServerOverrides{Transport: &transport})
	if err != nil || cfg.Backend.Token != "" || !cfg.Backend.RequireRequestCredentials {
		t.Error("HTTP 환경 호환 모드 실패")
	}
}

func TestLoadServerHTTPOverrideDoesNotReadToken(t *testing.T) {
	path := writeYAMLConfig(t, "version: 1\nserver: {transport: stdio}\nbackend: {base_url: 'https://backend.example.test'}\nauth: {store_file: 'auth.json', token_env: CUSTOM_TOKEN}\n")
	transport := "http"
	_, err := LoadServer(path, func(key string) string {
		if key == "CUSTOM_TOKEN" || key == "ABLEOPS_API_TOKEN" {
			t.Fatal("HTTP 실행 인자 적용 전에 토큰을 조회함")
		}
		return ""
	}, ServerOverrides{Transport: &transport})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadServerStdioOverrideDoesNotReadAuthStore(t *testing.T) {
	path := writeYAMLConfig(t, "version: 1\nserver: {transport: http}\nbackend: {base_url: 'https://backend.example.test'}\nauth: {store_file: '${MISSING_DATA_ROOT}/auth.json'}\n")
	transport := "stdio"
	cfg, err := LoadServer(path, func(key string) string {
		if key == "MCP_AUTH_STORE" || key == "MISSING_DATA_ROOT" {
			t.Fatal("stdio 모드가 사용하지 않는 인증 저장소 환경변수를 읽음")
		}
		if key == "ABLEOPS_API_TOKEN" {
			return "synthetic-session"
		}
		return ""
	}, ServerOverrides{Transport: &transport})
	if err != nil || cfg.AuthStore != "" {
		t.Error("stdio 전환이 사용하지 않는 HTTP 인증 저장소에 의존함")
	}
}

func TestLoadServerTokenEnvCannotAliasSettings(t *testing.T) {
	for _, name := range []string{"ABLEOPS_BASE_URL", "ABLEOPS_ALLOW_HTTP", "ABLEOPS_REQUEST_TIMEOUT", "ABLEOPS_CA_FILE", "MCP_AUTH_STORE", "MCP_LOG_LEVEL"} {
		for _, alias := range []string{name, strings.ToLower(name)} {
			t.Run(alias, func(t *testing.T) {
				path := writeYAMLConfig(t, "version: 1\nserver: {transport: http}\nbackend: {base_url: 'https://backend.example.test'}\nauth: {store_file: auth.json, token_env: "+alias+"}\n")
				_, err := LoadServer(path, func(key string) string {
					t.Fatal("기존 설정과 충돌하는 토큰 환경변수를 읽기 전에 거부해야 함")
					return ""
				}, ServerOverrides{})
				if err == nil {
					t.Error("기존 설정 환경변수를 토큰 환경변수로 지정할 수 없음")
				}
			})
		}
	}
}

func TestLoadEnrollmentAndAuthStore(t *testing.T) {
	path := writeYAMLConfig(t, `version: 1
server: {transport: http}
backend:
  base_url: 'https://backend.example.test'
  ca_file: '${CERT_ROOT}/backend.pem'
auth:
  store_file: '${DATA_ROOT}/auth.json'
  token_env: 'CUSTOM_TOKEN'
`)
	values := map[string]string{"CERT_ROOT": "certs", "DATA_ROOT": "data", "CUSTOM_TOKEN": "synthetic-session"}
	backend, store, err := LoadEnrollment(path, configEnv(values))
	if err != nil {
		t.Fatal(err)
	}
	if backend.Token != values["CUSTOM_TOKEN"] || backend.RequireRequestCredentials || store != filepath.Join(filepath.Dir(path), "data", "auth.json") || backend.CAFile != filepath.Join(filepath.Dir(path), "certs", "backend.pem") {
		t.Error("인증 등록 설정 불일치")
	}
	store, err = LoadAuthStore(path, func(key string) string {
		if key != "MCP_AUTH_STORE" && key != "DATA_ROOT" {
			t.Fatal("토큰 폐기가 백엔드 설정 환경변수에 의존함")
		}
		return values[key]
	})
	if err != nil || store != filepath.Join(filepath.Dir(path), "data", "auth.json") {
		t.Error("토큰 없이 인증 저장소 경로 읽기 실패")
	}
}

func TestLoadConfigStrictYAMLAndSafeErrors(t *testing.T) {
	cases := map[string]string{
		"버전 누락":            "auth: {store_file: store.json}",
		"지원하지 않는 버전":       "version: 2",
		"문자열 버전":           "version: '1'",
		"빈 전송 방식":          "version: 1\nserver: {transport: ''}",
		"잘못된 전송 방식":        "version: 1\nserver: {transport: private-secret-canary}",
		"빈 토큰 환경변수":        "version: 1\nauth: {token_env: ''}",
		"알 수 없는 키":         "version: 1\nprivate-secret-canary: value",
		"직접 토큰":            "version: 1\nauth: {token: private-secret-canary}",
		"직접 비밀번호":          "version: 1\nbackend: {password: private-secret-canary}",
		"직접 헤더":            "version: 1\nbackend: {headers: {Authorization: private-secret-canary}}",
		"최상위 중복":           "version: 1\nversion: 1",
		"중첩 중복":            "version: 1\nauth: {store_file: a, store_file: b}",
		"잘못된 키 타입":         "version: 1\ntrue: private-secret-canary",
		"문자열 아닌 URL":       "version: 1\nbackend: {base_url: 8080}",
		"문자열 아닌 타임아웃":      "version: 1\nbackend: {request_timeout: 15}",
		"문자열 아닌 토큰 환경변수":   "version: 1\nauth: {token_env: 1234}",
		"유효하지 않은 토큰 환경변수":  "version: 1\nauth: {token_env: 'private-secret-canary'}",
		"불리언 아닌 HTTP 허용":   "version: 1\nbackend: {allow_http: 'true'}",
		"목록 아닌 origin":     "version: 1\nserver: {allowed_origins: private-secret-canary}",
		"문자열 아닌 origin 항목": "version: 1\nserver: {allowed_origins: [true]}",
		"null":             "version: 1\nauth: {store_file: null}",
		"null 단축 표기":       "version: 1\nbackend:",
		"앵커":               "version: 1\nauth: {store_file: &anchor store.json}",
		"별칭":               "version: 1\nbackend: {base_url: &url 'https://backend.example.test'}\nauth: {store_file: *url}",
		"병합":               "version: 1\nauth: {<<: {store_file: store.json}}",
		"다중 문서":            "version: 1\n---\nversion: 1",
		"빈 추가 문서":          "version: 1\n---",
		"알 수 없는 태그":        "version: 1\nauth: {store_file: !private store.json}",
		"파서 오류":            "version: 1\nauth: [private-secret-canary",
		"최상위 목록":           "[private-secret-canary]",
		"문자열 최상위":          "private-secret-canary",
		"빈 파일":             "",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := writeYAMLConfig(t, content)
			_, err := LoadAuthStore(path, configEnv(map[string]string{"MCP_AUTH_STORE": "env-store.json"}))
			if err == nil {
				t.Fatal("잘못된 YAML을 허용함")
			}
			if strings.Contains(err.Error(), "private-secret-canary") || strings.Contains(err.Error(), path) {
				t.Error("설정 원문이나 경로가 오류에 노출됨")
			}
		})
	}
}

func TestLoadConfigPathExpansionBoundaries(t *testing.T) {
	cases := []string{"${MISSING}/auth.json", "${}/auth.json", "${UNCLOSED", "${INVALID-NAME}/auth.json", "${ABLEOPS_API_TOKEN}/auth.json", "${CUSTOM_TOKEN}/auth.json", "${ableops_api_token}/auth.json", "${custom_token}/auth.json"}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			for _, field := range []string{"store_file", "ca_file"} {
				content := "version: 1\nserver: {transport: http}\nbackend:\n  base_url: 'https://backend.example.test'\n"
				if field == "ca_file" {
					content += "  ca_file: '" + raw + "'\nauth: {store_file: store.json, token_env: CUSTOM_TOKEN}\n"
				} else {
					content += "auth: {store_file: '" + raw + "', token_env: CUSTOM_TOKEN}\n"
				}
				path := writeYAMLConfig(t, content)
				_, err := LoadServer(path, func(key string) string {
					if strings.EqualFold(key, "ABLEOPS_API_TOKEN") || strings.EqualFold(key, "CUSTOM_TOKEN") {
						t.Fatal("HTTP 파일 경로 확장으로 토큰 환경변수를 조회함")
					}
					return ""
				}, ServerOverrides{})
				if err == nil {
					t.Error("유효하지 않은 경로 확장을 허용함")
				}
			}
		})
	}
}

func TestLoadConfigFileLimitAndBOM(t *testing.T) {
	base := "version: 1\n#"
	for _, size := range []int{maxConfigBytes, maxConfigBytes + 1} {
		path := writeYAMLConfig(t, base+strings.Repeat("x", size-len(base)))
		_, err := LoadAuthStore(path, configEnv(map[string]string{"MCP_AUTH_STORE": "auth.json"}))
		if (err != nil) != (size > maxConfigBytes) {
			t.Error("64 KiB 설정 파일 경계 검사 실패")
		}
	}
	path := writeYAMLConfig(t, "\xef\xbb\xbfversion: 1\nauth: {store_file: auth.json}\n")
	if _, err := LoadAuthStore(path, configEnv(nil)); err != nil {
		t.Fatal("UTF-8 BOM 설정 파일을 읽지 못함")
	}
	for _, path := range []string{t.TempDir(), filepath.Join(t.TempDir(), "private-secret-canary.yaml")} {
		_, err := LoadAuthStore(path, configEnv(nil))
		if err == nil || strings.Contains(err.Error(), path) {
			t.Error("비정상 파일을 거부하거나 경로를 숨기는 검사 실패")
		}
	}
}

func TestLoadConfigReusesBackendValidation(t *testing.T) {
	cases := []string{
		"base_url: 'http://backend.example.test'\n  allow_http: true",
		"base_url: 'http://localhost:8080'\n  allow_http: false",
		"base_url: 'https://user:private-secret-canary@backend.example.test'",
		"base_url: 'https://backend.example.test?secret=private-secret-canary'",
		"base_url: 'https://backend.example.test'\n  request_timeout: '121s'",
	}
	for _, content := range cases {
		path := writeYAMLConfig(t, "version: 1\nserver: {transport: http}\nbackend:\n  "+content+"\nauth: {store_file: auth.json}\n")
		_, err := LoadServer(path, configEnv(nil), ServerOverrides{})
		if err == nil {
			t.Error("기존 백엔드 검증을 우회함")
		} else if strings.Contains(err.Error(), "private-secret-canary") {
			t.Error("백엔드 설정 오류에 민감값을 노출함")
		}
	}
}

func TestLoadConfigRequiredFieldsAndReader(t *testing.T) {
	path := writeYAMLConfig(t, "version: 1\nbackend: {base_url: 'https://backend.example.test'}\n")
	if _, err := LoadServer(path, configEnv(nil), ServerOverrides{}); err == nil {
		t.Error("stdio 서버가 빈 토큰을 허용함")
	}
	transport := "http"
	if _, err := LoadServer(path, configEnv(nil), ServerOverrides{Transport: &transport}); err == nil {
		t.Error("HTTP 서버가 빈 인증 저장소를 허용함")
	}
	if _, _, err := LoadEnrollment(path, configEnv(map[string]string{"ABLEOPS_API_TOKEN": "synthetic-session"})); err == nil {
		t.Error("인증 등록이 빈 인증 저장소를 허용함")
	}
	if _, err := LoadAuthStore(path, configEnv(nil)); err == nil {
		t.Error("토큰 폐기가 빈 인증 저장소를 허용함")
	}
	if _, err := LoadServer("", nil, ServerOverrides{}); err == nil {
		t.Error("환경변수 읽기 함수 누락 허용")
	}
	if _, _, err := LoadEnrollment("", nil); err == nil {
		t.Error("인증 등록의 환경변수 읽기 함수 누락 허용")
	}
	if _, err := LoadAuthStore("", nil); err == nil {
		t.Error("토큰 폐기의 환경변수 읽기 함수 누락 허용")
	}
}
