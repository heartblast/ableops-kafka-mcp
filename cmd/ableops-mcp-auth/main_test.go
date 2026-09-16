package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/localauth"
)

func TestAdminCLIEnrollmentAndRevocation(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/me" || r.Header.Get("Authorization") != "Bearer synthetic-backend-token" {
			t.Error("등록 검증 API 또는 자격증명 불일치")
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"synthetic-user","roles":["Viewer"]}`)
	}))
	defer backend.Close()
	dir := t.TempDir()
	path, output := filepath.Join(dir, "store.json"), filepath.Join(dir, "client.token")
	env := map[string]string{"ABLEOPS_BASE_URL": backend.URL, "ABLEOPS_API_TOKEN": "synthetic-backend-token", "ABLEOPS_ALLOW_HTTP": "true", "MCP_AUTH_STORE": path}
	getenv := func(key string) string { return env[key] }
	var stderr bytes.Buffer
	if code := run([]string{"enroll", "--client-id", "synthetic-client", "--token-output", output}, getenv, &stderr); code != 0 {
		t.Fatalf("등록 명령 실패: %s", stderr.String())
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(data))
	if strings.Contains(stderr.String(), token) || strings.Contains(stderr.String(), env["ABLEOPS_API_TOKEN"]) {
		t.Fatal("토큰이 CLI 출력에 노출되었습니다")
	}
	store, err := localauth.NewStore(path, func(context.Context, string) (string, error) { return "synthetic-user", nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authenticate(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	delete(env, "ABLEOPS_API_TOKEN")
	if code := run([]string{"revoke", "--client-id", "synthetic-client"}, getenv, &stderr); code != 0 {
		t.Fatal("폐기는 백엔드 연결 없이 실행되어야 합니다")
	}
	if _, err := store.Authenticate(context.Background(), token); err == nil {
		t.Fatal("CLI에서 폐기한 토큰이 허용되었습니다")
	}
}

func TestCLIInputDoesNotExposeValues(t *testing.T) {
	for _, args := range [][]string{{}, {"synthetic-secret"}, {"enroll", "--synthetic-secret"}, {"enroll", "--ttl", "synthetic-secret"}, {"enroll", "synthetic-secret"}, {"enroll", "--config="}, {"revoke", "--config= "}, {"enroll", "--config"}} {
		var stderr bytes.Buffer
		if code := run(args, func(key string) string {
			if key == "MCP_AUTH_STORE" {
				return "synthetic-path"
			}
			return ""
		}, &stderr); code == 0 {
			t.Fatal("잘못된 인자가 허용되었습니다")
		}
		if strings.Contains(stderr.String(), "synthetic-secret") {
			t.Fatal("입력값이 오류 출력에 노출되었습니다")
		}
	}
}

func TestAdminCLIYAMLEnrollmentAndRevocation(t *testing.T) {
	const backendToken = "synthetic-config-backend-token"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/me" || r.Header.Get("Authorization") != "Bearer "+backendToken {
			t.Error("YAML 인증 등록의 백엔드 사용자 검증이 올바르지 않습니다")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"synthetic-config-user","roles":["Viewer"]}`)
	}))
	defer backend.Close()
	dir := t.TempDir()
	configPath, storePath, tokenPath := filepath.Join(dir, "mcp-server-config.yaml"), filepath.Join(dir, "store.json"), filepath.Join(dir, "client.token")
	doc := fmt.Sprintf(`version: 1
server:
  transport: http
backend:
  base_url: %q
  allow_http: true
auth:
  store_file: "store.json"
  token_env: "SYNTHETIC_BACKEND_TOKEN"
`, backend.URL)
	if err := os.WriteFile(configPath, []byte(doc), 0600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"SYNTHETIC_BACKEND_TOKEN": backendToken}
	getenv := func(key string) string { return env[key] }
	var stderr bytes.Buffer
	if code := run([]string{"enroll", "--config", configPath, "--client-id", "synthetic-config-client", "--token-output", tokenPath}, getenv, &stderr); code != 0 {
		t.Fatalf("YAML 인증 등록 실패: %s", stderr.String())
	}
	data, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(data))
	store, err := localauth.NewStore(storePath, func(context.Context, string) (string, error) { return "synthetic-config-user", nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authenticate(context.Background(), token); err != nil {
		t.Fatal("YAML로 등록한 MCP 토큰이 인증되지 않습니다")
	}
	// 폐기는 백엔드 주소나 세션 토큰이 없어도 로컬 저장소만으로 수행한다.
	delete(env, "SYNTHETIC_BACKEND_TOKEN")
	if err := os.WriteFile(configPath, []byte("version: 1\nauth:\n  store_file: store.json\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"revoke", "--config", configPath, "--client-id", "synthetic-config-client"}, getenv, &stderr); code != 0 {
		t.Fatalf("백엔드 설정 없이 YAML 인증 폐기 실패: %s", stderr.String())
	}
	if _, err := store.Authenticate(context.Background(), token); err == nil {
		t.Fatal("YAML로 폐기한 MCP 토큰이 허용되었습니다")
	}
	for _, value := range []string{token, backendToken, configPath, storePath, tokenPath} {
		if strings.Contains(stderr.String(), value) {
			t.Fatal("CLI 출력에 토큰 또는 설정 경로가 노출되었습니다")
		}
	}
}

func TestAdminCLIYAMLHTTPEnrollmentStillRequiresBackendToken(t *testing.T) {
	dir := t.TempDir()
	configPath, tokenPath := filepath.Join(dir, "mcp-server-config.yaml"), filepath.Join(dir, "client.token")
	doc := `version: 1
server:
  transport: http
backend:
  base_url: "https://synthetic-backend.invalid"
auth:
  store_file: "store.json"
  token_env: "SYNTHETIC_BACKEND_TOKEN"
`
	if err := os.WriteFile(configPath, []byte(doc), 0600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if code := run([]string{"enroll", "--config", configPath, "--client-id", "synthetic-client", "--token-output", tokenPath}, func(string) string { return "" }, &stderr); code != 1 {
		t.Fatal("HTTP 설정을 이용한 인증 등록에서 백엔드 토큰 누락이 허용되었습니다")
	}
	for _, path := range []string{tokenPath, filepath.Join(dir, "store.json")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("백엔드 토큰 없이 인증 파일이 생성되었습니다")
		}
	}
}

func TestAdminCLIYAMLErrorsDoNotExposeInput(t *testing.T) {
	for _, command := range []string{"enroll", "revoke"} {
		t.Run(command, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "synthetic-path-secret.yaml")
			for _, doc := range []string{"version: 1\nsynthetic-key-secret: synthetic-value-secret\n", "version: 1\nauth: [synthetic-parse-secret\n"} {
				if err := os.WriteFile(configPath, []byte(doc), 0600); err != nil {
					t.Fatal(err)
				}
				var stderr bytes.Buffer
				if code := run([]string{command, "--config", configPath, "--client-id", "synthetic-client"}, func(string) string { return "" }, &stderr); code != 1 || stderr.Len() == 0 {
					t.Fatal("잘못된 YAML이 허용되었거나 오류 출력이 없습니다")
				}
				for _, value := range []string{configPath, "synthetic-key-secret", "synthetic-value-secret", "synthetic-parse-secret"} {
					if strings.Contains(stderr.String(), value) {
						t.Fatal("YAML 오류 원문에 포함된 경로 또는 값이 노출되었습니다")
					}
				}
			}
		})
	}
}

func TestCLIConfigurationErrorsIdentifySettingWithoutValues(t *testing.T) {
	tests := []struct {
		name, key, value, reason string
	}{
		{"빈 토큰", "ABLEOPS_API_TOKEN", "", "ABLEOPS_API_TOKEN is required"},
		{"공백 포함 토큰", "ABLEOPS_API_TOKEN", "synthetic token secret", "ABLEOPS_API_TOKEN must contain only printable ASCII without whitespace"},
		{"비ASCII 토큰", "ABLEOPS_API_TOKEN", "합성-토큰-secret", "ABLEOPS_API_TOKEN must contain only printable ASCII without whitespace"},
		{"잘못된 제한시간", "ABLEOPS_REQUEST_TIMEOUT", "synthetic-timeout-secret", "ABLEOPS_REQUEST_TIMEOUT must be a Go duration from 1s to 120s"},
		{"범위 밖 제한시간", "ABLEOPS_REQUEST_TIMEOUT", "987s", "ABLEOPS_REQUEST_TIMEOUT must be from 1s to 120s"},
		{"잘못된 로그 수준", "MCP_LOG_LEVEL", "synthetic-log-secret", "MCP_LOG_LEVEL must be debug, info, warn, or error"},
		{"잘못된 HTTP 허용값", "ABLEOPS_ALLOW_HTTP", "synthetic-http-secret", "ABLEOPS_ALLOW_HTTP must be true or false"},
		{"누락된 HTTP 허용값", "ABLEOPS_ALLOW_HTTP", "", "HTTP requires ABLEOPS_ALLOW_HTTP=true and a localhost, 127.0.0.1, or ::1 host"},
		{"누락된 주소", "ABLEOPS_BASE_URL", "", "ABLEOPS_BASE_URL is required"},
		{"파싱 불가 주소", "ABLEOPS_BASE_URL", "http://synthetic-origin-secret%", "ABLEOPS_BASE_URL is not a valid origin URL"},
		{"경로 포함 주소", "ABLEOPS_BASE_URL", "http://localhost:8080/synthetic-path-secret", "ABLEOPS_BASE_URL must be an origin without credentials, path, query, or fragment"},
		{"자격증명 포함 주소", "ABLEOPS_BASE_URL", "https://synthetic-user:synthetic-password@synthetic-host.invalid", "ABLEOPS_BASE_URL must be an origin without credentials, path, query, or fragment"},
		{"쿼리 포함 주소", "ABLEOPS_BASE_URL", "https://synthetic-host.invalid?synthetic-query-secret", "ABLEOPS_BASE_URL must be an origin without a query or fragment"},
		{"잘못된 주소 스킴", "ABLEOPS_BASE_URL", "ftp://synthetic-host.invalid", "ABLEOPS_BASE_URL must use HTTPS (or explicitly enabled loopback HTTP)"},
		{"잘못된 포트", "ABLEOPS_BASE_URL", "http://localhost:98765", "ABLEOPS_BASE_URL port must be from 1 to 65535"},
		{"외부 HTTP 주소", "ABLEOPS_BASE_URL", "http://synthetic-host.invalid", "HTTP requires ABLEOPS_ALLOW_HTTP=true and a localhost, 127.0.0.1, or ::1 host"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path, output := filepath.Join(dir, "synthetic-store.json"), filepath.Join(dir, "synthetic-token.txt")
			env := map[string]string{
				"ABLEOPS_BASE_URL":   "http://localhost:8080",
				"ABLEOPS_API_TOKEN":  "synthetic-backend-secret",
				"ABLEOPS_ALLOW_HTTP": "true",
				"MCP_AUTH_STORE":     path,
			}
			env[tt.key] = tt.value
			var stderr bytes.Buffer
			code := run([]string{"enroll", "--client-id", "synthetic-client", "--token-output", output}, func(key string) string { return env[key] }, &stderr)
			if code != 1 {
				t.Fatalf("설정 오류의 종료 코드: %d", code)
			}
			want := "로컬 인증 관리 실패: 백엔드 연결 설정이 올바르지 않습니다: " + tt.reason + "\n"
			if stderr.String() != want {
				t.Fatalf("고정 오류 문구 불일치: %q", stderr.String())
			}
			for _, value := range []string{env["ABLEOPS_API_TOKEN"], env["ABLEOPS_BASE_URL"], path, output, tt.value} {
				if value != "" && strings.Contains(stderr.String(), value) {
					t.Fatal("설정값이 오류 출력에 노출되었습니다")
				}
			}
			for _, file := range []string{path, output} {
				if _, err := os.Stat(file); !os.IsNotExist(err) {
					t.Fatal("설정 오류 후 인증 파일이 생성되었습니다")
				}
			}
		})
	}
}

func TestCLIClientErrorsIdentifyCAFileWithoutValues(t *testing.T) {
	for _, readable := range []bool{false, true} {
		t.Run(map[bool]string{false: "읽기 실패", true: "잘못된 인증서"}[readable], func(t *testing.T) {
			dir := t.TempDir()
			caPath := filepath.Join(dir, "synthetic-ca-secret.pem")
			reason := "ABLEOPS_CA_FILE could not be read"
			if readable {
				if err := os.WriteFile(caPath, []byte("synthetic-certificate-secret"), 0600); err != nil {
					t.Fatal(err)
				}
				reason = "ABLEOPS_CA_FILE does not contain a valid PEM certificate"
			}
			path, output := filepath.Join(dir, "synthetic-store.json"), filepath.Join(dir, "synthetic-token.txt")
			env := map[string]string{
				"ABLEOPS_BASE_URL":  "https://synthetic-backend.invalid",
				"ABLEOPS_API_TOKEN": "synthetic-backend-secret",
				"ABLEOPS_CA_FILE":   caPath,
				"MCP_AUTH_STORE":    path,
			}
			var stderr bytes.Buffer
			code := run([]string{"enroll", "--client-id", "synthetic-client", "--token-output", output}, func(key string) string { return env[key] }, &stderr)
			if code != 1 {
				t.Fatalf("클라이언트 생성 오류의 종료 코드: %d", code)
			}
			want := "로컬 인증 관리 실패: 백엔드 클라이언트를 생성할 수 없습니다: " + reason + "\n"
			if stderr.String() != want {
				t.Fatalf("고정 오류 문구 불일치: %q", stderr.String())
			}
			for _, value := range []string{env["ABLEOPS_API_TOKEN"], env["ABLEOPS_BASE_URL"], caPath, "synthetic-certificate-secret", path, output} {
				if strings.Contains(stderr.String(), value) {
					t.Fatal("인증서 경로 또는 설정값이 오류 출력에 노출되었습니다")
				}
			}
			for _, file := range []string{path, output} {
				if _, err := os.Stat(file); !os.IsNotExist(err) {
					t.Fatal("클라이언트 생성 오류 후 인증 파일이 생성되었습니다")
				}
			}
		})
	}
}
