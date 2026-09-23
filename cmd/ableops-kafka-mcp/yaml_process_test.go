package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/localauth"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type yamlProcessLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *yamlProcessLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *yamlProcessLog) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type yamlBearerTransport struct{ token string }

func (rt yamlBearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+rt.token)
	return http.DefaultTransport.RoundTrip(r)
}

// 실제 실행 파일과 공식 SDK로 YAML 설정, 상대 경로, 요청별 인증 경계를 검증한다.
func TestYAMLServerProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "ableops-kafka-mcp.exe")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("테스트 실행 파일 빌드: %v\n%s", err, output)
	}

	t.Run("http_file_and_explicit_flags", func(t *testing.T) {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer synthetic-yaml-backend-")
			if r.Method != http.MethodGet || (id != "a" && id != "b") {
				t.Error("사용자별 Backend 토큰 또는 조회 메서드 오류")
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/api/me":
				fmt.Fprintf(w, `{"id":%q}`, id)
			case "/api/clusters":
				fmt.Fprintf(w, `[{"id":%q,"name":"합성 클러스터","password":"synthetic-yaml-private"}]`, id)
			default:
				t.Error("계약에 없는 Backend 조회 경로")
				http.NotFound(w, r)
			}
		}))
		defer backend.Close()
		base, _ := url.Parse(backend.URL)
		rest, err := ableops.NewClient(config.Config{BaseURL: base, AllowHTTP: true, Timeout: 2 * time.Second, RequireRequestCredentials: true})
		if err != nil {
			t.Fatal(err)
		}
		defer rest.CloseIdleConnections()
		verify := func(ctx context.Context, token string) (string, error) {
			return rest.CurrentUserID(requestctx.WithPrincipal(ctx, requestctx.Principal{BackendToken: token}))
		}
		configDir := t.TempDir()
		storeDir := filepath.Join(configDir, "auth")
		if err := os.Mkdir(storeDir, 0700); err != nil {
			t.Fatal(err)
		}
		storePath := filepath.Join(storeDir, "local.auth-store.json")
		tokens := map[string]string{}
		for _, id := range []string{"a", "b"} {
			tokenPath := filepath.Join(storeDir, id+".mcp-token")
			if _, err := localauth.Enroll(ctx, storePath, id, "synthetic-yaml-backend-"+id, time.Hour, tokenPath, verify); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(tokenPath)
			if err != nil {
				t.Fatal(err)
			}
			tokens[id] = strings.TrimSpace(string(raw))
		}

		for _, override := range []bool{false, true} {
			t.Run(fmt.Sprintf("CLI_override_%t", override), func(t *testing.T) {
				address := yamlProcessAddress(t)
				fileAddress := address
				origin := "https://yaml.example.invalid"
				configPath := filepath.Join(configDir, "mcp-server-config.yaml")
				args := []string{"--config=" + configPath}
				if override {
					fileAddress = "127.0.0.1:0"
					origin = "https://cli.example.invalid"
					args = append(args, "--http-address="+address, "--allowed-origins="+origin)
				}
				yamlWriteFile(t, configPath, fmt.Sprintf(`version: 1
server:
  transport: http
  http_address: %q
  allowed_origins: ["https://yaml.example.invalid"]
backend:
  base_url: %q
  allow_http: true
  request_timeout: 2s
auth:
  store_file: auth/local.auth-store.json
logging:
  level: info
`, fileAddress, backend.URL))
				cmd := exec.CommandContext(ctx, binary, args...)
				cmd.Dir = t.TempDir()
				cmd.Env = testEnv(map[string]string{
					"ABLEOPS_API_TOKEN": "synthetic-yaml-shared-token-forbidden",
				})
				var stdout, stderr yamlProcessLog
				cmd.Stdout, cmd.Stderr = &stdout, &stderr
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				var stopOnce sync.Once
				stop := func() { stopOnce.Do(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }) }
				defer stop()
				endpoint := "http://" + address
				yamlWaitHTTP(t, ctx, endpoint, &stderr)
				client := &http.Client{Timeout: 2 * time.Second}
				request, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/mcp", strings.NewReader(`{}`))
				response, err := client.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if response.StatusCode != http.StatusUnauthorized {
					t.Fatalf("무인증 요청 상태: %d", response.StatusCode)
				}
				origins := []string{origin, "https://denied.example.invalid"}
				if override {
					origins = append(origins, "https://yaml.example.invalid")
				}
				for _, testedOrigin := range origins {
					request, _ := http.NewRequestWithContext(ctx, http.MethodOptions, endpoint+"/mcp", nil)
					request.Header.Set("Origin", testedOrigin)
					request.Header.Set("Access-Control-Request-Method", "POST")
					response, err := client.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					response.Body.Close()
					want := http.StatusNoContent
					if testedOrigin != origin {
						want = http.StatusForbidden
					}
					if response.StatusCode != want {
						t.Fatalf("Origin 설정 상태: %d, 기대: %d", response.StatusCode, want)
					}
				}
				for _, id := range []string{"a", "b", "a"} {
					client := mcp.NewClient(&mcp.Implementation{Name: "yaml-process-test", Version: "1"}, nil)
					session, err := client.Connect(ctx, &mcp.StreamableClientTransport{
						Endpoint:   endpoint + "/mcp",
						HTTPClient: &http.Client{Transport: yamlBearerTransport{token: tokens[id]}, Timeout: 3 * time.Second},
					}, nil)
					if err != nil {
						t.Fatalf("SDK HTTP 초기화 실패: %v", err)
					}
					func() {
						defer session.Close()
						yamlAssertToolsAndCluster(t, ctx, session, id)
					}()
				}
				stop()
				if stdout.String() != "" {
					t.Fatal("HTTP 프로세스의 stdout 로그 오염")
				}
				for _, secret := range []string{tokens["a"], tokens["b"], "synthetic-yaml-backend-a", "synthetic-yaml-backend-b", "synthetic-yaml-private", "synthetic-yaml-shared-token-forbidden"} {
					if strings.Contains(stderr.String(), secret) {
						t.Fatal("인증 또는 Backend 비공개 필드가 로그에 노출")
					}
				}
			})
		}
	})

	t.Run("stdio_and_transport_override", func(t *testing.T) {
		const token = "synthetic-yaml-stdio-token"
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/clusters" || r.Header.Get("Authorization") != "Bearer "+token {
				t.Error("YAML stdio 환경변수 토큰 또는 조회 경로 오류")
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			fmt.Fprint(w, `[{"id":"stdio","name":"합성 클러스터"}]`)
		}))
		defer backend.Close()
		for _, transport := range []string{"stdio", "http"} {
			t.Run(transport, func(t *testing.T) {
				configPath := filepath.Join(t.TempDir(), "mcp-server-config.yaml")
				yamlWriteFile(t, configPath, fmt.Sprintf(`version: 1
server:
  transport: %s
backend:
  base_url: %q
  allow_http: true
auth:
  token_env: ABLEOPS_YAML_TEST_TOKEN
logging:
  level: info
`, transport, backend.URL))
				args := []string{"--config=" + configPath}
				if transport == "http" {
					args = append(args, "--transport=stdio")
				}
				cmd := exec.CommandContext(ctx, binary, args...)
				cmd.Dir = t.TempDir()
				cmd.Env = testEnv(map[string]string{
					"ABLEOPS_YAML_TEST_TOKEN": token,
					"ABLEOPS_API_TOKEN":       "synthetic-yaml-wrong-default-token",
				})
				yamlAssertStdioProcess(t, ctx, cmd, "stdio", token)
			})
		}
	})

	t.Run("invalid_file_has_safe_error_and_no_stdout", func(t *testing.T) {
		for _, data := range []string{
			"version: 1\nbackend: [synthetic-yaml-secret: invalid\n",
			"version: 1\nsynthetic-yaml-secret: synthetic-yaml-secret\n",
			"version: 1\nauth:\n  token: synthetic-yaml-secret\n",
		} {
			configPath := filepath.Join(t.TempDir(), "mcp-server-config.yaml")
			yamlWriteFile(t, configPath, data)
			cmd := exec.CommandContext(ctx, binary, "--config="+configPath)
			cmd.Env = testEnv(nil)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			if err == nil || cmd.ProcessState.ExitCode() != 1 || stdout.Len() != 0 || stderr.Len() == 0 {
				t.Fatal("잘못된 YAML의 종료 코드 또는 출력 경계 오류")
			}
			if strings.Contains(stderr.String(), "synthetic-yaml-secret") {
				t.Fatal("YAML 파서 오류에 입력 키 또는 비밀 값 노출")
			}
		}
	})

	t.Run("no_config_does_not_autoload_yaml", func(t *testing.T) {
		const token = "synthetic-yaml-legacy-token"
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/clusters" || r.Header.Get("Authorization") != "Bearer "+token {
				t.Error("기존 환경변수 설정 호환성 오류")
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			fmt.Fprint(w, `[{"id":"legacy","name":"합성 클러스터"}]`)
		}))
		defer backend.Close()
		cmd := exec.CommandContext(ctx, binary)
		cmd.Dir = t.TempDir()
		yamlWriteFile(t, filepath.Join(cmd.Dir, "mcp-server-config.yaml"), "invalid: [synthetic-yaml-secret\n")
		cmd.Env = testEnv(map[string]string{"ABLEOPS_BASE_URL": backend.URL, "ABLEOPS_ALLOW_HTTP": "true", "ABLEOPS_API_TOKEN": token})
		yamlAssertStdioProcess(t, ctx, cmd, "legacy", token)
	})
}

func yamlWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func yamlProcessAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func yamlWaitHTTP(t *testing.T, ctx context.Context, endpoint string, stderr *yamlProcessLog) {
	t.Helper()
	readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, _ := http.NewRequestWithContext(readyCtx, http.MethodGet, endpoint+"/healthz", nil)
		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-readyCtx.Done():
			t.Fatalf("YAML HTTP 프로세스 시작 실패: %s", stderr.String())
		case <-ticker.C:
		}
	}
}

func yamlAssertToolsAndCluster(t *testing.T, ctx context.Context, session *mcp.ClientSession, wantID string) {
	t.Helper()
	list, err := session.ListTools(ctx, nil)
	if err != nil || len(list.Tools) != 34 {
		t.Fatal("공식 SDK의 11개 도구 목록 확인 실패")
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "list_clusters", Arguments: map[string]any{}})
	if err != nil || result == nil || result.IsError {
		t.Fatal("공식 SDK의 클러스터 조회 실패")
	}
	raw, err := json.Marshal(result.StructuredContent)
	var decoded struct {
		Data struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
		} `json:"data"`
	}
	if err != nil || json.Unmarshal(raw, &decoded) != nil || len(decoded.Data.Items) != 1 || decoded.Data.Items[0].ID != wantID {
		t.Fatal("사용자별 클러스터 결과 혼합 또는 누락")
	}
	if strings.Contains(string(raw), "synthetic-yaml-private") || strings.Contains(string(raw), "synthetic-yaml-backend-") {
		t.Fatal("도구 결과에 Backend 비공개 필드 또는 토큰 노출")
	}
}

func yamlAssertStdioProcess(t *testing.T, ctx context.Context, cmd *exec.Cmd, wantID, secret string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd.Stderr = &stderr
	readPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	writePipe, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	client := mcp.NewClient(&mcp.Implementation{Name: "yaml-stdio-process-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.IOTransport{
		Reader: capturedReader{Reader: io.TeeReader(readPipe, &stdout), Closer: readPipe}, Writer: writePipe,
	}, nil)
	if err != nil {
		t.Fatalf("YAML stdio 초기화: %v", err)
	}
	defer session.Close()
	yamlAssertToolsAndCluster(t, ctx, session, wantID)
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("YAML stdio 종료: %v", err)
	}
	waited = true
	frames := 0
	scanner := bufio.NewScanner(bytes.NewReader(stdout.Bytes()))
	scanner.Buffer(make([]byte, 4096), 256*1024)
	for scanner.Scan() {
		var frame struct {
			JSONRPC string `json:"jsonrpc"`
		}
		if json.Unmarshal(scanner.Bytes(), &frame) != nil || frame.JSONRPC != "2.0" {
			t.Fatal("stdio stdout에 JSON-RPC 외의 출력")
		}
		frames++
	}
	if scanner.Err() != nil || frames < 3 {
		t.Fatal("stdio JSON-RPC 출력 누락")
	}
	if bytes.Contains(stdout.Bytes(), []byte(secret)) || bytes.Contains(stderr.Bytes(), []byte(secret)) {
		t.Fatal("stdio 토큰 노출")
	}
}
