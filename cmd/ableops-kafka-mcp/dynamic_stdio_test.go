package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// staticToolCount는 Static 도구 수다(v0.2.0 기준 33개). Dynamic 도구는 여기에 더해진다.
const staticToolCount = 33

// lockedBuffer는 자식 프로세스 stderr 수집과 테스트 읽기를 직렬화한다.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

// 실제 실행파일에서 OpenAPI 적재 → tools/list → tools/call → REST 경로를 확인한다.
func TestDynamicStdioProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "ableops-kafka-mcp.exe")
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	spec, err := os.ReadFile(filepath.Join("..", "..", "internal", "openapi", "testdata", "ableops-openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	const token = "synthetic-dynamic-stdio-token"
	var mu sync.Mutex
	specStatus := http.StatusOK
	var paths []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/openapi.json" {
			if r.Header.Get("Authorization") != "" {
				t.Error("계약 조회에 인증 헤더를 보냄")
			}
			w.WriteHeader(specStatus)
			if specStatus == http.StatusOK {
				w.Write(spec)
			}
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("Bearer 인증 누락")
		}
		paths = append(paths, r.URL.EscapedPath()+"?"+r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/clusters":
			io.WriteString(w, `[{"id":"c1","name":"개발","environment":"dev","mode":"mock","isActive":true,"isDefault":true}]`)
		case "/api/clusters/c1/topics/orders v2":
			io.WriteString(w, `{"name":"orders v2","partitions":3,"configs":{"retention.ms":"1000"}}`)
		case "/api/events":
			io.WriteString(w, `{"items":[],"total":0,"page":1,"pageSize":20}`)
		case "/api/events/e1":
			io.WriteString(w, `{"id":"e1","clusterId":"c1","status":"OPEN"}`)
		case "/api/clusters/c1/topics/orders v2/partitions":
			io.WriteString(w, `{"topic":"orders v2","partitions":[{"partition":0,"leader":1,"replicas":[1,2],"isr":[1]}],"offsetHint":9007199254740993}`)
		case "/api/events/summary":
			io.WriteString(w, `{"open":0,"scope":{"all":false,"clusterIds":["c1"]},"collection":{"enabled":false}}`)
		case "/api/branding":
			io.WriteString(w, `{"productName":"AbleOps","demo":false}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	takePaths := func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := paths
		paths = nil
		return out
	}

	baseEnv := map[string]string{
		"ABLEOPS_BASE_URL":        backend.URL,
		"ABLEOPS_API_TOKEN":       token,
		"ABLEOPS_ALLOW_HTTP":      "true",
		"ABLEOPS_REQUEST_TIMEOUT": "2s",
		"ABLEOPS_DYNAMIC_TOOLS":   "true",
	}

	// 기본 파일럿 5개는 노출 안전 게이트에서 모두 SHADOW 또는 BLOCKED라 기본 서버에 추가되지 않는다.
	t.Run("default_pilot_is_not_exposed", func(t *testing.T) {
		session, stdout, stderr, stop := startStdio(t, ctx, binary, baseEnv)
		names := listNames(t, ctx, session)
		if len(names) != staticToolCount || contains(names, "get_topic") || contains(names, "list_events") {
			t.Fatalf("tools=%v", names)
		}
		callText(t, ctx, session, "list_clusters", map[string]any{})
		if got := takePaths(); strings.Join(got, "|") != "/api/clusters?" {
			t.Fatalf("paths=%q", got)
		}
		stop()
		requireProtocolOnly(t, stdout.Bytes())
		logs := stderr.Bytes()
		for _, fragment := range []string{
			"동적 도구 계약 적재", `"exposed":0`, `"shadow":4`, `"blocked":1`,
			"선택한 동적 도구를 노출하지 않음", `"code":"exposure_BLOCKED"`, `"code":"exposure_SHADOW"`, `"code":"static_tool_conflict"`,
			`"msg":"동적 도구 계약 주기 갱신 시작"`, `"interval":"5m0s"`,
		} {
			if !bytes.Contains(logs, []byte(fragment)) {
				t.Fatalf("stderr에 %s 없음: %s", fragment, logs)
			}
		}
		if bytes.Contains(logs, []byte(token)) || bytes.Contains(stdout.Bytes(), []byte(token)) {
			t.Fatal("토큰이 로그·stdout에 노출")
		}
	})

	t.Run("selected_safe_operations", func(t *testing.T) {
		env := map[string]string{
			// getEventSummary 는 Static 과 이름이 같아 노출되지 않으므로 SAFE 중 나머지 둘을 고른다.
			"ABLEOPS_DYNAMIC_OPERATIONS":       "getTopicPartitions,getBranding,getTopic,login",
			"ABLEOPS_DYNAMIC_REFRESH_INTERVAL": "1m",
		}
		for key, value := range baseEnv {
			env[key] = value
		}
		session, stdout, stderr, stop := startStdio(t, ctx, binary, env)
		names := listNames(t, ctx, session)
		if len(names) != staticToolCount+2 || !contains(names, "get_topic_partitions") || !contains(names, "get_branding") || contains(names, "get_topic") || contains(names, "login") {
			t.Fatalf("tools=%v", names)
		}
		text := callText(t, ctx, session, "get_topic_partitions", map[string]any{"id": "c1", "name": "orders v2"})
		if !strings.Contains(text, `"operation_id":"getTopicPartitions"`) || !strings.Contains(text, `"offsetHint":9007199254740993`) {
			t.Fatalf("get_topic_partitions=%s", text)
		}
		text = callText(t, ctx, session, "get_branding", map[string]any{})
		if !strings.Contains(text, `"http_status":200`) || !strings.Contains(text, `"body":{"productName":"AbleOps","demo":false}`) {
			t.Fatalf("get_branding=%s", text)
		}
		want := []string{"/api/clusters/c1/topics/orders%20v2/partitions?", "/api/branding?"}
		if got := takePaths(); strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("paths=%q", got)
		}
		stop()
		requireProtocolOnly(t, stdout.Bytes())
		logs := stderr.Bytes()
		for _, fragment := range []string{`"code":"unknown_selection"`, `"operation_id":"getTopic"`, `"code":"exposure_SHADOW"`, `"exposed":2`, `"interval":"1m0s"`, "동적 도구 조회 완료"} {
			if !bytes.Contains(logs, []byte(fragment)) {
				t.Fatalf("stderr에 %s 없음: %s", fragment, logs)
			}
		}
		if bytes.Contains(logs, []byte(token)) || bytes.Contains(logs, []byte("9007199254740993")) {
			t.Fatal("토큰 또는 응답 원문이 로그에 노출")
		}
	})

	t.Run("contract_unavailable_keeps_static", func(t *testing.T) {
		mu.Lock()
		specStatus = http.StatusServiceUnavailable
		mu.Unlock()
		session, _, stderr, stop := startStdio(t, ctx, binary, baseEnv)
		names := listNames(t, ctx, session)
		if len(names) != staticToolCount || contains(names, "get_topic_partitions") {
			t.Fatalf("tools=%v", names)
		}
		callText(t, ctx, session, "list_clusters", map[string]any{})
		if got := takePaths(); len(got) != 1 || got[0] != "/api/clusters?" {
			t.Fatalf("paths=%q", got)
		}
		// 주기 갱신 goroutine이 있어도 stdin 종료로 프로세스가 정상 종료한다(stop이 확인).
		stop()
		logs := stderr.Bytes()
		if !bytes.Contains(logs, []byte("동적 도구 계약 적재 실패")) || !bytes.Contains(logs, []byte(`"code":"backend_unavailable"`)) {
			t.Fatalf("적재 실패 경고 누락: %s", logs)
		}
	})

	t.Run("invalid_refresh_interval_fails_startup", func(t *testing.T) {
		env := map[string]string{"ABLEOPS_DYNAMIC_REFRESH_INTERVAL": "10s"}
		for key, value := range baseEnv {
			env[key] = value
		}
		cmd := exec.CommandContext(ctx, binary)
		cmd.Env = testEnv(env)
		cmd.Stdin = strings.NewReader("")
		output, err := cmd.CombinedOutput()
		if err == nil || !bytes.Contains(output, []byte("ABLEOPS_DYNAMIC_REFRESH_INTERVAL must be a Go duration from 1m to 24h")) {
			t.Fatalf("err=%v output=%s", err, output)
		}
	})
}

func startStdio(t *testing.T, ctx context.Context, binary string, env map[string]string) (*mcp.ClientSession, *bytes.Buffer, *lockedBuffer, func()) {
	t.Helper()
	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = testEnv(env)
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
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
	var stdout bytes.Buffer
	session, err := mcp.NewClient(&mcp.Implementation{Name: "dynamic-stdio-test", Version: "1"}, nil).Connect(ctx, &mcp.IOTransport{
		Reader: capturedReader{Reader: io.TeeReader(readPipe, &stdout), Closer: readPipe},
		Writer: writePipe,
	}, nil)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("initialize: %v\n%s", err, stderr.Bytes())
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		if err := session.Close(); err != nil {
			t.Errorf("session close: %v", err)
		}
		if err := cmd.Wait(); err != nil {
			t.Errorf("process exit: %v", err)
		}
	}
	t.Cleanup(func() {
		if !stopped {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return session, &stdout, stderr, stop
}

func listNames(t *testing.T, ctx context.Context, session *mcp.ClientSession) []string {
	t.Helper()
	listed, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	var names []string
	for _, tool := range listed.Tools {
		if tool.InputSchema == nil || tool.OutputSchema == nil || tool.Annotations == nil {
			t.Fatalf("불완전한 도구 정의: %s", tool.Name)
		}
		// Dynamic 도구는 조회 전용이다. Static 은 백엔드 스냅샷 부작용에 따라 false 일 수 있다.
		if tool.Meta["ableops/openapi"] != nil && !tool.Annotations.ReadOnlyHint {
			t.Fatalf("Dynamic 도구에 읽기 전용 표시 누락: %s", tool.Name)
		}
		names = append(names, tool.Name)
	}
	sort.Strings(names)
	return names
}

func callText(t *testing.T, ctx context.Context, session *mcp.ClientSession, name string, args map[string]any) string {
	t.Helper()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil || result.IsError || result.StructuredContent == nil || len(result.Content) == 0 {
		t.Fatalf("%s: result=%+v err=%v", name, result, err)
	}
	return result.Content[0].(*mcp.TextContent).Text
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func requireProtocolOnly(t *testing.T, stdout []byte) {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(stdout))
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		var frame struct {
			JSONRPC string `json:"jsonrpc"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil || frame.JSONRPC != "2.0" {
			t.Fatalf("non-protocol stdout: %q", scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}
