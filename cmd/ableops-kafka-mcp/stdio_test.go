package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type capturedReader struct {
	io.Reader
	io.Closer
}

// 실제 실행파일의 stdin/stdout을 공식 SDK 클라이언트에 연결한다.
func TestStdioProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "ableops-kafka-mcp.exe")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	const token = "synthetic-stdio-session-token"
	var requests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/api/clusters" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("Bearer authentication missing")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"id":"dev-1","name":"개발","environment":"dev","mode":"mock","isActive":true,"isDefault":true,"username":"synthetic-private-user","tlsKeyFile":"synthetic-private-key"}]`)
	}))
	defer backend.Close()

	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = testEnv(map[string]string{
		"ABLEOPS_BASE_URL":        backend.URL,
		"ABLEOPS_API_TOKEN":       token,
		"ABLEOPS_ALLOW_HTTP":      "true",
		"ABLEOPS_REQUEST_TIMEOUT": "2s",
		"MCP_LOG_LEVEL":           "debug",
	})
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
	client := mcp.NewClient(&mcp.Implementation{Name: "stdio-integration-test", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &mcp.IOTransport{
		Reader: capturedReader{Reader: io.TeeReader(readPipe, &stdout), Closer: readPipe},
		Writer: writePipe,
	}, nil)
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}
	defer session.Close()
	if got := session.InitializeResult().ServerInfo.Name; got != "ableops-kafka-mcp" {
		t.Fatalf("server name: %s", got)
	}
	list, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	want := map[string]bool{"list_clusters": true, "get_cluster_health": true, "list_topics": true, "list_consumer_groups": true, "get_consumer_group_lag": true, "list_cluster_events": true, "get_topic_detail": true, "get_consumer_group_members": true, "get_event_detail": true, "get_asset_impact": true, "get_request_status": true}
	if len(list.Tools) != len(want) {
		t.Fatalf("tool count: %d", len(list.Tools))
	}
	for _, tool := range list.Tools {
		if !want[tool.Name] || tool.InputSchema == nil || tool.OutputSchema == nil || tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Fatalf("incomplete tool definition: %+v", tool)
		}
		delete(want, tool.Name)
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "list_clusters", Arguments: map[string]any{}})
	if err != nil || result.IsError || result.StructuredContent == nil || len(result.Content) == 0 {
		t.Fatalf("tools/call: result=%+v error=%v", result, err)
	}
	encoded, err := json.Marshal(result)
	if err != nil || !bytes.Contains(encoded, []byte("dev-1")) {
		t.Fatalf("unexpected result: %s; %v", encoded, err)
	}
	for _, forbidden := range []string{token, "synthetic-private-user", "synthetic-private-key"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatal("private data leaked in MCP result")
		}
	}
	invalid, invalidErr := session.CallTool(ctx, &mcp.CallToolParams{Name: "list_topics", Arguments: map[string]any{}})
	if invalidErr == nil && (invalid == nil || !invalid.IsError) {
		t.Fatal("missing cluster_id was accepted")
	}
	if requests.Load() != 1 {
		t.Fatalf("unexpected backend request count: %d", requests.Load())
	}
	if err := session.Close(); err != nil {
		t.Fatalf("session close: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("process exit: %v", err)
	}
	waited = true

	// stdout의 모든 줄이 JSON-RPC임을 검사하여 로그 혼입을 탐지한다.
	scanner := bufio.NewScanner(bytes.NewReader(stdout.Bytes()))
	scanner.Buffer(make([]byte, 4096), 256*1024)
	frames := 0
	for scanner.Scan() {
		var frame struct {
			JSONRPC string `json:"jsonrpc"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil || frame.JSONRPC != "2.0" {
			t.Fatalf("non-protocol stdout: %q", scanner.Text())
		}
		frames++
	}
	if err := scanner.Err(); err != nil || frames < 3 {
		t.Fatalf("stdout frames: %d; %v", frames, err)
	}
	if bytes.Contains(stdout.Bytes(), []byte(token)) || bytes.Contains(stderr.Bytes(), []byte(token)) {
		t.Fatal("session token leaked")
	}
	if !bytes.Contains(stderr.Bytes(), []byte("list_clusters")) {
		t.Fatal("tool execution log missing from stderr")
	}

	t.Run("invalid_configuration_has_no_stdout", func(t *testing.T) {
		bad := exec.CommandContext(ctx, binary)
		bad.Env = testEnv(map[string]string{"ABLEOPS_BASE_URL": "https://ableops.example.invalid", "ABLEOPS_API_TOKEN": ""})
		var out, log bytes.Buffer
		bad.Stdout, bad.Stderr = &out, &log
		if err := bad.Run(); err == nil || out.Len() != 0 || log.Len() == 0 {
			t.Fatalf("invalid configuration: err=%v stdout=%d stderr=%d", err, out.Len(), log.Len())
		}
	})
}

// 실행자 환경의 실제 세션 토큰/CA 설정이 테스트로 유입되지 않게 한다.
func testEnv(values map[string]string) []string {
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(key)
		if !strings.HasPrefix(upper, "ABLEOPS_") && upper != "MCP_LOG_LEVEL" && upper != "MCP_AUTH_STORE" {
			env = append(env, entry)
		}
	}
	for key, value := range values {
		env = append(env, key+"="+value)
	}
	return env
}
