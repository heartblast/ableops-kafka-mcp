package mcpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestProcessShutdownCancelsBackendRequest(t *testing.T) {
	testCtx, stopTest := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopTest()
	started := make(chan struct{})
	backendCanceled := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/clusters" {
			t.Errorf("unexpected backend path: %s", r.URL.Path)
		}
		close(started)
		<-r.Context().Done()
		close(backendCanceled)
	}))
	defer backend.Close()
	base, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := ableops.NewClient(config.Config{
		BaseURL: base, Token: "synthetic-shutdown-session", Timeout: 30 * time.Second, AllowHTTP: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	server := New(client, slog.New(slog.DiscardHandler))
	processCtx, stopProcess := context.WithCancel(context.Background())
	defer stopProcess()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverDone := make(chan error, 1)
	go func() { serverDone <- run(processCtx, server, serverTransport) }()
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "shutdown-test", Version: "1.0.0"}, nil)
	session, err := mcpClient.Connect(testCtx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopProcess()
		stopTest()
		_ = session.Close()
	}()
	callDone := make(chan error, 1)
	go func() {
		_, err := session.CallTool(testCtx, &mcp.CallToolParams{Name: "list_clusters", Arguments: map[string]any{}})
		callDone <- err
	}()
	select {
	case <-started:
	case <-testCtx.Done():
		t.Fatal("backend request did not start")
	}

	// REST timeout은 30초지만 프로세스 취소는 즉시 진행 중 요청을 중단해야 한다.
	stopProcess()
	select {
	case <-backendCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("process shutdown did not cancel the backend request promptly")
	}
	select {
	case err := <-serverDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("server shutdown error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server waited for the REST timeout during shutdown")
	}
	select {
	case <-callDone:
	case <-time.After(2 * time.Second):
		t.Fatal("tool caller remained blocked after server shutdown")
	}
}
