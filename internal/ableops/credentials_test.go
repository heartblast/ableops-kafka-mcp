package ableops

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

func TestRequestCredentialsIsolationAndNoFallback(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer synthetic-backend-")
		if id != "A" && id != "B" {
			t.Error("요청 자격증명 불일치")
			w.WriteHeader(401)
			return
		}
		if r.Header.Get("X-Request-ID") != "trace-"+id {
			t.Error("추적 ID 불일치")
		}
		if r.URL.Path == "/api/me" {
			fmt.Fprintf(w, `{"id":%q,"roles":["ignored"]}`, id)
			return
		}
		if r.URL.Path != "/api/clusters/"+id+"/topics" {
			w.WriteHeader(403)
			return
		}
		fmt.Fprintf(w, `{"clusterId":%q,"items":[],"syncedAt":"2026-09-16T00:00:00Z"}`, id)
	}))
	defer backend.Close()
	base, _ := url.Parse(backend.URL)
	client, err := NewClient(config.Config{BaseURL: base, Timeout: time.Second, AllowHTTP: true, RequireRequestCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = client.ListClusters(context.Background())
	requireErrorCode(t, err, "authentication_required")
	var wg sync.WaitGroup
	for range 20 {
		for _, id := range []string{"A", "B"} {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				ctx := requestctx.WithPrincipal(context.Background(), requestctx.Principal{UserID: id, BackendToken: "synthetic-backend-" + id, MCPToken: "synthetic-mcp-" + id, RequestID: "trace-" + id})
				got, err := client.CurrentUserID(ctx)
				if err != nil || got != id {
					t.Error("사용자 혼합")
				}
				out, err := client.ListTopics(ctx, id)
				if err != nil || out.ClusterID != id {
					t.Error("클러스터 혼합")
				}
				_, err = client.ListTopics(ctx, "denied")
				requireErrorCode(t, err, "access_denied")
			}(id)
		}
	}
	wg.Wait()
}

func TestBothRequestTokensAreProtected(t *testing.T) {
	for _, token := range []string{"synthetic-backend-private", "synthetic-mcp-private"} {
		t.Run(token, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, `{"id":%q}`, token) }))
			defer server.Close()
			client := mockClient(t, server)
			ctx := requestctx.WithPrincipal(context.Background(), requestctx.Principal{BackendToken: "synthetic-backend-private", MCPToken: "synthetic-mcp-private"})
			_, err := client.CurrentUserID(ctx)
			requireErrorCode(t, err, "invalid_response")
			if got := client.RedactContext(ctx, "synthetic-backend-private synthetic-mcp-private"); strings.Contains(got, "private") {
				t.Fatal("토큰 마스킹 실패")
			}
		})
	}
}
