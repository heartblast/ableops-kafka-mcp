package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/localauth"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type lockedLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}
func (b *lockedLogBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buffer.String() }

// 실제 로컬 저장소·SDK HTTP·공유 REST 전송을 연결하여 두 사용자 경계를 검증한다.
func TestLocalAuthSDKHTTPUserIsolationAndRevocation(t *testing.T) {
	var serving, loggedOut atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer synthetic-delegated-")
		if id != "a" && id != "b" {
			t.Error("Backend에 다른 audience 토큰 전달")
			w.WriteHeader(401)
			return
		}
		if serving.Load() && r.Header.Get("X-Request-ID") == "" {
			t.Error("Backend 추적 ID 누락")
		}
		if r.URL.Path == "/api/me" {
			if loggedOut.Load() && id == "b" {
				w.WriteHeader(401)
				return
			}
			fmt.Fprintf(w, `{"id":%q}`, id)
			return
		}
		if r.URL.Path == "/api/clusters" {
			fmt.Fprintf(w, `[{"id":%q,"name":%q}]`, id, id)
			return
		}
		if r.URL.Path == "/api/clusters/expired/topics" {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path != "/api/clusters/"+id+"/topics" {
			w.WriteHeader(403)
			return
		}
		fmt.Fprintf(w, `{"clusterId":%q,"syncedAt":null,"items":[],"configs":{"password":"synthetic-private-data"}}`, id)
	}))
	defer backend.Close()
	base, _ := url.Parse(backend.URL)
	rest, err := ableops.NewClient(config.Config{BaseURL: base, AllowHTTP: true, Timeout: time.Second, RequireRequestCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rest.CloseIdleConnections()
	verify := func(ctx context.Context, token string) (string, error) {
		p, _ := requestctx.FromContext(ctx)
		p.BackendToken = token
		return rest.CurrentUserID(requestctx.WithPrincipal(ctx, p))
	}
	dir := t.TempDir()
	storePath := filepath.Join(dir, "local.auth-store.json")
	tokens := map[string]string{}
	for _, id := range []string{"a", "b"} {
		out := filepath.Join(dir, id+".mcp-token")
		if _, err := localauth.Enroll(context.Background(), storePath, id, "synthetic-delegated-"+id, time.Hour, out, verify); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		tokens[id] = strings.TrimSpace(string(raw))
	}
	store, err := localauth.NewStore(storePath, verify)
	if err != nil {
		t.Fatal(err)
	}
	var logs lockedLogBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler, err := NewHTTPHandler(New(rest, logger), HTTPOptions{Authenticate: store.Authenticate, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	serving.Store(true)
	server := httptest.NewServer(handler)
	defer server.Close()
	sessions := map[string]*mcp.ClientSession{}
	for _, id := range []string{"a", "b"} {
		sessions[id] = sdkHTTPSession(t, server, tokens[id])
	}
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for range 10 {
				result, err := sessions[id].CallTool(context.Background(), &mcp.CallToolParams{Name: "list_clusters"})
				if err != nil || result.IsError {
					t.Error("동시 클러스터 조회 실패")
					return
				}
				raw, _ := json.Marshal(result.StructuredContent)
				var decoded struct {
					Data struct {
						Items []struct {
							ID string `json:"id"`
						} `json:"items"`
					} `json:"data"`
				}
				if json.Unmarshal(raw, &decoded) != nil || len(decoded.Data.Items) != 1 || decoded.Data.Items[0].ID != id {
					t.Error("사용자별 결과 혼합")
				}
			}
		}(id)
	}
	wg.Wait()
	for _, tc := range []struct{ cluster, code string }{{"a", `"status":"partial"`}, {"b", `"code":"access_denied"`}, {"expired", `"code":"authentication_required"`}} {
		result, err := sessions["a"].CallTool(context.Background(), &mcp.CallToolParams{Name: "list_topics", Arguments: map[string]any{"cluster_id": tc.cluster}})
		if err != nil {
			t.Fatal("도구 조회 실패")
		}
		raw, _ := json.Marshal(result.StructuredContent)
		if !strings.Contains(string(raw), tc.code) || strings.Contains(string(raw), "synthetic-private-data") {
			t.Fatal("실패 또는 부분 결과 보존 오류")
		}
	}
	if err := localauth.Revoke(storePath, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := sessions["a"].ListTools(context.Background(), nil); err == nil {
		t.Fatal("폐기된 토큰의 기존 SDK 연결 재사용")
	}
	if _, err := sessions["b"].ListTools(context.Background(), nil); err != nil {
		t.Fatal("다른 사용자 폐기의 영향")
	}
	loggedOut.Store(true)
	if _, err := sessions["b"].ListTools(context.Background(), nil); err == nil {
		t.Fatal("Backend 로그아웃 무시")
	}
	for _, secret := range []string{tokens["a"], tokens["b"], "synthetic-delegated-a", "synthetic-delegated-b", "synthetic-private-data"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("인증 또는 민감 응답이 로그에 노출")
		}
	}
	if !strings.Contains(logs.String(), `"result_code":"access_denied"`) || !strings.Contains(logs.String(), `"code":"backend_authentication_required"`) {
		t.Fatal("권한 및 Backend 세션 실패 로그 구분 누락")
	}
}
