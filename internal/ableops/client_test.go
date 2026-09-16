package ableops

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
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

	"github.com/heartblast/ableops-kafka-mcp/internal/config"
)

const testToken = "synthetic-session-token"

func mockClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(config.Config{BaseURL: base, Token: testToken, Timeout: time.Second, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func requireErrorCode(t *testing.T, err error, code string) *Error {
	t.Helper()
	var public *Error
	if !errors.As(err, &public) || public.Code != code {
		t.Fatalf("error=%v, want code=%s", err, code)
	}
	if strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), "backend-secret") {
		t.Fatal("sensitive value in returned error")
	}
	return public
}

func TestGetUsesBearerAndEncodesEveryRawPathSegment(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Error("missing session authorization")
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Error("missing JSON accept header")
		}
		want := "/api/clusters/c%2Fname/groups/a%2Fb%20%3F%23%25%ED%95%9C%EA%B8%80/lag"
		if r.URL.EscapedPath() != want {
			t.Errorf("escaped path=%s want=%s", r.URL.EscapedPath(), want)
		}
		if r.URL.Query().Get("search") != "a & b" {
			t.Error("query was not encoded")
		}
		io.WriteString(w, `{"count":9007199254740993}`)
	}))
	defer server.Close()
	client := mockClient(t, server)
	var result struct {
		Count int64 `json:"count"`
	}
	if err := client.Get(context.Background(), []string{"clusters", "c/name", "groups", "a/b ?#%한글", "lag"}, url.Values{"search": {"a & b"}}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Count != 9007199254740993 {
		t.Fatal("integer precision lost")
	}
}

func TestHTTPFailuresAreClassifiedAndNeverRetried(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{400, "backend_error"}, {401, "authentication_required"}, {403, "access_denied"}, {404, "not_found"}, {429, "rate_limited"}, {500, "backend_unavailable"}, {502, "backend_unavailable"}, {503, "backend_unavailable"}} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				io.WriteString(w, `{"error":"backend-secret `+testToken+`"}`)
			}))
			defer server.Close()
			var out any
			err := mockClient(t, server).Get(context.Background(), []string{"clusters"}, nil, &out)
			public := requireErrorCode(t, err, tc.code)
			if public.HTTPStatus != tc.status {
				t.Fatal("HTTP status was not preserved")
			}
			if calls.Load() != 1 {
				t.Fatal("request was retried")
			}
		})
	}
}

func TestResponseValidationAndTokenDisclosureGuard(t *testing.T) {
	for _, tc := range []struct{ name, body, code string }{
		{"malformed", `{"items":`, "invalid_response"},
		{"trailing_document", `{} {}`, "invalid_response"},
		{"null", `null`, "invalid_response"},
		{"empty", ``, "invalid_response"},
		{"oversized", `{"value":"` + strings.Repeat("x", MaxResponseBytes) + `"}`, "response_too_large"},
		{"token_value", `{"value":"prefix ` + testToken + ` suffix"}`, "invalid_response"},
		{"escaped_token_value", `{"value":"synthetic\u002dsession-token"}`, "invalid_response"},
		{"escaped_token_key", `{"synthetic\u002dsession-token":"value"}`, "invalid_response"},
		{"nested_token", `{"items":[{"value":"` + testToken + `"}]}`, "invalid_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) }))
			defer server.Close()
			var out any
			requireErrorCode(t, mockClient(t, server).Get(context.Background(), []string{"clusters"}, nil, &out), tc.code)
			if out != nil {
				t.Fatal("invalid response populated destination")
			}
		})
	}
}

func TestRedirectCannotForwardAuthorization(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Error("token forwarded to redirect target")
		}
		io.WriteString(w, `{}`)
	}))
	defer target.Close()
	for _, status := range []int{301, 302, 303, 307, 308} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL+"/stolen", status) }))
		var out any
		requireErrorCode(t, mockClient(t, server).Get(context.Background(), []string{"clusters"}, nil, &out), "redirect_blocked")
		server.Close()
	}
	if targetCalls.Load() != 0 {
		t.Fatal("redirect target was contacted")
	}
	var calls atomic.Int32
	sameOrigin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, "/api/other", 302)
	}))
	defer sameOrigin.Close()
	var out any
	requireErrorCode(t, mockClient(t, sameOrigin).Get(context.Background(), []string{"clusters"}, nil, &out), "redirect_blocked")
	if calls.Load() != 1 {
		t.Fatal("same-origin redirect was followed")
	}
}

func TestTimeoutAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	client := mockClient(t, server)
	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		var out any
		requireErrorCode(t, client.Get(ctx, []string{"clusters"}, nil, &out), "timeout")
	})
	t.Run("configured_timeout", func(t *testing.T) {
		var out any
		requireErrorCode(t, client.Get(context.Background(), []string{"clusters"}, nil, &out), "timeout")
	})
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var out any
		requireErrorCode(t, client.Get(ctx, []string{"clusters"}, nil, &out), "canceled")
	})
}

func TestCancellationInterruptsResponseBodyRead(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"items":[`)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := mockClient(t, server)
	result := make(chan error, 1)
	go func() {
		var out any
		result <- client.Get(ctx, []string{"clusters"}, nil, &out)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("response body did not start")
	}
	cancel()
	select {
	case err := <-result:
		requireErrorCode(t, err, "canceled")
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not interrupt body reading")
	}
}

func TestConcurrencyLimitAndCanceledQueue(t *testing.T) {
	started := make(chan struct{}, MaxConcurrentRequests+1)
	release := make(chan struct{})
	var active, peak atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, `{}`)
	}))
	defer server.Close()
	client := mockClient(t, server)
	var wg sync.WaitGroup
	errCh := make(chan error, MaxConcurrentRequests)
	for range MaxConcurrentRequests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out any
			errCh <- client.Get(context.Background(), []string{"clusters"}, nil, &out)
		}()
	}
	for range MaxConcurrentRequests {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("requests did not start")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var out any
	requireErrorCode(t, client.Get(ctx, []string{"clusters"}, nil, &out), "timeout")
	select {
	case <-started:
		t.Error("queued fifth request reached backend")
	default:
	}
	close(release)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}
	if peak.Load() != MaxConcurrentRequests {
		t.Fatalf("maximum concurrency=%d", peak.Load())
	}
}

func TestCustomCAValidatesTLSAndUntrustedCertificateFails(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{}`) }))
	defer server.Close()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{BaseURL: base, Token: testToken, Timeout: time.Second}
	untrusted, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.CloseIdleConnections()
	var out any
	requireErrorCode(t, untrusted.Get(context.Background(), []string{"clusters"}, nil, &out), "backend_unavailable")
	if _, err := x509.ParseCertificate(server.Certificate().Raw); err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.CAFile = caPath
	trusted, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer trusted.CloseIdleConnections()
	if err := trusted.Get(context.Background(), []string{"clusters"}, nil, &out); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, []byte("backend-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(cfg); err == nil || strings.Contains(err.Error(), "backend-secret") {
		t.Fatal("invalid CA was accepted or exposed")
	}
	cfg.CAFile = filepath.Join(t.TempDir(), "missing-backend-secret.pem")
	if _, err := NewClient(cfg); err == nil || strings.Contains(err.Error(), "backend-secret") {
		t.Fatal("missing CA was accepted or exposed")
	}
}

func TestInvalidSegmentsAreRejectedAndRedactionIsAvailable(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, `{}`) }))
	defer server.Close()
	client := mockClient(t, server)
	for _, segments := range [][]string{nil, {""}, {"."}, {".."}, {"clusters", "x\n"}} {
		var out any
		requireErrorCode(t, client.Get(context.Background(), segments, nil, &out), "invalid_request")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid path reached backend")
	}
	if client.Redact("prefix "+testToken+" suffix") != "prefix [REDACTED] suffix" {
		t.Fatal("token redaction failed")
	}
}

func TestNewClientCannotBypassConfigurationValidation(t *testing.T) {
	base, err := url.Parse("http://backend.example")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(config.Config{BaseURL: base, Token: testToken, Timeout: time.Second, AllowHTTP: true}); err == nil {
		t.Fatal("direct configuration allowed non-loopback HTTP")
	}
	if _, err := NewClient(config.Config{}); err == nil {
		t.Fatal("direct empty configuration was accepted")
	}
}
