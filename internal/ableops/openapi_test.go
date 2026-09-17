package ableops

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

const syntheticSpec = `{"openapi":"3.1.0","paths":{}}`

func TestFetchOpenAPIWithoutAuthorizationAndETag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/openapi.json" || r.URL.RawQuery != "" {
			t.Errorf("unexpected request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("계약 조회에 인증 헤더를 보냄")
		}
		if r.Header.Get("If-None-Match") != "" {
			t.Error("최초 조회에 조건부 헤더를 보냄")
		}
		w.Header().Set("ETag", `"abc123"`)
		io.WriteString(w, syntheticSpec)
	}))
	defer server.Close()
	doc, err := mockClient(t, server).FetchOpenAPI(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if doc.NotModified || doc.ETag != `"abc123"` || string(doc.Body) != syntheticSpec {
		t.Fatalf("doc=%+v", doc)
	}
}

func TestFetchOpenAPINotModified(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != `W/"v1"` {
			t.Errorf("If-None-Match=%q", r.Header.Get("If-None-Match"))
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()
	doc, err := mockClient(t, server).FetchOpenAPI(context.Background(), `W/"v1"`)
	if err != nil {
		t.Fatal(err)
	}
	if !doc.NotModified || doc.Body != nil || doc.ETag != `W/"v1"` {
		t.Fatalf("doc=%+v", doc)
	}
}

func TestFetchOpenAPIRejectsUnsafeResponses(t *testing.T) {
	cases := []struct {
		name    string
		etag    string
		handler http.HandlerFunc
		code    string
	}{
		{"조건 없는 304", "", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotModified) }, "invalid_response"},
		{"리다이렉트", "", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://elsewhere.example.test/openapi.json", http.StatusFound)
		}, "redirect_blocked"},
		{"서버 오류", "", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "backend-secret", 500) }, "backend_unavailable"},
		{"미인증", "", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "backend-secret", 401) }, "authentication_required"},
		{"204", "", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }, "invalid_response"},
		{"크기 초과", "", func(w http.ResponseWriter, _ *http.Request) {
			io.WriteString(w, strings.Repeat(" ", MaxOpenAPIBytes+1))
		}, "response_too_large"},
		{"토큰 포함", "", func(w http.ResponseWriter, _ *http.Request) {
			io.WriteString(w, `{"description":"`+testToken+`"}`)
		}, "invalid_response"},
		{"잘못된 ETag 요청", "no-quotes", func(w http.ResponseWriter, _ *http.Request) {
			t.Error("형식이 틀린 ETag로 요청을 보냄")
		}, "invalid_request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			_, err := mockClient(t, server).FetchOpenAPI(context.Background(), tc.etag)
			requireErrorCode(t, err, tc.code)
		})
	}
}

func TestFetchOpenAPIIgnoresMalformedETag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", "unquoted-value")
		io.WriteString(w, syntheticSpec)
	}))
	defer server.Close()
	doc, err := mockClient(t, server).FetchOpenAPI(context.Background(), "")
	if err != nil || doc.ETag != "" {
		t.Fatalf("doc=%+v err=%v", doc, err)
	}
}

func TestFetchOpenAPIWorksWithoutSharedTokenInHTTPMode(t *testing.T) {
	var auth atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		io.WriteString(w, syntheticSpec)
	}))
	defer server.Close()
	base, _ := url.Parse(server.URL)
	client, err := NewClient(config.Config{BaseURL: base, Timeout: time.Second, AllowHTTP: true, RequireRequestCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	if _, err := client.FetchOpenAPI(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if auth.Load() != "" {
		t.Fatal("HTTP 모드 계약 조회가 인증 헤더를 보냄")
	}
}

func TestFetchOpenAPITimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	_, err := mockClient(t, server).FetchOpenAPI(context.Background(), "")
	requireErrorCode(t, err, "timeout")
}

func TestValidETag(t *testing.T) {
	for value, want := range map[string]bool{
		`"abc"`:                              true,
		`W/"abc"`:                            true,
		`""`:                                 true,
		`abc`:                                false,
		`"a"b"`:                              false,
		`"한글"`:                               false,
		`W/abc`:                              false,
		`"` + strings.Repeat("a", 300) + `"`: false,
	} {
		if got := ValidETag(value); got != want {
			t.Errorf("ValidETag(%q)=%v", value, got)
		}
	}
}

func TestGetRawPreservesBodyAndNull(t *testing.T) {
	bodies := map[string]string{
		"/api/big":  `{"offset": 9007199254740993, "items": null, "status": "UNAVAILABLE"}`,
		"/api/null": "null\n",
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Error("Bearer 인증 누락")
		}
		io.WriteString(w, bodies[r.URL.Path])
	}))
	defer server.Close()
	client := mockClient(t, server)
	raw, err := client.GetRaw(context.Background(), []string{"big"}, nil, false)
	if err != nil || raw.Status != 200 || string(raw.Body) != `{"offset":9007199254740993,"items":null,"status":"UNAVAILABLE"}` {
		t.Fatalf("raw=%+v body=%s err=%v", raw, raw.Body, err)
	}
	raw, err = client.GetRaw(context.Background(), []string{"null"}, nil, false)
	if err != nil || string(raw.Body) != "null" {
		t.Fatalf("최상위 null 보존 실패: %s %v", raw.Body, err)
	}
	// DTO 경로는 기존대로 최상위 null을 거부한다.
	var out []any
	requireErrorCode(t, client.Get(context.Background(), []string{"null"}, nil, &out), "invalid_response")
}

func TestGetRawNoContentOnlyWhenDeclared(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := mockClient(t, server)
	raw, err := client.GetRaw(context.Background(), []string{"issue"}, nil, true)
	if err != nil || raw.Status != http.StatusNoContent || raw.Body != nil {
		t.Fatalf("raw=%+v err=%v", raw, err)
	}
	_, err = client.GetRaw(context.Background(), []string{"issue"}, nil, false)
	requireErrorCode(t, err, "invalid_response")
}

func TestGetRawRejectsTokensAndReportsStatus(t *testing.T) {
	const delegated = "synthetic-delegated-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/leak":
			io.WriteString(w, `{"note":"`+delegated+`"}`)
		case "/api/escaped":
			// JSON 이스케이프로 숨긴 토큰도 해석 후 검사한다.
			io.WriteString(w, `{"note":"synthetic\u002ddelegated-token"}`)
		default:
			http.Error(w, "backend-secret", http.StatusForbidden)
		}
	}))
	defer server.Close()
	client := mockClient(t, server)
	ctx := requestctx.WithPrincipal(context.Background(), requestctx.Principal{BackendToken: delegated})
	for _, path := range []string{"leak", "escaped"} {
		_, err := client.GetRaw(ctx, []string{path}, nil, false)
		requireErrorCode(t, err, "invalid_response")
	}
	_, err := client.GetRaw(ctx, []string{"denied"}, nil, false)
	if public := requireErrorCode(t, err, "access_denied"); public.HTTPStatus != http.StatusForbidden {
		t.Fatalf("status=%d", public.HTTPStatus)
	}
}
