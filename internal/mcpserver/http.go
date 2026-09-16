package mcpserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// HTTPOptions는 로컬 인증 모드의 HTTP 전송과 자원 제한을 설정한다.
type HTTPOptions struct {
	Address              string
	AllowedOrigins       []string
	Authenticate         func(context.Context, string) (requestctx.Principal, error)
	Logger               *slog.Logger
	MaxRequestBytes      int64
	MaxConcurrent        int
	MaxConcurrentPerUser int
	RequestTimeout       time.Duration
	ReadHeaderTimeout    time.Duration
	ReadTimeout          time.Duration
	IdleTimeout          time.Duration
	WriteIdleTimeout     time.Duration
	ShutdownTimeout      time.Duration
	MaxHeaderBytes       int
}

func (o HTTPOptions) normalized() (HTTPOptions, error) {
	if o.Address == "" {
		o.Address = "127.0.0.1:8081"
	}
	host, port, err := net.SplitHostPort(o.Address)
	ip := net.ParseIP(host)
	p, portErr := strconv.Atoi(port)
	if err != nil || ip == nil || !ip.IsLoopback() || portErr != nil || p < 0 || p > 65535 {
		return o, errors.New("로컬 HTTP 주소는 포트를 포함한 loopback IP여야 합니다")
	}
	if o.Authenticate == nil {
		return o, errors.New("HTTP 요청별 인증기가 필요합니다")
	}
	if o.MaxRequestBytes < 0 || o.MaxConcurrent < 0 || o.MaxConcurrentPerUser < 0 || o.RequestTimeout < 0 || o.ReadHeaderTimeout < 0 || o.ReadTimeout < 0 || o.IdleTimeout < 0 || o.WriteIdleTimeout < 0 || o.ShutdownTimeout < 0 || o.MaxHeaderBytes < 0 {
		return o, errors.New("HTTP 제한은 양수여야 합니다")
	}
	if o.MaxRequestBytes == 0 {
		o.MaxRequestBytes = 64 << 10
	}
	if o.MaxConcurrent == 0 {
		o.MaxConcurrent = 32
	}
	if o.MaxConcurrentPerUser == 0 {
		o.MaxConcurrentPerUser = 4
	}
	if o.RequestTimeout == 0 {
		o.RequestTimeout = 30 * time.Second
	}
	if o.ReadHeaderTimeout == 0 {
		o.ReadHeaderTimeout = 5 * time.Second
	}
	if o.ReadTimeout == 0 {
		o.ReadTimeout = 10 * time.Second
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = 60 * time.Second
	}
	if o.WriteIdleTimeout == 0 {
		o.WriteIdleTimeout = 10 * time.Second
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = 5 * time.Second
	}
	if o.MaxHeaderBytes == 0 {
		o.MaxHeaderBytes = 16 << 10
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	for _, origin := range o.AllowedOrigins {
		u, err := url.Parse(origin)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(origin, "*?#") || u.String() != origin {
			return o, errors.New("허용 Origin은 경로 없는 정확한 HTTP 또는 HTTPS 출처여야 합니다")
		}
	}
	return o, nil
}

type httpRequestContextKey struct{}

// NewHTTPHandler는 SDK의 무상태 전송에 요청별 인증과 제한을 적용한다.
// 동일 서버에는 초기화 시 한 번만 적용한다.
func NewHTTPHandler(server *mcp.Server, opts HTTPOptions) (http.Handler, error) {
	if server == nil {
		return nil, errors.New("MCP 서버가 필요합니다")
	}
	opts, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	origins := make(map[string]bool, len(opts.AllowedOrigins))
	for _, origin := range opts.AllowedOrigins {
		origins[origin] = true
	}
	// 구버전 프로토콜도 SDK가 보존하는 문맥 값을 통해 HTTP 취소를 전파한다.
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			original, ok := ctx.Value(httpRequestContextKey{}).(context.Context)
			if !ok {
				return next(ctx, method, req)
			}
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			stop := context.AfterFunc(original, cancel)
			defer stop()
			if original.Err() != nil {
				cancel()
			}
			if deadline, ok := original.Deadline(); ok {
				var cancelDeadline context.CancelFunc
				ctx, cancelDeadline = context.WithDeadline(ctx, deadline)
				defer cancelDeadline()
			}
			return next(ctx, method, req)
		}
	})
	transport := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{
		Stateless:                    true,
		MaxRequestBodyBytes:          opts.MaxRequestBytes,
		PropagateRequestCancellation: true,
		// SDK 오류에는 입력값이 포함될 수 있으므로 별도 안전한 감사 로그만 사용한다.
		Logger: slog.New(slog.DiscardHandler),
	})
	global := make(chan struct{}, opts.MaxConcurrent)
	var usersMu sync.Mutex
	users := make(map[string]int)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := rand.Text()
		w.Header().Set("X-Request-ID", requestID)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		safe := &safeHTTPWriter{ResponseWriter: w, writeIdleTimeout: opts.WriteIdleTimeout}
		w = safe
		code := "ok"
		var userID, clientID string
		defer func() {
			opts.Logger.Info("HTTP 요청 완료", "request_id", requestID, "user_id", userID, "client_id", clientID, "code", code, "http_status", safe.statusCode(), "duration_ms", time.Since(started).Milliseconds())
		}()
		fail := func(status int, failure string) { code = failure; http.Error(w, failure, status) }
		if !loopbackHost(r.Host) {
			fail(http.StatusForbidden, "invalid_host")
			return
		}
		origin := r.Header.Get("Origin")
		if len(r.Header.Values("Origin")) > 1 || (len(r.Header.Values("Origin")) > 0 && origin == "") || (origin != "" && !origins[origin]) {
			fail(http.StatusForbidden, "origin_denied")
			return
		}
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID, MCP-Protocol-Version")
			w.Header().Add("Vary", "Origin")
		}
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				w.Header().Set("Allow", "GET, HEAD")
				fail(http.StatusMethodNotAllowed, "method_not_allowed")
				return
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			if r.Method != http.MethodHead {
				_, _ = w.Write([]byte("ok\n"))
			}
			return
		}
		if r.URL.Path != "/mcp" {
			fail(http.StatusNotFound, "not_found")
			return
		}
		if r.Method == http.MethodOptions {
			if origin == "" || r.Header.Get("Access-Control-Request-Method") != http.MethodPost || !allowedPreflightHeaders(r.Header.Get("Access-Control-Request-Headers")) {
				fail(http.StatusForbidden, "preflight_denied")
				return
			}
			w.Header().Set("Access-Control-Allow-Methods", "POST")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, MCP-Protocol-Version, MCP-Session-Id, MCP-Method, MCP-Name")
			w.Header().Add("Vary", "Access-Control-Request-Method")
			w.Header().Add("Vary", "Access-Control-Request-Headers")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		select {
		case global <- struct{}{}:
			defer func() { <-global }()
		default:
			w.Header().Set("Retry-After", "1")
			fail(http.StatusTooManyRequests, "global_limit")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), opts.RequestTimeout)
		defer cancel()
		token, ok := bearerToken(r.Header)
		if !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			fail(http.StatusUnauthorized, "authentication_required")
			return
		}
		ctx = requestctx.WithPrincipal(ctx, requestctx.Principal{RequestID: requestID})
		principal, err := opts.Authenticate(ctx, token)
		if err != nil {
			status, authCode := authenticationFailure(err)
			if status == http.StatusUnauthorized {
				w.Header().Set("WWW-Authenticate", "Bearer")
			}
			fail(status, authCode)
			return
		}
		if principal.UserID == "" || principal.BackendToken == "" || principal.BackendToken == token {
			fail(http.StatusUnauthorized, "authentication_required")
			return
		}
		principal.MCPToken, principal.RequestID = token, requestID
		userID, clientID = principal.UserID, principal.ClientID
		for _, credential := range []string{token, principal.BackendToken} {
			userID = strings.ReplaceAll(userID, credential, "[REDACTED]")
			clientID = strings.ReplaceAll(clientID, credential, "[REDACTED]")
		}
		userKey := principal.UserID
		usersMu.Lock()
		if users[userKey] >= opts.MaxConcurrentPerUser {
			usersMu.Unlock()
			w.Header().Set("Retry-After", "1")
			fail(http.StatusTooManyRequests, "user_limit")
			return
		}
		users[userKey]++
		usersMu.Unlock()
		defer func() {
			usersMu.Lock()
			users[userKey]--
			if users[userKey] == 0 {
				delete(users, userKey)
			}
			usersMu.Unlock()
		}()
		// 호환 환경변수로 SDK의 세션 동작이 바뀌어도 기존 세션을 재사용하지 않는다.
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			fail(http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		body, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, opts.MaxRequestBytes))
		_ = r.Body.Close()
		if readErr != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(readErr, &tooLarge) {
				fail(http.StatusRequestEntityTooLarge, "request_too_large")
			} else {
				fail(http.StatusBadRequest, "invalid_request")
			}
			return
		}
		// JSON-RPC 식별자와 알 수 없는 메서드도 SDK 오류에 반사되기 전에 검사한다.
		var input any
		_ = json.Unmarshal(body, &input)
		if bytes.Contains(body, []byte(token)) || bytes.Contains(body, []byte(principal.BackendToken)) || inputHasCredential(input, token, principal.BackendToken) {
			fail(http.StatusBadRequest, "credential_in_payload")
			return
		}
		r = r.Clone(requestctx.WithPrincipal(ctx, principal))
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.Header.Del("Mcp-Session-Id")
		r.Header.Del("Authorization")
		r = r.WithContext(context.WithValue(r.Context(), httpRequestContextKey{}, r.Context()))
		transport.ServeHTTP(w, r)
		if ctx.Err() != nil {
			code = "canceled"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				code = "timeout"
			}
		} else if safe.statusCode() >= 400 {
			code = "protocol_error"
		}
	}), nil
}

func inputHasCredential(input any, credentials ...string) bool {
	switch value := input.(type) {
	case string:
		for _, credential := range credentials {
			if credential != "" && strings.Contains(value, credential) {
				return true
			}
		}
	case []any:
		for _, item := range value {
			if inputHasCredential(item, credentials...) {
				return true
			}
		}
	case map[string]any:
		for key, item := range value {
			if inputHasCredential(key, credentials...) || inputHasCredential(item, credentials...) {
				return true
			}
		}
	}
	return false
}

func loopbackHost(hostport string) bool {
	host := hostport
	if strings.Contains(hostport, ":") {
		var err error
		host, _, err = net.SplitHostPort(hostport)
		if err != nil {
			return false
		}
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func bearerToken(header http.Header) (string, bool) {
	values := header.Values("Authorization")
	if len(values) != 1 {
		return "", false
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) > 4096 {
		return "", false
	}
	return parts[1], true
}

func allowedPreflightHeaders(value string) bool {
	for _, name := range strings.Split(value, ",") {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "", "authorization", "content-type", "mcp-protocol-version", "mcp-session-id", "mcp-method", "mcp-name":
		default:
			return false
		}
	}
	return true
}

func authenticationFailure(err error) (int, string) {
	var coded interface{ AuthCode() string }
	if errors.As(err, &coded) {
		switch code := coded.AuthCode(); code {
		case "access_denied":
			return http.StatusForbidden, code
		case "backend_authentication_required":
			return http.StatusUnauthorized, code
		case "backend_unavailable", "authentication_unavailable":
			return http.StatusServiceUnavailable, code
		case "timeout":
			return http.StatusGatewayTimeout, code
		case "canceled":
			return http.StatusRequestTimeout, code
		}
	}
	return http.StatusUnauthorized, "authentication_required"
}

// safeHTTPWriter는 스트리밍을 유지하면서 SDK의 HTTP 오류 본문에서 입력 반사를 막는다.
type safeHTTPWriter struct {
	http.ResponseWriter
	status           int
	errorWritten     bool
	writeIdleTimeout time.Duration
}

func (w *safeHTTPWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *safeHTTPWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}
func (w *safeHTTPWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	// 로컬 모드는 항상 무상태이며 인증 수단과 혼동할 세션 ID를 반환하지 않는다.
	w.Header().Del("Mcp-Session-Id")
	if status >= 400 {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Del("Content-Length")
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *safeHTTPWriter) Write(p []byte) (int, error) {
	w.advanceWriteDeadline()
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= 400 {
		if w.errorWritten {
			return len(p), nil
		}
		w.errorWritten = true
		_, err := fmt.Fprintln(w.ResponseWriter, http.StatusText(w.status))
		return len(p), err
	}
	return w.ResponseWriter.Write(p)
}
func (w *safeHTTPWriter) Flush() {
	w.advanceWriteDeadline()
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *safeHTTPWriter) advanceWriteDeadline() {
	if w.writeIdleTimeout > 0 {
		// 각 전송 직전에 갱신하므로 활성 스트림의 전체 수명을 제한하지 않는다.
		_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.writeIdleTimeout))
	}
}

// RunHTTP는 loopback에서 수신하고 종료 시 진행 중 요청을 취소한 후 연결을 정리한다.
func RunHTTP(ctx context.Context, server *mcp.Server, opts HTTPOptions) error {
	opts, err := opts.normalized()
	if err != nil {
		return err
	}
	handler, err := NewHTTPHandler(server, opts)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", opts.Address)
	if err != nil {
		return errors.New("HTTP loopback 수신 주소를 열 수 없습니다")
	}
	return serveHTTP(ctx, listener, handler, opts)
}

func serveHTTP(ctx context.Context, listener net.Listener, handler http.Handler, opts HTTPOptions) error {
	requestCtx, stopRequests := context.WithCancel(ctx)
	defer stopRequests()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: opts.ReadHeaderTimeout, ReadTimeout: opts.ReadTimeout, IdleTimeout: opts.IdleTimeout, MaxHeaderBytes: opts.MaxHeaderBytes, ErrorLog: log.New(io.Discard, "", 0), BaseContext: func(net.Listener) context.Context { return requestCtx }}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	select {
	case err := <-stopped:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("HTTP 수신이 중단되었습니다")
	case <-ctx.Done():
		stopRequests()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			<-stopped
			return errors.New("HTTP 정상 종료 제한 시간을 초과했습니다")
		}
		<-stopped
		return ctx.Err()
	}
}
