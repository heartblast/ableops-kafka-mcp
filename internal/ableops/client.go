// Package ableops는 REST 전송과 확인된 백엔드 API 계약을 구현한다.
package ableops

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

const (
	// MaxResponseBytes는 JSON 해석 전에 압축 해제된 응답 본문 크기를 제한한다.
	MaxResponseBytes      = 2 << 20
	MaxConcurrentRequests = 4
)

// Client는 동시 사용이 가능하며 애플리케이션 재시도 없이 GET 요청만 전송한다.
type Client struct {
	baseURL            url.URL
	token              string
	requestCredentials bool
	timeout            time.Duration
	httpClient         *http.Client
	semaphore          chan struct{}
}

// NewClient는 설정을 검증하고 TLS 인증서를 검증하는 클라이언트를 생성한다.
func NewClient(cfg config.Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, errors.New("ABLEOPS_CA_FILE could not be read")
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("ABLEOPS_CA_FILE does not contain a valid PEM certificate")
		}
		tlsConfig.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	transport.MaxConnsPerHost = MaxConcurrentRequests
	transport.MaxIdleConnsPerHost = MaxConcurrentRequests
	transport.ResponseHeaderTimeout = cfg.Timeout
	transport.MaxResponseHeaderBytes = 64 << 10
	baseURL := *cfg.BaseURL
	baseURL.Path = ""
	return &Client{
		baseURL:            baseURL,
		token:              cfg.Token,
		requestCredentials: cfg.RequireRequestCredentials,
		timeout:            cfg.Timeout,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   cfg.Timeout,
			// 같은 출처도 포함해 모든 리다이렉트를 차단하고 토큰 전달을 방지한다.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		},
		semaphore: make(chan struct{}, MaxConcurrentRequests),
	}, nil
}

// CloseIdleConnections는 프로세스 종료 시 유휴 연결을 해제한다.
func (c *Client) CloseIdleConnections() { c.httpClient.CloseIdleConnections() }

// Redact는 외부 문자열의 세션 토큰을 가린다. 호출자가 입력한 식별자에도
// 로그 기록이나 JSON 인코딩 전에 적용한다.
func (c *Client) Redact(value string) string {
	if c.token == "" {
		return value
	}
	return strings.ReplaceAll(value, c.token, "[REDACTED]")
}

// RedactContext는 현재 요청의 두 토큰을 함께 가리며 공유 클라이언트를 변경하지 않는다.
func (c *Client) RedactContext(ctx context.Context, value string) string {
	value = c.Redact(value)
	if p, ok := requestctx.FromContext(ctx); ok {
		for _, token := range []string{p.BackendToken, p.MCPToken} {
			if token != "" {
				value = strings.ReplaceAll(value, token, "[REDACTED]")
			}
		}
	}
	return value
}

// Get은 각 경로 조각을 개별 인코딩해 설정된 출처의 /api 아래에 추가한다.
// 내부 전송 보조 함수이며 범용 MCP 도구로 노출하지 않는다.
// 응답 크기, JSON 문법, 토큰 검사를 통과한 뒤에만 목적지에 값을 기록한다.
func (c *Client) Get(ctx context.Context, segments []string, query url.Values, out any) error {
	return c.get(ctx, segments, query, out, false)
}

var errNoContent = errors.New("선택 관계가 없습니다")

// GetOptional은 계약에 명시된 HTTP 204를 빈 관계로 구분한다.
// 일반 Get은 204를 유효한 상세 응답으로 인정하지 않는다.
func (c *Client) GetOptional(ctx context.Context, segments []string, query url.Values, out any) (bool, error) {
	err := c.get(ctx, segments, query, out, true)
	if errors.Is(err, errNoContent) {
		return false, nil
	}
	return err == nil, err
}

func (c *Client) get(ctx context.Context, segments []string, query url.Values, out any, optional bool) error {
	if err := reserveOperationRequest(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	token := c.token
	p, authenticated := requestctx.FromContext(ctx)
	if authenticated {
		token = p.BackendToken
	}
	if (c.requestCredentials && !authenticated) || token == "" {
		return publicError("authentication_required", 0)
	}
	for _, ch := range token {
		if ch < 0x21 || ch > 0x7e {
			return publicError("authentication_required", 0)
		}
	}
	if len(segments) == 0 || out == nil {
		return publicError("invalid_request", 0)
	}
	escaped := make([]string, len(segments))
	for i, segment := range segments {
		if segment == "" || segment == "." || segment == ".." || strings.ContainsAny(segment, "\x00\r\n") {
			return publicError("invalid_request", 0)
		}
		escaped[i] = url.PathEscape(segment)
	}
	requestURL := c.baseURL
	requestURL.RawPath = "/api/" + strings.Join(escaped, "/")
	requestURL.Path, _ = url.PathUnescape(requestURL.RawPath)
	requestURL.RawQuery = query.Encode()

	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return requestError(ctx, ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return requestError(ctx, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return publicError("invalid_request", 0)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if p.RequestID != "" {
		req.Header.Set("X-Request-ID", p.RequestID)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return requestError(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return statusError(resp.StatusCode)
	}
	if optional && resp.StatusCode == http.StatusNoContent {
		return errNoContent
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		return requestError(ctx, err)
	}
	if len(body) > MaxResponseBytes {
		return publicError("response_too_large", resp.StatusCode)
	}
	// 큰 정수의 정밀도를 보존하며 해석한 키와 값을 검사한다.
	// JSON 이스케이프를 이용한 토큰 노출도 차단한다.
	var inspected any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&inspected); err != nil {
		return publicError("invalid_response", resp.StatusCode)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return publicError("invalid_response", resp.StatusCode)
	}
	if inspected == nil || bytes.Contains(body, []byte(token)) || containsToken(inspected, token) ||
		(p.MCPToken != "" && (bytes.Contains(body, []byte(p.MCPToken)) || containsToken(inspected, p.MCPToken))) {
		return publicError("invalid_response", resp.StatusCode)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return publicError("invalid_response", resp.StatusCode)
	}
	return nil
}

func containsToken(value any, token string) bool {
	switch value := value.(type) {
	case string:
		return strings.Contains(value, token)
	case []any:
		for _, child := range value {
			if containsToken(child, token) {
				return true
			}
		}
	case map[string]any:
		for key, child := range value {
			if strings.Contains(key, token) || containsToken(child, token) {
				return true
			}
		}
	}
	return false
}

func requestError(ctx context.Context, err error) *Error {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return publicError("canceled", 0)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return publicError("timeout", 0)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return publicError("timeout", 0)
	}
	return publicError("backend_unavailable", 0)
}
