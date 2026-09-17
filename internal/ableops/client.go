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

// Client는 동시 사용이 가능하며 자동 재시도 없이 조회와 허용된 미리보기만 전송한다.
type Client struct {
	baseURL            url.URL
	token              string
	requestCredentials bool
	timeout            time.Duration
	httpClient         *http.Client
	semaphore          chan struct{}
	sampleEnabled      bool
	sampleTopics       map[string]bool
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
	sampleTopics := make(map[string]bool, len(cfg.SampleTopics))
	for _, topic := range cfg.SampleTopics {
		sampleTopics[topic] = true
	}
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
		semaphore:     make(chan struct{}, MaxConcurrentRequests),
		sampleEnabled: cfg.SampleEnabled,
		sampleTopics:  sampleTopics,
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
	_, err := c.get(ctx, segments, query, out, getMode{})
	return err
}

var errNoContent = errors.New("선택 관계가 없습니다")

// GetOptional은 계약에 명시된 HTTP 204를 빈 관계로 구분한다.
// 일반 Get은 204를 유효한 상세 응답으로 인정하지 않는다.
func (c *Client) GetOptional(ctx context.Context, segments []string, query url.Values, out any) (bool, error) {
	_, err := c.get(ctx, segments, query, out, getMode{optional: true})
	if errors.Is(err, errNoContent) {
		return false, nil
	}
	return err == nil, err
}

// RawResponse는 백엔드 JSON을 DTO로 재구성하지 않고 전달하기 위한 응답이다.
// Body는 크기·문법·토큰 검사를 통과한 원문이며 204이면 nil이다.
type RawResponse struct {
	Status int
	Body   json.RawMessage
}

// GetRaw는 OpenAPI 기반 동적 도구의 전송 경로다. 인증·경로 인코딩·리다이렉트 차단·
// 크기 제한·토큰 검사는 Get과 같고, 최상위 JSON null을 계약상 유효한 값으로 보존한다.
// allowNoContent는 계약이 204를 선언한 경우에만 true로 둔다.
func (c *Client) GetRaw(ctx context.Context, segments []string, query url.Values, allowNoContent bool) (RawResponse, error) {
	var body json.RawMessage
	status, err := c.get(ctx, segments, query, &body, getMode{optional: allowNoContent, raw: true})
	if errors.Is(err, errNoContent) {
		return RawResponse{Status: status}, nil
	}
	if err != nil {
		return RawResponse{}, err
	}
	return RawResponse{Status: status, Body: body}, nil
}

type getMode struct {
	// optional은 HTTP 204를 errNoContent로 구분한다.
	optional bool
	// raw는 out(*json.RawMessage)에 검사한 원문을 그대로 기록하고 최상위 null을 허용한다.
	raw bool
	// allowNull은 DTO 경로에서 계약상 허용된 최상위 null을 받아들인다.
	allowNull bool
}

func (c *Client) get(ctx context.Context, segments []string, query url.Values, out any, mode getMode) (int, error) {
	return c.request(ctx, http.MethodGet, segments, query, nil, out, mode, MaxResponseBytes)
}

// request는 고정 API 메서드만 사용하는 내부 전송부다. 도구 인자로 URL·헤더를 받지 않는다.
func (c *Client) request(ctx context.Context, method string, segments []string, query url.Values, bodyBytes []byte, out any, mode getMode, maxBytes int64) (int, error) {
	if err := reserveOperationRequest(ctx); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	token := c.token
	p, authenticated := requestctx.FromContext(ctx)
	if authenticated {
		token = p.BackendToken
	}
	if (c.requestCredentials && !authenticated) || token == "" {
		return 0, publicError("authentication_required", 0)
	}
	for _, ch := range token {
		if ch < 0x21 || ch > 0x7e {
			return 0, publicError("authentication_required", 0)
		}
	}
	if len(segments) == 0 || out == nil {
		return 0, publicError("invalid_request", 0)
	}
	escaped := make([]string, len(segments))
	for i, segment := range segments {
		if segment == "" || segment == "." || segment == ".." || strings.ContainsAny(segment, "\x00\r\n") {
			return 0, publicError("invalid_request", 0)
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
		return 0, requestError(ctx, ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return 0, requestError(ctx, err)
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, publicError("invalid_request", 0)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if p.RequestID != "" {
		req.Header.Set("X-Request-ID", p.RequestID)
	}
	req.Header.Set("Accept", "application/json")
	if bodyBytes != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, requestError(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, statusError(resp.StatusCode)
	}
	if mode.optional && resp.StatusCode == http.StatusNoContent {
		return resp.StatusCode, errNoContent
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return resp.StatusCode, requestError(ctx, err)
	}
	if int64(len(body)) > maxBytes {
		return resp.StatusCode, publicError("response_too_large", resp.StatusCode)
	}
	// 큰 정수의 정밀도를 보존하며 해석한 키와 값을 검사한다.
	// JSON 이스케이프를 이용한 토큰 노출도 차단한다.
	var inspected any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&inspected); err != nil {
		return resp.StatusCode, publicError("invalid_response", resp.StatusCode)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return resp.StatusCode, publicError("invalid_response", resp.StatusCode)
	}
	// DTO 해석은 계약이 허용한 경우에만 최상위 null을 받는다. 원문 전달은 nullable 응답을 그대로 보존한다.
	if (inspected == nil && !mode.raw && !mode.allowNull) || bytes.Contains(body, []byte(token)) || containsToken(inspected, token) ||
		(p.MCPToken != "" && (bytes.Contains(body, []byte(p.MCPToken)) || containsToken(inspected, p.MCPToken))) {
		return resp.StatusCode, publicError("invalid_response", resp.StatusCode)
	}
	if raw, ok := out.(*json.RawMessage); ok && mode.raw {
		var compact bytes.Buffer
		if err := json.Compact(&compact, body); err != nil {
			return resp.StatusCode, publicError("invalid_response", resp.StatusCode)
		}
		*raw = compact.Bytes()
		return resp.StatusCode, nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return resp.StatusCode, publicError("invalid_response", resp.StatusCode)
	}
	return resp.StatusCode, nil
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
