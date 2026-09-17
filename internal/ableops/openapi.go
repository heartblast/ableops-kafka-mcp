package ableops

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

// MaxOpenAPIBytes는 계약 문서 본문의 상한이다. 업무 응답 상한(2 MiB)과 별개로 둔다.
// 2026-09 기준 upstream 문서는 약 340 KiB다.
const MaxOpenAPIBytes = 4 << 20

// maxETagBytes를 넘거나 출력 가능한 ASCII가 아닌 ETag는 보관·재전송하지 않는다.
const maxETagBytes = 256

// OpenAPIDocument는 `GET /openapi.json`의 전송 결과다. 본문 해석은 openapi 패키지가 담당한다.
type OpenAPIDocument struct {
	// Body는 HTTP 200의 원문이다. NotModified이면 nil이다.
	Body []byte
	// ETag는 응답의 ETag다. 형식이 올바르지 않으면 빈 값이다. NotModified이면 요청에 보낸 값이다.
	ETag string
	// NotModified는 If-None-Match에 대한 HTTP 304다.
	NotModified bool
}

// FetchOpenAPI는 Backend 계약 문서를 인증 없이 조회한다. 계약을 읽는 것과 업무 API를
// 호출하는 것은 별개이며, upstream은 이 경로를 미인증으로 보장한다.
// timeout·TLS/사설 CA·리다이렉트 차단·동시 호출 제한은 업무 GET과 같은 클라이언트를 쓴다.
// etag가 있으면 If-None-Match로 보내고 304를 NotModified로 돌려준다.
func (c *Client) FetchOpenAPI(ctx context.Context, etag string) (OpenAPIDocument, error) {
	if err := reserveOperationRequest(ctx); err != nil {
		return OpenAPIDocument{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if etag != "" && !ValidETag(etag) {
		return OpenAPIDocument{}, publicError("invalid_request", 0)
	}
	requestURL := c.baseURL
	requestURL.Path = "/openapi.json"
	requestURL.RawPath = ""
	requestURL.RawQuery = ""

	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return OpenAPIDocument{}, requestError(ctx, ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return OpenAPIDocument{}, requestError(ctx, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return OpenAPIDocument{}, publicError("invalid_request", 0)
	}
	// Authorization은 의도적으로 보내지 않는다. 문서 조회에 사용자 세션이 필요하지 않다.
	req.Header.Set("Accept", "application/json")
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	p, _ := requestctx.FromContext(ctx)
	if p.RequestID != "" {
		req.Header.Set("X-Request-ID", p.RequestID)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return OpenAPIDocument{}, requestError(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		// 조건부 요청을 보내지 않았는데 304가 오면 보관할 계약이 없으므로 오류다.
		if etag == "" {
			return OpenAPIDocument{}, publicError("invalid_response", resp.StatusCode)
		}
		return OpenAPIDocument{ETag: etag, NotModified: true}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return OpenAPIDocument{}, statusError(resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return OpenAPIDocument{}, publicError("invalid_response", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxOpenAPIBytes+1))
	if err != nil {
		return OpenAPIDocument{}, requestError(ctx, err)
	}
	if len(body) > MaxOpenAPIBytes {
		return OpenAPIDocument{}, publicError("response_too_large", resp.StatusCode)
	}
	// 계약 문서의 설명은 도구 설명으로 LLM에 전달되므로 세션 토큰이 섞이면 거부한다.
	for _, token := range []string{c.token, p.BackendToken, p.MCPToken} {
		if token != "" && bytes.Contains(body, []byte(token)) {
			return OpenAPIDocument{}, publicError("invalid_response", resp.StatusCode)
		}
	}
	doc := OpenAPIDocument{Body: body}
	if value := resp.Header.Get("ETag"); ValidETag(value) {
		doc.ETag = value
	}
	return doc, nil
}

// ValidETag는 If-None-Match로 되돌려 보내도 안전한 ETag 형식인지 확인한다.
// 강한 ETag(`"..."`)와 약한 ETag(`W/"..."`)만 허용한다.
func ValidETag(value string) bool {
	if len(value) < 2 || len(value) > maxETagBytes {
		return false
	}
	opaque := strings.TrimPrefix(value, "W/")
	if len(opaque) < 2 || opaque[0] != '"' || opaque[len(opaque)-1] != '"' {
		return false
	}
	for _, ch := range opaque[1 : len(opaque)-1] {
		// RFC 9110 etagc: %x21 / %x23-7E (obs-text 제외)
		if ch != 0x21 && (ch < 0x23 || ch > 0x7e) {
			return false
		}
	}
	return true
}
