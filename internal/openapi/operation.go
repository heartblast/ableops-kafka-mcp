// Package openapi는 AbleOps Backend의 `/openapi.json` 계약을 읽어 내부 모델로 바꾼다.
//
// 범용 OpenAPI 엔진이 아니다. 현재 AbleOps 계약을 안전하게 처리하는 데 필요한 범위만
// 해석하며, 그 밖의 구성은 추측하지 않고 Operation 단위로 제외(Issue)한다.
// 응답 스키마는 계약 문서일 뿐 런타임 검증기가 아니므로 해석하지 않는다.
package openapi

import "strings"

// Contract는 검증을 통과한 계약 문서다. 생성 후 변경하지 않으므로 여러 요청이 공유해도 된다.
type Contract struct {
	// OpenAPI는 문서의 `openapi` 버전 문자열이다.
	OpenAPI string
	// Title과 Version은 `info` 값이다. Version은 포털 빌드 버전이며 계약 버전이 아니다.
	Title   string
	Version string
	// ETag는 이 문서를 받은 응답의 ETag다. 없으면 다음 조회는 조건부 요청을 보내지 않는다.
	ETag string
	// Discovered는 paths에서 발견한 (경로, 메서드) 수다. 제외된 Operation도 센다.
	Discovered int
	// MCPDeclared는 operationId 형식이 올바르고 x-mcp-enabled가 JSON true인 Operation 수다.
	// 이후 파라미터·응답·중복 검증에서 제외된 것도 센다.
	MCPDeclared int
	// Operations는 구조 검증을 통과한 Operation이다(operationId 순).
	// x-mcp-enabled=true인 Operation만 파라미터·응답까지 검증하고 나머지는 식별 정보만 담는다.
	Operations []Operation
	// Issues는 제외했거나 주의가 필요한 Operation의 사유다. 계약 전체를 거부하는 오류는 Parse가 반환한다.
	Issues []Issue
}

// MCPOperations는 x-mcp-enabled가 명시적으로 true인 Operation만 돌려준다.
func (c *Contract) MCPOperations() []Operation {
	var out []Operation
	for _, op := range c.Operations {
		if op.MCPEnabled {
			out = append(out, op)
		}
	}
	return out
}

// Operation은 OpenAPI Operation 하나의 내부 표현이다.
type Operation struct {
	ID          string
	Method      string // 대문자 HTTP 메서드
	Path        string // OpenAPI 경로 템플릿(예: /api/clusters/{id}/topics)
	Summary     string
	Description string
	Tags        []string
	Deprecated  bool
	// MCPEnabled는 x-mcp-enabled가 JSON true일 때만 참이다. 누락·오형식은 거짓이다.
	MCPEnabled bool
	MCPNote    string
	// Parameters는 경로 항목과 Operation의 파라미터를 합친 결과다(MCP 후보만 채운다).
	Parameters []Parameter
	// RequestBody는 선언된 경우에만 값이 있다.
	RequestBody *RequestBody
	// Responses는 상태코드("200", "204", "default" 등)별 응답이다.
	Responses map[string]Response
}

// Parameter는 경로·쿼리 파라미터다. 헤더·쿠키 파라미터는 지원하지 않는다.
type Parameter struct {
	Name        string
	In          string // "path" 또는 "query"
	Description string
	Required    bool
	// Explode는 쿼리 배열을 `a=1&a=2`(true)로 보낼지 `a=1,2`(false)로 보낼지다.
	Explode bool
	// Schema는 $ref를 풀고 지원 키워드만 남긴 JSON Schema 조각이다.
	// 3.0의 nullable은 3.1 형식(type 배열에 "null")으로 바꿔 둔다.
	Schema map[string]any
}

// RequestBody는 요청 본문 선언의 존재와 미디어 타입만 기록한다.
type RequestBody struct {
	Required   bool
	MediaTypes []string
}

// Response는 응답 선언의 설명과 미디어 타입만 기록한다. 응답 스키마는 해석하지 않는다.
type Response struct {
	Description string
	MediaTypes  []string
}

// AllowsNoContent는 계약이 HTTP 204를 성공 응답으로 선언했는지다.
func (o Operation) AllowsNoContent() bool {
	_, ok := o.Responses["204"]
	return ok
}

// HasJSONSuccess는 200 응답에 JSON 미디어 타입이 선언되었는지다.
func (o Operation) HasJSONSuccess() bool {
	for _, media := range o.Responses["200"].MediaTypes {
		if isJSONMediaType(media) {
			return true
		}
	}
	return false
}

// PathSegments는 `/api/` 뒤의 경로 조각이다. `{name}` 조각은 파라미터 자리다.
func (o Operation) PathSegments() []string {
	return strings.Split(strings.TrimPrefix(o.Path, apiPrefix), "/")
}

// Issue는 Operation을 제외하거나 주의가 필요한 사유다. 값은 upstream 문서에서 온 식별자와
// 이 패키지가 정한 고정 설명만 담으며 문서 본문을 인용하지 않는다.
type Issue struct {
	OperationID string `json:"operation_id,omitempty"`
	Method      string `json:"method,omitempty"`
	Path        string `json:"path,omitempty"`
	Code        string `json:"code"`
	Message     string `json:"message"`
}

func isJSONMediaType(media string) bool {
	media = strings.ToLower(strings.TrimSpace(strings.SplitN(media, ";", 2)[0]))
	return media == "application/json" || strings.HasSuffix(media, "+json")
}
