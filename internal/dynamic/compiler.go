package dynamic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 컴파일 제외 사유. openapi.Issue.Code로 기록한다.
const (
	IssueNotMCPEnabled     = "mcp_not_enabled"
	IssueWriteMethod       = "write_method_rejected"
	IssueUnsupportedMethod = "unsupported_method"
	IssueRequestBody       = "unsupported_request_body"
	IssueNonJSONResponse   = "unsupported_response"
	IssueToolName          = "invalid_tool_name"
	IssueDuplicateToolName = "duplicate_tool_name"
	IssueSensitiveParam    = "sensitive_parameter"
	IssueInputSchema       = "unsupported_parameter_schema"
)

const (
	maxToolDescriptionBytes  = 8 << 10
	maxParamDescriptionBytes = 2 << 10
)

// 인증 자료로 보이는 이름의 인자는 LLM이 값을 채우게 하지 않는다(토큰은 인자로 받지 않는다).
var sensitiveParamWords = []string{"token", "password", "passwd", "secret", "credential", "authorization", "apikey", "api_key", "api-key"}

// Tool은 OpenAPI Operation 하나에서 만든 MCP 도구다. 생성 후 변경하지 않는다.
type Tool struct {
	Name      string
	Operation openapi.Operation

	inputSchema *jsonschema.Schema
	description string
	// definitionKey는 tools/list로 클라이언트에 보이는 정의 전체(이름·설명·입출력 스키마·
	// 주석·operationId·메서드·경로)의 정규화 JSON이다. 달라지면 도구 변경으로 알린다.
	definitionKey string
	// bindingKey는 정의에는 드러나지 않지만 실행을 바꾸는 계약 값(파라미터 위치·explode·
	// 선언된 204)의 정규화 JSON이다. 정의가 같으면 알림 없이 실행 대상만 교체한다.
	bindingKey string
}

// CompileError는 Operation을 도구로 만들 수 없는 사유다.
type CompileError struct {
	Code    string
	Message string
}

func (e *CompileError) Error() string { return e.Code + ": " + e.Message }

func compileError(code, message string) error { return &CompileError{Code: code, Message: message} }

// Compile은 Operation을 조회 전용 MCP 도구로 만든다. 다음은 거부한다.
//   - x-mcp-enabled가 true가 아닌 Operation
//   - GET 이외의 메서드(쓰기 메서드 POST·PUT·PATCH·DELETE 포함)
//   - 요청 본문 선언, 200 JSON 응답 부재, 인증 자료로 보이는 인자
func Compile(op openapi.Operation) (*Tool, error) {
	if !op.MCPEnabled {
		return nil, compileError(IssueNotMCPEnabled, "x-mcp-enabled가 true가 아니어서 도구로 만들지 않습니다.")
	}
	switch op.Method {
	case http.MethodGet:
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return nil, compileError(IssueWriteMethod, "쓰기 메서드는 Dynamic 도구로 실행하지 않습니다.")
	default:
		return nil, compileError(IssueUnsupportedMethod, "GET Operation만 Dynamic 도구로 만듭니다.")
	}
	if op.RequestBody != nil {
		return nil, compileError(IssueRequestBody, "요청 본문을 선언한 GET은 지원하지 않습니다.")
	}
	if !op.HasJSONSuccess() {
		return nil, compileError(IssueNonJSONResponse, "200 JSON 응답이 선언되지 않아 결과를 전달할 수 없습니다.")
	}
	name, err := ToolName(op.ID)
	if err != nil {
		return nil, compileError(IssueToolName, err.Error())
	}
	schema, err := inputSchema(op)
	if err != nil {
		return nil, err
	}
	tool := &Tool{Name: name, Operation: op, inputSchema: schema, description: toolDescription(op)}
	if tool.definitionKey, err = canonicalJSON(tool.definition()); err != nil {
		return nil, compileError(IssueInputSchema, "도구 정의를 직렬화할 수 없습니다.")
	}
	if tool.bindingKey, err = canonicalJSON(map[string]any{
		"method":          op.Method,
		"path":            op.Path,
		"parameters":      op.Parameters,
		"allowsNoContent": op.AllowsNoContent(),
	}); err != nil {
		return nil, compileError(IssueInputSchema, "파라미터 바인딩을 직렬화할 수 없습니다.")
	}
	return tool, nil
}

// canonicalJSON은 값을 JSON으로 바꾼 뒤 한 번 더 해석·직렬화해 객체 키 순서를 고정한다.
// 숫자는 json.Number로 읽어 원문 표기를 유지한다.
func canonicalJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil {
		return "", err
	}
	out, err := json.Marshal(generic)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// inputSchema는 path·query 파라미터를 하나의 객체 스키마로 모은다. 선언에 없는 인자는 거부한다.
func inputSchema(op openapi.Operation) (*jsonschema.Schema, error) {
	properties := map[string]any{}
	required := []string{}
	for _, param := range op.Parameters {
		lower := strings.ToLower(param.Name)
		for _, word := range sensitiveParamWords {
			if strings.Contains(lower, word) {
				return nil, compileError(IssueSensitiveParam, "인증 자료로 보이는 이름의 파라미터가 있어 도구로 만들지 않습니다.")
			}
		}
		property, err := cloneSchema(param.Schema)
		if err != nil {
			return nil, compileError(IssueInputSchema, "파라미터 스키마를 복사할 수 없습니다.")
		}
		if param.Description != "" {
			property["description"] = limitText(param.Description, maxParamDescriptionBytes)
		}
		// 빈 path 조각은 다른 경로를 호출하게 되므로 전송 안전을 위해 1자 이상을 요구한다.
		if param.In == "path" && property["type"] == "string" {
			if current, ok := property["minLength"].(json.Number); !ok {
				property["minLength"] = 1
			} else if value, err := current.Float64(); err != nil || value < 1 {
				property["minLength"] = 1
			}
		}
		properties[param.Name] = property
		if param.Required {
			required = append(required, param.Name)
		}
	}
	raw, err := json.Marshal(map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	})
	if err != nil {
		return nil, compileError(IssueInputSchema, "입력 스키마를 직렬화할 수 없습니다.")
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, compileError(IssueInputSchema, "입력 스키마를 JSON Schema로 읽을 수 없습니다.")
	}
	// SDK AddTool과 같은 옵션으로 미리 해석한다. 실패하면 등록 시 panic 대신 이 Operation만 제외한다.
	if _, err := schema.Resolve(&jsonschema.ResolveOptions{ValidateDefaults: true}); err != nil {
		return nil, compileError(IssueInputSchema, "입력 스키마 검증에 실패했습니다(pattern·default 등).")
	}
	return &schema, nil
}

// toolDescription은 OpenAPI summary와 description만 조합한다. 업무 의미를 새로 만들지 않고,
// 출처와 결과 해석 주의만 고정 문구로 덧붙인다.
func toolDescription(op openapi.Operation) string {
	var parts []string
	for _, part := range []string{op.Summary, op.Description} {
		if part = strings.TrimSpace(part); part != "" {
			parts = append(parts, part)
		}
	}
	body := limitText(strings.Join(parts, "\n\n"), maxToolDescriptionBytes)
	footer := fmt.Sprintf("[OpenAPI %s · %s %s] 결과 body는 백엔드 JSON 원문입니다. HTTP 200은 조회 성공·정상 판정을 뜻하지 않습니다.", op.ID, op.Method, op.Path)
	if body == "" {
		return footer
	}
	return body + "\n\n" + footer
}

// definition은 등록할 때마다 새 mcp.Tool을 만든다. SDK는 등록 후 Tool 변경을 금지한다.
func (t *Tool) definition() *mcp.Tool {
	return &mcp.Tool{
		Name:         t.Name,
		Description:  t.description,
		InputSchema:  t.inputSchema,
		OutputSchema: resultSchema(),
		Annotations:  &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true},
		Meta: mcp.Meta{"ableops/openapi": map[string]any{
			"operationId": t.Operation.ID,
			"method":      t.Operation.Method,
			"path":        t.Operation.Path,
		}},
	}
}

// resultSchema는 Executor 결과 봉투의 스키마다. body는 OpenAPI 응답 스키마로 제약하지 않는다.
// 응답 스키마는 계약 문서이지 런타임 검증기가 아니므로(nil 슬라이스·열린 enum), 이를 출력 스키마로
// 선언하면 정상 응답이 클라이언트 검증에서 거부될 수 있다.
func resultSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"operation_id":  map[string]any{"type": "string", "description": "OpenAPI operationId"},
			"method":        map[string]any{"type": "string"},
			"path_template": map[string]any{"type": "string", "description": "OpenAPI 경로 템플릿이며 인자 값을 포함하지 않는다"},
			"http_status":   map[string]any{"type": "integer", "description": "백엔드 HTTP 상태. 전송 전 실패나 네트워크 오류이면 없다"},
			"queried_at":    map[string]any{"type": "string", "description": "MCP 조회 완료 시각이며 원본 데이터 관측 시각이 아니다"},
			"body":          map[string]any{"description": "백엔드 JSON 원문. 해석·보정하지 않으며 null도 그대로 전달한다"},
			"body_bytes":    map[string]any{"type": "integer", "description": "압축한 백엔드 JSON 크기(바이트)"},
			"no_content":    map[string]any{"type": "boolean", "description": "계약이 선언한 HTTP 204(본문 없음)"},
			"error": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"code":        map[string]any{"type": "string"},
					"message":     map[string]any{"type": "string"},
					"http_status": map[string]any{"type": "integer"},
				},
				"required":             []string{"code", "message"},
				"additionalProperties": false,
			},
			"notes": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		},
		"required":             []string{"operation_id", "method", "path_template", "queried_at", "notes"},
		"additionalProperties": false,
	}
}

func cloneSchema(schema map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// limitText는 UTF-8 경계에서 자르고 생략 표시를 붙인다.
func limitText(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	const marker = "\n…(설명이 길어 이하 생략)"
	cut := limit - len(marker)
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut] + marker
}
