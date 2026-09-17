package openapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// apiPrefix 아래의 경로만 기존 REST Client의 `/api/` 전송 경계로 옮길 수 있다.
const apiPrefix = "/api/"

// Issue 코드. 로그·테스트가 문자열이 아니라 이 상수로 분기한다.
const (
	IssueInvalidOperation       = "invalid_operation"
	IssueMissingOperationID     = "missing_operation_id"
	IssueInvalidOperationID     = "invalid_operation_id"
	IssueDuplicateOperationID   = "duplicate_operation_id"
	IssueMissingMCPEnabled      = "missing_x_mcp_enabled"
	IssueMalformedMCPEnabled    = "malformed_x_mcp_enabled"
	IssueUnsupportedPathItem    = "unsupported_path_item"
	IssueUnsupportedPath        = "unsupported_path"
	IssueInvalidPathParameter   = "invalid_path_parameter"
	IssueUnsupportedParameter   = "unsupported_parameter"
	IssueUnsupportedSchema      = "unsupported_parameter_schema"
	IssueDuplicateParameter     = "duplicate_parameter"
	IssueUnsupportedServers     = "unsupported_servers_override"
	IssueUnsupportedResponse    = "unsupported_response"
	IssueUnsupportedRequestBody = "unsupported_request_body"
)

var (
	operationIDPattern    = regexp.MustCompile(`^[a-z][A-Za-z0-9]{0,127}$`)
	parameterNamePattern  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,63}$`)
	literalSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)
)

var httpMethods = []string{"get", "put", "post", "delete", "options", "head", "patch", "trace"}

// ContractError는 계약 문서 전체를 받을 수 없다는 오류다. 호출자는 기존 정상 계약을 유지한다.
type ContractError struct {
	Code    string
	Message string
}

func (e *ContractError) Error() string { return e.Code + ": " + e.Message }

func contractError(code, message string) error { return &ContractError{Code: code, Message: message} }

// issueError는 Operation 하나를 제외하는 사유다.
type issueError struct {
	code    string
	message string
}

func (e *issueError) Error() string { return e.message }

func newIssue(code, message string) error { return &issueError{code: code, message: message} }

// Parse는 `/openapi.json` 본문을 검증해 내부 계약으로 바꾼다.
// 문서 수준 결함은 오류로, Operation 수준 결함은 해당 Operation만 제외한 Issue로 돌려준다.
func Parse(body []byte) (*Contract, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, contractError("invalid_json", "OpenAPI 문서가 올바른 JSON이 아닙니다.")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, contractError("invalid_json", "OpenAPI 문서 뒤에 추가 JSON 값이 있습니다.")
	}
	root, ok := document.(map[string]any)
	if !ok {
		return nil, contractError("invalid_document", "OpenAPI 문서의 최상위 값이 객체가 아닙니다.")
	}
	version, _ := root["openapi"].(string)
	legacy := strings.HasPrefix(version, "3.0.")
	if !legacy && !strings.HasPrefix(version, "3.1.") {
		return nil, contractError("unsupported_version", "OpenAPI 3.0.x 또는 3.1.x 문서만 지원합니다.")
	}
	if err := checkServers(root["servers"]); err != nil {
		return nil, err
	}
	paths, ok := root["paths"].(map[string]any)
	if !ok || len(paths) == 0 {
		return nil, contractError("missing_paths", "OpenAPI 문서에 paths가 없습니다. 빈 계약으로 기존 도구를 대체하지 않습니다.")
	}
	components, _ := root["components"].(map[string]any)
	info, _ := root["info"].(map[string]any)
	p := &parser{
		contract: &Contract{OpenAPI: version, Title: text(info["title"]), Version: text(info["version"])},
		resolver: &resolver{components: components},
		legacy:   legacy,
	}
	for _, path := range sortedKeys(paths) {
		p.pathItem(path, paths[path])
	}
	p.dropDuplicates()
	if len(p.contract.Operations) == 0 {
		return nil, contractError("no_operations", "OpenAPI 문서에 사용할 수 있는 Operation이 없습니다.")
	}
	sort.SliceStable(p.contract.Operations, func(i, j int) bool {
		return p.contract.Operations[i].ID < p.contract.Operations[j].ID
	})
	return p.contract, nil
}

// checkServers는 servers[0].url이 상대 경로 "/"인지 확인한다. 다른 호스트나 접두 경로를
// 따르면 사용자 세션 토큰이 설정 외 대상으로 갈 수 있으므로 계약 전체를 거부한다.
func checkServers(raw any) error {
	if raw == nil {
		return nil
	}
	servers, ok := raw.([]any)
	if !ok {
		return contractError("unsupported_servers", "servers 형식이 올바르지 않습니다.")
	}
	if len(servers) == 0 {
		return nil
	}
	first, _ := servers[0].(map[string]any)
	if url, _ := first["url"].(string); url != "/" {
		return contractError("unsupported_servers", `servers[0].url은 상대 경로 "/"여야 합니다. 설정한 ABLEOPS_BASE_URL 외의 대상으로 호출하지 않습니다.`)
	}
	return nil
}

type parser struct {
	contract *Contract
	resolver *resolver
	legacy   bool
}

func (p *parser) issue(base Issue, err error) {
	var ie *issueError
	if errors.As(err, &ie) {
		base.Code, base.Message = ie.code, ie.message
	} else {
		base.Code, base.Message = IssueInvalidOperation, err.Error()
	}
	p.contract.Issues = append(p.contract.Issues, base)
}

func (p *parser) pathItem(path string, raw any) {
	base := Issue{Path: safeText(path)}
	item, ok := raw.(map[string]any)
	if !ok {
		p.issue(base, newIssue(IssueUnsupportedPathItem, "경로 항목이 객체가 아닙니다."))
		return
	}
	if _, has := item["$ref"]; has {
		p.issue(base, newIssue(IssueUnsupportedPathItem, "경로 항목의 $ref는 지원하지 않습니다."))
		return
	}
	for _, method := range httpMethods {
		if raw, has := item[method]; has {
			p.contract.Discovered++
			p.operation(path, strings.ToUpper(method), item, raw)
		}
	}
}

func (p *parser) operation(path, method string, item map[string]any, raw any) {
	base := Issue{Method: method, Path: safeText(path)}
	fields, ok := raw.(map[string]any)
	if !ok {
		p.issue(base, newIssue(IssueInvalidOperation, "Operation이 객체가 아닙니다."))
		return
	}
	id, _ := fields["operationId"].(string)
	if strings.TrimSpace(id) == "" {
		p.issue(base, newIssue(IssueMissingOperationID, "operationId가 없습니다."))
		return
	}
	base.OperationID = safeText(id)
	if !operationIDPattern.MatchString(id) {
		p.issue(base, newIssue(IssueInvalidOperationID, "operationId는 lowerCamelCase 영숫자여야 합니다."))
		return
	}
	op := Operation{
		ID:          id,
		Method:      method,
		Path:        path,
		Summary:     text(fields["summary"]),
		Description: text(fields["description"]),
		Tags:        texts(fields["tags"]),
		Deprecated:  fields["deprecated"] == true,
		MCPNote:     text(fields["x-mcp-note"]),
	}
	// 노출은 JSON true일 때만 허용한다. 누락·문자열 "true" 등은 노출하지 않고 기록한다.
	switch flag, has := fields["x-mcp-enabled"]; value := flag.(type) {
	case bool:
		op.MCPEnabled = value
	default:
		if has {
			p.issue(base, newIssue(IssueMalformedMCPEnabled, "x-mcp-enabled가 불리언이 아니어서 노출하지 않습니다."))
		} else {
			p.issue(base, newIssue(IssueMissingMCPEnabled, "x-mcp-enabled가 없어서 노출하지 않습니다."))
		}
	}
	if op.MCPEnabled {
		p.contract.MCPDeclared++
		if err := p.validateCandidate(&op, item, fields); err != nil {
			p.issue(base, err)
			return
		}
	}
	p.contract.Operations = append(p.contract.Operations, op)
}

// validateCandidate는 MCP 노출 후보의 경로·파라미터·본문·응답 선언을 검증한다.
func (p *parser) validateCandidate(op *Operation, item, fields map[string]any) error {
	if _, has := fields["servers"]; has {
		return newIssue(IssueUnsupportedServers, "Operation 단위 servers 재정의는 지원하지 않습니다.")
	}
	if _, has := item["servers"]; has {
		return newIssue(IssueUnsupportedServers, "경로 단위 servers 재정의는 지원하지 않습니다.")
	}
	vars, err := templateVars(op.Path)
	if err != nil {
		return err
	}
	params, err := p.parameters(item["parameters"], fields["parameters"])
	if err != nil {
		return err
	}
	declared := map[string]bool{}
	for _, param := range params {
		if param.In == "path" {
			declared[param.Name] = true
		}
	}
	for _, name := range vars {
		if !declared[name] {
			return newIssue(IssueInvalidPathParameter, "경로 템플릿 변수에 대응하는 path 파라미터 선언이 없습니다.")
		}
		delete(declared, name)
	}
	if len(declared) > 0 {
		return newIssue(IssueInvalidPathParameter, "경로 템플릿에 없는 path 파라미터가 선언되었습니다.")
	}
	op.Parameters = params
	if raw, has := fields["requestBody"]; has {
		body, err := p.requestBody(raw)
		if err != nil {
			return err
		}
		op.RequestBody = body
	}
	op.Responses, err = p.responses(fields["responses"])
	return err
}

// templateVars는 `/api/` 아래의 경로 템플릿을 검사하고 변수 이름을 순서대로 돌려준다.
// 조각 전체가 `{name}`인 경우만 변수로 인정한다(`v{n}` 같은 부분 변수는 지원하지 않는다).
func templateVars(path string) ([]string, error) {
	if !strings.HasPrefix(path, apiPrefix) || len(path) == len(apiPrefix) {
		return nil, newIssue(IssueUnsupportedPath, "/api/ 아래 경로만 지원합니다.")
	}
	var vars []string
	seen := map[string]bool{}
	for _, segment := range strings.Split(path[len(apiPrefix):], "/") {
		if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			name := segment[1 : len(segment)-1]
			if !parameterNamePattern.MatchString(name) || seen[name] {
				return nil, newIssue(IssueInvalidPathParameter, "경로 템플릿 변수 이름이 올바르지 않거나 중복되었습니다.")
			}
			seen[name] = true
			vars = append(vars, name)
			continue
		}
		if strings.ContainsAny(segment, "{}") {
			return nil, newIssue(IssueInvalidPathParameter, "경로 조각의 일부만 변수인 템플릿은 지원하지 않습니다.")
		}
		if segment == "." || segment == ".." || !literalSegmentPattern.MatchString(segment) {
			return nil, newIssue(IssueUnsupportedPath, "경로 조각에 지원하지 않는 문자가 있습니다.")
		}
	}
	return vars, nil
}

// parameters는 경로 항목과 Operation의 파라미터를 합친다. 같은 (in, name)은 Operation 쪽이 이긴다.
func (p *parser) parameters(pathLevel, operationLevel any) ([]Parameter, error) {
	var merged []Parameter
	index := map[string]int{}
	for _, raw := range []any{pathLevel, operationLevel} {
		if raw == nil {
			continue
		}
		list, ok := raw.([]any)
		if !ok {
			return nil, newIssue(IssueUnsupportedParameter, "parameters가 배열이 아닙니다.")
		}
		seen := map[string]bool{}
		for _, item := range list {
			param, err := p.parameter(item)
			if err != nil {
				return nil, err
			}
			key := param.In + ":" + param.Name
			if seen[key] {
				return nil, newIssue(IssueDuplicateParameter, "같은 위치에 같은 이름의 파라미터가 중복 선언되었습니다.")
			}
			seen[key] = true
			if i, ok := index[key]; ok {
				merged[i] = param
				continue
			}
			index[key] = len(merged)
			merged = append(merged, param)
		}
	}
	// MCP 인자는 이름 하나로 전달하므로 path와 query의 동명 파라미터는 구분할 수 없다.
	location := map[string]string{}
	for _, param := range merged {
		if in, ok := location[param.Name]; ok && in != param.In {
			return nil, newIssue(IssueUnsupportedParameter, "path와 query에 같은 이름의 파라미터가 있어 인자로 구분할 수 없습니다.")
		}
		location[param.Name] = param.In
	}
	return merged, nil
}

func (p *parser) parameter(raw any) (Parameter, error) {
	fields, ok := raw.(map[string]any)
	if !ok {
		return Parameter{}, newIssue(IssueUnsupportedParameter, "파라미터가 객체가 아닙니다.")
	}
	fields, err := p.resolver.deref(fields, "parameters")
	if err != nil {
		return Parameter{}, newIssue(IssueUnsupportedParameter, "파라미터 $ref: "+err.Error())
	}
	name, _ := fields["name"].(string)
	in, _ := fields["in"].(string)
	if !parameterNamePattern.MatchString(name) {
		return Parameter{}, newIssue(IssueUnsupportedParameter, "파라미터 이름 형식을 지원하지 않습니다.")
	}
	switch in {
	case "path", "query":
	case "header", "cookie":
		return Parameter{}, newIssue(IssueUnsupportedParameter, "header·cookie 파라미터는 지원하지 않습니다.")
	default:
		return Parameter{}, newIssue(IssueUnsupportedParameter, "파라미터 위치(in)가 올바르지 않습니다.")
	}
	param := Parameter{Name: name, In: in, Description: text(fields["description"]), Explode: in == "query"}
	if raw, has := fields["required"]; has {
		if param.Required, ok = raw.(bool); !ok {
			return Parameter{}, newIssue(IssueUnsupportedParameter, "required는 불리언이어야 합니다.")
		}
	}
	if in == "path" && !param.Required {
		return Parameter{}, newIssue(IssueInvalidPathParameter, "path 파라미터는 required=true여야 합니다.")
	}
	if _, has := fields["content"]; has {
		return Parameter{}, newIssue(IssueUnsupportedParameter, "content로 선언한 파라미터는 지원하지 않습니다.")
	}
	style, _ := fields["style"].(string)
	if _, has := fields["style"]; has && !((in == "path" && style == "simple") || (in == "query" && style == "form")) {
		return Parameter{}, newIssue(IssueUnsupportedParameter, "path는 simple, query는 form 스타일만 지원합니다.")
	}
	if raw, has := fields["explode"]; has {
		if param.Explode, ok = raw.(bool); !ok {
			return Parameter{}, newIssue(IssueUnsupportedParameter, "explode는 불리언이어야 합니다.")
		}
	}
	for _, key := range []string{"allowReserved", "allowEmptyValue"} {
		if raw, has := fields[key]; has && raw != false {
			return Parameter{}, newIssue(IssueUnsupportedParameter, key+" 파라미터 옵션은 지원하지 않습니다.")
		}
	}
	schemaRaw, has := fields["schema"]
	if !has {
		return Parameter{}, newIssue(IssueUnsupportedSchema, "파라미터 스키마가 없습니다.")
	}
	schema, err := p.resolver.paramSchema(schemaRaw, in == "query", p.legacy)
	if err != nil {
		return Parameter{}, newIssue(IssueUnsupportedSchema, "파라미터 스키마: "+err.Error())
	}
	if param.Description == "" {
		param.Description, _ = schema["description"].(string)
	}
	param.Schema = schema
	return param, nil
}

func (p *parser) requestBody(raw any) (*RequestBody, error) {
	fields, ok := raw.(map[string]any)
	if !ok {
		return nil, newIssue(IssueUnsupportedRequestBody, "requestBody가 객체가 아닙니다.")
	}
	fields, err := p.resolver.deref(fields, "requestBodies")
	if err != nil {
		return nil, newIssue(IssueUnsupportedRequestBody, "requestBody $ref: "+err.Error())
	}
	body := &RequestBody{Required: fields["required"] == true}
	if content, ok := fields["content"].(map[string]any); ok {
		body.MediaTypes = sortedKeys(content)
	}
	return body, nil
}

func (p *parser) responses(raw any) (map[string]Response, error) {
	fields, ok := raw.(map[string]any)
	if !ok || len(fields) == 0 {
		return nil, newIssue(IssueUnsupportedResponse, "responses 선언이 없습니다.")
	}
	out := make(map[string]Response, len(fields))
	for code, value := range fields {
		if strings.HasPrefix(code, "x-") {
			continue
		}
		item, ok := value.(map[string]any)
		if !ok {
			return nil, newIssue(IssueUnsupportedResponse, "응답 선언이 객체가 아닙니다.")
		}
		item, err := p.resolver.deref(item, "responses")
		if err != nil {
			return nil, newIssue(IssueUnsupportedResponse, "응답 $ref: "+err.Error())
		}
		response := Response{Description: text(item["description"])}
		if content, ok := item["content"].(map[string]any); ok {
			response.MediaTypes = sortedKeys(content)
		}
		out[code] = response
	}
	return out, nil
}

// dropDuplicates는 operationId가 겹치는 Operation을 모두 제외한다. 어느 쪽이 정본인지 추측하지 않는다.
func (p *parser) dropDuplicates() {
	count := map[string]int{}
	for _, op := range p.contract.Operations {
		count[op.ID]++
	}
	kept := p.contract.Operations[:0]
	for _, op := range p.contract.Operations {
		if count[op.ID] > 1 {
			p.issue(Issue{OperationID: op.ID, Method: op.Method, Path: safeText(op.Path)},
				newIssue(IssueDuplicateOperationID, "operationId가 중복되어 같은 ID의 Operation을 모두 제외했습니다."))
			continue
		}
		kept = append(kept, op)
	}
	p.contract.Operations = kept
}

func text(value any) string {
	s, _ := value.(string)
	return s
}

func texts(value any) []string {
	list, _ := value.([]any)
	var out []string
	for _, item := range list {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// safeText는 upstream 문서의 식별자를 로그·Issue에 싣기 전에 제어문자를 지우고 길이를 줄인다.
func safeText(value string) string {
	const limit = 128
	var b strings.Builder
	for _, r := range value {
		if r == utf8.RuneError || unicode.IsControl(r) {
			continue
		}
		if b.Len()+utf8.RuneLen(r) > limit {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}
