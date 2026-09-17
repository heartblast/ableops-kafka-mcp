package dynamic

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
)

func fixtureContract(t *testing.T) *openapi.Contract {
	t.Helper()
	body, err := os.ReadFile("../openapi/testdata/ableops-openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	contract, err := openapi.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	return contract
}

// syntheticContract는 합성 경로 정의로 계약을 만든다.
func syntheticContract(t *testing.T, paths string) *openapi.Contract {
	t.Helper()
	contract, err := openapi.Parse([]byte(`{"openapi":"3.1.0","servers":[{"url":"/"}],"paths":` + paths + `,"components":{"schemas":{
		"Level":{"type":"string","enum":["LOW","HIGH"],"description":"수준"}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	return contract
}

const jsonOK = `"responses":{"200":{"description":"ok","content":{"application/json":{"schema":{"type":"object"}}}}}`

func findOperation(t *testing.T, c *openapi.Contract, id string) openapi.Operation {
	t.Helper()
	for _, op := range c.Operations {
		if op.ID == id {
			return op
		}
	}
	t.Fatalf("operation %s 없음", id)
	return openapi.Operation{}
}

func schemaMap(t *testing.T, tool *Tool) map[string]any {
	t.Helper()
	raw, err := json.Marshal(tool.inputSchema)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestCompileInputSchema(t *testing.T) {
	contract := syntheticContract(t, `{"/api/clusters/{id}/items/{name}": {"get": {
		"operationId":"getItems","summary":"항목 조회","description":"설명 본문","x-mcp-enabled":true,
		"parameters":[
			{"name":"id","in":"path","required":true,"description":"클러스터","schema":{"type":"string"}},
			{"name":"name","in":"path","required":true,"schema":{"type":"string","description":"스키마 설명","minLength":0}},
			{"name":"limit","in":"query","required":true,"schema":{"type":"integer","minimum":1,"maximum":200,"default":20}},
			{"name":"ratio","in":"query","schema":{"type":"number"}},
			{"name":"live","in":"query","schema":{"type":"boolean","default":false}},
			{"name":"owner","in":"query","schema":{"type":["string","null"]}},
			{"name":"level","in":"query","schema":{"$ref":"#/components/schemas/Level"}},
			{"name":"levels","in":"query","explode":false,"schema":{"type":"array","items":{"$ref":"#/components/schemas/Level"},"maxItems":3}}
		],`+jsonOK+`}}}`)
	tool, err := Compile(findOperation(t, contract, "getItems"))
	if err != nil {
		t.Fatal(err)
	}
	if tool.Name != "get_items" {
		t.Fatalf("name=%s", tool.Name)
	}
	schema := schemaMap(t, tool)
	if schema["type"] != "object" || schema["additionalProperties"] != false {
		t.Fatalf("schema=%v", schema)
	}
	if !reflect.DeepEqual(schema["required"], []any{"id", "name", "limit"}) {
		t.Fatalf("required=%v", schema["required"])
	}
	props := schema["properties"].(map[string]any)
	if len(props) != 8 {
		t.Fatalf("props=%v", props)
	}
	id := props["id"].(map[string]any)
	if id["type"] != "string" || id["minLength"] != float64(1) || id["description"] != "클러스터" {
		t.Fatalf("id=%v", id)
	}
	// 파라미터 설명이 없으면 스키마 설명을 쓰고, path 문자열은 1자 이상을 요구한다.
	name := props["name"].(map[string]any)
	if name["description"] != "스키마 설명" || name["minLength"] != float64(1) {
		t.Fatalf("name=%v", name)
	}
	limit := props["limit"].(map[string]any)
	if limit["type"] != "integer" || limit["maximum"] != float64(200) || limit["default"] != float64(20) {
		t.Fatalf("limit=%v", limit)
	}
	if owner := props["owner"].(map[string]any); !reflect.DeepEqual(owner["type"], []any{"string", "null"}) {
		t.Fatalf("owner=%v", owner)
	}
	if level := props["level"].(map[string]any); !reflect.DeepEqual(level["enum"], []any{"LOW", "HIGH"}) {
		t.Fatalf("level=%v", level)
	}
	levels := props["levels"].(map[string]any)
	if levels["type"] != "array" || levels["items"].(map[string]any)["type"] != "string" {
		t.Fatalf("levels=%v", levels)
	}
	if !strings.HasPrefix(tool.description, "항목 조회\n\n설명 본문\n\n[OpenAPI getItems · GET /api/clusters/{id}/items/{name}]") {
		t.Fatalf("description=%q", tool.description)
	}
	def := tool.definition()
	if def.Annotations == nil || !def.Annotations.ReadOnlyHint || def.OutputSchema == nil || def.Meta["ableops/openapi"] == nil {
		t.Fatalf("definition=%+v", def)
	}
	// 등록할 때마다 새 정의를 만든다.
	if tool.definition() == def {
		t.Fatal("정의를 공유함")
	}

	// 스키마가 실제 검증기로 동작하는지 확인한다.
	resolved, err := tool.inputSchema.Resolve(&jsonschema.ResolveOptions{ValidateDefaults: true})
	if err != nil {
		t.Fatal(err)
	}
	valid := []map[string]any{
		{"id": "c1", "name": "n", "limit": 5},
		{"id": "c1", "name": "n", "limit": 5, "owner": nil, "ratio": 0.5, "live": true, "level": "LOW", "levels": []any{"LOW", "HIGH"}},
	}
	invalid := []map[string]any{
		{"id": "c1", "name": "n"},                              // 필수 query 누락
		{"id": "", "name": "n", "limit": 5},                    // 빈 path
		{"id": "c1", "name": "n", "limit": 0},                  // minimum
		{"id": "c1", "name": "n", "limit": 1.5},                // 정수 아님
		{"id": "c1", "name": "n", "limit": 5, "level": "MID"},  // enum
		{"id": "c1", "name": "n", "limit": 5, "levels": "LOW"}, // 배열 아님
		{"id": "c1", "name": "n", "limit": 5, "levels": []any{"LOW", "LOW", "HIGH", "LOW"}},
		{"id": "c1", "name": "n", "limit": 5, "extra": 1}, // 선언 외 인자
		{"id": "c1", "name": "n", "limit": 5, "live": "true"},
	}
	for _, args := range valid {
		value := any(args)
		if err := resolved.Validate(&value); err != nil {
			t.Errorf("valid %v: %v", args, err)
		}
	}
	for _, args := range invalid {
		value := any(args)
		if err := resolved.Validate(&value); err == nil {
			t.Errorf("invalid 허용: %v", args)
		}
	}
}

func TestCompileRejectsUnsafeOperations(t *testing.T) {
	contract := syntheticContract(t, `{
	  "/api/a": {
	    "get": {"operationId":"notExposed","x-mcp-enabled":false,`+jsonOK+`},
	    "post": {"operationId":"createA","x-mcp-enabled":true,`+jsonOK+`},
	    "put": {"operationId":"replaceA","x-mcp-enabled":true,`+jsonOK+`},
	    "patch": {"operationId":"patchA","x-mcp-enabled":true,`+jsonOK+`},
	    "delete": {"operationId":"deleteA","x-mcp-enabled":true,`+jsonOK+`},
	    "head": {"operationId":"headA","x-mcp-enabled":true,`+jsonOK+`}
	  },
	  "/api/b": {"get": {"operationId":"withBody","x-mcp-enabled":true,"requestBody":{"content":{"application/json":{}}},`+jsonOK+`}},
	  "/api/c": {"get": {"operationId":"markdown","x-mcp-enabled":true,"responses":{"200":{"description":"md","content":{"text/markdown":{}}}}}},
	  "/api/d": {"get": {"operationId":"noContentOnly","x-mcp-enabled":true,"responses":{"204":{"description":"none"}}}},
	  "/api/e": {"get": {"operationId":"tokenParam","x-mcp-enabled":true,"parameters":[{"name":"accessToken","in":"query","schema":{"type":"string"}}],`+jsonOK+`}},
	  "/api/f": {"get": {"operationId":"badPattern","x-mcp-enabled":true,"parameters":[{"name":"q","in":"query","schema":{"type":"string","pattern":"(?=lookahead)"}}],`+jsonOK+`}},
	  "/api/g": {"get": {"operationId":"badDefault","x-mcp-enabled":true,"parameters":[{"name":"n","in":"query","schema":{"type":"integer","minimum":1,"default":0}}],`+jsonOK+`}},
	  "/api/h": {"get": {"operationId":"vendorJSON","x-mcp-enabled":true,"responses":{"200":{"description":"ok","content":{"application/problem+json; charset=utf-8":{}}}}}}
	}`)
	want := map[string]string{
		"notExposed":    IssueNotMCPEnabled,
		"createA":       IssueWriteMethod,
		"replaceA":      IssueWriteMethod,
		"patchA":        IssueWriteMethod,
		"deleteA":       IssueWriteMethod,
		"headA":         IssueUnsupportedMethod,
		"withBody":      IssueRequestBody,
		"markdown":      IssueNonJSONResponse,
		"noContentOnly": IssueNonJSONResponse,
		"tokenParam":    IssueSensitiveParam,
		"badPattern":    IssueInputSchema,
		"badDefault":    IssueInputSchema,
	}
	for id, code := range want {
		_, err := Compile(findOperation(t, contract, id))
		var ce *CompileError
		if !errors.As(err, &ce) || ce.Code != code {
			t.Errorf("%s: err=%v want %s", id, err, code)
		}
	}
	if _, err := Compile(findOperation(t, contract, "vendorJSON")); err != nil {
		t.Fatalf("+json 미디어 타입 거부: %v", err)
	}
}

func TestCompileUpstreamContract(t *testing.T) {
	contract := fixtureContract(t)
	for _, op := range contract.MCPOperations() {
		tool, err := Compile(op)
		if err != nil {
			t.Errorf("%s: %v", op.ID, err)
			continue
		}
		if len(tool.description) > maxToolDescriptionBytes+512 {
			t.Errorf("%s: 설명 %d바이트", op.ID, len(tool.description))
		}
	}
	for _, id := range []string{"login", "getClusterStorage", "getAssetGraphReport"} {
		var ce *CompileError
		if _, err := Compile(findOperation(t, contract, id)); !errors.As(err, &ce) {
			t.Errorf("%s가 컴파일됨", id)
		}
	}
	// 기본값은 스키마에 설명으로 남지만 원본 인자에는 없으므로 REST로 보내지 않는다(executor_test).
	events, err := Compile(findOperation(t, contract, "listEvents"))
	if err != nil {
		t.Fatal(err)
	}
	props := schemaMap(t, events)["properties"].(map[string]any)
	status := props["status"].(map[string]any)
	if status["type"] != "array" || status["items"].(map[string]any)["enum"] == nil {
		t.Fatalf("status=%v", status)
	}
	if _, ok := props["pageSize"].(map[string]any)["default"]; !ok {
		t.Fatal("default 설명 누락")
	}
}

func TestLimitText(t *testing.T) {
	long := strings.Repeat("가", 5000)
	cut := limitText(long, 1000)
	if len(cut) > 1000 || !strings.HasSuffix(cut, "(설명이 길어 이하 생략)") || !utf8.ValidString(cut) {
		t.Fatalf("cut=%d", len(cut))
	}
	if limitText("짧음", 1000) != "짧음" {
		t.Fatal("짧은 설명 변경")
	}
}
