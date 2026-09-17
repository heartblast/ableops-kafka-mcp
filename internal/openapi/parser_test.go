package openapi

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

// testdata/ableops-openapi.json은 upstream(ableops-kafka) `GET /openapi.json` 본문의 스냅샷이다.
// 출처와 갱신 방법은 docs/api-mapping.md에 기록한다.
func loadFixture(t *testing.T) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/ableops-openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func operation(t *testing.T, c *Contract, id string) Operation {
	t.Helper()
	for _, op := range c.Operations {
		if op.ID == id {
			return op
		}
	}
	t.Fatalf("operation %s 없음", id)
	return Operation{}
}

func param(t *testing.T, op Operation, name string) Parameter {
	t.Helper()
	for _, p := range op.Parameters {
		if p.Name == name {
			return p
		}
	}
	t.Fatalf("%s 파라미터 %s 없음", op.ID, name)
	return Parameter{}
}

func TestParseUpstreamContract(t *testing.T) {
	c, err := Parse(loadFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if c.OpenAPI != "3.1.0" || c.Discovered != 31 || c.MCPDeclared != 28 || len(c.Operations) != 31 || len(c.MCPOperations()) != 28 {
		t.Fatalf("version=%s discovered=%d declared=%d ops=%d mcp=%d", c.OpenAPI, c.Discovered, c.MCPDeclared, len(c.Operations), len(c.MCPOperations()))
	}
	if len(c.Issues) != 0 {
		t.Fatalf("현재 계약에서 제외가 발생함: %+v", c.Issues)
	}
	for _, id := range []string{"login", "getClusterStorage", "getAssetGraphReport"} {
		if operation(t, c, id).MCPEnabled {
			t.Errorf("%s는 x-mcp-enabled=false여야 함", id)
		}
	}
	// 요청서가 열거한 MCP 허용 대표 operationId
	for _, id := range []string{
		"listClusters", "getCluster", "getClusterHealth", "getClusterPartitionHealth",
		"listDefaultClusterTopics", "listTopics", "getTopic", "getTopicPartitions",
		"listConsumerGroups", "listConsumerGroupAlerts", "getConsumerGroupState", "getConsumerGroupLag",
		"getConsumerGroupMembers", "getConsumerLagOverview",
		"listEvents", "getEventSummary", "getEvent", "listEventOccurrences", "getEventIssue", "listClusterEvents", "getEventPlaybook",
		"getAssetGraph", "getAssetImpact", "searchAssetGraph", "listRequests", "getRequest", "getCurrentUser", "getBranding",
	} {
		op := operation(t, c, id)
		if !op.MCPEnabled || op.Method != "GET" || !op.HasJSONSuccess() || op.RequestBody != nil {
			t.Errorf("%s: mcp=%v method=%s json=%v", id, op.MCPEnabled, op.Method, op.HasJSONSuccess())
		}
	}

	topic := operation(t, c, "getTopic")
	if topic.Path != "/api/clusters/{id}/topics/{name}" || !reflect.DeepEqual(topic.PathSegments(), []string{"clusters", "{id}", "topics", "{name}"}) {
		t.Fatalf("path=%s segments=%v", topic.Path, topic.PathSegments())
	}
	if p := param(t, topic, "name"); p.In != "path" || !p.Required || p.Schema["type"] != "string" {
		t.Fatalf("name=%+v", p)
	}
	if !operation(t, c, "getEventIssue").AllowsNoContent() || topic.AllowsNoContent() {
		t.Fatal("204 선언 인식 오류")
	}

	events := operation(t, c, "listEvents")
	status := param(t, events, "status")
	items, _ := status.Schema["items"].(map[string]any)
	if status.In != "query" || status.Required || !status.Explode || status.Schema["type"] != "array" || !reflect.DeepEqual(items["enum"], []any{"OPEN", "ACKNOWLEDGED", "SUPPRESSED", "RESOLVED"}) {
		t.Fatalf("status=%+v", status)
	}
	// $ref(EventResourceType)를 풀어 단일 enum 문자열로 만든다.
	if resource := param(t, events, "resourceType"); resource.Schema["type"] != "string" || resource.Schema["enum"] == nil || resource.Schema["$ref"] != nil {
		t.Fatalf("resourceType=%+v", resource.Schema)
	}
	if pageSize := param(t, events, "pageSize"); pageSize.Schema["maximum"] != json.Number("200") || pageSize.Schema["default"] != json.Number("20") {
		t.Fatalf("pageSize=%+v", pageSize.Schema)
	}
	playbook := param(t, operation(t, c, "getEventPlaybook"), "eventCode")
	if playbook.In != "path" || playbook.Schema["type"] != "string" || playbook.Schema["enum"] == nil {
		t.Fatalf("eventCode=%+v", playbook.Schema)
	}
}

func specWith(paths, components string) []byte {
	if components == "" {
		components = "{}"
	}
	return []byte(`{"openapi":"3.1.0","info":{"title":"t","version":"v"},"servers":[{"url":"/"}],"paths":` + paths + `,"components":` + components + `}`)
}

const okResponses = `"responses":{"200":{"description":"ok","content":{"application/json":{"schema":{"type":"object"}}}}}`

func TestParseRejectsWholeContract(t *testing.T) {
	good := `{"/api/a":{"get":{"operationId":"getA","x-mcp-enabled":true,` + okResponses + `}}}`
	cases := map[string]struct {
		body []byte
		code string
	}{
		"JSON 오류":     {[]byte(`{"openapi":`), "invalid_json"},
		"뒤따르는 JSON":   {[]byte(`{"openapi":"3.1.0"} {}`), "invalid_json"},
		"배열 문서":       {[]byte(`[]`), "invalid_document"},
		"버전 누락":       {[]byte(`{"paths":` + good + `}`), "unsupported_version"},
		"Swagger 2":   {[]byte(`{"swagger":"2.0","openapi":"2.0","paths":` + good + `}`), "unsupported_version"},
		"다른 호스트":      {[]byte(`{"openapi":"3.1.0","servers":[{"url":"https://evil.example.test"}],"paths":` + good + `}`), "unsupported_servers"},
		"접두 경로":       {[]byte(`{"openapi":"3.1.0","servers":[{"url":"/portal"}],"paths":` + good + `}`), "unsupported_servers"},
		"servers 형식":  {[]byte(`{"openapi":"3.1.0","servers":{"url":"/"},"paths":` + good + `}`), "unsupported_servers"},
		"paths 누락":    {[]byte(`{"openapi":"3.1.0"}`), "missing_paths"},
		"빈 paths":     {[]byte(`{"openapi":"3.1.0","paths":{}}`), "missing_paths"},
		"사용 가능 Op 없음": {specWith(`{"/api/a":{"get":{"x-mcp-enabled":true}}}`, ""), "no_operations"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c, err := Parse(tc.body)
			var ce *ContractError
			if c != nil || !errors.As(err, &ce) || ce.Code != tc.code {
				t.Fatalf("contract=%v err=%v", c, err)
			}
		})
	}
	// servers가 없거나 비어 있으면 설정한 base URL을 그대로 쓴다.
	for _, body := range []string{`{"openapi":"3.0.3","paths":` + good + `}`, `{"openapi":"3.1.0","servers":[],"paths":` + good + `}`} {
		if _, err := Parse([]byte(body)); err != nil {
			t.Fatalf("%s: %v", body, err)
		}
	}
}

func TestParseIsolatesOperationIssues(t *testing.T) {
	paths := `{
	  "/api/good/{id}": {
	    "parameters": [{"name":"verbose","in":"query","schema":{"type":"boolean"}}],
	    "get": {"operationId":"getGood","summary":"s","x-mcp-enabled":true,"x-mcp-note":"n",
	      "parameters":[{"name":"id","in":"path","required":true,"schema":{"type":"string"}},
	                    {"name":"verbose","in":"query","description":"덮어씀","schema":{"type":"integer"}}],` + okResponses + `},
	    "post": {"operationId":"createGood","x-mcp-enabled":false,` + okResponses + `},
	    "delete": {"x-mcp-enabled":true,` + okResponses + `}
	  },
	  "/api/bad-id": {"get": {"operationId":"Get_Bad","x-mcp-enabled":true,` + okResponses + `}},
	  "/api/dup1": {"get": {"operationId":"dupOp","x-mcp-enabled":true,` + okResponses + `}},
	  "/api/dup2": {"get": {"operationId":"dupOp","x-mcp-enabled":false,` + okResponses + `}},
	  "/api/flag-missing": {"get": {"operationId":"flagMissing",` + okResponses + `}},
	  "/api/flag-string": {"get": {"operationId":"flagString","x-mcp-enabled":"true",` + okResponses + `}},
	  "/api/t1/{id}": {"get": {"operationId":"missingPathParam","x-mcp-enabled":true,` + okResponses + `}},
	  "/api/t2": {"get": {"operationId":"extraPathParam","x-mcp-enabled":true,"parameters":[{"name":"id","in":"path","required":true,"schema":{"type":"string"}}],` + okResponses + `}},
	  "/api/t3/{id}": {"get": {"operationId":"optionalPathParam","x-mcp-enabled":true,"parameters":[{"name":"id","in":"path","schema":{"type":"string"}}],` + okResponses + `}},
	  "/api/t4/v{n}": {"get": {"operationId":"partialTemplate","x-mcp-enabled":true,"parameters":[{"name":"n","in":"path","required":true,"schema":{"type":"string"}}],` + okResponses + `}},
	  "/api/t5/{id}": {"get": {"operationId":"arrayPathParam","x-mcp-enabled":true,"parameters":[{"name":"id","in":"path","required":true,"schema":{"type":"array","items":{"type":"string"}}}],` + okResponses + `}},
	  "/api/t6": {"get": {"operationId":"sameNameTwice","x-mcp-enabled":true,"parameters":[{"name":"q","in":"query","schema":{"type":"string"}},{"name":"q","in":"query","schema":{"type":"string"}}],` + okResponses + `}},
	  "/api/s1": {"get": {"operationId":"headerParam","x-mcp-enabled":true,"parameters":[{"name":"X-Trace","in":"header","schema":{"type":"string"}}],` + okResponses + `}},
	  "/api/s2": {"get": {"operationId":"objectParam","x-mcp-enabled":true,"parameters":[{"name":"filter","in":"query","schema":{"type":"object","properties":{}}}],` + okResponses + `}},
	  "/api/s3": {"get": {"operationId":"oneOfParam","x-mcp-enabled":true,"parameters":[{"name":"v","in":"query","schema":{"type":"string","oneOf":[{"type":"string"}]}}],` + okResponses + `}},
	  "/api/s4": {"get": {"operationId":"missingRef","x-mcp-enabled":true,"parameters":[{"name":"v","in":"query","schema":{"$ref":"#/components/schemas/Nope"}}],` + okResponses + `}},
	  "/api/s5": {"get": {"operationId":"cyclicRef","x-mcp-enabled":true,"parameters":[{"name":"v","in":"query","schema":{"$ref":"#/components/schemas/Loop"}}],` + okResponses + `}},
	  "/api/s6": {"get": {"operationId":"externalRef","x-mcp-enabled":true,"parameters":[{"name":"v","in":"query","schema":{"$ref":"other.json#/X"}}],` + okResponses + `}},
	  "/api/s7": {"get": {"operationId":"noSchema","x-mcp-enabled":true,"parameters":[{"name":"v","in":"query"}],` + okResponses + `}},
	  "/api/s8": {"get": {"operationId":"deepObject","x-mcp-enabled":true,"parameters":[{"name":"v","in":"query","style":"deepObject","schema":{"type":"string"}}],` + okResponses + `}},
	  "/api/s9": {"get": {"operationId":"serversOverride","x-mcp-enabled":true,"servers":[{"url":"https://evil.example.test"}],` + okResponses + `}},
	  "/api/s10": {"get": {"operationId":"noResponses","x-mcp-enabled":true}},
	  "/openapi.json": {"get": {"operationId":"outsideApi","x-mcp-enabled":true,` + okResponses + `}},
	  "/api/ref-item": {"$ref":"#/components/pathItems/X"}
	}`
	components := `{"schemas":{"Loop":{"$ref":"#/components/schemas/Loop"}}}`
	c, err := Parse(specWith(paths, components))
	if err != nil {
		t.Fatal(err)
	}
	good := operation(t, c, "getGood")
	if !good.MCPEnabled || good.MCPNote != "n" || len(good.Parameters) != 2 {
		t.Fatalf("good=%+v", good)
	}
	// 경로 항목 파라미터를 Operation 파라미터가 덮어쓴다.
	if v := param(t, good, "verbose"); v.Description != "덮어씀" || v.Schema["type"] != "integer" {
		t.Fatalf("verbose=%+v", v)
	}
	if op := operation(t, c, "createGood"); op.MCPEnabled || op.Method != "POST" {
		t.Fatalf("createGood=%+v", op)
	}
	for _, id := range []string{"flagMissing", "flagString"} {
		if operation(t, c, id).MCPEnabled {
			t.Fatalf("%s가 노출 후보가 됨", id)
		}
	}
	want := map[string]string{
		"Get_Bad":           IssueInvalidOperationID,
		"flagMissing":       IssueMissingMCPEnabled,
		"flagString":        IssueMalformedMCPEnabled,
		"missingPathParam":  IssueInvalidPathParameter,
		"extraPathParam":    IssueInvalidPathParameter,
		"optionalPathParam": IssueInvalidPathParameter,
		"partialTemplate":   IssueInvalidPathParameter,
		"arrayPathParam":    IssueUnsupportedSchema,
		"sameNameTwice":     IssueDuplicateParameter,
		"headerParam":       IssueUnsupportedParameter,
		"objectParam":       IssueUnsupportedSchema,
		"oneOfParam":        IssueUnsupportedSchema,
		"missingRef":        IssueUnsupportedSchema,
		"cyclicRef":         IssueUnsupportedSchema,
		"externalRef":       IssueUnsupportedSchema,
		"noSchema":          IssueUnsupportedSchema,
		"deepObject":        IssueUnsupportedParameter,
		"serversOverride":   IssueUnsupportedServers,
		"noResponses":       IssueUnsupportedResponse,
		"outsideApi":        IssueUnsupportedPath,
	}
	got := map[string]string{}
	dup := 0
	for _, issue := range c.Issues {
		switch {
		case issue.Code == IssueDuplicateOperationID:
			dup++
		case issue.Code == IssueMissingOperationID:
			if issue.Method != "DELETE" || issue.Path != "/api/good/{id}" {
				t.Errorf("missing id issue=%+v", issue)
			}
		case issue.Code == IssueUnsupportedPathItem:
			if issue.Path != "/api/ref-item" {
				t.Errorf("path item issue=%+v", issue)
			}
		default:
			got[issue.OperationID] = issue.Code
		}
		if issue.Message == "" {
			t.Errorf("설명 없는 issue: %+v", issue)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("issues\n got=%v\nwant=%v", got, want)
	}
	// 중복 operationId는 x-mcp-enabled와 무관하게 모두 제외한다.
	if dup != 2 {
		t.Fatalf("duplicate issues=%d", dup)
	}
	for _, op := range c.Operations {
		if op.ID == "dupOp" {
			t.Fatal("중복 operationId가 남음")
		}
		if _, excluded := want[op.ID]; excluded && op.MCPEnabled {
			t.Fatalf("제외 대상이 노출 후보로 남음: %s", op.ID)
		}
	}
	// 제외된 Operation도 발견 수(25)와 선언 수(19)에는 포함한다.
	// 경로 항목 $ref는 메서드를 알 수 없어 발견 수에서, 누락·오형식 ID는 선언 수에서 빠진다.
	if c.Discovered != 25 || c.MCPDeclared != 19 {
		t.Fatalf("discovered=%d declared=%d", c.Discovered, c.MCPDeclared)
	}
}

func TestParamSchemaNormalization(t *testing.T) {
	r := &resolver{components: map[string]any{"schemas": map[string]any{
		"Level": map[string]any{"type": "string", "enum": []any{"LOW", "HIGH"}, "description": "원본 설명", "title": "Level"},
	}}}
	decode := func(raw string) any {
		var v any
		d := json.NewDecoder(strings.NewReader(raw))
		d.UseNumber()
		if err := d.Decode(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	cases := []struct {
		name   string
		raw    string
		array  bool
		legacy bool
		want   string
		err    string
	}{
		{name: "3.1 nullable enum", raw: `{"type":["string","null"],"enum":["A"]}`, want: `{"enum":["A",null],"type":["string","null"]}`},
		{name: "3.0 nullable", raw: `{"type":"integer","nullable":true,"minimum":1}`, legacy: true, want: `{"minimum":1,"type":["integer","null"]}`},
		{name: "3.1 nullable 키워드", raw: `{"type":"string","nullable":true}`, err: "3.1"},
		{name: "ref 형제 키워드 덮어쓰기", raw: `{"$ref":"#/components/schemas/Level","description":"덮어쓴 설명"}`, want: `{"description":"덮어쓴 설명","enum":["LOW","HIGH"],"type":"string"}`},
		{name: "배열 ref 원소", raw: `{"type":"array","items":{"$ref":"#/components/schemas/Level"},"maxItems":20}`, array: true, want: `{"items":{"description":"원본 설명","enum":["LOW","HIGH"],"type":"string"},"maxItems":20,"type":"array"}`},
		{name: "주석·확장 제거와 example", raw: `{"type":"string","example":"orders","x-internal":1,"deprecated":true,"pattern":"^[a-z]+$","maxLength":10}`, want: `{"examples":["orders"],"maxLength":10,"pattern":"^[a-z]+$","type":"string"}`},
		{name: "path 배열 거부", raw: `{"type":"array","items":{"type":"string"}}`, err: "배열"},
		{name: "중첩 배열 거부", raw: `{"type":"array","items":{"type":"array","items":{"type":"string"}}}`, array: true, err: "배열"},
		{name: "items 누락", raw: `{"type":"array"}`, array: true, err: "items"},
		{name: "3.0 불리언 exclusiveMinimum", raw: `{"type":"integer","minimum":0,"exclusiveMinimum":true}`, legacy: true, err: "exclusiveMinimum"},
		{name: "enum 타입 불일치", raw: `{"type":"integer","enum":["1"]}`, err: "enum"},
		{name: "정수 enum", raw: `{"type":"integer","enum":[1,2]}`, want: `{"enum":[1,2],"type":"integer"}`},
		{name: "다중 타입", raw: `{"type":["string","integer"]}`, err: "여러 타입"},
		{name: "null 단독", raw: `{"type":["null"]}`, err: "타입"},
		{name: "문자열 제약을 숫자에", raw: `{"type":"integer","maxLength":3}`, err: "maxLength"},
		{name: "알 수 없는 키워드", raw: `{"type":"string","contentEncoding":"base64"}`, err: "contentEncoding"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.paramSchema(decode(tc.raw), tc.array, tc.legacy)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err=%v, want %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(got)
			if string(raw) != tc.want {
				t.Fatalf("\n got=%s\nwant=%s", raw, tc.want)
			}
		})
	}
}
