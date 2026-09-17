// Package dynamic은 OpenAPI 계약에서 MCP 도구를 만들고(Compiler·Registry)
// 도구 호출을 기존 REST Client의 GET으로 실행한다(Generic Executor).
//
// 기존 Static 도구를 대체하지 않는다. 같은 이름의 Static 도구가 있으면 Dynamic 도구는
// shadow로만 보관하며, 쓰기 메서드와 x-mcp-enabled가 true가 아닌 Operation은 만들지 않는다.
package dynamic

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

var operationIDPattern = regexp.MustCompile(`^[a-z][A-Za-z0-9]{0,127}$`)

// ToolName은 lowerCamelCase operationId를 snake_case MCP 도구 이름으로 바꾼다.
//
//	listClusters         → list_clusters
//	getConsumerGroupLag  → get_consumer_group_lag
//	getHTTPStatus        → get_http_status (연속 대문자는 한 단어)
//	getV2Topic           → get_v2_topic   (숫자 뒤 대문자는 새 단어)
//
// operationId 형식이 계약(^[a-z][A-Za-z0-9]*$)과 다르면 오류다.
func ToolName(operationID string) (string, error) {
	if !operationIDPattern.MatchString(operationID) {
		return "", fmt.Errorf("operationId 형식이 도구 이름 규칙과 맞지 않습니다")
	}
	runes := []rune(operationID)
	var b strings.Builder
	for i, r := range runes {
		if !unicode.IsUpper(r) {
			b.WriteRune(r)
			continue
		}
		prev := runes[i-1] // 첫 글자는 소문자이므로 i > 0이다.
		nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
		if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	name := b.String()
	if len(name) > 128 {
		return "", fmt.Errorf("도구 이름이 128자를 넘습니다")
	}
	return name, nil
}
