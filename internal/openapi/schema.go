package openapi

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
)

// maxRefDepth는 연쇄 $ref의 상한이다. 순환 참조도 이 상한에서 끊는다.
const maxRefDepth = 16

// resolver는 문서 안의 `#/components/...` 참조만 해석한다. 외부 문서 참조는 지원하지 않는다.
type resolver struct {
	components map[string]any
}

func (r *resolver) lookup(ref any, kind string) (map[string]any, error) {
	text, ok := ref.(string)
	prefix := "#/components/" + kind + "/"
	if !ok || !strings.HasPrefix(text, prefix) {
		return nil, fmt.Errorf("문서 내부 components.%s 이외의 $ref는 지원하지 않습니다", kind)
	}
	name := text[len(prefix):]
	if name == "" || strings.ContainsAny(name, "/~") {
		return nil, fmt.Errorf("지원하지 않는 $ref 이름 형식입니다")
	}
	table, _ := r.components[kind].(map[string]any)
	target, ok := table[name].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("해석할 수 없는 $ref입니다(components.%s)", kind)
	}
	return target, nil
}

// deref는 $ref를 따라가며 참조한 쪽의 형제 키워드로 대상 값을 덮어쓴다(OpenAPI 3.1 의미).
func (r *resolver) deref(value map[string]any, kind string) (map[string]any, error) {
	current := value
	for depth := 0; ; depth++ {
		ref, has := current["$ref"]
		if !has {
			return current, nil
		}
		if depth >= maxRefDepth {
			return nil, fmt.Errorf("$ref가 순환하거나 너무 깊습니다")
		}
		target, err := r.lookup(ref, kind)
		if err != nil {
			return nil, err
		}
		next := make(map[string]any, len(target)+len(current))
		for key, item := range target {
			next[key] = item
		}
		for key, item := range current {
			if key != "$ref" {
				next[key] = item
			}
		}
		current = next
	}
}

// 값의 의미를 바꾸지 않는 주석 키워드는 MCP 입력 스키마에서 뺀다.
var ignoredSchemaKeywords = map[string]bool{
	"title": true, "deprecated": true, "readOnly": true, "writeOnly": true,
	"externalDocs": true, "xml": true, "$comment": true,
}

var scalarTypes = map[string]bool{"string": true, "integer": true, "number": true, "boolean": true}

// paramSchema는 파라미터 스키마를 MCP 입력으로 옮길 수 있는 형태로 정규화한다.
// 허용 목록에 없는 키워드(oneOf·object·properties 등)가 있으면 추측하지 않고 오류로 돌려준다.
// allowArray가 거짓이면 배열 타입을 거부한다(path 파라미터와 배열 원소).
func (r *resolver) paramSchema(raw any, allowArray bool, legacyNullable bool) (map[string]any, error) {
	source, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("스키마가 객체가 아닙니다")
	}
	schema, err := r.deref(source, "schemas")
	if err != nil {
		return nil, err
	}
	base, nullable, err := schemaType(schema, legacyNullable)
	if err != nil {
		return nil, err
	}
	if base == "array" && !allowArray {
		return nil, fmt.Errorf("이 위치에서는 배열 스키마를 지원하지 않습니다")
	}
	if base == "array" && nullable {
		return nil, fmt.Errorf("nullable 배열 파라미터는 지원하지 않습니다")
	}
	out := map[string]any{"type": base}
	if nullable {
		out["type"] = []any{base, "null"}
	}
	for key, value := range schema {
		if strings.HasPrefix(key, "x-") || ignoredSchemaKeywords[key] {
			continue
		}
		switch key {
		case "type", "nullable", "enum":
			// schemaType과 아래 enum 처리에서 다룬다.
		case "description", "format", "pattern":
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("%s는 문자열이어야 합니다", key)
			}
			if key == "pattern" && base != "string" {
				return nil, fmt.Errorf("문자열이 아닌 타입의 pattern은 지원하지 않습니다")
			}
			out[key] = text
		case "example":
			if _, has := schema["examples"]; !has {
				out["examples"] = []any{value}
			}
		case "examples":
			list, ok := value.([]any)
			if !ok {
				return nil, fmt.Errorf("examples는 배열이어야 합니다")
			}
			out[key] = list
		case "default", "const":
			out[key] = value
		case "minLength", "maxLength":
			if base != "string" || !isNonNegativeInteger(value) {
				return nil, fmt.Errorf("%s는 문자열 타입의 0 이상 정수여야 합니다", key)
			}
			out[key] = value
		case "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf":
			// OpenAPI 3.0의 불리언 exclusiveMinimum 등은 의미가 달라 옮기지 않는다.
			if (base != "integer" && base != "number") || !isNumber(value) {
				return nil, fmt.Errorf("%s는 숫자 타입의 숫자 값이어야 합니다", key)
			}
			out[key] = value
		case "items":
			if base != "array" {
				return nil, fmt.Errorf("배열이 아닌 타입의 items는 지원하지 않습니다")
			}
			items, err := r.paramSchema(value, false, legacyNullable)
			if err != nil {
				return nil, fmt.Errorf("배열 원소: %w", err)
			}
			if typ, ok := items["type"].(string); !ok || !scalarTypes[typ] {
				return nil, fmt.Errorf("배열 원소는 null을 허용하지 않는 단일 스칼라 타입이어야 합니다")
			}
			out[key] = items
		case "minItems", "maxItems":
			if base != "array" || !isNonNegativeInteger(value) {
				return nil, fmt.Errorf("%s는 배열 타입의 0 이상 정수여야 합니다", key)
			}
			out[key] = value
		case "uniqueItems":
			if _, ok := value.(bool); !ok || base != "array" {
				return nil, fmt.Errorf("uniqueItems는 배열 타입의 불리언이어야 합니다")
			}
			out[key] = value
		default:
			return nil, fmt.Errorf("지원하지 않는 스키마 키워드 %q가 있습니다", safeText(key))
		}
	}
	if base == "array" {
		if _, ok := out["items"]; !ok {
			return nil, fmt.Errorf("배열 스키마에 items가 없습니다")
		}
	}
	if raw, has := schema["enum"]; has {
		enum, err := enumValues(raw, base, nullable)
		if err != nil {
			return nil, err
		}
		out["enum"] = enum
	}
	return out, nil
}

// schemaType은 3.1의 type 배열과 3.0의 nullable을 같은 형태(기본 타입 + null 허용 여부)로 읽는다.
func schemaType(schema map[string]any, legacyNullable bool) (string, bool, error) {
	var base string
	nullable := false
	switch typ := schema["type"].(type) {
	case string:
		base = typ
	case []any:
		for _, item := range typ {
			name, ok := item.(string)
			switch {
			case !ok:
				return "", false, fmt.Errorf("type 배열의 원소가 문자열이 아닙니다")
			case name == "null" && !nullable:
				nullable = true
			case base == "" && name != "null":
				base = name
			default:
				return "", false, fmt.Errorf("여러 타입을 허용하는 스키마는 지원하지 않습니다")
			}
		}
	case nil:
		return "", false, fmt.Errorf("type이 없는 스키마는 지원하지 않습니다")
	default:
		return "", false, fmt.Errorf("type 형식이 올바르지 않습니다")
	}
	if raw, has := schema["nullable"]; has {
		flag, ok := raw.(bool)
		if !ok {
			return "", false, fmt.Errorf("nullable은 불리언이어야 합니다")
		}
		if !legacyNullable && flag {
			// 3.1에는 nullable 키워드가 없다. 의미를 추측하지 않는다.
			return "", false, fmt.Errorf("OpenAPI 3.1 문서의 nullable 키워드는 지원하지 않습니다")
		}
		nullable = nullable || flag
	}
	if !scalarTypes[base] && base != "array" {
		return "", false, fmt.Errorf("지원하지 않는 파라미터 타입입니다")
	}
	return base, nullable, nil
}

func enumValues(raw any, base string, nullable bool) ([]any, error) {
	list, ok := raw.([]any)
	if !ok || len(list) == 0 {
		return nil, fmt.Errorf("enum은 비어 있지 않은 배열이어야 합니다")
	}
	if base == "array" {
		return nil, fmt.Errorf("배열 타입의 enum은 지원하지 않습니다")
	}
	out := make([]any, 0, len(list)+1)
	hasNull := false
	for _, value := range list {
		switch {
		case value == nil && nullable:
			hasNull = true
		case matchesType(value, base):
		default:
			return nil, fmt.Errorf("enum 값이 선언된 타입과 다릅니다")
		}
		out = append(out, value)
	}
	// null을 허용하는 타입이면 enum도 null을 받아야 스키마가 모순되지 않는다.
	if nullable && !hasNull {
		out = append(out, nil)
	}
	return out, nil
}

func matchesType(value any, base string) bool {
	switch base {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "number":
		return isNumber(value)
	case "integer":
		return isInteger(value)
	}
	return false
}

func isNumber(value any) bool {
	switch v := value.(type) {
	case json.Number:
		_, ok := new(big.Rat).SetString(v.String())
		return ok
	case float64:
		return true
	}
	return false
}

func isInteger(value any) bool {
	switch v := value.(type) {
	case json.Number:
		r, ok := new(big.Rat).SetString(v.String())
		return ok && r.IsInt()
	case float64:
		return v == float64(int64(v))
	}
	return false
}

func isNonNegativeInteger(value any) bool {
	if !isInteger(value) {
		return false
	}
	switch v := value.(type) {
	case json.Number:
		r, _ := new(big.Rat).SetString(v.String())
		return r.Sign() >= 0
	case float64:
		return v >= 0
	}
	return false
}
