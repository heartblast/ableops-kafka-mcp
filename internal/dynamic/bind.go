package dynamic

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/url"
	"strconv"
	"strings"

	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
)

var errBinding = errors.New("도구 인자를 REST 요청으로 바꿀 수 없습니다")

// decodeArguments는 원본 도구 인자를 숫자 정밀도를 보존해 읽는다.
// SDK가 검증용으로 채운 스키마 default는 원본에 없으므로 REST로 보내지 않는다.
func decodeArguments(raw json.RawMessage) (map[string]any, error) {
	args := map[string]any{}
	if len(bytes.TrimSpace(raw)) == 0 {
		return args, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errBinding
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errBinding
	}
	switch v := value.(type) {
	case nil:
		return args, nil
	case map[string]any:
		return v, nil
	default:
		return nil, errBinding
	}
}

// bind는 도구 인자를 `/api/` 아래 경로 조각과 쿼리로 바꾼다.
//
// path 값은 문자열 결합하지 않고 조각 단위로 넘겨 Client가 각각 인코딩한다.
// 따라서 토픽명·Consumer Group·Principal·이벤트 ID의 `/`·`?`·`%`·공백도 한 조각으로 남는다.
func bind(op openapi.Operation, args map[string]any) ([]string, url.Values, error) {
	params := map[string]openapi.Parameter{}
	for _, param := range op.Parameters {
		params[param.Name] = param
	}
	for name := range args {
		if _, ok := params[name]; !ok {
			return nil, nil, errBinding
		}
	}
	var segments []string
	for _, segment := range op.PathSegments() {
		if !strings.HasPrefix(segment, "{") {
			segments = append(segments, segment)
			continue
		}
		param := params[segment[1:len(segment)-1]]
		value, err := scalar(args[param.Name], param.Schema)
		if err != nil || strings.TrimSpace(value) == "" {
			return nil, nil, errBinding
		}
		segments = append(segments, value)
	}
	query := url.Values{}
	for _, param := range op.Parameters {
		if param.In != "query" {
			continue
		}
		value, present := args[param.Name]
		if !present || value == nil {
			if param.Required {
				return nil, nil, errBinding
			}
			continue
		}
		list, isList := value.([]any)
		if !isList {
			text, err := scalar(value, param.Schema)
			if err != nil {
				return nil, nil, errBinding
			}
			query.Set(param.Name, text)
			continue
		}
		items, _ := param.Schema["items"].(map[string]any)
		if items == nil {
			return nil, nil, errBinding
		}
		var texts []string
		for _, item := range list {
			text, err := scalar(item, items)
			// 콤마 목록 표기에서는 값 안의 콤마를 구분할 수 없으므로 거부한다.
			if err != nil || (!param.Explode && strings.Contains(text, ",")) {
				return nil, nil, errBinding
			}
			texts = append(texts, text)
		}
		switch {
		case len(texts) == 0:
			// 빈 배열은 필터를 보내지 않는 것과 같게 취급한다.
		case param.Explode:
			query[param.Name] = texts
		default:
			query.Set(param.Name, strings.Join(texts, ","))
		}
	}
	return segments, query, nil
}

// scalar는 스키마의 기본 타입에 맞춰 값을 문자열로 바꾼다.
func scalar(value any, schema map[string]any) (string, error) {
	var text string
	switch baseType(schema) {
	case "string":
		v, ok := value.(string)
		if !ok {
			return "", errBinding
		}
		text = v
	case "boolean":
		v, ok := value.(bool)
		if !ok {
			return "", errBinding
		}
		text = strconv.FormatBool(v)
	case "integer":
		n, ok := value.(json.Number)
		if !ok {
			return "", errBinding
		}
		r, ok := new(big.Rat).SetString(n.String())
		if !ok || !r.IsInt() {
			return "", errBinding
		}
		text = r.Num().String()
	case "number":
		n, ok := value.(json.Number)
		if !ok {
			return "", errBinding
		}
		f, err := n.Float64()
		if err != nil {
			return "", errBinding
		}
		text = strconv.FormatFloat(f, 'f', -1, 64)
	default:
		return "", errBinding
	}
	if strings.ContainsAny(text, "\x00\r\n") {
		return "", errBinding
	}
	return text, nil
}

// baseType은 정규화된 스키마의 null이 아닌 타입이다.
func baseType(schema map[string]any) string {
	switch typ := schema["type"].(type) {
	case string:
		return typ
	case []any:
		for _, item := range typ {
			if name, ok := item.(string); ok && name != "null" {
				return name
			}
		}
	}
	return ""
}
