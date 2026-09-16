// Package tools는 AbleOps 조회 API를 범위가 제한된 MCP 도구로 제공한다.
package tools

import (
	"encoding/json"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	DefaultLimit = 50
	MaxLimit     = 100
	// 구조화 결과와 호환용 텍스트를 합친 MCP 결과도 별도로 제한한다.
	MaxOutputBytes = 64 * 1024
	MaxResultBytes = 128 * 1024
)

type Failure struct {
	Component     string `json:"component,omitempty" jsonschema:"실패한 조회 구성 요소"`
	Code          string `json:"code" jsonschema:"안전하게 분류한 오류 코드"`
	Message       string `json:"message" jsonschema:"내부 오류 본문을 제외한 안내"`
	HTTPStatus    int    `json:"http_status,omitempty"`
	BackendStatus string `json:"backend_status,omitempty" jsonschema:"백엔드가 반환한 업무 상태"`
}

type Envelope[T any] struct {
	ClusterID   string    `json:"cluster_id,omitempty"`
	Status      string    `json:"status" jsonschema:"ok, partial, error 중 하나이며 Kafka 정상 여부 판정과 구분된다"`
	QueriedAt   string    `json:"queried_at" jsonschema:"MCP 조회 완료 시각이며 원본 데이터 관측 시각이 아니다"`
	Source      string    `json:"source,omitempty" jsonschema:"로컬 백엔드 구현으로 확인한 데이터 출처"`
	Data        *T        `json:"data,omitempty"`
	Truncated   bool      `json:"truncated" jsonschema:"출력 제한 또는 백엔드 제한으로 결과 일부가 생략되었는지 여부"`
	Limitations []string  `json:"limitations,omitempty"`
	Errors      []Failure `json:"errors,omitempty"`
}

type ListData[T any] struct {
	Items    []T `json:"items"`
	Returned int `json:"returned"`
	Received int `json:"received" jsonschema:"이 REST 응답에서 수신한 개수이며 전체 데이터 수와 다를 수 있다"`
	Limit    int `json:"limit" jsonschema:"MCP 출력의 최대 항목 수"`
}

func newEnvelope[T any](cluster, source string) Envelope[T] {
	return Envelope[T]{ClusterID: cluster, Status: "ok", Source: source}
}

func limitOrDefault(limit int) int {
	if limit == 0 {
		return DefaultLimit
	}
	return limit
}

func listData[T any](items []T, limit int) ListData[T] {
	limit = limitOrDefault(limit)
	count := len(items)
	if len(items) > limit {
		items = items[:limit]
	}
	if items == nil {
		items = []T{}
	}
	return ListData[T]{Items: items, Returned: len(items), Received: count, Limit: limit}
}

func shrinkItems[T any](items *[]T) bool {
	if len(*items) == 0 {
		return false
	}
	*items = (*items)[:len(*items)/2]
	return true
}

// bound는 실제 MCP 결과의 JSON 크기까지 계산한다. 긴 단일 항목도 무제한 반환하지 않는다.
func bound[T any](out *Envelope[T], shrink func() bool) {
	out.QueriedAt = time.Now().UTC().Format(time.RFC3339Nano)
	for {
		body, err := json.Marshal(out)
		if err == nil {
			result, resultErr := json.Marshal(&mcp.CallToolResult{
				StructuredContent: json.RawMessage(body),
				Content:           []mcp.Content{&mcp.TextContent{Text: string(body)}},
				IsError:           out.Status == "error",
			})
			if resultErr == nil && len(body) <= MaxOutputBytes && len(result) <= MaxResultBytes {
				return
			}
		}
		if !out.Truncated {
			out.Truncated = true
			out.Limitations = append(out.Limitations, "MCP 응답 바이트 제한으로 데이터 일부를 생략했습니다.")
		}
		if shrink != nil && shrink() {
			continue
		}
		out.Data = nil
		out.Status = "error"
		out.Errors = []Failure{{Code: "output_too_large", Message: "MCP 응답 크기 제한 안에 조회 결과를 담을 수 없습니다."}}
		out.Limitations = []string{"MCP 응답 바이트 제한으로 조회 데이터를 생략했습니다."}
		return
	}
}
