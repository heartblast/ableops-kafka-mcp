package tools

type ListInput struct {
	Limit int `json:"limit,omitempty" jsonschema:"MCP 출력 최대 개수, 기본 50, 최대 100"`
}

type ClusterInput struct {
	ClusterID string `json:"cluster_id" jsonschema:"반드시 명시하는 대상 클러스터 식별자"`
	Limit     int    `json:"limit,omitempty" jsonschema:"각 목록의 MCP 출력 최대 개수, 기본 50, 최대 100"`
}

type GroupInput struct {
	ClusterID string `json:"cluster_id"`
	GroupName string `json:"group_name"`
	Limit     int    `json:"limit,omitempty"`
}

type EventsInput struct {
	ClusterID        string   `json:"cluster_id"`
	Page             int      `json:"page,omitempty"`
	PageSize         int      `json:"page_size,omitempty"`
	Status           []string `json:"status,omitempty"`
	Severity         []string `json:"severity,omitempty"`
	Category         []string `json:"category,omitempty"`
	Search           string   `json:"search,omitempty"`
	From             string   `json:"from,omitempty"`
	To               string   `json:"to,omitempty"`
	Attention        []string `json:"attention,omitempty"`
	EventCode        []string `json:"event_code,omitempty"`
	ResourceType     string   `json:"resource_type,omitempty"`
	ResourceID       string   `json:"resource_id,omitempty"`
	Module           string   `json:"module,omitempty"`
	AssignedTo       string   `json:"assigned_to,omitempty"`
	Sort             string   `json:"sort,omitempty"`
	ExcludeSynthetic bool     `json:"exclude_synthetic,omitempty"`
}

func inputSchema(cluster, group, events bool) map[string]any {
	props := map[string]any{}
	required := []string{}
	if cluster {
		props["cluster_id"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 512, "description": "대상 클러스터 ID. 기본값으로 대체하지 않습니다."}
		required = append(required, "cluster_id")
	}
	if group {
		props["group_name"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 1024, "description": "대상 Consumer Group 이름"}
		required = append(required, "group_name")
	}
	if !events {
		props["limit"] = map[string]any{"type": "integer", "minimum": 1, "maximum": MaxLimit, "description": "MCP 출력 최대 개수. 생략하면 50이며 REST API에 없는 페이지 인자로 전달하지 않습니다."}
	} else {
		props["page"] = map[string]any{"type": "integer", "minimum": 1, "maximum": 10000, "description": "백엔드 이벤트 페이지 번호. 기본 1"}
		props["page_size"] = map[string]any{"type": "integer", "minimum": 1, "maximum": MaxLimit, "description": "백엔드 이벤트 페이지 크기. 기본 50"}
		for key, values := range map[string][]string{
			"status":    {"OPEN", "ACKNOWLEDGED", "SUPPRESSED", "RESOLVED"},
			"severity":  {"INFO", "WARN", "HIGH", "CRITICAL"},
			"category":  {"AVAILABILITY", "PERFORMANCE", "CAPACITY", "SECURITY", "CONFIGURATION", "INTEGRATION"},
			"attention": {"UNEVALUATED", "OBSERVE", "REVIEW", "URGENT"},
		} {
			props[key] = map[string]any{"type": "array", "maxItems": 20, "items": map[string]any{"type": "string", "enum": values}}
		}
		props["search"] = map[string]any{"type": "string", "maxLength": 200, "description": "백엔드 이벤트 검색 문자열"}
		props["event_code"] = map[string]any{"type": "array", "maxItems": 20, "uniqueItems": true, "items": map[string]any{"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Z][A-Z0-9_]*$"}, "description": "백엔드 카탈로그의 이벤트 코드"}
		props["resource_type"] = map[string]any{"type": "string", "enum": []string{"CLUSTER", "BROKER", "FILESYSTEM", "TOPIC", "PARTITION", "CONSUMER_GROUP", "PRINCIPAL", "ACL", "SCRAM", "METRIC_SOURCE"}}
		for key, maxLength := range map[string]int{"resource_id": 256, "module": 64, "assigned_to": 128} {
			props[key] = map[string]any{"type": "string", "maxLength": maxLength}
		}
		props["sort"] = map[string]any{"type": "string", "enum": []string{"-lastSeenAt", "lastSeenAt", "-firstSeenAt", "firstSeenAt", "-severity", "severity", "status", "-status", "-attention", "attention"}}
		props["exclude_synthetic"] = map[string]any{"type": "boolean", "description": "합성 이벤트 제외 여부. 생략하면 백엔드 기본값 false"}
		for _, key := range []string{"from", "to"} {
			props[key] = map[string]any{"type": "string", "maxLength": 64, "description": "RFC3339 시각 또는 YYYY-MM-DD 날짜"}
		}
	}
	return map[string]any{"type": "object", "properties": props, "required": required, "additionalProperties": false}
}
