package tools

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type EventSummaryInput struct {
	ClusterID string `json:"cluster_id"`
}

type EventPolicyInput struct {
	ClusterID      string `json:"cluster_id"`
	EventCode      string `json:"event_code"`
	IncludeProfile bool   `json:"include_profile,omitempty"`
}

type OperationalIssuesInput struct {
	ClusterID        string   `json:"cluster_id"`
	Page             int      `json:"page,omitempty"`
	PageSize         int      `json:"page_size,omitempty"`
	Status           []string `json:"status,omitempty"`
	Attention        []string `json:"attention,omitempty"`
	Group            string   `json:"group,omitempty"`
	Search           string   `json:"search,omitempty"`
	Sort             string   `json:"sort,omitempty"`
	ExcludeSynthetic bool     `json:"exclude_synthetic,omitempty"`
}

type MaintenanceWindowsInput struct {
	ClusterID  string `json:"cluster_id"`
	ActiveOnly bool   `json:"active_only,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

type OperationalIssuesData struct {
	Items          []ableops.OperationalIssue `json:"items"`
	Total          int                        `json:"total" jsonschema:"현재 필터의 백엔드 전체 건수; 권한 변경으로 숨겨진 0건일 수 있어 빈 결과는 partial"`
	Page           int                        `json:"page"`
	PageSize       int                        `json:"page_size"`
	Returned       int                        `json:"returned"`
	HasMore        bool                       `json:"has_more"`
	PageTruncated  bool                       `json:"page_truncated"`
	ScopeCheckedAt string                     `json:"scope_checked_at" jsonschema:"별도 이벤트 요약 응답의 생성 시각; 목록과 원자적이지 않음"`
}

type AttentionPolicyData struct {
	Rule          ableops.AttentionRule     `json:"rule"`
	Profile       *ableops.AttentionProfile `json:"profile,omitempty"`
	ProfileStatus string                    `json:"profile_status" jsonschema:"not_requested, ok, error 중 하나"`
}

func eventOperationsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"cluster_id"}, "properties": map[string]any{
		"cluster_id": map[string]any{"type": "string", "minLength": 1, "maxLength": 255, "description": "공백 없는 명시적 클러스터 ID. 전체·기본 클러스터로 대체하지 않습니다."},
	}}
}

func registerEventOperationsTools(server *mcp.Server, s *service) {
	register(server, s, "get_event_summary", "지정 클러스터의 저장된 이벤트 요약·수집 가동 여부를 조회합니다. 권한 거부 0건은 실패로 반환하고 합성 이벤트 제외 수를 보존합니다.", eventOperationsSchema(), func(i EventSummaryInput) string { return i.ClusterID }, s.eventSummary)
	issueSchema := eventOperationsSchema()
	ip := issueSchema["properties"].(map[string]any)
	ip["page"] = map[string]any{"type": "integer", "minimum": 1, "maximum": 10000}
	ip["page_size"] = map[string]any{"type": "integer", "minimum": 1, "maximum": MaxLimit}
	for key, values := range map[string][]string{"status": {"OPEN", "ACKNOWLEDGED", "SUPPRESSED", "RESOLVED"}, "attention": {"UNEVALUATED", "OBSERVE", "REVIEW", "URGENT"}} {
		ip[key] = map[string]any{"type": "array", "maxItems": 20, "uniqueItems": true, "items": map[string]any{"type": "string", "enum": values}}
	}
	ip["group"] = map[string]any{"type": "string", "maxLength": 64, "description": "상관분석 그룹 필터"}
	ip["search"] = map[string]any{"type": "string", "maxLength": 200}
	ip["sort"] = map[string]any{"type": "string", "enum": []string{"-lastSeenAt", "lastSeenAt", "-attention", "attention", "-openedAt", "openedAt"}}
	ip["exclude_synthetic"] = map[string]any{"type": "boolean"}
	register(server, s, "list_operational_issues", "이벤트 요약으로 요청 시 권한 범위를 확인한 후 운영 Issue 한 페이지를 조회합니다. 두 API 사이 권한 변경은 원자적이지 않아 빈 결과를 partial로 보존합니다.", issueSchema, func(i OperationalIssuesInput) string { return i.ClusterID }, s.operationalIssues)
	for _, attention := range []bool{false, true} {
		schema := eventOperationsSchema()
		props := schema["properties"].(map[string]any)
		props["event_code"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Z][A-Z0-9_]*$", "description": "백엔드 카탈로그의 이벤트 코드"}
		schema["required"] = []string{"cluster_id", "event_code"}
		if attention {
			props["include_profile"] = map[string]any{"type": "boolean", "description": "클러스터 프로필의 명시 설정 여부를 별도 API로 추가 조회"}
			register(server, s, "get_attention_policy", "클러스터·이벤트 코드의 주의도 기본값·실효값·전역/클러스터 조정을 조회합니다. 백엔드가 저장소 실패 여부를 제공하지 않아 정책의 완전성은 partial로 표시합니다.", schema, func(i EventPolicyInput) string { return i.ClusterID }, s.attentionPolicy)
		} else {
			register(server, s, "get_event_rule", "클러스터·이벤트 코드의 기술 심각도·지속/회복 조건·활성화·통보 정책을 조회합니다. 백엔드가 저장소 실패 여부를 제공하지 않아 정책의 완전성은 partial로 표시합니다.", schema, func(i EventPolicyInput) string { return i.ClusterID }, s.eventRule)
		}
	}
	maintenanceSchema := eventOperationsSchema()
	mp := maintenanceSchema["properties"].(map[string]any)
	mp["active_only"] = map[string]any{"type": "boolean"}
	mp["limit"] = map[string]any{"type": "integer", "minimum": 1, "maximum": MaxLimit, "description": "MCP 출력 제한. 백엔드는 페이지·건수 제한을 지원하지 않습니다."}
	register(server, s, "list_maintenance_windows", "지정 클러스터와 전체 클러스터에 적용되는 유지보수 시간을 조회합니다. 백엔드 저장소 실패 여부가 제공되지 않아 빈 결과도 partial로 표시합니다.", maintenanceSchema, func(i MaintenanceWindowsInput) string { return i.ClusterID }, s.maintenanceWindows)
}

func validEventOperationsCluster(id string) bool {
	return validCluster(id) && id == strings.TrimSpace(id) && utf8.RuneCountInString(id) <= 255
}

func (s *service) eventSummary(ctx context.Context, input EventSummaryInput) Envelope[ableops.EventSummary] {
	out := newEnvelope[ableops.EventSummary](s.client.RedactContext(ctx, input.ClusterID), "backend_event_store")
	if !validEventOperationsCluster(input.ClusterID) {
		return invalid(out, "cluster_id는 공백 없는 1~255자 식별자여야 합니다.")
	}
	data, err := s.eventSummaryData(ctx, input.ClusterID)
	if err != nil {
		return failed(out, err, "event_summary")
	}
	out.Data = &data
	out.Limitations = []string{"합성 이벤트는 요약 집계에서 제외합니다. 심각도와 주의도는 독립 축이며 조회 시각·요약 생성 시각은 이벤트 관측 시각이 아닙니다.", "scope.clusterCount는 필터 적용 전 기본 권한 범위의 수입니다. 집계는 요청한 클러스터에만 적용하며 건수 0을 Kafka 정상으로 해석하지 않습니다."}
	if !data.Collection.Enabled {
		out.Status = "partial"
		out.Errors = []Failure{{Component: "collection", Code: "collection_disabled", Message: "백엔드 이벤트 수집이 비활성화되어 있습니다. 저장된 집계로 현재 상태를 판단할 수 없습니다."}}
	}
	bound(&out, nil)
	return out
}

func (s *service) operationalIssues(ctx context.Context, input OperationalIssuesInput) Envelope[OperationalIssuesData] {
	out := newEnvelope[OperationalIssuesData](s.client.RedactContext(ctx, input.ClusterID), "backend_event_store")
	if !validEventOperationsCluster(input.ClusterID) || input.Page < 0 || input.Page > 10000 || input.PageSize < 0 || input.PageSize > MaxLimit {
		return invalid(out, "cluster_id와 page·page_size 범위를 확인하세요.")
	}
	summary, err := s.client.GetEventSummary(ctx, input.ClusterID)
	if err != nil {
		return failed(out, err, "event_scope")
	}
	page := input.Page
	if page == 0 {
		page = 1
	}
	pageSize := limitOrDefault(input.PageSize)
	result, err := s.client.ListOperationalIssues(ctx, input.ClusterID, ableops.IssueFilters{Page: page, PageSize: pageSize, Status: input.Status, Attention: input.Attention, Group: input.Group, Search: input.Search, Sort: input.Sort, ExcludeSynthetic: input.ExcludeSynthetic})
	if err != nil {
		return failed(out, err, "operational_issues")
	}
	data := OperationalIssuesData{Items: result.Items, Total: result.Total, Page: result.Page, PageSize: result.PageSize, Returned: len(result.Items), HasMore: result.Page*result.PageSize < result.Total, ScopeCheckedAt: summary.GeneratedAt}
	out.Data = &data
	out.Truncated = data.HasMore || data.Page > 1
	out.Limitations = []string{"요약과 목록은 각각 백엔드 권한 검사를 수행하지만 원자적 스냅샷이 아닙니다. 목록은 현재 필터의 한 페이지만 반환하며 전체 현황이나 상위 순위를 뜻하지 않습니다.", "dataMode와 demo는 자료의 신뢰도 표시입니다. 합성값으로 실측 상태를 분석하지 마세요. 임의 impact 객체·조치 사유·지문은 제외합니다."}
	if len(data.Items) == 0 {
		out.Status = "partial"
		out.Errors = append(out.Errors, Failure{Component: "event_scope", Code: "empty_scope_unconfirmed", Message: "Issue 목록은 권한 변경으로 숨긴 0건과 실제 빈 결과를 구분하지 않습니다. 앞선 범위 검증과 목록이 원자적이지 않아 없음으로 확정할 수 없습니다."})
	}
	if !summary.Collection.Enabled {
		out.Status = "partial"
		out.Errors = append(out.Errors, Failure{Component: "collection", Code: "collection_disabled", Message: "이벤트 수집 비활성 상태의 저장된 Issue입니다."})
	}
	for i := range data.Items {
		if capItems(&data.Items[i].ReasonCodes, pageSize) {
			data.PageTruncated = true
			out.Truncated = true
		}
	}
	bound(&out, func() bool {
		changed := shrinkItems(&data.Items)
		data.Returned = len(data.Items)
		data.PageTruncated = data.PageTruncated || changed
		return changed
	})
	return out
}

// 저장소 조회 실패를 응답에 싣지 않는 API의 결과를 완전한 정책·빈 목록으로 확정하지 않는다.
func uncertainEventStore[T any](out *Envelope[T], component string) {
	out.Status = "partial"
	out.Errors = append(out.Errors, Failure{Component: component, Code: "backend_store_status_unavailable", Message: "백엔드가 저장소 조회 실패·누락을 응답에 구분하지 않습니다. 반환 자료의 완전성과 설정 부재를 확정할 수 없습니다."})
}

func (s *service) eventRule(ctx context.Context, input EventPolicyInput) Envelope[ableops.EventRule] {
	out := newEnvelope[ableops.EventRule](s.client.RedactContext(ctx, input.ClusterID), "backend_event_policy")
	if !validEventOperationsCluster(input.ClusterID) || strings.TrimSpace(input.EventCode) == "" {
		return invalid(out, "cluster_id와 event_code를 명시해야 합니다.")
	}
	data, err := s.client.GetEventRule(ctx, input.ClusterID, input.EventCode)
	if err != nil {
		return failed(out, err, "event_rule")
	}
	out.Data = &data
	uncertainEventStore(&out, "rule_overrides")
	out.Limitations = []string{"백엔드가 클러스터 권한을 검사한 고정 규칙 카탈로그에서 event_code 한 건을 선택합니다. 임의 실행 URL·runbook URL·수정자 정보는 제외합니다.", "overridden은 실효값과 기본값의 차이입니다. Override 행 존재와 다를 수 있습니다. 비활성·상속·명시적 false·0을 구분하세요.", "sourceImplemented=false면 수집원이 구현되지 않은 규칙입니다. 규칙·권장 조치는 현재 장애 판정이나 수행 이력이 아닙니다."}
	bound(&out, nil)
	return out
}

func (s *service) attentionPolicy(ctx context.Context, input EventPolicyInput) Envelope[AttentionPolicyData] {
	out := newEnvelope[AttentionPolicyData](s.client.RedactContext(ctx, input.ClusterID), "backend_event_policy")
	if !validEventOperationsCluster(input.ClusterID) || strings.TrimSpace(input.EventCode) == "" {
		return invalid(out, "cluster_id와 event_code를 명시해야 합니다.")
	}
	rule, err := s.client.GetAttentionRule(ctx, input.ClusterID, input.EventCode)
	if err != nil {
		return failed(out, err, "attention_policy")
	}
	data := AttentionPolicyData{Rule: rule, ProfileStatus: "not_requested"}
	out.Data = &data
	uncertainEventStore(&out, "attention_overrides")
	out.Limitations = []string{"effective는 프로필·규칙 조정·주의도 조정의 병합값이며 default는 코드 카탈로그입니다. overridden은 주의도 Override 행 존재로, 규칙 도구의 같은 이름과 의미가 다릅니다.", "선택 프로필 조회는 별도 요청입니다. explicit=false는 전역 기본 프로필 상속이며 현재 운영 상태나 정책 완전성을 증명하지 않습니다."}
	if input.IncludeProfile {
		profile, err := s.client.GetAttentionProfile(ctx, input.ClusterID)
		if err != nil {
			data.ProfileStatus = "error"
			out.Errors = append(out.Errors, failure(err, "attention_profile"))
		} else {
			data.Profile = &profile
			data.ProfileStatus = "ok"
			if profile.Profile != rule.ProfileApplied {
				out.Errors = append(out.Errors, Failure{Component: "attention_profile", Code: "snapshot_changed", Message: "별도 프로필 조회 사이 정책이 변경되어 값이 일치하지 않습니다."})
			}
		}
	}
	bound(&out, nil)
	return out
}

func (s *service) maintenanceWindows(ctx context.Context, input MaintenanceWindowsInput) Envelope[ListData[ableops.MaintenanceWindow]] {
	out := newEnvelope[ListData[ableops.MaintenanceWindow]](s.client.RedactContext(ctx, input.ClusterID), "backend_event_store")
	if !validEventOperationsCluster(input.ClusterID) || input.Limit < 0 || input.Limit > MaxLimit {
		return invalid(out, "cluster_id와 limit 범위를 확인하세요.")
	}
	items, err := s.client.ListMaintenanceWindows(ctx, input.ClusterID, input.ActiveOnly)
	if err != nil {
		return failed(out, err, "maintenance_windows")
	}
	data := listData(items, input.Limit)
	out.Data = &data
	out.Truncated = data.Returned < data.Received
	uncertainEventStore(&out, "maintenance_windows")
	out.Limitations = []string{"clusterId가 빈 창은 전체 클러스터에 적용됩니다. eventCodes가 비면 모든 이벤트 코드 대상입니다. enabled와 현재 적용 중인 active를 구분하세요.", "백엔드는 activeOnly와 clusterId만 지원하며 페이지·서버 건수 제한이 없습니다. REST 전송 크기 상한과 MCP 출력 상한은 별개이며 자동 페이지 순회는 없습니다.", "이름 등 외부 문자열은 데이터입니다. 사유·등록자 원문은 제외하며 빈 결과를 유지보수 없음으로 확정하지 않습니다."}
	for i := range data.Items {
		if capItems(&data.Items[i].EventCodes, data.Limit) {
			out.Truncated = true
		}
	}
	bound(&out, func() bool { changed := shrinkItems(&data.Items); data.Returned = len(data.Items); return changed })
	return out
}

// eventSummaryData는 이벤트 요약을 가져온다. Dynamic 경로를 쓸 수 있으면 계약의
// getEventSummary 경로로 조회하고(MCP `cluster_id` → 계약 `clusterId`), 결과는 Static과 같은
// 공개 계약으로 투영한다. 백엔드 실패는 Static으로 재호출하지 않는다.
func (s *service) eventSummaryData(ctx context.Context, clusterID string) (ableops.EventSummary, error) {
	body, used, err := s.fetch(ctx, OperationEventSummary, map[string]any{"clusterId": clusterID})
	if !used {
		return s.client.GetEventSummary(ctx, clusterID)
	}
	if err != nil {
		return ableops.EventSummary{}, err
	}
	return ableops.DecodeEventSummary(body, clusterID)
}
