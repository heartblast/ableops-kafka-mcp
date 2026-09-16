package tools

import (
	"context"
	"strings"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type ResourceBackupsInput struct {
	ClusterID string `json:"cluster_id"`
	Status    string `json:"status,omitempty"`
	Search    string `json:"search,omitempty"`
	From      string `json:"from,omitempty"`
	To        string `json:"to,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

type ResourceBackupsData struct {
	ableops.ResourceBackupPage
	Returned      int  `json:"returned"`
	HasMore       bool `json:"has_more"`
	PageTruncated bool `json:"page_truncated"`
}

type SampleMessagesInput struct {
	ClusterID string `json:"cluster_id"`
	TopicName string `json:"topic_name"`
	Limit     int    `json:"limit,omitempty"`
}

type SampleMessagesData struct {
	ListData[ableops.SampleMessage]
	TopicName     string `json:"topic_name"`
	Selection     string `json:"selection"`
	DataMode      string `json:"data_mode"`
	ContentPolicy string `json:"content_policy"`
}

type ACLPreviewInput struct {
	ClusterID   string   `json:"cluster_id"`
	TemplateKey string   `json:"template_key"`
	Principal   string   `json:"principal"`
	Topic       string   `json:"topic"`
	Group       string   `json:"group,omitempty"`
	Hosts       []string `json:"hosts"`
	Limit       int      `json:"limit,omitempty"`
}

type FlinkACLPreviewInput struct {
	ClusterID   string `json:"cluster_id"`
	Principal   string `json:"principal"`
	SourceTopic string `json:"source_topic,omitempty"`
	SourceGroup string `json:"source_group,omitempty"`
	SinkTopic   string `json:"sink_topic,omitempty"`
	TxnIDPrefix string `json:"txn_id_prefix,omitempty"`
	ExactlyOnce bool   `json:"exactly_once,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type DDLPreviewInput struct {
	ClusterID string              `json:"cluster_id"`
	TopicName string              `json:"topic_name"`
	TableName string              `json:"table_name"`
	Columns   []ableops.DDLColumn `json:"columns"`
}

func dataString(max int) map[string]any {
	return map[string]any{"type": "string", "minLength": 1, "maxLength": max}
}

func registerDataOperationsTools(server *mcp.Server, s *service) {
	backups := inputSchema(true, false, false)
	p := backups["properties"].(map[string]any)
	p["status"] = map[string]any{"type": "string", "enum": []string{"PENDING", "RUNNING", "COMPLETE", "PARTIAL", "FAILED", "IMPORTED"}}
	p["search"] = dataString(200)
	p["offset"] = map[string]any{"type": "integer", "minimum": 0, "maximum": 10000}
	for _, key := range []string{"from", "to"} {
		p[key] = dataString(64)
	}
	register(server, s, "list_resource_backups", "논리 구성 백업의 상태와 수집 시각을 한 페이지만 조회합니다. 메시지 데이터 백업이 아닙니다.", backups, func(i ResourceBackupsInput) string { return i.ClusterID }, s.resourceBackups)

	sample := inputSchema(true, false, false)
	p = sample["properties"].(map[string]any)
	p["topic_name"] = dataString(249)
	p["limit"] = map[string]any{"type": "integer", "minimum": 1, "maximum": 100, "description": "샘플 최대 개수. 기본 20"}
	sample["required"] = []string{"cluster_id", "topic_name"}
	registerWithAnnotations(server, s, "sample_topic_messages", "기능 플래그와 정확한 허용 토픽 정책이 필요합니다. 샘플 위치 메타데이터만 반환하며 key/value/header는 LLM에 전달하지 않습니다. 백엔드 감사 기록을 생성합니다.", sample, func(i SampleMessagesInput) string { return i.ClusterID }, &mcp.ToolAnnotations{ReadOnlyHint: false, IdempotentHint: false}, s.sampleMessages)

	acl := inputSchema(true, false, false)
	p = acl["properties"].(map[string]any)
	p["template_key"] = map[string]any{"type": "string", "enum": []string{"producer", "consumer", "flink-source", "flink-sink", "dlq-producer", "readonly", "flink", "siem"}}
	for _, key := range []string{"principal", "topic", "group"} {
		p[key] = dataString(255)
	}
	p["principal"] = map[string]any{"type": "string", "pattern": "^User:[a-zA-Z0-9._@-]{1,249}$"}
	p["hosts"] = map[string]any{"type": "array", "minItems": 1, "maxItems": 20, "uniqueItems": true, "items": dataString(255)}
	acl["required"] = []string{"cluster_id", "template_key", "principal", "topic", "hosts"}
	registerWithAnnotations(server, s, "preview_acl_plan", "템플릿의 생성 예정·중복 ACL과 정책 판정을 미리봅니다. 신청·ACL 반영은 없으며 최초 자산 스냅샷 동기화가 발생할 수 있습니다.", acl, func(i ACLPreviewInput) string { return i.ClusterID }, &mcp.ToolAnnotations{ReadOnlyHint: false, IdempotentHint: false}, s.previewACL)

	flink := inputSchema(true, false, false)
	p = flink["properties"].(map[string]any)
	for _, key := range []string{"principal", "source_topic", "source_group", "sink_topic", "txn_id_prefix"} {
		p[key] = dataString(255)
	}
	p["principal"] = map[string]any{"type": "string", "pattern": "^User:[a-zA-Z0-9._@-]{1,249}$"}
	p["exactly_once"] = map[string]any{"type": "boolean"}
	flink["required"] = []string{"cluster_id", "principal"}
	registerWithAnnotations(server, s, "preview_flink_acl_plan", "Flink Source/Sink 필요·보유·부족 ACL과 ACL 관측 여부를 확인합니다. 신청·반영은 없으며 최초 스냅샷 동기화가 발생할 수 있습니다.", flink, func(i FlinkACLPreviewInput) string { return i.ClusterID }, &mcp.ToolAnnotations{ReadOnlyHint: false, IdempotentHint: false}, s.previewFlinkACL)

	ddl := inputSchema(true, false, false)
	p = ddl["properties"].(map[string]any)
	delete(p, "limit")
	p["topic_name"] = dataString(249)
	p["table_name"] = map[string]any{"type": "string", "pattern": "^[a-z][a-z0-9_]{0,62}$"}
	p["columns"] = map[string]any{"type": "array", "minItems": 1, "maxItems": 100, "items": map[string]any{"type": "object", "additionalProperties": false, "required": []string{"name", "type"}, "properties": map[string]any{"name": map[string]any{"type": "string", "pattern": "^[a-z][a-z0-9_]{0,62}$"}, "type": map[string]any{"type": "string", "enum": []string{"STRING", "BOOLEAN", "INT", "BIGINT", "DOUBLE", "BYTES", "DATE", "TIMESTAMP(3)", "TIMESTAMP_LTZ(3)"}}}}}
	ddl["required"] = []string{"cluster_id", "topic_name", "table_name", "columns"}
	register(server, s, "preview_flink_ddl", "사용자가 명시한 컬럼으로 저장 없는 Source DDL 미리보기를 요청합니다. 샘플·임의 SQL·실행 없이 컬럼 선언만 반환하고 접속·인증 옵션은 제외합니다.", ddl, func(i DDLPreviewInput) string { return i.ClusterID }, s.previewDDL)
}

func (s *service) resourceBackups(ctx context.Context, i ResourceBackupsInput) Envelope[ResourceBackupsData] {
	out := newEnvelope[ResourceBackupsData](s.client.RedactContext(ctx, i.ClusterID), "backend_resource_backup_headers")
	if !validCluster(i.ClusterID) || i.Limit < 0 || i.Limit > 100 || !validDate(i.From) || !validDate(i.To) {
		return invalid(out, "cluster_id, limit과 날짜 형식을 확인하세요.")
	}
	page, err := s.client.ResourceBackups(ctx, i.ClusterID, ableops.BackupFilters{Status: i.Status, Search: i.Search, From: i.From, To: i.To, Limit: limitOrDefault(i.Limit), Offset: i.Offset})
	if err != nil {
		return failed(out, err, "resource_backups")
	}
	data := ResourceBackupsData{ResourceBackupPage: page, Returned: len(page.Items), HasMore: page.Offset+len(page.Items) < page.Total}
	out.Data = &data
	out.Truncated = data.HasMore || page.Offset > 0
	out.Status = "partial"
	out.Errors = []Failure{{Component: "resource_backups", Code: "observation_completeness_unknown", Message: "백엔드 저장소 조회 오류가 빈 목록·부분 목록으로 숨겨질 수 있어 집계 완전성을 확인할 수 없습니다."}}
	out.Limitations = []string{"논리 구성 백업이며 Kafka 메시지 데이터 백업이 아닙니다. 백업 상태는 조회 성공 상태와 별개입니다.", "이름·상태·시각·건수 헤더만 공개합니다. 전체 설정·경고·실패 원문은 제외합니다.", "현재 페이지이며 전체 백업 현황으로 해석하지 마세요. capture 시각과 MCP 조회 시각은 별개입니다."}
	bound(&out, func() bool {
		changed := shrinkItems(&data.Items)
		data.Returned = len(data.Items)
		data.PageTruncated = data.PageTruncated || changed
		return changed
	})
	return out
}

func (s *service) sampleMessages(ctx context.Context, i SampleMessagesInput) Envelope[SampleMessagesData] {
	out := newEnvelope[SampleMessagesData](s.client.RedactContext(ctx, i.ClusterID), "backend_message_positions_unknown_mode")
	limit := i.Limit
	if limit == 0 {
		limit = 20
	}
	items, err := s.client.SampleTopicMessages(ctx, i.ClusterID, i.TopicName, limit)
	if err != nil {
		return failed(out, err, "message_sample")
	}
	data := SampleMessagesData{ListData: listData(items, limit), TopicName: s.client.RedactContext(ctx, i.TopicName), Selection: "backend_partition_tail", DataMode: "unknown", ContentPolicy: "metadata_only"}
	out.Data = &data
	out.Status = "partial"
	out.Truncated = len(items) >= limit
	for _, m := range items {
		out.Truncated = out.Truncated || m.Truncated
	}
	out.Errors = []Failure{{Component: "message_sample", Code: "observation_completeness_unknown", Message: "백엔드가 실측·mock 구분과 파티션별 실패·시간 만료 여부를 제공하지 않습니다."}}
	out.Limitations = []string{"key/value/header는 응답·로그에 포함하지 않습니다. 정규식 비식별화나 원문 LLM 전달을 지원하지 않습니다.", "실제 어댑터는 파티션별 끝 offset 부근에서 읽습니다. 전체 최신순·무작위·모든 파티션의 대표 표본을 보장하지 않습니다.", "빈 표본은 빈 토픽 또는 조회 정상의 증거가 아닙니다. data_mode=unknown 결과를 실측 장애 분석에 사용하지 마세요.", "최대 5초·응답 256 KiB로 제한하며 서버 감사 기록을 생성합니다. timestamp는 레코드 시각이고 조회·관측 완료 시각이 아닙니다."}
	bound(&out, func() bool { changed := shrinkItems(&data.Items); data.Returned = len(data.Items); return changed })
	return out
}

func previewText(s string, required bool) bool {
	return (!required || strings.TrimSpace(s) != "") && len(s) <= 255 && !strings.ContainsAny(s, "\x00\r\n")
}

func (s *service) previewACL(ctx context.Context, i ACLPreviewInput) Envelope[ableops.ACLPlan] {
	out := newEnvelope[ableops.ACLPlan](s.client.RedactContext(ctx, i.ClusterID), "backend_acl_plan")
	if !validCluster(i.ClusterID) || !previewText(i.Principal, true) || !previewText(i.Topic, true) || !previewText(i.Group, false) || len(i.Hosts) < 1 || len(i.Hosts) > 20 || i.Limit < 0 || i.Limit > 100 {
		return invalid(out, "명시적인 대상과 템플릿·principal·topic·hosts가 필요합니다.")
	}
	for _, host := range i.Hosts {
		if !previewText(host, true) {
			return invalid(out, "hosts 형식을 확인하세요.")
		}
	}
	plan, err := s.client.PreviewACLPlan(ctx, i.ClusterID, ableops.ACLPlanInput{TemplateKey: i.TemplateKey, Principal: i.Principal, Topic: i.Topic, Group: i.Group, Hosts: i.Hosts})
	if err != nil {
		return failed(out, err, "acl_plan")
	}
	out.Data = &plan
	out.Status = "partial"
	out.Errors = []Failure{{Component: "acl_snapshot", Code: "observation_completeness_unknown", Message: "미리보기 API가 ACL 스냅샷 조회 실패·관측 시각을 제공하지 않아 중복 판정의 완전성을 확인할 수 없습니다."}}
	out.Limitations = []string{"정책 passed는 승인·반영·실제 접속 가능 여부가 아닙니다. 생성 예정 ACL만 계산하며 신청·반영 API는 호출하지 않습니다.", "백엔드 최초 조회가 자산 스냅샷을 저장할 수 있습니다. 정책 설명·임의 오류 원문은 제외합니다."}
	limit := limitOrDefault(i.Limit)
	for _, items := range []*[]ableops.PreviewACL{&plan.Plan.Generated, &plan.Plan.ToCreate, &plan.Plan.Duplicates} {
		out.Truncated = capItems(items, limit) || out.Truncated
	}
	out.Truncated = capItems(&plan.Policy.Violations, limit) || out.Truncated
	bound(&out, func() bool {
		return shrinkItems(&plan.Plan.Generated) || shrinkItems(&plan.Plan.ToCreate) || shrinkItems(&plan.Plan.Duplicates) || shrinkItems(&plan.Policy.Violations)
	})
	return out
}

func (s *service) previewFlinkACL(ctx context.Context, i FlinkACLPreviewInput) Envelope[ableops.FlinkACLPlan] {
	out := newEnvelope[ableops.FlinkACLPlan](s.client.RedactContext(ctx, i.ClusterID), "backend_flink_acl_plan")
	if !validCluster(i.ClusterID) || !previewText(i.Principal, true) || (i.SourceTopic == "" && i.SinkTopic == "") || i.Limit < 0 || i.Limit > 100 {
		return invalid(out, "cluster_id, principal과 Source 또는 Sink 토픽이 필요합니다.")
	}
	for _, text := range []string{i.SourceTopic, i.SourceGroup, i.SinkTopic, i.TxnIDPrefix} {
		if !previewText(text, false) {
			return invalid(out, "토픽·그룹·트랜잭션 식별자 형식을 확인하세요.")
		}
	}
	plan, err := s.client.PreviewFlinkACLPlan(ctx, i.ClusterID, ableops.FlinkACLPlanInput{Principal: i.Principal, SourceTopic: i.SourceTopic, SourceGroup: i.SourceGroup, SinkTopic: i.SinkTopic, TxnIDPrefix: i.TxnIDPrefix, ExactlyOnce: i.ExactlyOnce})
	if err != nil {
		return failed(out, err, "flink_acl_plan")
	}
	out.Data = &plan
	out.Limitations = []string{"ACL 보유는 실제 접속 가능 여부가 아닙니다. 정책 통과는 승인·반영·검증 완료가 아닙니다.", "스냅샷 조회 실패가 기존 값으로 숨겨질 수 있고 관측 시각이 없어 최신성을 보장하지 않습니다. 최초 스냅샷 동기화가 발생할 수 있습니다.", "aclKnown=false이면 held가 비어도 missing을 실제 권한 부족으로 확정하지 마세요. 신청·반영 경로는 호출하지 않습니다."}
	out.Status = "partial"
	out.Errors = []Failure{{Component: "acl_snapshot", Code: "observation_completeness_unknown", Message: "백엔드가 스냅샷 신선도와 조회 오류를 모두 제공하지 않습니다."}}
	limit := limitOrDefault(i.Limit)
	for _, items := range []*[]ableops.PreviewACL{&plan.Required, &plan.Held, &plan.Missing} {
		out.Truncated = capItems(items, limit) || out.Truncated
	}
	out.Truncated = capItems(&plan.PolicyResult.Violations, limit) || out.Truncated
	bound(&out, func() bool {
		return shrinkItems(&plan.Required) || shrinkItems(&plan.Held) || shrinkItems(&plan.Missing) || shrinkItems(&plan.PolicyResult.Violations)
	})
	return out
}

func (s *service) previewDDL(ctx context.Context, i DDLPreviewInput) Envelope[ableops.DDLPreview] {
	out := newEnvelope[ableops.DDLPreview](s.client.RedactContext(ctx, i.ClusterID), "backend_static_ddl_preview")
	preview, err := s.client.PreviewFlinkDDL(ctx, i.ClusterID, i.TopicName, i.TableName, i.Columns)
	if err != nil {
		return failed(out, err, "ddl_preview")
	}
	out.Data = &preview
	out.Truncated = true
	out.Limitations = []string{"접속·인증 정보를 포함하는 WITH 옵션 전체를 제외한 컬럼 선언 조각입니다. 실행 가능한 전체 DDL이 아닙니다.", "명시한 컬럼만 사용하는 정적 미리보기입니다. 샘플·스키마 추론·저장·SQL 실행·배포를 수행하지 않습니다. static_ok는 런타임 성공을 보장하지 않습니다."}
	if preview.ValidationStatus != "static_ok" {
		out.Status = "partial"
		out.Errors = []Failure{{Component: "ddl_validation", Code: "backend_validation_result", BackendStatus: preview.ValidationStatus, Message: "백엔드 정적 검증에서 경고 또는 오류가 보고되었습니다. 인증 설정이 포함될 수 있는 상세 문구는 생략합니다."}}
	}
	bound(&out, nil)
	return out
}
