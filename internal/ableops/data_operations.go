package ableops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ResourceBackup은 논리 구성 백업의 헤더만 공개하며 자원 본문·경고 원문을 제외한다.
type ResourceBackup struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	ClusterID          string  `json:"clusterId"`
	Status             string  `json:"status"`
	SchemaVersion      string  `json:"schemaVersion"`
	Profile            string  `json:"profile,omitempty"`
	WarningCount       int     `json:"warningCount"`
	FailureCount       int     `json:"failureCount"`
	SizeBytes          int64   `json:"sizeBytes"`
	Imported           bool    `json:"imported"`
	CreatedAt          string  `json:"createdAt"`
	UpdatedAt          string  `json:"updatedAt"`
	CaptureStartedAt   *string `json:"captureStartedAt,omitempty"`
	CaptureCompletedAt *string `json:"captureCompletedAt,omitempty"`
}

type ResourceBackupPage struct {
	Items  []ResourceBackup `json:"items"`
	Total  int              `json:"total"`
	Limit  int              `json:"limit"`
	Offset int              `json:"offset"`
}

type BackupFilters struct {
	Status, Search, From, To string
	Limit, Offset            int
}

func (c *Client) ResourceBackups(ctx context.Context, cluster string, f BackupFilters) (ResourceBackupPage, error) {
	var out ResourceBackupPage
	if !operationTarget(cluster) || f.Limit < 1 || f.Limit > 100 || f.Offset < 0 || f.Offset > 10000 || (f.Status != "" && !backupStatus(f.Status)) {
		return out, publicError("invalid_request", 0)
	}
	q := url.Values{"limit": {strconv.Itoa(f.Limit)}, "offset": {strconv.Itoa(f.Offset)}}
	for k, v := range map[string]string{"status": f.Status, "q": f.Search, "from": f.From, "to": f.To} {
		if v != "" {
			q.Set(k, v)
		}
	}
	if err := c.getCluster(ctx, cluster, []string{"resource-backups"}, q, &out); err != nil {
		return ResourceBackupPage{}, err
	}
	if out.Items == nil || out.Limit != f.Limit || out.Offset != f.Offset || out.Total < 0 || len(out.Items) > f.Limit || len(out.Items) > out.Total {
		return ResourceBackupPage{}, publicError("invalid_response", 200)
	}
	for _, item := range out.Items {
		if item.ID == "" || item.ClusterID != cluster || !backupStatus(item.Status) {
			return ResourceBackupPage{}, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

func backupStatus(s string) bool {
	switch s {
	case "PENDING", "RUNNING", "COMPLETE", "PARTIAL", "FAILED", "IMPORTED":
		return true
	}
	return false
}

// SampleMessage는 메시지 본문·키·헤더를 구조적으로 제외한 위치 메타데이터다.
type SampleMessage struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Truncated bool   `json:"truncated,omitempty"`
}

const MaxSampleResponseBytes = 256 * 1024

func operationTarget(s string) bool {
	return strings.TrimSpace(s) == s && s != "" && s != "." && s != ".." && len(s) <= 512 && !strings.ContainsAny(s, "/;,\x00\r\n")
}

func (c *Client) SampleTopicMessages(ctx context.Context, cluster, topic string, limit int) ([]SampleMessage, error) {
	if !operationTarget(cluster) || !operationTarget(topic) || limit < 1 || limit > 100 {
		return nil, publicError("invalid_request", 0)
	}
	if !c.sampleEnabled {
		return nil, &Error{Code: "feature_disabled", Message: "메시지 샘플 기능은 기본 비활성화되어 있습니다."}
	}
	if !c.sampleTopics[cluster+"/"+topic] {
		return nil, &Error{Code: "topic_not_allowed", Message: "샘플 허용 정책에 등록되지 않은 클러스터·토픽입니다."}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var out []SampleMessage
	// 이 고정 경로는 읽을 offset이 없을 때 null을 반환한다. 일반 Get의 null 거부는 유지한다.
	err := c.request(ctx, http.MethodGet, []string{"clusters", cluster, "topics", topic, "messages"}, url.Values{"limit": {strconv.Itoa(limit)}}, nil, &out, false, MaxSampleResponseBytes, true)
	if err != nil {
		return nil, err
	}
	if len(out) > limit {
		return nil, publicError("invalid_response", 200)
	}
	for _, m := range out {
		if _, err := time.Parse(time.RFC3339Nano, m.Timestamp); err != nil || m.Partition < 0 || m.Offset < 0 {
			return nil, publicError("invalid_response", 200)
		}
	}
	return out, nil
}

type ACLPlanInput struct {
	TemplateKey string   `json:"templateKey"`
	Principal   string   `json:"principal"`
	Topic       string   `json:"topic"`
	Group       string   `json:"group"`
	Hosts       []string `json:"hosts"`
}

// PreviewACL은 ACL 일곱 필드만 공개한다. 정책 메시지·설명·인증 설정은 포함하지 않는다.
type PreviewACL = SecurityACL

type ACLPlan struct {
	Plan struct {
		Generated  []PreviewACL `json:"generated"`
		ToCreate   []PreviewACL `json:"toCreate"`
		Duplicates []PreviewACL `json:"duplicates"`
	} `json:"plan"`
	Policy RequestPolicyResult `json:"policy"`
}

type FlinkACLPlanInput struct {
	Principal   string `json:"principal"`
	SourceTopic string `json:"sourceTopic"`
	SourceGroup string `json:"sourceGroup"`
	SinkTopic   string `json:"sinkTopic"`
	TxnIDPrefix string `json:"txnIdPrefix"`
	ExactlyOnce bool   `json:"exactlyOnce"`
}

type FlinkACLPlan struct {
	Required     []PreviewACL        `json:"required"`
	Held         []PreviewACL        `json:"held"`
	Missing      []PreviewACL        `json:"missing"`
	PolicyResult RequestPolicyResult `json:"policyResult"`
	ACLKnown     *bool               `json:"aclKnown"`
}

// preview는 호출부에서 확인한 세 미리보기 경로만 허용하며 변경 경로로 재시도하지 않는다.
func (c *Client) preview(ctx context.Context, cluster, kind string, in, out any) error {
	if !operationTarget(cluster) {
		return publicError("invalid_request", 0)
	}
	var segments []string
	switch kind {
	case "acls":
		segments = []string{"clusters", cluster, "acls", "plan"}
	case "flink-acl":
		segments = []string{"clusters", cluster, "flink-acl-plan"}
	case "ddl":
		segments = []string{"flink", "ddl-preview"}
	default:
		return publicError("unsupported", 0)
	}
	body, err := json.Marshal(in)
	if err != nil || len(body) > 32*1024 {
		return publicError("invalid_request", 0)
	}
	return c.request(ctx, http.MethodPost, segments, nil, body, out, false, MaxResponseBytes, false)
}

func (c *Client) PreviewACLPlan(ctx context.Context, cluster string, in ACLPlanInput) (ACLPlan, error) {
	var out ACLPlan
	if !previewPrincipal.MatchString(in.Principal) {
		return out, publicError("invalid_request", 0)
	}
	if err := c.preview(ctx, cluster, "acls", in, &out); err != nil {
		return ACLPlan{}, err
	}
	if out.Plan.Generated == nil || out.Plan.ToCreate == nil || out.Plan.Duplicates == nil || out.Policy.RiskLevel == "" {
		return ACLPlan{}, publicError("invalid_response", 200)
	}
	for _, items := range [][]PreviewACL{out.Plan.Generated, out.Plan.ToCreate, out.Plan.Duplicates} {
		for _, acl := range items {
			if acl.Principal != in.Principal {
				return ACLPlan{}, publicError("invalid_response", 200)
			}
		}
	}
	return out, nil
}

func (c *Client) PreviewFlinkACLPlan(ctx context.Context, cluster string, in FlinkACLPlanInput) (FlinkACLPlan, error) {
	var out FlinkACLPlan
	if !previewPrincipal.MatchString(in.Principal) {
		return out, publicError("invalid_request", 0)
	}
	if err := c.preview(ctx, cluster, "flink-acl", in, &out); err != nil {
		return FlinkACLPlan{}, err
	}
	if out.Required == nil || out.Held == nil || out.Missing == nil || out.ACLKnown == nil || out.PolicyResult.RiskLevel == "" {
		return FlinkACLPlan{}, publicError("invalid_response", 200)
	}
	for _, items := range [][]PreviewACL{out.Required, out.Held, out.Missing} {
		for _, acl := range items {
			if acl.Principal != in.Principal {
				return FlinkACLPlan{}, publicError("invalid_response", 200)
			}
		}
	}
	return out, nil
}

type DDLColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type DDLPreview struct {
	TableName        string `json:"table_name"`
	ColumnDDL        string `json:"column_ddl" jsonschema:"백엔드 생성 DDL의 컬럼 선언 부분. 접속·인증 옵션을 생략한 실행 불가능한 조각"`
	ValidationStatus string `json:"validation_status"`
}

var ddlIdentifier = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
var ddlTopic = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
var previewPrincipal = regexp.MustCompile(`^User:[a-zA-Z0-9._@-]{1,249}$`)

func ValidDDLColumns(table, topic string, columns []DDLColumn) bool {
	if !ddlIdentifier.MatchString(table) || !ddlTopic.MatchString(topic) || topic == "." || topic == ".." || len(columns) == 0 || len(columns) > 100 {
		return false
	}
	seen := map[string]bool{}
	for _, col := range columns {
		if !ddlIdentifier.MatchString(col.Name) || seen[col.Name] {
			return false
		}
		seen[col.Name] = true
		switch col.Type {
		case "STRING", "BOOLEAN", "INT", "BIGINT", "DOUBLE", "BYTES", "DATE", "TIMESTAMP(3)", "TIMESTAMP_LTZ(3)":
		default:
			return false
		}
	}
	return true
}

// PreviewFlinkDDL은 명시한 컬럼만 보내며 샘플 추론·SQL 입력·저장·실행은 호출하지 않는다.
func (c *Client) PreviewFlinkDDL(ctx context.Context, cluster, topic, table string, columns []DDLColumn) (DDLPreview, error) {
	if !ValidDDLColumns(table, topic, columns) {
		return DDLPreview{}, publicError("invalid_request", 0)
	}
	type field struct {
		FinalName string `json:"finalName"`
		FinalType string `json:"finalType"`
		Nullable  bool   `json:"nullable"`
	}
	fields := make([]field, 0, len(columns))
	for _, col := range columns {
		fields = append(fields, field{col.Name, col.Type, true})
	}
	in := struct {
		ClusterID string `json:"clusterId"`
		Topic     string `json:"topic"`
		TableName string `json:"tableName"`
		Format    string `json:"format"`
		Schema    struct {
			Fields []field `json:"fields"`
		} `json:"schema"`
		Metadata []struct{} `json:"metadata"`
	}{ClusterID: cluster, Topic: topic, TableName: table, Format: "json", Metadata: []struct{}{}}
	in.Schema.Fields = fields
	var out struct {
		TableName  string `json:"tableName"`
		SourceDDL  string `json:"sourceDdl"`
		Validation struct {
			Status string `json:"status"`
		} `json:"validation"`
	}
	if err := c.preview(ctx, cluster, "ddl", in, &out); err != nil {
		return DDLPreview{}, err
	}
	if out.TableName != table {
		return DDLPreview{}, publicError("invalid_response", 200)
	}
	switch out.Validation.Status {
	case "static_ok", "static_warn", "static_error":
	default:
		return DDLPreview{}, publicError("invalid_response", 200)
	}
	// 생성기의 고정 컬럼 선언과 일치할 때만 해당 부분을 공개한다. 인증 옵션은 검사·반환하지 않는다.
	var expected strings.Builder
	expected.WriteString("CREATE TABLE `" + table + "` (\n")
	for i, col := range columns {
		expected.WriteString("  `" + col.Name + "` " + col.Type)
		if i < len(columns)-1 {
			expected.WriteByte(',')
		}
		expected.WriteByte('\n')
	}
	prefix := expected.String()
	if !strings.HasPrefix(out.SourceDDL, prefix+") WITH (\n") {
		return DDLPreview{}, publicError("invalid_response", 200)
	}
	return DDLPreview{TableName: table, ColumnDDL: prefix + ")", ValidationStatus: out.Validation.Status}, nil
}
