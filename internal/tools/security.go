package tools

import (
	"context"
	"strings"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type IdentitiesInput struct {
	ClusterID   string `json:"cluster_id"`
	Query       string `json:"query,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Stage       string `json:"stage,omitempty"`
	Department  string `json:"department,omitempty"`
	Environment string `json:"environment,omitempty"`
	Severity    string `json:"severity,omitempty"`
	Finding     string `json:"finding,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type IdentityDetailInput struct {
	ClusterID string `json:"cluster_id"`
	Principal string `json:"principal"`
	Limit     int    `json:"limit,omitempty"`
}

type ACLsInput struct {
	ClusterID string `json:"cluster_id"`
	Principal string `json:"principal,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type IdentityInventoryData struct {
	ListData[ableops.Identity]
	Summary ableops.IdentitySummary `json:"summary" jsonschema:"백엔드 전체 인벤토리 기준 집계로 검색 필터 및 출력 limit과 무관하다"`
}

var identityFilterEnums = map[string][]string{
	"kind":     {"SERVICE", "PERSON", "SYSTEM", "UNKNOWN"},
	"stage":    {"UNMANAGED", "DRAFT", "INCOMPLETE", "ACTIVE", "REVIEW_DUE", "EXPIRING", "EXPIRED", "LOCKED", "DORMANT", "REVOKED"},
	"severity": {"OK", "WARN", "CRITICAL"},
	"finding":  {"UNMANAGED", "NO_OWNER", "NO_PURPOSE", "NO_ENVIRONMENT", "AUTH_GAP", "ORPHAN_CREDENTIAL", "NAMING_VIOLATION", "WILDCARD_HOST", "HIGH_RISK_ACL", "REVIEW_OVERDUE", "EXPIRY_IMMINENT", "EXPIRED"},
}

func registerSecurityTools(server *mcp.Server, s *service) {
	identities := inputSchema(true, false, false)
	props := identities["properties"].(map[string]any)
	for _, key := range []string{"query", "department", "environment"} {
		props[key] = map[string]any{"type": "string", "maxLength": 200}
	}
	for key, values := range identityFilterEnums {
		props[key] = map[string]any{"type": "string", "enum": values}
	}
	// 목록과 진단은 백엔드 자산 캐시의 지연 동기화를 유발할 수 있다.
	sideEffects := &mcp.ToolAnnotations{ReadOnlyHint: false, IdempotentHint: false}
	registerWithAnnotations(server, s, "list_identities", "클러스터 계정의 선언 상태·계산 단계·추론 출처를 구분해 조회합니다. 원천 조회 실패를 보고하지 않는 백엔드 제약으로 완전성을 확정하지 않습니다.", identities, func(i IdentitiesInput) string { return i.ClusterID }, sideEffects, s.listIdentities)
	detail := inputSchema(true, false, false)
	detail["properties"].(map[string]any)["principal"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 1024, "description": "실제 Principal 식별자. 예: User:svc-orders"}
	detail["required"] = []string{"cluster_id", "principal"}
	registerWithAnnotations(server, s, "get_identity_detail", "계정 메타·생명주기·ACL·비밀 없는 SCRAM 메타를 조회합니다. 전체 감사 payload와 클러스터가 없는 Grant는 공개하지 않습니다.", detail, func(i IdentityDetailInput) string { return i.ClusterID }, sideEffects, s.identityDetail)
	acls := inputSchema(true, false, false)
	acls["properties"].(map[string]any)["principal"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 1024, "description": "선택 시 백엔드 Principal별 ACL 경로로 범위를 제한합니다."}
	registerWithAnnotations(server, s, "list_acls", "클러스터 ACL 스냅샷을 조회하며 Principal 조건을 지원합니다. 전체 ACL 조회는 백엔드 자동 동기화와 감사 기록을 유발할 수 있습니다. ACL 보유는 실제 접속 보장이 아닙니다.", acls, func(i ACLsInput) string { return i.ClusterID }, sideEffects, s.listACLs)
	registerWithAnnotations(server, s, "get_acl_risk", "기존 ACL의 정책 진단을 조회합니다. 미참조·미사용 의심은 실제 미사용 확정이 아니며 보조 데이터 조회 완전성을 확인할 수 없습니다.", inputSchema(true, false, false), func(i ClusterInput) string { return i.ClusterID }, sideEffects, s.aclRisk)
	registerWithAnnotations(server, s, "get_scram_audit", "SCRAM 정책 진단을 조회합니다. 변경 감사 이력이 아닙니다. 백엔드가 자산 자동 동기화와 조회 감사 기록을 수행할 수 있습니다.", inputSchema(true, false, false), func(i ClusterInput) string { return i.ClusterID }, sideEffects, s.scramAudit)
	register(server, s, "list_requests", "백엔드가 객체별 조회 권한과 cluster 필터를 적용한 신청 목록을 조회합니다. 승인·반영·검증 상태를 구분하며 신청 payload는 대상 식별자만 공개합니다.", inputSchema(true, false, false), func(i ClusterInput) string { return i.ClusterID }, s.listRequests)
}

func validSecurityInput(cluster string, limit int) bool {
	return validCluster(cluster) && len(cluster) <= 512 && limit >= 0 && limit <= MaxLimit
}

func securityIncomplete[T any](out *Envelope[T], component, message string) {
	out.Status = "partial"
	out.Errors = append(out.Errors, Failure{Component: component, Code: "observation_completeness_unknown", Message: message, HTTPStatus: 200})
}

const identityIncomplete = "백엔드는 SCRAM·ACL 원천 조회 실패를 응답에 표시하지 않습니다. 빈 목록·부재·계산 단계를 완전한 관측으로 확정하지 마세요."
const securityModeUnknown = "백엔드 응답에 실측·mock 어댑터 구분이 없습니다. 이 결과만으로 실측 데이터임을 확정하지 마세요."

func identityLimitations() []string {
	return []string{identityIncomplete, "status는 선언 상태, lifecycle은 계산 단계, identityKindSource=inferred는 추론입니다. ACL 보유는 접속 가능, credential.locked는 인증 차단을 뜻하지 않습니다.", "관측 시각을 제공하지 않습니다. queried_at은 MCP 조회 완료 시각입니다. limit은 출력 제한이며 백엔드 전체 인벤토리 수집량을 줄이지 않습니다.", securityModeUnknown}
}

func trimIdentity(v *ableops.Identity, limit int) bool {
	changed := capItems(&v.Credential.Items, limit)
	changed = capItems(&v.Findings, limit) || changed
	changed = capItems(&v.Lifecycle.Reasons, limit) || changed
	return capItems(&v.ACLs, limit) || changed
}

func (s *service) listIdentities(ctx context.Context, input IdentitiesInput) Envelope[IdentityInventoryData] {
	out := newEnvelope[IdentityInventoryData](s.client.RedactContext(ctx, input.ClusterID), "backend_identity_composite")
	if !validSecurityInput(input.ClusterID, input.Limit) {
		return invalid(out, "cluster_id와 허용 범위의 limit을 명시해야 합니다.")
	}
	for _, value := range []string{input.Query, input.Department, input.Environment} {
		if len(value) > 800 {
			return invalid(out, "검색 문자열이 너무 깁니다.")
		}
	}
	for key, value := range map[string]string{"kind": input.Kind, "stage": input.Stage, "severity": input.Severity, "finding": input.Finding} {
		if value == "" {
			continue
		}
		found := false
		for _, allowed := range identityFilterEnums[key] {
			found = found || value == allowed
		}
		if !found {
			return invalid(out, "지원하지 않는 계정 필터 값입니다.")
		}
	}
	items, err := s.client.ListIdentities(ctx, input.ClusterID, ableops.IdentityFilters{Query: input.Query, Kind: input.Kind, Stage: input.Stage, Department: input.Department, Environment: input.Environment, Severity: input.Severity, Finding: input.Finding})
	if err != nil {
		return failed(out, err, "identities")
	}
	data := IdentityInventoryData{ListData: listData(items.Items, input.Limit), Summary: items.Summary}
	out.Data = &data
	out.Truncated = data.Returned < data.Received
	for i := range data.Items {
		out.Truncated = trimIdentity(&data.Items[i], data.Limit) || out.Truncated
	}
	out.Limitations = append(identityLimitations(), "summary는 필터 적용 전 전체 인벤토리 집계입니다. received는 이번 필터 결과이며 returned는 MCP 출력 개수입니다.")
	securityIncomplete(&out, "identities", identityIncomplete)
	bound(&out, func() bool { changed := shrinkItems(&data.Items); data.Returned = len(data.Items); return changed })
	return out
}

func (s *service) identityDetail(ctx context.Context, input IdentityDetailInput) Envelope[ableops.Identity] {
	out := newEnvelope[ableops.Identity](s.client.RedactContext(ctx, input.ClusterID), "backend_identity_composite")
	if !validSecurityInput(input.ClusterID, input.Limit) || strings.TrimSpace(input.Principal) == "" || len(input.Principal) > 1024 {
		return invalid(out, "cluster_id, principal과 허용 범위의 limit을 명시해야 합니다.")
	}
	data, err := s.client.IdentityDetail(ctx, input.ClusterID, input.Principal)
	if err != nil {
		return failed(out, err, "identity")
	}
	out.Data = &data
	out.Truncated = trimIdentity(&data, limitOrDefault(input.Limit))
	out.Limitations = append(identityLimitations(), "백엔드는 미등록 Principal에도 빈 상세를 생성할 수 있어 상세 응답만으로 계정 존재를 확정하지 않습니다.", "Grant에는 클러스터 식별자가 없고 최근 감사에는 별도 감사 권한 검사가 없어 두 필드를 공개하지 않습니다. 도달성·연계 부가 정보도 공개하지 않습니다.")
	securityIncomplete(&out, "identity", identityIncomplete)
	bound(&out, func() bool {
		return shrinkItems(&data.ACLs) || shrinkItems(&data.Credential.Items) || shrinkItems(&data.Findings) || shrinkItems(&data.Lifecycle.Reasons)
	})
	return out
}

func (s *service) listACLs(ctx context.Context, input ACLsInput) Envelope[SnapshotData[ableops.SecurityACL]] {
	out := newEnvelope[SnapshotData[ableops.SecurityACL]](s.client.RedactContext(ctx, input.ClusterID), "backend_asset_snapshot")
	if !validSecurityInput(input.ClusterID, input.Limit) || len(input.Principal) > 1024 || (input.Principal != "" && strings.TrimSpace(input.Principal) == "") {
		return invalid(out, "cluster_id, principal과 허용 범위의 limit을 확인하세요.")
	}
	var snapshot ableops.Snapshot[ableops.SecurityACL]
	var err error
	if input.Principal == "" {
		snapshot, err = s.client.ListACLs(ctx, input.ClusterID)
	} else {
		snapshot, err = s.client.ListPrincipalACLs(ctx, input.ClusterID, input.Principal)
	}
	if err != nil {
		return failed(out, err, "acls")
	}
	out = snapshotResult(out, snapshot, input.Limit)
	if input.Principal != "" {
		out.Limitations = []string{"백엔드가 지정 Principal의 ACL 목록을 반환합니다. limit은 MCP 출력에만 적용되며 REST 전송 크기를 줄이지 않습니다.", "Principal별 API는 동기화 시각과 원천 조회 실패를 제공하지 않습니다. 빈 결과와 최신성을 확정하지 마세요."}
	}
	out.Limitations = append(out.Limitations, "ACL 보유는 실제 접속 가능 여부나 사용 실적을 증명하지 않습니다. 백엔드가 지원하는 Principal 범위 외의 필터·페이지는 제공하지 않습니다.", securityModeUnknown)
	bound(&out, func() bool {
		if out.Data == nil {
			return false
		}
		changed := shrinkItems(&out.Data.Items)
		out.Data.Returned = len(out.Data.Items)
		return changed
	})
	return out
}

func (s *service) aclRisk(ctx context.Context, input ClusterInput) Envelope[ableops.ACLRiskReport] {
	out := newEnvelope[ableops.ACLRiskReport](s.client.RedactContext(ctx, input.ClusterID), "backend_acl_policy_diagnosis")
	if !validSecurityInput(input.ClusterID, input.Limit) {
		return invalid(out, "cluster_id와 허용 범위의 limit을 명시해야 합니다.")
	}
	data, err := s.client.ACLRisk(ctx, input.ClusterID)
	if err != nil {
		return failed(out, err, "acl_risk")
	}
	out.Data = &data
	out.Truncated = capItems(&data.Findings, limitOrDefault(input.Limit))
	out.Truncated = capItems(&data.Principals, limitOrDefault(input.Limit)) || out.Truncated
	out.Limitations = []string{"정책 진단은 접속 성공이나 실제 미사용 확정이 아닙니다. 잠금 면제는 포털 삭제·재발급 통제이며 Kafka 인증 차단이 아닙니다.", "total·critical·warn은 백엔드 원본 집계이며 MCP에서 잘린 목록의 개수와 다를 수 있습니다.", "부가 SCRAM·자산 조회의 실패/최신성을 응답에서 확인할 수 없습니다. generatedAt은 진단 생성 시각이며 각 원천 관측 시각과 다릅니다.", securityModeUnknown}
	if failure := backendFailure(data.Status, "acl_risk"); failure != nil {
		out.Status = "error"
		out.Errors = []Failure{*failure}
	} else {
		securityIncomplete(&out, "acl_risk_enrichment", "부가 SCRAM·자산 조회 성공 여부가 제공되지 않아 전체 진단 완전성을 확정할 수 없습니다.")
	}
	bound(&out, func() bool { return shrinkItems(&data.Findings) || shrinkItems(&data.Principals) })
	return out
}

func (s *service) scramAudit(ctx context.Context, input ClusterInput) Envelope[ableops.ScramAuditReport] {
	out := newEnvelope[ableops.ScramAuditReport](s.client.RedactContext(ctx, input.ClusterID), "backend_scram_policy_diagnosis")
	if !validSecurityInput(input.ClusterID, input.Limit) {
		return invalid(out, "cluster_id와 허용 범위의 limit을 명시해야 합니다.")
	}
	data, err := s.client.ScramAudit(ctx, input.ClusterID)
	if err != nil {
		return failed(out, err, "scram_audit")
	}
	out.Data = &data
	out.Truncated = capItems(&data.Findings, limitOrDefault(input.Limit))
	out.Limitations = []string{"SCRAM 정책 진단이며 비밀번호 변경 감사 이력이 아닙니다. 인증 설정과 비밀번호는 공개하지 않습니다.", "assetSyncedAt은 자산 스냅샷 시각, generatedAt은 진단 생성 시각입니다. 백엔드 ACL 부가 조회 실패는 응답에 표시되지 않습니다.", "limit은 MCP 발견사항 출력만 제한합니다. 백엔드의 자산 자동 동기화·조회 감사 기록 및 전량 조회는 실행될 수 있습니다.", securityModeUnknown}
	if failure := backendFailure(data.Status, "scram_audit"); failure != nil {
		out.Status = "error"
		out.Errors = []Failure{*failure}
	} else if !data.Supported {
		out.Status = "partial"
		out.Errors = []Failure{{Component: "scram_audit", Code: "unsupported", Message: "백엔드가 SCRAM 정책 진단 미지원 상태를 보고했습니다.", HTTPStatus: 200}}
	} else {
		securityIncomplete(&out, "scram_audit_enrichment", "ACL 부가 조회 성공 여부를 확인할 수 없습니다. unresolved와 진단 발견사항을 함께 확인하세요.")
	}
	bound(&out, func() bool { return shrinkItems(&data.Findings) })
	return out
}

func (s *service) listRequests(ctx context.Context, input ClusterInput) Envelope[ListData[ableops.RequestListItem]] {
	out := newEnvelope[ListData[ableops.RequestListItem]](s.client.RedactContext(ctx, input.ClusterID), "backend_authorized_request_registry")
	if !validSecurityInput(input.ClusterID, input.Limit) {
		return invalid(out, "cluster_id와 허용 범위의 limit을 명시해야 합니다.")
	}
	items, err := s.client.ListRequests(ctx, input.ClusterID)
	if err != nil {
		return failed(out, err, "requests")
	}
	data := listData(items, input.Limit)
	out.Data = &data
	out.Truncated = data.Returned < data.Received
	out.Limitations = []string{"백엔드가 현재 사용자가 열람 가능한 신청만 반환합니다. 빈 결과는 클러스터 전체 신청 0건을 뜻하지 않습니다.", "APPROVED는 APPLIED·VERIFIED와 다릅니다. 상태를 그대로 반환하며 처리 완료를 추정하지 않습니다.", "백엔드는 cluster 외 필터와 페이지를 지원하지 않습니다. limit은 출력 제한이며 전송량과 백엔드 전체 신청 조회량을 줄이지 않습니다.", "백엔드 신청 저장소의 조회 실패는 빈 목록으로, 개별 행 오류는 누락으로 응답할 수 있습니다. 조회 완전성이나 신청 부재를 확정할 수 없습니다."}
	securityIncomplete(&out, "requests", "백엔드가 신청 저장소의 조회 실패·행 누락을 응답에 표시하지 않습니다. 반환한 신청과 빈 목록의 완전성을 확인할 수 없습니다.")
	bound(&out, func() bool { changed := shrinkItems(&data.Items); data.Returned = len(data.Items); return changed })
	return out
}
