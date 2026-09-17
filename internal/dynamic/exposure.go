package dynamic

import (
	"net/http"
	"slices"
	"sort"

	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
)

// Exposure는 Dynamic 도구를 LLM에 노출해도 되는지에 대한 분류다.
//
// upstream의 x-mcp-enabled=true는 "도구로 쓸 가치가 있다"는 판단이지 "원문 그대로 LLM에 넘겨도
// 안전하다"는 보장이 아니다. Dynamic 도구는 응답을 마스킹·재구성하지 않으므로, 노출 여부는
// 이 패키지의 명시적 정책으로 따로 정한다. 잘못된 자동 마스킹보다 노출하지 않는 편을 택한다.
type Exposure string

const (
	// ExposureSafe는 원문 그대로 기본 서버에 노출해도 되는 Operation이다.
	ExposureSafe Exposure = "SAFE"
	// ExposureShadow는 실행은 가능하지만 Static 도구나 MCP 원칙과 입력·출력·공개 범위·의미가
	// 달라 기본 서버에 노출하지 않는 Operation이다. 비교용 서버에서만 실행한다.
	ExposureShadow Exposure = "SHADOW"
	// ExposureBlocked는 민감정보나 명확한 안전 문제 때문에 어떤 서버에도 등록하지 않는 Operation이다.
	ExposureBlocked Exposure = "BLOCKED"
)

func (e Exposure) valid() bool {
	return e == ExposureSafe || e == ExposureShadow || e == ExposureBlocked
}

// ExposureRule은 Operation 하나의 노출 분류와 근거(고정 문구)다.
type ExposureRule struct {
	Exposure Exposure
	Reason   string
	// Path와 Parameters는 SAFE 판정을 검토한 시점의 GET 경로 템플릿과 파라미터("위치:이름")다.
	// 실행 중 계약 갱신으로 둘 중 하나라도 달라지면 다시 검토할 때까지 SHADOW로 내린다.
	// 새 파라미터가 응답 공개 범위를 넓힐 수 있기 때문이다. SAFE가 아니면 비워 둔다.
	Path       string
	Parameters []string
}

func blocked(reason string) ExposureRule {
	return ExposureRule{Exposure: ExposureBlocked, Reason: reason}
}

func shadow(reason string) ExposureRule {
	return ExposureRule{Exposure: ExposureShadow, Reason: reason}
}

func safe(reason, path string, parameters ...string) ExposureRule {
	return ExposureRule{Exposure: ExposureSafe, Reason: reason, Path: path, Parameters: parameters}
}

// reviewedFor는 op가 SAFE 검토 당시의 메서드·경로·파라미터 구성과 같은지다.
func (r ExposureRule) reviewedFor(op openapi.Operation) bool {
	if op.Method != http.MethodGet || op.Path != r.Path {
		return false
	}
	got := make([]string, 0, len(op.Parameters))
	for _, param := range op.Parameters {
		got = append(got, param.In+":"+param.Name)
	}
	want := slices.Clone(r.Parameters)
	sort.Strings(got)
	sort.Strings(want)
	return slices.Equal(got, want)
}

// reasonUnclassified는 정책표에 없는 Operation의 근거다. upstream이 새 Operation을
// x-mcp-enabled=true로 추가해도 이 저장소가 응답 공개 범위를 확인하기 전까지는 노출하지 않는다.
const reasonUnclassified = "노출 정책표에 없는 Operation입니다. 응답 공개 범위를 확인해 정책표에 추가하기 전까지 기본 서버에 노출하지 않습니다."

// reasonPolicyDrift는 SAFE 검토 이후 경로나 파라미터가 바뀐 Operation의 근거다.
const reasonPolicyDrift = "SAFE 검토 시점과 경로 또는 파라미터 구성이 달라졌습니다. 응답 공개 범위를 다시 확인해 정책표를 갱신하기 전까지 기본 서버에 노출하지 않습니다."

// 분류 근거 문구. 로그와 문서에 그대로 쓰므로 응답 값이나 경로를 넣지 않는다.
const (
	reasonAuthConfig      = "응답에 SASL 사용자명과 포털 서버의 TLS 파일 경로가 평문으로 실립니다. Static은 이 값을 버리며 Dynamic은 마스킹하지 않습니다."
	reasonNoClusterRBAC   = "클러스터별 권한 게이트가 없어 id를 아는 인증 사용자면 다른 클러스터도 조회합니다. 응답에 인증 설정 식별자와 서버 파일 경로도 실립니다."
	reasonAdapterError    = "연결 실패 시 error에 어댑터 생성 오류 원문이 실려 서버의 TLS 파일 경로가 드러날 수 있습니다. Static은 이 값을 고정 문구로 바꿉니다."
	reasonDefaultCluster  = "클러스터를 지정할 수 없어 기본 클러스터로 대체하는 레거시 경로이며 클러스터별 권한 판정도 거치지 않습니다."
	reasonCredentialGraph = "Principal 노드에 SCRAM 사용자명과 자격증명 요약, 원본 ACL·Host, Kafka 오류 원문이 실립니다. Static은 허용 속성만 남기고 오류 원문을 바꿉니다."
	reasonRequestHistory  = "반영 실패 이력 comment에 어댑터 오류 원문(서버 TLS 파일 경로 포함 가능)과 신청 payload 전체가 실리고, 레거시 신청은 서버가 기본 클러스터로 판정합니다."

	reasonStaticName     = "같은 이름의 Static 도구가 있고 입력 이름·출력 봉투·판정이 달라 자동 교체하지 않습니다."
	reasonTopicConfigs   = "Static이 버리거나 허용 목록으로 제한하는 토픽 configs 전체 맵을 기본으로 내보냅니다."
	reasonProbeReasons   = "실패 상태의 reasons에 Kafka·저장소 오류 원문이 실립니다. Static은 내부 접속 정보가 섞일 수 있어 고정 문구로 바꿉니다."
	reasonEmptyIsFailure = "관측 실패와 권한 부족도 200과 빈 배열로 나가고 관측 시각이 없어 '경보 없음'과 구분되지 않습니다."
	reasonPrincipalList  = "부분 문자열로 클러스터의 Principal과 SCRAM 자격증명 보유 계정을 열거할 수 있습니다. Static은 지정한 자산의 관계만 공개합니다."
	reasonEventEvidence  = "Static이 허용 목록으로 줄이는 evidence와 실행 링크·담당자 필드를 원문으로 내보냅니다. 클러스터 소속 대조도 하지 않습니다."
	reasonGroupMembers   = "Static은 경로 특수문자가 든 그룹명을 호출 전에 거부하는데, 응답에 그룹 식별자가 없어 Dynamic은 대상 일치를 검증할 수 없습니다."
	reasonImpactPartial  = "ACL 수집 실패가 응답에 드러나지 않아 '회수 안전' 같은 잘못된 안전 신호가 나갈 수 있습니다. Static은 그래프 오류를 먼저 확인합니다."
	reasonEventIssue     = "Static이 버리는 Issue 멤버·조치 이력·운영자 자유 입력을 원문으로 내보내고 클러스터 소속 대조를 하지 않습니다."
	reasonPlaybook       = "Static이 제외하는 자리표시자 실행 명령과 포털 경로·참고 링크를 내보냅니다. 명령을 실행 지시로 오인할 수 있습니다."
	reasonCurrentUser    = "이메일·이름·부서 등 개인정보를 원문으로 내보내고, 권한 목록이 클러스터별 부여를 반영하지 않아 사전 권한 판단을 오도합니다."

	reasonPublicBranding = "미인증 공개 정보이며 자격증명·클러스터·자산 데이터가 없습니다."
	reasonAggregateOnly  = "집계 숫자와 조회 범위 메타만 있고, 권한 밖 필터와 미수집을 본문에 명시합니다."
	reasonPartitionView  = "응답 필드가 Static 공개 범위 안이고 오류 원문이 없으며 클러스터별 권한 게이트가 적용됩니다. 호출마다 Kafka Metadata를 조회합니다."
)

// exposurePolicy는 upstream 계약(2026-09-17 스냅샷, x-mcp-enabled=true 28개)의 노출 분류다.
// 근거는 docs/dynamic-mcp.md 「노출 안전 게이트」 표와 같다. 표를 바꾸면 문서와 테스트를 함께 고친다.
// SAFE로 올리려면 응답 필드 전체가 Static 공개 범위 안이고, 오류 원문·권한 우회·기본 클러스터
// 대체가 없어야 한다.
var exposurePolicy = map[string]ExposureRule{
	// BLOCKED: 민감정보 또는 명확한 안전 문제
	"listClusters":             blocked(reasonAuthConfig),
	"getCluster":               blocked(reasonNoClusterRBAC),
	"getClusterHealth":         blocked(reasonAdapterError),
	"listDefaultClusterTopics": blocked(reasonDefaultCluster),
	"getAssetGraph":            blocked(reasonCredentialGraph),
	"listRequests":             blocked(reasonRequestHistory),
	"getRequest":               blocked(reasonRequestHistory),

	// SHADOW: 실행은 가능하지만 입력·출력·공개 범위·의미가 달라 기본 노출하지 않음
	"listTopics":                shadow(reasonTopicConfigs),
	"getTopic":                  shadow(reasonTopicConfigs),
	"listConsumerGroups":        shadow(reasonStaticName),
	"getConsumerGroupLag":       shadow(reasonProbeReasons),
	"getConsumerGroupMembers":   shadow(reasonGroupMembers),
	"getAssetImpact":            shadow(reasonImpactPartial),
	"listClusterEvents":         shadow(reasonEventEvidence),
	"getClusterPartitionHealth": shadow(reasonProbeReasons),
	"getConsumerGroupState":     shadow(reasonProbeReasons),
	"getConsumerLagOverview":    shadow(reasonProbeReasons),
	"listConsumerGroupAlerts":   shadow(reasonEmptyIsFailure),
	"searchAssetGraph":          shadow(reasonPrincipalList),
	"listEvents":                shadow(reasonEventEvidence),
	"getEvent":                  shadow(reasonEventEvidence),
	"listEventOccurrences":      shadow(reasonEventEvidence),
	"getEventIssue":             shadow(reasonEventIssue),
	"getEventPlaybook":          shadow(reasonPlaybook),
	"getCurrentUser":            shadow(reasonCurrentUser),

	// SAFE: 원문 그대로 노출 가능. 검토한 경로·파라미터 구성을 함께 고정한다.
	"getBranding":        safe(reasonPublicBranding, "/api/branding"),
	"getEventSummary":    safe(reasonAggregateOnly, "/api/events/summary", "query:clusterId"),
	"getTopicPartitions": safe(reasonPartitionView, "/api/clusters/{id}/topics/{name}/partitions", "path:id", "path:name"),
}

// ClassifyExposure는 내장 정책표로 Operation을 분류한다. 표에 없으면 SHADOW다.
// SAFE여도 검토 당시와 경로·파라미터 구성이 다르면 SHADOW로 내린다.
func ClassifyExposure(op openapi.Operation) ExposureRule {
	rule, ok := exposurePolicy[op.ID]
	switch {
	case !ok:
		return shadow(reasonUnclassified)
	case rule.Exposure == ExposureSafe && !rule.reviewedFor(op):
		return shadow(reasonPolicyDrift)
	default:
		return rule
	}
}

// ExposurePolicy는 정책표 사본이다(operationId → 분류). 보고와 테스트에 쓴다.
func ExposurePolicy() map[string]ExposureRule {
	out := make(map[string]ExposureRule, len(exposurePolicy))
	for id, rule := range exposurePolicy {
		rule.Parameters = slices.Clone(rule.Parameters)
		out[id] = rule
	}
	return out
}
