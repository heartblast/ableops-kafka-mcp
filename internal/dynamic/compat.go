package dynamic

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// CompatClass는 Static 도구를 같은 이름의 Dynamic 도구로 바꿀 수 있는지에 대한 판정이다.
// 이 판정은 교체를 하지 않는다. 후속 단계의 전환 후보를 객관적으로 고르기 위한 것이다.
type CompatClass string

const (
	// CompatCompatible은 입력·출력·의미·보안이 모두 같다. 이것만 전환 후보다.
	CompatCompatible CompatClass = "COMPATIBLE"
	// CompatInputMismatch는 입력 이름·필수 여부·타입·제약이 다르다(기계 비교).
	CompatInputMismatch CompatClass = "INPUT_MISMATCH"
	// CompatOutputMismatch는 결과 구조(기계 비교)나 공개 필드 범위(선언)가 다르다.
	CompatOutputMismatch CompatClass = "OUTPUT_MISMATCH"
	// CompatSemanticMismatch는 호출하는 Operation 구성(기계 비교)이나 Static이 붙이는 판정·검증(선언)이 다르다.
	CompatSemanticMismatch CompatClass = "SEMANTIC_MISMATCH"
	// CompatSecurityMismatch는 노출 안전 게이트가 BLOCKED로 분류했거나(기계),
	// Static이 보안상 제거·치환·거부하는 것을 Dynamic이 하지 않는다(선언).
	CompatSecurityMismatch CompatClass = "SECURITY_MISMATCH"
)

// StaticProfile은 코드로 기계 비교할 수 없는 Static 도구의 동작을 선언한 것이다.
// 근거는 internal/tools·internal/ableops의 구현이며 docs/dynamic-mcp.md 「Static/Dynamic 호환성」 표와 같다.
type StaticProfile struct {
	// Operations는 Static 도구가 호출하는 upstream operationId 전체다(선택 호출 포함).
	Operations []string
	// Output은 Static이 제거·치환·절단해 공개 범위가 달라지는 항목이다.
	Output []string
	// Semantic은 Static이 덧붙이는 판정·응답 검증이다.
	Semantic []string
	// Security는 Static이 보안상 제거·치환·거부하는 항목이다.
	Security []string
}

// staticProfiles는 Dynamic 도구와 이름이 같은 Static 도구 8개의 선언이다.
// 이름이 같은 Static 도구가 새로 생기면 테스트가 이 표에 추가하도록 강제한다.
var staticProfiles = map[string]StaticProfile{
	"list_clusters": {
		Operations: []string{"listClusters"},
		Output:     []string{"Cluster 8개 필드만 공개(인증 설정·브로커·감사 필드 제거)", "limit 절단"},
		Semantic:   []string{"최상위 null 거부", "received·returned·truncated 판정"},
		Security:   []string{"SASL 사용자명·TLS 파일 경로 제거"},
	},
	"get_cluster_health": {
		Operations: []string{"getClusterHealth", "getClusterPartitionHealth"},
		Output:     []string{"연결 상태와 파티션 건강을 한 봉투로 합성", "limit 절단"},
		Semantic:   []string{"clusterId 대조", "reachable·cluster 누락·FORBIDDEN/UNAVAILABLE을 partial/error로 판정", "ProbeStatus enum 검증"},
		Security:   []string{"health.error 원문을 고정 문구로 치환", "실패 reasons 원문을 고정 문구로 치환"},
	},
	"list_topics": {
		Operations: []string{"listTopics"},
		Output:     []string{"토픽 configs 제거", "limit 절단"},
		Semantic:   []string{"clusterId 대조", "syncedAt=null을 partial과 제한 설명으로 판정", "items=null을 빈 목록으로 정규화"},
	},
	"list_consumer_groups": {
		Operations: []string{"listConsumerGroups"},
		Output:     []string{"topicLag 키를 limit으로 절단"},
		Semantic:   []string{"clusterId 대조", "syncedAt=null을 partial과 제한 설명으로 판정"},
	},
	"get_consumer_group_lag": {
		Operations: []string{"getConsumerGroupLag"},
		Output:     []string{"partitions[].clientHost 제거", "topic_name 지정 시 partitions·topicLag를 limit 전에 Topic으로 절단(topicFilter)", "목록 limit 절단"},
		Semantic:   []string{"group·ProbeStatus 대조", "found=false·FORBIDDEN·UNAVAILABLE·오류 파티션을 error/partial로 판정", "Topic 파티션 없음을 topic_not_found로 그룹 미존재와 구분"},
		Security:   []string{"partitions[].error·실패 reasons의 Kafka 오류 원문을 고정 문구로 치환"},
	},
	"get_consumer_group_members": {
		Operations: []string{"getConsumerGroupMembers"},
		Output:     []string{"멤버·할당 limit 절단", "해석 제한 설명 추가"},
		Semantic:   []string{"최상위 null·빈 memberId 거부"},
		Security:   []string{"경로 특수문자(/ ; ,)가 든 cluster_id·group_name을 호출 전에 거부"},
	},
	"list_cluster_events": {
		Operations: []string{"listClusterEvents"},
		Output:     []string{"Event 허용 필드만 공개(evidence·실행 링크·담당자·조치자 제거)", "기본 page_size 50·상한 100"},
		Semantic:   []string{"모든 item의 clusterId 대조", "has_more·page_truncated 계산"},
		Security:   []string{"evidence 원문과 runbookUrl·deepLink 제거"},
	},
	"get_event_summary": {
		Operations: []string{"getEventSummary"},
		Output:     []string{"EventSummary 허용 필드만 공개"},
		Semantic: []string{"cluster_id 필수(생략 시 권한 범위 합산으로 넓히지 않음)",
			"collection.enabled=false를 partial·collection_disabled로 판정", "합성 이벤트 제외와 축 혼동 방지 제한 설명 추가"},
	},
	"get_consumer_lag_overview": {
		Operations: []string{"getConsumerLagOverview"},
		Output:     []string{"ConsumerLagOverview 허용 필드만 공개", "limit 절단"},
		Semantic: []string{"관측 상태·경보 상태·topicsSyncedAt으로 partial 판정",
			"경보 비활성·조회 실패를 경보 0건으로 보지 않도록 제한 설명 추가"},
		Security: []string{"실패 상태 reasons의 Kafka·저장소 오류 원문을 고정 문구로 치환"},
	},
	"list_requests": {
		Operations: []string{"listRequests"},
		Output:     []string{"RequestListItem 허용 필드만 공개(payload는 대상 식별자만)", "limit 절단"},
		Semantic: []string{"cluster_id 필수", "빈 목록·행 누락을 조회 완전성으로 보지 않도록 제한 설명과 불완전 표시 추가",
			"APPROVED를 APPLIED·VERIFIED로 추정하지 않음"},
		Security: []string{"신청 payload 상세와 이력 원문을 공개하지 않음"},
	},
	"get_asset_impact": {
		Operations: []string{"getAssetGraph", "getAssetImpact"},
		Output:     []string{"그래프 노드·엣지 속성을 허용 필드만 남김", "영향 요약은 include=[impact] 옵트인"},
		Semantic:   []string{"그래프 선조회 후 errors·syncedAt·중심 노드로 partial 판정", "center·depth·view 대조"},
	},
}

// StaticProfiles는 선언 표의 사본이다.
func StaticProfiles() map[string]StaticProfile {
	out := make(map[string]StaticProfile, len(staticProfiles))
	for name, profile := range staticProfiles {
		out[name] = profile
	}
	return out
}

// StaticTool은 비교할 Static 도구 정의다. tools/list로 받은 스키마(JSON 객체)를 넣는다.
type StaticTool struct {
	Name         string
	InputSchema  any
	OutputSchema any
}

// Compatibility는 도구 하나의 판정 결과다. 각 목록은 차이 설명이며 값은 고정 문구와 스키마 식별자뿐이다.
type Compatibility struct {
	Tool        string        `json:"tool"`
	OperationID string        `json:"operation_id"`
	Exposure    Exposure      `json:"exposure"`
	Classes     []CompatClass `json:"classes"`
	Input       []string      `json:"input,omitempty"`
	Output      []string      `json:"output,omitempty"`
	Semantic    []string      `json:"semantic,omitempty"`
	Security    []string      `json:"security,omitempty"`
}

// Compatible은 전환 후보인지다.
func (c Compatibility) Compatible() bool {
	return len(c.Classes) == 1 && c.Classes[0] == CompatCompatible
}

// AssessCompatibility는 Static 도구와 같은 이름의 Dynamic 항목을 비교한다.
// 입력과 결과 구조, 호출 Operation 구성, BLOCKED 분류는 기계적으로 비교하고,
// 공개 범위·판정·보안 처리는 StaticProfile 선언을 따른다. 선언이 없으면 판단을 추측하지 않고
// 의미 불일치로 둔다.
func AssessCompatibility(static StaticTool, entry Entry) Compatibility {
	tool := entry.Tool
	result := Compatibility{Tool: static.Name, OperationID: tool.Operation.ID, Exposure: entry.Exposure}
	definition := tool.definition()
	result.Input = CompareInputSchemas(static.InputSchema, definition.InputSchema)
	result.Output = compareTopLevel("출력", static.OutputSchema, definition.OutputSchema)
	profile, declared := staticProfiles[static.Name]
	if !declared {
		result.Semantic = append(result.Semantic, "Static 동작 선언이 없어 의미 동등성을 확인할 수 없습니다")
	} else {
		if !reflect.DeepEqual(profile.Operations, []string{tool.Operation.ID}) {
			result.Semantic = append(result.Semantic, fmt.Sprintf("호출 Operation 구성이 다릅니다: Static %s, Dynamic %s",
				strings.Join(profile.Operations, "+"), tool.Operation.ID))
		}
		result.Output = append(result.Output, profile.Output...)
		result.Semantic = append(result.Semantic, profile.Semantic...)
		result.Security = append(result.Security, profile.Security...)
	}
	if entry.Exposure == ExposureBlocked {
		result.Security = append(result.Security, "노출 안전 게이트가 BLOCKED로 분류했습니다")
	}
	for _, check := range []struct {
		class CompatClass
		diffs []string
	}{
		{CompatInputMismatch, result.Input},
		{CompatOutputMismatch, result.Output},
		{CompatSemanticMismatch, result.Semantic},
		{CompatSecurityMismatch, result.Security},
	} {
		if len(check.diffs) > 0 {
			result.Classes = append(result.Classes, check.class)
		}
	}
	if len(result.Classes) == 0 {
		result.Classes = []CompatClass{CompatCompatible}
	}
	return result
}

// CompareInputSchemas는 두 입력 스키마의 속성 이름, 필수 여부, 타입, SDK가 검증에 쓰는 제약
// (enum·const·범위·배수·길이·pattern·배열), 추가 속성 허용을 비교한다. 설명(description)·예시·
// format·기본값은 입력 검증 결과를 바꾸지 않으므로 비교하지 않는다.
func CompareInputSchemas(staticSchema, dynamicSchema any) []string {
	a, errA := schemaObject(staticSchema)
	b, errB := schemaObject(dynamicSchema)
	if errA != nil || errB != nil {
		return []string{"입력 스키마를 JSON 객체로 해석할 수 없습니다"}
	}
	var diffs []string
	propsA, _ := a["properties"].(map[string]any)
	propsB, _ := b["properties"].(map[string]any)
	reqA, reqB := stringSet(a["required"]), stringSet(b["required"])
	for _, name := range sortedUnion(propsA, propsB) {
		pa, inA := propsA[name].(map[string]any)
		pb, inB := propsB[name].(map[string]any)
		switch {
		case !inB:
			diffs = append(diffs, fmt.Sprintf("Static에만 있는 입력: %s", name))
			continue
		case !inA:
			diffs = append(diffs, fmt.Sprintf("Dynamic에만 있는 입력: %s", name))
			continue
		}
		if reqA[name] != reqB[name] {
			diffs = append(diffs, fmt.Sprintf("필수 여부가 다른 입력: %s", name))
		}
		for _, keyword := range []string{"type", "enum", "const", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
			"minLength", "maxLength", "pattern", "minItems", "maxItems", "uniqueItems", "items"} {
			if !reflect.DeepEqual(pa[keyword], pb[keyword]) {
				diffs = append(diffs, fmt.Sprintf("제약이 다른 입력: %s.%s", name, keyword))
			}
		}
	}
	if !reflect.DeepEqual(a["additionalProperties"], b["additionalProperties"]) {
		diffs = append(diffs, "추가 입력 허용 여부가 다릅니다")
	}
	return diffs
}

// compareTopLevel은 결과 봉투의 최상위 속성 이름을 비교한다. 클라이언트가 읽는 경로가 달라지면 불일치다.
func compareTopLevel(label string, staticSchema, dynamicSchema any) []string {
	a, errA := schemaObject(staticSchema)
	b, errB := schemaObject(dynamicSchema)
	if errA != nil || errB != nil {
		return []string{label + " 스키마를 JSON 객체로 해석할 수 없습니다"}
	}
	propsA, _ := a["properties"].(map[string]any)
	propsB, _ := b["properties"].(map[string]any)
	var onlyA, onlyB []string
	for _, name := range sortedUnion(propsA, propsB) {
		_, inA := propsA[name]
		_, inB := propsB[name]
		switch {
		case !inB:
			onlyA = append(onlyA, name)
		case !inA:
			onlyB = append(onlyB, name)
		}
	}
	var diffs []string
	if len(onlyA) > 0 {
		diffs = append(diffs, fmt.Sprintf("Static 결과에만 있는 최상위 필드: %s", strings.Join(onlyA, ", ")))
	}
	if len(onlyB) > 0 {
		diffs = append(diffs, fmt.Sprintf("Dynamic 결과에만 있는 최상위 필드: %s", strings.Join(onlyB, ", ")))
	}
	return diffs
}

// schemaObject는 스키마 값을 JSON 왕복으로 일반 객체로 바꾼다. 숫자는 float64로 통일해 비교한다.
func schemaObject(value any) (map[string]any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

func stringSet(value any) map[string]bool {
	out := map[string]bool{}
	list, _ := value.([]any)
	for _, item := range list {
		if name, ok := item.(string); ok {
			out[name] = true
		}
	}
	return out
}

func sortedUnion(a, b map[string]any) []string {
	seen := map[string]bool{}
	var names []string
	for _, m := range []map[string]any{a, b} {
		for name := range m {
			if !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
	}
	sort.Strings(names)
	return names
}
