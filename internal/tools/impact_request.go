package tools

import (
	"context"
	"strings"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type AssetImpactInput struct {
	ClusterID string   `json:"cluster_id"`
	AssetType string   `json:"asset_type"`
	AssetKey  string   `json:"asset_key"`
	Depth     int      `json:"depth,omitempty"`
	Include   []string `json:"include,omitempty"`
	Limit     int      `json:"limit,omitempty"`
}

type RequestStatusInput struct {
	ClusterID string   `json:"cluster_id"`
	RequestID string   `json:"request_id"`
	Include   []string `json:"include,omitempty"`
	Limit     int      `json:"limit,omitempty"`
}

type AssetImpactData struct {
	AssetType     string               `json:"asset_type"`
	AssetKey      string               `json:"asset_key"`
	Graph         ableops.AssetGraph   `json:"graph"`
	Impact        *ableops.AssetImpact `json:"impact,omitempty"`
	ImpactStatus  string               `json:"impact_status" jsonschema:"not_requested, skipped, error, ok, omitted_output_limit 중 하나"`
	ReceivedNodes int                  `json:"received_nodes"`
	ReceivedEdges int                  `json:"received_edges"`
	Limit         int                  `json:"limit"`
}

type RequestStatusData struct {
	Request ableops.RequestStatus             `json:"request"`
	History *ListData[ableops.RequestHistory] `json:"history,omitempty"`
}

func registerImpactRequestTools(server *mcp.Server, s *service) {
	assetSchema := inputSchema(true, false, false)
	props := assetSchema["properties"].(map[string]any)
	props["asset_type"] = map[string]any{"type": "string", "enum": []string{"TOPIC", "PRINCIPAL", "CONSUMER_GROUP"}}
	props["asset_key"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 1024, "description": "백엔드 자산 키. PRINCIPAL은 User:이름 형식 등 실제 Principal 식별자를 사용합니다."}
	props["depth"] = map[string]any{"type": "integer", "minimum": 1, "maximum": 2, "description": "백엔드 관계 탐색 깊이. 기본 1, 구조 관계의 추가 확장 없음"}
	props["include"] = impactIncludeSchema("impact")
	assetSchema["required"] = []string{"cluster_id", "asset_type", "asset_key"}
	register(server, s, "get_asset_impact", "자산 중심의 제한 깊이 관계와 백엔드 방향·관계 유형을 조회합니다. include=[impact]로 별도 영향 요약을 추가합니다. 연결 관계는 장애 전파의 확정이 아닙니다.", assetSchema, func(i AssetImpactInput) string { return i.ClusterID }, s.assetImpact)

	requestSchema := inputSchema(true, false, false)
	props = requestSchema["properties"].(map[string]any)
	props["request_id"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 512}
	props["include"] = impactIncludeSchema("history")
	requestSchema["required"] = []string{"cluster_id", "request_id"}
	register(server, s, "get_request_status", "객체 접근 권한과 클러스터 소속을 확인한 요청의 원본 업무 상태·정책 결과를 조회합니다. include=[history]로 제한된 최근 상태 이력을 추가합니다. APPROVED는 APPLIED나 VERIFIED가 아닙니다.", requestSchema, func(i RequestStatusInput) string { return i.ClusterID }, s.requestStatus)
}

func impactIncludeSchema(value string) map[string]any {
	return map[string]any{"type": "array", "maxItems": 1, "uniqueItems": true, "items": map[string]any{"type": "string", "enum": []string{value}}}
}

func validImpactInclude(include []string, allowed string) bool {
	return len(include) == 0 || (len(include) == 1 && include[0] == allowed)
}

func (s *service) assetImpact(ctx context.Context, input AssetImpactInput) Envelope[AssetImpactData] {
	out := newEnvelope[AssetImpactData](s.client.RedactContext(ctx, input.ClusterID), "backend_live_asset_relations")
	if !validCluster(input.ClusterID) || strings.TrimSpace(input.AssetKey) == "" || input.Limit < 0 || input.Limit > MaxLimit || !validImpactInclude(input.Include, "impact") {
		return invalid(out, "cluster_id, asset_type, asset_key와 허용된 include·limit을 명시해야 합니다.")
	}
	depth := input.Depth
	if depth == 0 {
		depth = 1
	}
	graph, err := s.client.AssetRelations(ctx, input.ClusterID, input.AssetType, input.AssetKey, depth)
	if err != nil {
		return failed(out, err, "asset_graph")
	}
	data := AssetImpactData{AssetType: input.AssetType, AssetKey: s.client.RedactContext(ctx, input.AssetKey), Graph: graph, ImpactStatus: "not_requested", ReceivedNodes: len(graph.Nodes), ReceivedEdges: len(graph.Edges), Limit: limitOrDefault(input.Limit)}
	out.Data = &data
	out.Limitations = []string{
		"연결은 백엔드가 관측한 권한·소비·구조 관계입니다. 장애 전파나 변경 안전성을 확정하지 않습니다.",
		"그래프는 요청 깊이의 일부 관계입니다. limit은 각 목록의 MCP 출력 제한이며 백엔드 전체 자산 수집과 REST 전송량을 줄이지 않습니다.",
		"자격증명은 존재 여부와 포털 잠금 표식만 공개합니다. 잠금은 인증 불가 상태가 아닙니다.",
	}
	if len(graph.Errors) > 0 {
		out.Status = "partial"
		out.Errors = []Failure{{Component: "asset_graph", Code: "backend_partial_failure", Message: "백엔드 자산 수집 일부가 실패했습니다."}}
	}
	if graph.SyncedAt == "" {
		out.Status = "partial"
		out.Limitations = append(out.Limitations, "백엔드 수집 시각이 없어 관측 시각을 확인할 수 없습니다.")
	}
	if len(graph.Nodes) == 0 {
		out.Status = "partial"
		out.Limitations = append(out.Limitations, "요청한 중심 노드가 그래프에 없습니다. 존재하지 않는 자산과 수집 누락을 확정 구분할 수 없습니다.")
	}
	if graph.Summary.HiddenNodes > 0 {
		out.Truncated = true
		out.Limitations = append(out.Limitations, "백엔드가 요청 범위 밖의 노드를 숨겼습니다. 전체 영향 분석 결과가 아닙니다.")
	}
	if len(input.Include) > 0 {
		data.ImpactStatus = "skipped"
	}
	if len(input.Include) > 0 && len(graph.Nodes) > 0 {
		impact, e := s.client.AssetImpact(ctx, input.ClusterID, input.AssetType, input.AssetKey)
		if e != nil {
			data.ImpactStatus = "error"
			out.Status = "partial"
			out.Errors = append(out.Errors, failure(e, "impact"))
		} else {
			data.ImpactStatus = "ok"
			data.Impact = &impact
		}
		out.Limitations = append(out.Limitations, "영향 요약 API는 별도로 자산을 수집하며 수집 오류·클러스터 ID·관측 시각을 응답하지 않습니다. 완전성과 그래프와의 동일 시점을 확인할 수 없습니다.")
	}
	if trimAssetData(&data, data.Limit) {
		out.Truncated = true
	}
	bound(&out, func() bool { return shrinkAssetData(&data) })
	return out
}

// trimAssetData는 중심 노드를 남기고 출력에서 빠진 노드로 연결되는 관계도 함께 생략한다.
func trimAssetData(data *AssetImpactData, limit int) bool {
	changed := false
	for i, node := range data.Graph.Nodes {
		if node.ID == data.Graph.Center {
			data.Graph.Nodes[0], data.Graph.Nodes[i] = data.Graph.Nodes[i], data.Graph.Nodes[0]
			break
		}
	}
	changed = trimImpactItems(&data.Graph.Nodes, limit) || changed
	ids := make(map[string]bool, len(data.Graph.Nodes))
	for _, node := range data.Graph.Nodes {
		ids[node.ID] = true
	}
	edges := data.Graph.Edges[:0]
	for _, edge := range data.Graph.Edges {
		if ids[edge.Source] && ids[edge.Target] {
			edges = append(edges, edge)
		} else {
			changed = true
		}
	}
	data.Graph.Edges = edges
	changed = trimImpactItems(&data.Graph.Edges, limit) || changed
	changed = trimImpactItems(&data.Graph.Errors, limit) || changed
	for i := range data.Graph.Edges {
		if a := data.Graph.Edges[i].Attrs; a != nil {
			for _, items := range []*[]string{&a.Operations, &a.DeniedOperations, &a.PermissionTypes, &a.PatternTypes, &a.Hosts} {
				changed = trimImpactItems(items, limit) || changed
			}
		}
	}
	if a := data.Impact; a != nil {
		for _, items := range []*[]string{&a.Producers, &a.Consumers, &a.Topics, &a.Groups, &a.ACLs, &a.Users, &a.Risks, &a.Notes} {
			changed = trimImpactItems(items, limit) || changed
		}
	}
	return changed
}

func trimImpactItems[T any](items *[]T, limit int) bool {
	if len(*items) <= limit {
		return false
	}
	*items = (*items)[:limit]
	return true
}

func shrinkAssetData(data *AssetImpactData) bool {
	maxItems := len(data.Graph.Nodes)
	if len(data.Graph.Edges) > maxItems {
		maxItems = len(data.Graph.Edges)
	}
	if maxItems > 1 {
		return trimAssetData(data, maxItems/2)
	}
	if data.Impact != nil {
		data.Impact = nil
		data.ImpactStatus = "omitted_output_limit"
		return true
	}
	return false
}

func (s *service) requestStatus(ctx context.Context, input RequestStatusInput) Envelope[RequestStatusData] {
	out := newEnvelope[RequestStatusData](s.client.RedactContext(ctx, input.ClusterID), "backend_request_workflow")
	if !validCluster(input.ClusterID) || strings.TrimSpace(input.RequestID) == "" || input.Limit < 0 || input.Limit > MaxLimit || !validImpactInclude(input.Include, "history") {
		return invalid(out, "cluster_id, request_id와 허용된 include·limit을 명시해야 합니다.")
	}
	request, err := s.client.RequestStatus(ctx, input.ClusterID, input.RequestID)
	if err != nil {
		return failed(out, err, "request")
	}
	data := RequestStatusData{Request: request}
	data.Request.History = nil
	out.Data = &data
	out.Limitations = []string{
		"status와 history.state는 백엔드 원본 업무 상태입니다. APPROVED는 승인, APPLIED는 반영, VERIFIED는 검증 상태이며 서로 대신하지 않습니다.",
		"자유 입력 의견·실패 원문·정책 설명과 payload의 대상 이외 필드는 민감정보 보호를 위해 생략합니다. 정책 코드·심각도와 상태 이력으로 근거를 확인하세요.",
		"백엔드가 재시도 가능 여부를 제공하지 않아 추정하지 않습니다. updatedAt은 요청 수정 시각이며 Kafka 관측 시각이 아닙니다.",
		"이력은 백엔드가 전체 반환하며 include와 limit은 MCP 출력에만 적용됩니다.",
	}
	if len(input.Include) > 0 {
		// 백엔드는 상태 전이 순서로 이력을 추가하므로 끝부분을 최근 이력으로 반환한다.
		limit := limitOrDefault(input.Limit)
		received := len(request.History)
		items := request.History
		if received > limit {
			items = items[received-limit:]
			out.Truncated = true
		}
		history := listData(items, limit)
		history.Received = received
		data.History = &history
	}
	if data.Request.PolicyResult != nil && trimImpactItems(&data.Request.PolicyResult.Violations, limitOrDefault(input.Limit)) {
		out.Truncated = true
	}
	bound(&out, func() bool {
		if data.History != nil && len(data.History.Items) > 0 {
			data.History.Items = data.History.Items[len(data.History.Items)/2+1:]
			data.History.Returned = len(data.History.Items)
			return true
		}
		return data.Request.PolicyResult != nil && shrinkItems(&data.Request.PolicyResult.Violations)
	})
	return out
}
