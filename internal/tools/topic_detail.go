package tools

import (
	"context"
	"strings"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type TopicDetailInput struct {
	ClusterID string   `json:"cluster_id"`
	TopicName string   `json:"topic_name"`
	Include   []string `json:"include,omitempty"`
	Limit     int      `json:"limit,omitempty"`
}

type TopicDetailData struct {
	Topic      ableops.TopicDetail  `json:"topic"`
	Partitions *TopicPartitionsData `json:"partitions,omitempty"`
}

type TopicPartitionsData struct {
	Topic    string                                 `json:"topic"`
	Internal bool                                   `json:"internal"`
	Health   ableops.TopicHealth                    `json:"health"`
	Items    ListData[ableops.TopicPartitionDetail] `json:"items"`
}

type ConsumerGroupMembersData struct {
	GroupName string                                `json:"group_name"`
	Members   ListData[ableops.ConsumerGroupMember] `json:"members"`
}

func registerTopicMemberTools(server *mcp.Server, s *service) {
	schema := inputSchema(true, false, false)
	props := schema["properties"].(map[string]any)
	props["topic_name"] = map[string]any{"type": "string", "minLength": 1, "maxLength": 1024, "description": "대상 토픽 이름"}
	props["include"] = map[string]any{"type": "array", "maxItems": 2, "uniqueItems": true, "items": map[string]any{"type": "string", "enum": []string{"configs", "partitions"}}, "description": "configs는 기본 응답의 허용된 설정 키를 공개하며 partitions는 라이브 조회 1회를 추가합니다."}
	schema["required"] = []string{"cluster_id", "topic_name"}
	registerWithAnnotations(server, s, "get_topic_detail", "대상 클러스터의 토픽 스냅샷과 소유 메타를 조회합니다. 설정 공개와 라이브 파티션 복제 상태는 include로 선택하며 메시지 본문은 조회하지 않습니다.", schema, func(i TopicDetailInput) string { return i.ClusterID }, &mcp.ToolAnnotations{ReadOnlyHint: false, IdempotentHint: false}, s.topicDetail)
	register(server, s, "get_consumer_group_members", "대상 클러스터 Consumer Group의 라이브 멤버와 토픽·파티션 할당을 조회합니다. 빈 멤버 목록만으로 그룹의 존재 또는 장애를 판단하지 않습니다.", inputSchema(true, true, false), func(i GroupInput) string { return i.ClusterID }, s.groupMembers)
}

func (s *service) topicDetail(ctx context.Context, input TopicDetailInput) Envelope[TopicDetailData] {
	out := newEnvelope[TopicDetailData](s.client.RedactContext(ctx, input.ClusterID), "backend_asset_snapshot")
	if !validCluster(input.ClusterID) || strings.TrimSpace(input.TopicName) == "" {
		return invalid(out, "cluster_id와 topic_name을 명시해야 합니다.")
	}
	includeConfigs, includePartitions := false, false
	for _, item := range input.Include {
		switch item {
		case "configs":
			includeConfigs = true
		case "partitions":
			includePartitions = true
		default:
			return invalid(out, "지원하지 않는 include 항목입니다.")
		}
	}
	topic, err := s.client.TopicDetail(ctx, input.ClusterID, input.TopicName)
	if err != nil {
		return failed(out, err, "topic")
	}
	if !includeConfigs {
		topic.Configs = nil
	}
	data := TopicDetailData{Topic: topic}
	out.Data = &data
	out.Limitations = []string{"기본 정보는 백엔드 자산 스냅샷이며 상세 API는 동기화 시각을 제공하지 않습니다. 소유 메타는 토픽명 기준의 공용 카탈로그가 병합되므로 클러스터별 독립 메타임을 보장하지 않습니다."}
	if includeConfigs {
		out.Limitations = append(out.Limitations, "설정은 cleanup.policy, retention.ms, retention.bytes, min.insync.replicas, compression.type, segment.bytes만 공개합니다. 누락된 값은 미제공이며 직접 지정값·브로커 상속값의 출처는 API가 제공하지 않습니다.")
	}
	if includePartitions {
		out.Source = "backend_asset_snapshot_and_live_adapter"
		partitions, err := s.client.TopicPartitions(ctx, input.ClusterID, input.TopicName)
		if err != nil {
			out.Status = "partial"
			out.Errors = append(out.Errors, failure(err, "partitions"))
		} else {
			data.Partitions = &TopicPartitionsData{Topic: partitions.Topic, Internal: partitions.Internal, Health: partitions.Health, Items: listData(partitions.Partitions, input.Limit)}
			out.Truncated = data.Partitions.Items.Returned < data.Partitions.Items.Received
			limit := limitOrDefault(input.Limit)
			out.Truncated = capItems(&data.Partitions.Health.Reasons, limit) || out.Truncated
			for i := range data.Partitions.Items.Items {
				p := &data.Partitions.Items.Items[i]
				out.Truncated = capItems(&p.Replicas, limit) || out.Truncated
				out.Truncated = capItems(&p.ISR, limit) || out.Truncated
				out.Truncated = capItems(&p.OfflineReplicas, limit) || out.Truncated
			}
		}
		out.Limitations = append(out.Limitations, "파티션은 라이브 조회이며 원본 관측 시각이 없습니다. 기본 상세와 동일 순간의 원자적 스냅샷이 아니며 서로 다른 파티션 수를 임의로 보정하지 않습니다. limit는 MCP 출력에만 적용됩니다.")
	}
	bound(&out, func() bool {
		if data.Partitions == nil {
			return false
		}
		if shrinkItems(&data.Partitions.Items.Items) {
			data.Partitions.Items.Returned = len(data.Partitions.Items.Items)
			return true
		}
		return shrinkItems(&data.Partitions.Health.Reasons)
	})
	return out
}

func (s *service) groupMembers(ctx context.Context, input GroupInput) Envelope[ConsumerGroupMembersData] {
	out := newEnvelope[ConsumerGroupMembersData](s.client.RedactContext(ctx, input.ClusterID), "backend_live_adapter")
	if !validCluster(input.ClusterID) || strings.TrimSpace(input.GroupName) == "" {
		return invalid(out, "cluster_id와 group_name을 명시해야 합니다.")
	}
	members, err := s.client.ConsumerGroupMembers(ctx, input.ClusterID, input.GroupName)
	if err != nil {
		return failed(out, err, "consumer_group_members")
	}
	data := ConsumerGroupMembersData{GroupName: s.client.RedactContext(ctx, input.GroupName), Members: listData(members, input.Limit)}
	out.Data = &data
	out.Truncated = data.Members.Returned < data.Members.Received
	limit := limitOrDefault(input.Limit)
	for i := range data.Members.Items {
		member := &data.Members.Items[i]
		out.Truncated = capItems(&member.Assignments, limit) || out.Truncated
		for j := range member.Assignments {
			out.Truncated = capItems(&member.Assignments[j].Partitions, limit) || out.Truncated
		}
	}
	out.Limitations = []string{
		"멤버 API는 그룹 존재 여부·그룹 상태·멤버 상태를 제공하지 않습니다. 빈 목록은 그룹 미존재와 멤버 없음 등을 구분하지 않으며 장애 또는 정상으로 단정할 수 없습니다.",
		"lastSeenAt은 백엔드 조회 시각이며 Kafka heartbeat 시각이 아닙니다. consumer 이외 프로토콜의 할당은 빈 목록일 수 있습니다.",
		"limit는 멤버·할당 토픽·할당 파티션의 MCP 출력에 각각 적용되며 REST 전송 크기를 줄이지 않습니다. 별도로 조회한 Lag와 동일 순간의 스냅샷이 아니므로 memberId·clientId·topic·partition이 맞지 않으면 강제로 연결하지 마세요.",
	}
	bound(&out, func() bool {
		changed := shrinkItems(&data.Members.Items)
		data.Members.Returned = len(data.Members.Items)
		return changed
	})
	return out
}
