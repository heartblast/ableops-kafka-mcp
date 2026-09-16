package ableops

import (
	"context"
	"strings"
)

// TopicConfigValues는 실제 토픽 설정 맵에서 운영 분석에 필요한 공개 키만 수신한다.
// 출처 정보가 없는 API이므로 직접 지정값과 브로커 상속값을 추측하지 않는다.
type TopicConfigValues struct {
	CleanupPolicy     *string `json:"cleanup.policy,omitempty"`
	RetentionMS       *string `json:"retention.ms,omitempty"`
	RetentionBytes    *string `json:"retention.bytes,omitempty"`
	MinInSyncReplicas *string `json:"min.insync.replicas,omitempty"`
	CompressionType   *string `json:"compression.type,omitempty"`
	SegmentBytes      *string `json:"segment.bytes,omitempty"`
}

// TopicDetail은 스냅샷 기본 정보와 백엔드가 병합한 카탈로그 소유 메타를 담는다.
type TopicDetail struct {
	Topic
	Configs *TopicConfigValues `json:"configs,omitempty"`
}

type TopicPartitionDetail struct {
	Partition       int32   `json:"partition"`
	Leader          int32   `json:"leader"`
	LeaderEpoch     int32   `json:"leaderEpoch"`
	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	OfflineReplicas []int32 `json:"offlineReplicas"`
	State           string  `json:"state"`
}

type TopicHealth struct {
	Status            string   `json:"status"`
	ReplicationFactor int      `json:"replicationFactor"`
	MinISR            int      `json:"minIsr"`
	UnderReplicated   int      `json:"underReplicated"`
	Offline           int      `json:"offline"`
	Leaderless        int      `json:"leaderless"`
	Reasons           []string `json:"reasons"`
}

type TopicMetadataView struct {
	Topic      string                 `json:"topic"`
	Internal   bool                   `json:"internal"`
	Partitions []TopicPartitionDetail `json:"partitions"`
	Health     TopicHealth            `json:"health"`
}

type ConsumerGroupMember struct {
	MemberID    string                    `json:"memberId"`
	ClientID    string                    `json:"clientId,omitempty"`
	ClientHost  string                    `json:"clientHost,omitempty"`
	InstanceID  string                    `json:"instanceId,omitempty"`
	LastSeenAt  string                    `json:"lastSeenAt,omitempty" jsonschema:"백엔드 조회 시각이며 Kafka heartbeat 시각이 아니다"`
	Assignments []ConsumerGroupAssignment `json:"assignments"`
}

type ConsumerGroupAssignment struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

func (c *Client) TopicDetail(ctx context.Context, clusterID, name string) (TopicDetail, error) {
	var out TopicDetail
	if strings.TrimSpace(name) == "" {
		return out, publicError("invalid_request", 0)
	}
	if err := c.getCluster(ctx, clusterID, []string{"topics", name}, nil, &out); err != nil {
		return TopicDetail{}, err
	}
	if out.Name != name {
		return TopicDetail{}, publicError("invalid_response", 200)
	}
	return out, nil
}

func (c *Client) TopicPartitions(ctx context.Context, clusterID, name string) (TopicMetadataView, error) {
	var out TopicMetadataView
	if strings.TrimSpace(name) == "" {
		return out, publicError("invalid_request", 0)
	}
	if err := c.getCluster(ctx, clusterID, []string{"topics", name, "partitions"}, nil, &out); err != nil {
		return TopicMetadataView{}, err
	}
	if out.Topic != name || (out.Health.Status != "OK" && out.Health.Status != "WARN" && out.Health.Status != "CRITICAL") || out.Partitions == nil {
		return TopicMetadataView{}, publicError("invalid_response", 200)
	}
	return out, nil
}

func (c *Client) ConsumerGroupMembers(ctx context.Context, clusterID, name string) ([]ConsumerGroupMember, error) {
	if strings.TrimSpace(name) == "" {
		return nil, publicError("invalid_request", 0)
	}
	// / ; , 는 세그먼트 인코딩과 전체 경로 인코딩이 달라 RawPath가 유지된다.
	// 백엔드는 RawPath로 매칭한 경로 인자를 디코딩하지 않을 수 있다.
	// 멤버 배열에는 그룹 식별자가 없어 대상 일치를 검증할 수 없으므로 조회하지 않는다.
	if strings.ContainsAny(name, "/;,") || strings.ContainsAny(clusterID, "/;,") {
		return nil, publicError("unsupported", 0)
	}
	var out []ConsumerGroupMember
	if err := c.getCluster(ctx, clusterID, []string{"consumer-groups", name, "members"}, nil, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, publicError("invalid_response", 200)
	}
	for i := range out {
		if strings.TrimSpace(out[i].MemberID) == "" {
			return nil, publicError("invalid_response", 200)
		}
		if out[i].Assignments == nil {
			out[i].Assignments = []ConsumerGroupAssignment{}
		}
	}
	return out, nil
}
