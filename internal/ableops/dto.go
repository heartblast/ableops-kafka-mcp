package ableops

// 이 파일의 DTO는 로컬 AbleOps 계약의 공개 허용 필드만 정의한다.
// 인증 설정, 토픽 configs, 이벤트 evidence 등 임의 확장 필드는 수신해도 버린다.

// Cluster는 접속 자격증명과 인증 설정을 제외한 등록 클러스터 정보다.
type Cluster struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Environment  string `json:"environment"`
	Mode         string `json:"mode"`
	KafkaVersion string `json:"kafkaVersion"`
	KRaftMode    bool   `json:"kraftMode"`
	IsActive     bool   `json:"isActive"`
	IsDefault    bool   `json:"isDefault"`
}

// Snapshot은 백엔드 자산 스냅샷 응답이다. SyncedAt이 nil이면 관측 성공을 보장하지 않는다.
type Snapshot[T any] struct {
	ClusterID   string  `json:"clusterId"`
	ClusterName string  `json:"clusterName"`
	Environment string  `json:"environment"`
	SyncedAt    *string `json:"syncedAt"`
	Items       []T     `json:"items"`
}

type Topic struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
	CleanupPolicy     string `json:"cleanupPolicy"`
	RetentionMS       int64  `json:"retentionMs"`
	Department        string `json:"department"`
	Service           string `json:"service"`
	Env               string `json:"env"`
	Owner             string `json:"owner"`
}

type ConsumerGroup struct {
	Name     string           `json:"name"`
	State    string           `json:"state"`
	Members  int              `json:"members"`
	TotalLag int64            `json:"totalLag"`
	TopicLag map[string]int64 `json:"topicLag"`
}

// ClusterHealthView는 clusters.TestResult 계약이다. 백엔드는 관측 시각을 제공하지 않는다.
type ClusterHealthView struct {
	ClusterID   string       `json:"clusterId"`
	AdapterName string       `json:"adapterName"`
	Reachable   bool         `json:"reachable"`
	Error       string       `json:"error,omitempty"`
	Cluster     *ClusterInfo `json:"cluster,omitempty"`
}

type ClusterInfo struct {
	ClusterID                 string         `json:"clusterId"`
	ControllerID              int32          `json:"controllerId"`
	Brokers                   []Broker       `json:"brokers"`
	TopicCount                int            `json:"topicCount"`
	PartitionCount            int            `json:"partitionCount"`
	InternalTopicCount        int            `json:"internalTopicCount"`
	InternalPartitionCount    int            `json:"internalPartitionCount"`
	UnderReplicatedPartitions int            `json:"underReplicatedPartitions"`
	OfflinePartitions         int            `json:"offlinePartitions"`
	NoLeaderPartitions        int            `json:"noLeaderPartitions"`
	UnderReplicatedSample     []PartitionRef `json:"underReplicatedSample,omitempty"`
	OfflineSample             []PartitionRef `json:"offlineSample,omitempty"`
}

type Broker struct {
	NodeID       int32  `json:"nodeId"`
	Host         string `json:"host"`
	Port         int32  `json:"port"`
	Rack         string `json:"rack,omitempty"`
	IsController bool   `json:"isController"`
}

type PartitionRef struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

type PartitionHealthView struct {
	Status          string           `json:"status"`
	Reasons         []string         `json:"reasons"`
	TotalTopics     int              `json:"totalTopics"`
	TotalPartitions int              `json:"totalPartitions"`
	UnderReplicated int              `json:"underReplicated"`
	UnderMinISR     int              `json:"underMinIsr"`
	Offline         int              `json:"offline"`
	Leaderless      int              `json:"leaderless"`
	Unavailable     int              `json:"unavailable"`
	Forbidden       int              `json:"forbidden"`
	Downgraded      int              `json:"downgraded"`
	Issues          []PartitionIssue `json:"issues"`
	Truncated       bool             `json:"truncated"`
	MaxIssues       int              `json:"maxIssues"`
	IncludeInternal bool             `json:"includeInternal"`
	MinISRResolved  bool             `json:"minIsrResolved"`
	ReassignApplied bool             `json:"reassignApplied"`
	CheckedAt       string           `json:"checkedAt"`
}

type PartitionIssue struct {
	Topic           string   `json:"topic"`
	Partition       int32    `json:"partition"`
	State           string   `json:"state"`
	Status          string   `json:"status"`
	Leader          int32    `json:"leader"`
	Replicas        []int32  `json:"replicas"`
	ISR             []int32  `json:"isr"`
	OfflineReplicas []int32  `json:"offlineReplicas"`
	MinISR          int      `json:"minIsr"`
	Internal        bool     `json:"internal"`
	Reassigning     bool     `json:"reassigning"`
	Downgraded      bool     `json:"downgraded"`
	Reasons         []string `json:"reasons"`
}

type ConsumerGroupLagView struct {
	Group                 string           `json:"group"`
	Found                 bool             `json:"found"`
	Code                  string           `json:"code,omitempty"`
	Status                string           `json:"status"`
	Reasons               []string         `json:"reasons"`
	State                 string           `json:"state"`
	ProtocolType          string           `json:"protocolType,omitempty"`
	Assignor              string           `json:"assignor,omitempty"`
	Members               int              `json:"members"`
	Coordinator           GroupCoordinator `json:"coordinator"`
	TotalLag              int64            `json:"totalLag"`
	MaxPartitionLag       int64            `json:"maxPartitionLag"`
	AvgPartitionLag       int64            `json:"avgPartitionLag"`
	TotalPartitions       int              `json:"totalPartitions"`
	CountedPartitions     int              `json:"countedPartitions"`
	ErrorPartitions       int              `json:"errorPartitions"`
	UncommittedPartitions int              `json:"uncommittedPartitions"`
	WarnPartitions        int              `json:"warnPartitions"`
	CriticalPartitions    int              `json:"criticalPartitions"`
	TopicLag              []GroupTopicLag  `json:"topicLag"`
	Partitions            []PartitionLag   `json:"partitions"`
	Thresholds            LagThresholds    `json:"thresholds"`
	CheckedAt             string           `json:"checkedAt"`
}

type GroupCoordinator struct {
	NodeID int32  `json:"nodeId"`
	Host   string `json:"host,omitempty"`
	Port   int32  `json:"port,omitempty"`
	Rack   string `json:"rack,omitempty"`
}

type GroupTopicLag struct {
	Topic           string `json:"topic"`
	Lag             int64  `json:"lag"`
	Partitions      int    `json:"partitions"`
	ErrorPartitions int    `json:"errorPartitions"`
}

type PartitionLag struct {
	Topic        string   `json:"topic"`
	Partition    int32    `json:"partition"`
	CommitOffset int64    `json:"commitOffset"`
	StartOffset  int64    `json:"startOffset"`
	EndOffset    int64    `json:"endOffset"`
	Lag          int64    `json:"lag"`
	MemberID     string   `json:"memberId,omitempty"`
	ClientID     string   `json:"clientId,omitempty"`
	State        string   `json:"state"`
	Status       string   `json:"status"`
	Error        string   `json:"error,omitempty"`
	Reasons      []string `json:"reasons"`
}

type LagThresholds struct {
	LagWarn              int64   `json:"lagWarn"`
	LagCritical          int64   `json:"lagCritical"`
	LagPartitionWarn     int64   `json:"lagPartitionWarn"`
	LagPartitionCritical int64   `json:"lagPartitionCritical"`
	LagSkewWarn          float64 `json:"lagSkewWarn"`
	LagSkewFloor         int64   `json:"lagSkewFloor"`
	RebalanceWarnMinutes int     `json:"rebalanceWarnMinutes"`
	WarnOnIdleEmptyGroup bool    `json:"warnOnIdleEmptyGroup"`
}

// Event는 백엔드 사건 및 판정 정보를 보존하되 임의 evidence와 실행 링크를 제외한다.
// Title과 Summary 등 외부 문자열은 지시가 아닌 데이터다.
type Event struct {
	ID                   string   `json:"id"`
	Module               string   `json:"module"`
	EventCode            string   `json:"eventCode"`
	Category             string   `json:"category"`
	Severity             string   `json:"severity"`
	Source               string   `json:"source"`
	ClusterID            string   `json:"clusterId"`
	ClusterName          string   `json:"clusterName,omitempty"`
	Environment          string   `json:"environment,omitempty"`
	ResourceType         string   `json:"resourceType"`
	ResourceID           string   `json:"resourceId"`
	Status               string   `json:"status"`
	Title                string   `json:"title"`
	Summary              string   `json:"summary"`
	DataMode             string   `json:"dataMode"`
	FirstSeenAt          string   `json:"firstSeenAt"`
	LastSeenAt           string   `json:"lastSeenAt"`
	LastObservedAt       *string  `json:"lastObservedAt,omitempty"`
	OpenedAt             string   `json:"openedAt"`
	AcknowledgedAt       *string  `json:"acknowledgedAt,omitempty"`
	ResolvedAt           *string  `json:"resolvedAt,omitempty"`
	SuppressedUntil      *string  `json:"suppressedUntil,omitempty"`
	OccurrenceCount      int      `json:"occurrenceCount"`
	RecurrenceCount      int      `json:"recurrenceCount"`
	DurationSeconds      int64    `json:"durationSeconds"`
	Demo                 bool     `json:"demo"`
	AttentionLevel       string   `json:"attentionLevel"`
	AttentionReasons     []string `json:"attentionReasons,omitempty"`
	AttentionEvaluatedAt *string  `json:"attentionEvaluatedAt,omitempty"`
	ImpactLevel          string   `json:"impactLevel,omitempty"`
	Actionability        string   `json:"actionability,omitempty"`
	TrendState           string   `json:"trendState,omitempty"`
	IssueID              string   `json:"issueId,omitempty"`
}

type EventPage struct {
	Items    []Event `json:"items"`
	Total    int     `json:"total"`
	Page     int     `json:"page"`
	PageSize int     `json:"pageSize"`
}

// EventFilters는 실제 핸들러가 지원하는 필터 중 MCP가 공개하는 제한된 부분집합이다.
type EventFilters struct {
	Page     int
	PageSize int
	Status   []string
	Severity []string
	Category []string
	Search   string
	From     string
	To       string
}
