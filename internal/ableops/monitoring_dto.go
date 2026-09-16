package ableops

// 모니터링 DTO는 확인한 로컬 REST 계약의 공개 필드만 선언한다.
// 로그 디렉터리·접속 정보·설정 원문·진단 오류 원문은 수신해도 제외한다.

type LagPolicyValue struct {
	Mode  string `json:"mode"`
	Value *int64 `json:"value,omitempty"`
}

type ConsumerLagPolicy struct {
	ID                   int64          `json:"id"`
	ClusterID            string         `json:"clusterId"`
	Scope                string         `json:"scope"`
	Topic                string         `json:"topic,omitempty"`
	Group                string         `json:"group,omitempty"`
	TopicLagWarn         LagPolicyValue `json:"topicLagWarn"`
	TopicLagCritical     LagPolicyValue `json:"topicLagCritical"`
	PartitionLagWarn     LagPolicyValue `json:"partitionLagWarn"`
	PartitionLagCritical LagPolicyValue `json:"partitionLagCritical"`
	AlertAfterSec        LagPolicyValue `json:"alertAfterSec"`
	RecoverAfterSec      LagPolicyValue `json:"recoverAfterSec"`
	Monitoring           string         `json:"monitoring"`
	CreatedAt            string         `json:"createdAt"`
	UpdatedAt            string         `json:"updatedAt"`
}

type EffectiveLagValue struct {
	Enabled   bool   `json:"enabled"`
	Value     int64  `json:"value"`
	RuleBased bool   `json:"ruleBased,omitempty"`
	Source    string `json:"source"`
}

type EffectiveLagPolicy struct {
	TopicLagWarn         EffectiveLagValue `json:"topicLagWarn"`
	TopicLagCritical     EffectiveLagValue `json:"topicLagCritical"`
	PartitionLagWarn     EffectiveLagValue `json:"partitionLagWarn"`
	PartitionLagCritical EffectiveLagValue `json:"partitionLagCritical"`
	AlertAfterSec        EffectiveLagValue `json:"alertAfterSec"`
	RecoverAfterSec      EffectiveLagValue `json:"recoverAfterSec"`
	Monitoring           bool              `json:"monitoring"`
	MonitoringSource     string            `json:"monitoringSource"`
	Adjustments          []string          `json:"adjustments"`
}

type ConsumerLagPolicyLayers struct {
	Common     EffectiveLagPolicy `json:"common"`
	Cluster    *ConsumerLagPolicy `json:"cluster"`
	Topic      *ConsumerLagPolicy `json:"topic"`
	GroupTopic *ConsumerLagPolicy `json:"groupTopic"`
}

type ConsumerLagPolicyDetail struct {
	ClusterID string                  `json:"clusterId"`
	Scope     string                  `json:"scope"`
	Topic     string                  `json:"topic,omitempty"`
	Group     string                  `json:"group,omitempty"`
	Stored    *ConsumerLagPolicy      `json:"stored"`
	Layers    ConsumerLagPolicyLayers `json:"layers"`
	Inherited EffectiveLagPolicy      `json:"inherited"`
	Effective EffectiveLagPolicy      `json:"effective"`
	CanEdit   bool                    `json:"canEdit"`
	Limits    ConsumerLagPolicyLimits `json:"limits"`
}

type ConsumerLagPolicyLimits struct {
	ThresholdMin   int64 `json:"thresholdMin"`
	ThresholdMax   int64 `json:"thresholdMax"`
	DurationMaxSec int64 `json:"durationMaxSec"`
}

type ConsumerLagAlertState struct {
	Phase         string  `json:"phase"`
	EventID       string  `json:"eventId,omitempty"`
	Severity      string  `json:"severity,omitempty"`
	SinceAt       *string `json:"sinceAt,omitempty"`
	LastSeenAt    *string `json:"lastSeenAt,omitempty"`
	Hits          int     `json:"hits"`
	Misses        int     `json:"misses"`
	Recovering    bool    `json:"recovering"`
	NormalSinceAt *string `json:"normalSinceAt,omitempty"`
}

type ConsumerLagTarget struct {
	Group                 string                 `json:"group"`
	Topic                 string                 `json:"topic"`
	BusinessName          string                 `json:"businessName,omitempty"`
	Partitions            int                    `json:"partitions"`
	CountedPartitions     int                    `json:"countedPartitions"`
	ErrorPartitions       int                    `json:"errorPartitions"`
	UncommittedPartitions int                    `json:"uncommittedPartitions"`
	WarnPartitions        int                    `json:"warnPartitions"`
	CriticalPartitions    int                    `json:"criticalPartitions"`
	TopicLag              int64                  `json:"topicLag"`
	MaxPartitionLag       int64                  `json:"maxPartitionLag"`
	MaxPartition          int32                  `json:"maxPartition"`
	AvgPartitionLag       int64                  `json:"avgPartitionLag"`
	Observation           string                 `json:"observation"`
	Monitored             bool                   `json:"monitored"`
	Status                string                 `json:"status"`
	Codes                 []string               `json:"codes"`
	Reasons               []string               `json:"reasons"`
	Policy                EffectiveLagPolicy     `json:"policy"`
	GroupTotalRule        bool                   `json:"groupTotalRule"`
	EventFiring           bool                   `json:"eventFiring"`
	EventCauses           []string               `json:"eventCauses"`
	Alert                 *ConsumerLagAlertState `json:"alert,omitempty"`
}

type ConsumerLagGroup struct {
	Group              string                 `json:"group"`
	State              string                 `json:"state"`
	GroupType          string                 `json:"groupType"`
	ProtocolType       string                 `json:"protocolType,omitempty"`
	Assignor           string                 `json:"assignor,omitempty"`
	GroupEpoch         int32                  `json:"groupEpoch"`
	AssignmentEpoch    int32                  `json:"assignmentEpoch"`
	Members            int                    `json:"members"`
	Rebalancing        bool                   `json:"rebalancing"`
	TotalLag           int64                  `json:"totalLag"`
	GroupRuleLag       int64                  `json:"groupRuleLag"`
	Topics             int                    `json:"topics"`
	WarnTopics         int                    `json:"warnTopics"`
	CriticalTopics     int                    `json:"criticalTopics"`
	DisabledTopics     int                    `json:"disabledTopics"`
	ErrorPartitions    int                    `json:"errorPartitions"`
	MaxLagTopic        string                 `json:"maxLagTopic,omitempty"`
	MaxTopicLag        int64                  `json:"maxTopicLag"`
	Observation        string                 `json:"observation"`
	Status             string                 `json:"status"`
	GroupStatus        string                 `json:"groupStatus"`
	Codes              []string               `json:"codes"`
	Reasons            []string               `json:"reasons"`
	EventFiring        bool                   `json:"eventFiring"`
	EventCauses        []string               `json:"eventCauses"`
	Alert              *ConsumerLagAlertState `json:"alert,omitempty"`
	ActiveTargetAlerts int                    `json:"activeTargetAlerts"`
}

type ConsumerLagTopic struct {
	Topic           string `json:"topic"`
	BusinessName    string `json:"businessName,omitempty"`
	Groups          int    `json:"groups"`
	WarnGroups      int    `json:"warnGroups"`
	CriticalGroups  int    `json:"criticalGroups"`
	DisabledGroups  int    `json:"disabledGroups"`
	ErrorGroups     int    `json:"errorGroups"`
	MaxLagGroup     string `json:"maxLagGroup,omitempty"`
	MaxGroupLag     int64  `json:"maxGroupLag"`
	MaxPartitionLag int64  `json:"maxPartitionLag"`
	Observation     string `json:"observation"`
	Status          string `json:"status"`
	ActiveAlerts    int    `json:"activeAlerts"`
}

type ConsumerLagSummary struct {
	Groups           int `json:"groups"`
	Topics           int `json:"topics"`
	Targets          int `json:"targets"`
	CriticalTargets  int `json:"criticalTargets"`
	WarnTargets      int `json:"warnTargets"`
	DisabledTargets  int `json:"disabledTargets"`
	CriticalGroups   int `json:"criticalGroups"`
	WarnGroups       int `json:"warnGroups"`
	UnresolvedGroups int `json:"unresolvedGroups"`
	PartialGroups    int `json:"partialGroups"`
	FiringTargets    int `json:"firingTargets"`
	ActiveAlerts     int `json:"activeAlerts"`
	PendingAlerts    int `json:"pendingAlerts"`
	NoConsumerTopics int `json:"noConsumerTopics"`
}

type ConsumerLagSnapshot struct {
	ClusterID    string              `json:"clusterId"`
	Status       string              `json:"status"`
	Partial      bool                `json:"partial"`
	Reasons      []string            `json:"reasons"`
	CheckedAt    string              `json:"checkedAt"`
	Thresholds   LagThresholds       `json:"thresholds"`
	CommonPolicy EffectiveLagPolicy  `json:"commonPolicy"`
	Groups       []ConsumerLagGroup  `json:"groups"`
	Topics       []ConsumerLagTopic  `json:"topics"`
	Targets      []ConsumerLagTarget `json:"targets"`
	Summary      ConsumerLagSummary  `json:"summary"`
}

type ConsumerLagEventRule struct {
	Enabled           bool `json:"enabled"`
	ConsecutiveHits   int  `json:"consecutiveHits"`
	AutoResolveMisses int  `json:"autoResolveMisses"`
	MinDurationSec    int  `json:"minDurationSec"`
}

type ConsumerLagOverview struct {
	ConsumerLagSnapshot
	AlertsStatus   string                `json:"alertsStatus"`
	EventRule      *ConsumerLagEventRule `json:"eventRule,omitempty"`
	TopicsSyncedAt *string               `json:"topicsSyncedAt"`
	PolicyCount    int                   `json:"policyCount"`
	CanEditPolicy  bool                  `json:"canEditPolicy"`
}

type LargestPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Size      int64  `json:"size"`
}

type BrokerStorage struct {
	Broker             int32            `json:"broker"`
	Size               int64            `json:"size"`
	PartitionCount     int              `json:"partitionCount"`
	LargestPartition   LargestPartition `json:"largestPartition"`
	SkewPct            int              `json:"skewPct"`
	FutureReplicas     int              `json:"futureReplicas"`
	MaxOffsetLag       int64            `json:"maxOffsetLag"`
	FutureMaxOffsetLag int64            `json:"futureMaxOffsetLag"`
	State              string           `json:"state"`
	Status             string           `json:"status"`
	Reasons            []string         `json:"reasons"`
}

type BrokerLeaderDist struct {
	Broker   int32    `json:"broker"`
	Leaders  int      `json:"leaders"`
	Replicas int      `json:"replicas"`
	Size     int64    `json:"size"`
	SharePct int      `json:"sharePct"`
	SkewPct  int      `json:"skewPct"`
	Status   string   `json:"status"`
	Reasons  []string `json:"reasons"`
}

type LeaderDistributionView struct {
	Status        string             `json:"status"`
	Reasons       []string           `json:"reasons"`
	TotalLeaders  int                `json:"totalLeaders"`
	TotalReplicas int                `json:"totalReplicas"`
	Brokers       []BrokerLeaderDist `json:"brokers"`
	Evaluated     bool               `json:"evaluated"`
}

type ClusterStorageView struct {
	Status         string                 `json:"status"`
	Reasons        []string               `json:"reasons"`
	TotalSize      int64                  `json:"totalSize"`
	AvgSize        int64                  `json:"avgSize"`
	BrokerCount    int                    `json:"brokerCount"`
	FutureReplicas int                    `json:"futureReplicas"`
	Brokers        []BrokerStorage        `json:"brokers"`
	Leaders        LeaderDistributionView `json:"leaders"`
	Partial        bool                   `json:"partial"`
	FailedBrokers  []string               `json:"failedBrokers"`
	SkewSuppressed bool                   `json:"skewSuppressed"`
	SkewEvaluated  bool                   `json:"skewEvaluated"`
	Thresholds     StorageThresholds      `json:"thresholds"`
	CheckedAt      string                 `json:"checkedAt"`
}

type StorageThresholds struct {
	PartitionWarnBytes      int64 `json:"partitionWarnBytes"`
	PartitionCriticalBytes  int64 `json:"partitionCriticalBytes"`
	SkewWarnPct             int   `json:"skewWarnPct"`
	SkewCriticalPct         int   `json:"skewCriticalPct"`
	OffsetLagWarn           int64 `json:"offsetLagWarn"`
	FutureOffsetLagCritical int64 `json:"futureOffsetLagCritical"`
	MinTotalBytesForSkew    int64 `json:"minTotalBytesForSkew"`
}

type MonitoringFinding struct {
	Code     string `json:"code"`
	Status   string `json:"status"`
	Target   string `json:"target,omitempty"`
	Exempted bool   `json:"exempted,omitempty"`
}

type ConfigAuditLimits struct {
	MinInsyncReplicas       int      `json:"minInsyncReplicas"`
	MaxRetentionMs          int64    `json:"maxRetentionMs"`
	AllowUnlimitedRetention bool     `json:"allowUnlimitedRetention"`
	MaxMessageBytes         int64    `json:"maxMessageBytes"`
	RequireRetentionBytes   bool     `json:"requireRetentionBytes"`
	AllowedCleanupPolicies  []string `json:"allowedCleanupPolicies"`
}

type TopicConfigAudit struct {
	Topic    string              `json:"topic"`
	Status   string              `json:"status"`
	Findings []MonitoringFinding `json:"findings"`
}

type ConfigAuditReport struct {
	ClusterID   string             `json:"clusterId"`
	Status      string             `json:"status"`
	Topics      []TopicConfigAudit `json:"topics"`
	Total       int                `json:"total"`
	Unavailable int                `json:"unavailable"`
	Truncated   bool               `json:"truncated,omitempty"`
	Note        string             `json:"note,omitempty"`
	Limits      ConfigAuditLimits  `json:"limits"`
	GeneratedAt string             `json:"generatedAt"`
}

type PartitionReassignment struct {
	Topic            string   `json:"topic"`
	Partition        int32    `json:"partition"`
	Replicas         []int32  `json:"replicas"`
	AddingReplicas   []int32  `json:"addingReplicas"`
	RemovingReplicas []int32  `json:"removingReplicas"`
	SyncedAdding     int      `json:"syncedAdding"`
	ProgressPct      int      `json:"progressPct"`
	ObservedSince    string   `json:"observedSince"`
	ObservedMinutes  int      `json:"observedMinutes"`
	Estimated        bool     `json:"estimated"`
	WithOffline      bool     `json:"withOffline"`
	Status           string   `json:"status"`
	Code             string   `json:"code,omitempty"`
	Reasons          []string `json:"reasons"`
}

type PartitionReassignmentView struct {
	Status      string                  `json:"status"`
	Reasons     []string                `json:"reasons"`
	Total       int                     `json:"total"`
	LongRunning int                     `json:"longRunning"`
	NoProgress  int                     `json:"noProgress"`
	WithOffline int                     `json:"withOffline"`
	Items       []PartitionReassignment `json:"items"`
	Estimated   bool                    `json:"estimated"`
	Thresholds  ReassignThresholds      `json:"thresholds"`
	CheckedAt   string                  `json:"checkedAt"`
}

type ReassignThresholds struct {
	WarnMinutes       int `json:"warnMinutes"`
	CriticalMinutes   int `json:"criticalMinutes"`
	NoProgressMinutes int `json:"noProgressMinutes"`
	MaxConcurrent     int `json:"maxConcurrent"`
}

type MetricPoint struct {
	Ts     string             `json:"ts"`
	Values map[string]float64 `json:"values"`
}

type MetricCluster struct {
	ClusterID   string `json:"clusterId,omitempty"`
	ClusterName string `json:"clusterName,omitempty"`
	Scoped      bool   `json:"clusterScoped"`
}

type MetricSeries struct {
	MetricCluster
	Metric string        `json:"metric"`
	Range  string        `json:"range"`
	Unit   string        `json:"unit,omitempty"`
	Source string        `json:"source,omitempty"`
	Demo   bool          `json:"demo,omitempty"`
	Series []string      `json:"series"`
	Points []MetricPoint `json:"points"`
}
