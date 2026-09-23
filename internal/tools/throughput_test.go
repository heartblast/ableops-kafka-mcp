package tools_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// throughputBackend는 세 REST 계약(현재값·시계열·Lag 관측)을 경로별로 흉내 낸다. 빈 문자열이면 500을 돌려준다.
type throughputBackend struct {
	current, history, lag string
}

func (b throughputBackend) handler(t *testing.T, requests *atomic.Int32, queries *[]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if queries != nil {
			*queries = append(*queries, r.URL.RequestURI())
		}
		var body string
		switch r.URL.Path {
		case "/api/clusters/c1/consumer-lag/throughput":
			body = b.current
		case "/api/clusters/c1/consumer-lag/throughput/history":
			body = b.history
		case "/api/clusters/c1/consumer-lag":
			body = b.lag
		default:
			t.Errorf("허용되지 않은 경로: %s", r.URL.RequestURI())
		}
		if body == "" {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, `{"error":"prometheus http://10.0.0.1:9090 password=private-marker"}`)
			return
		}
		io.WriteString(w, body)
	}
}

func measured(v float64) string {
	return fmt.Sprintf(`{"status":"MEASURED","value":%g,"source":"Prometheus(JMX MessagesInPerSec)","windowSec":300}`, v)
}

// 다른 그룹·다른 Topic 대상이 섞인 클러스터 리포트다. 요청 대상은 g1 × t2 하나다.
func currentReport(target string) string {
	return `{"clusterId":"c1","status":"OK","partial":false,"reasons":[],"checkedAt":"2026-09-23T00:00:00Z",` +
		`"produceSource":{"kind":"prometheus","label":"Prometheus","query":"sum(rate(private_label{secret=\"private-marker\"}[5m]))","windowSec":300},` +
		`"consumeSource":{"kind":"prometheus","label":"Prometheus","query":"private-marker","windowSec":300,"warning":"dial tcp 10.0.0.1: private-marker"},` +
		`"topics":[],"summary":{"targets":3},"targets":[` +
		`{"group":"g1","topic":"t1","currentLag":999,"lagObservation":"OBSERVED","produceRate":` + measured(9) + `,"consumeRate":` + measured(9) + `,"lagGrowthRate":` + measured(0) + `,"processingRatio":` + measured(1) + `,"recoveryEtaSec":{"status":"NOT_APPLICABLE"},"processingState":"HEALTHY"},` +
		target + `,` +
		`{"group":"g2","topic":"t2","currentLag":777,"lagObservation":"OBSERVED","produceRate":` + measured(5) + `,"consumeRate":` + measured(5) + `,"lagGrowthRate":` + measured(0) + `,"processingRatio":` + measured(1) + `,"recoveryEtaSec":{"status":"NOT_APPLICABLE"},"processingState":"HEALTHY"}]}`
}

const recoveringTarget = `{"group":"g1","topic":"t2","currentLag":120,"lagObservation":"OBSERVED","monitored":true,` +
	`"produceRate":{"status":"MEASURED","value":10,"windowSec":300},"consumeRate":{"status":"MEASURED","value":15,"windowSec":300},` +
	`"lagGrowthRate":{"status":"MEASURED","value":-5},"processingRatio":{"status":"MEASURED","value":1.5},` +
	`"recoveryEtaSec":{"status":"MEASURED","value":24},"processingState":"RECOVERING","processingReason":"Lag 120 이 줄어드는 중"}`

func historyBody(rng string, produce, consume, lag string) string {
	return `{"clusterId":"c1","group":"g1","topic":"t2","range":"` + rng + `","stepSec":30,"from":"2026-09-23T00:00:00Z","to":"2026-09-23T00:15:00Z",` +
		`"produce":` + produce + `,"consume":` + consume + `,"lag":` + lag + `}`
}

func series(unit string, n int) string {
	points := make([]string, n)
	for i := range points {
		points[i] = fmt.Sprintf(`{"ts":"2026-09-23T00:%02d:00Z","value":%d}`, i%60, i)
	}
	return `{"status":"MEASURED","source":"Prometheus","query":"private-marker","unit":"` + unit + `","windowSec":300,"points":[` + strings.Join(points, ",") + `]}`
}

func unmeasuredSeries(status, unit string) string {
	return `{"status":"` + status + `","unit":"` + unit + `","reason":"Prometheus 조회에 실패했습니다: dial tcp 10.0.0.1 private-marker","points":[]}`
}

func lagBody(correlation string) string {
	return `{"clusterId":"c1","status":"OK","checkedAt":"2026-09-23T00:00:01Z","alertsStatus":"OK",` +
		`"groups":[{"group":"g1","state":"Stable","rebalancing":false,"observation":"OBSERVED","rebalance":{"state":"STABLE","lastObservedAt":"2026-09-22T23:58:00Z","observedDurationSec":40,"recentCount":1,"recentWindowSec":3600,"prolonged":false}},` +
		`{"group":"g2","state":"PreparingRebalance","rebalancing":true,"observation":"OBSERVED","rebalance":{"state":"REBALANCING","recentCount":5,"recentWindowSec":3600}}],` +
		`"targets":[{"group":"g2","topic":"t2","observation":"OBSERVED","rebalanceCorrelation":{"state":"REBALANCE_REPEATED","reason":"다른 대상"}},` +
		`{"group":"g1","topic":"t2","observation":"OBSERVED","processingState":"RECOVERING"` + correlation + `}]}`
}

func TestTargetThroughputSchemaAndInput(t *testing.T) {
	var requests atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1) })
	listed, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var tool *mcp.Tool
	for _, item := range listed.Tools {
		if item.Name == "get_consumer_target_throughput" {
			tool = item
		}
	}
	if tool == nil {
		t.Fatal("신규 도구 미등록")
	}
	if tool.Annotations == nil || tool.Annotations.ReadOnlyHint || tool.Annotations.IdempotentHint || tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint {
		t.Fatalf("Lag 관측 부작용 annotation 불일치: %+v", tool.Annotations)
	}
	raw, _ := json.Marshal(tool.InputSchema)
	var schema struct {
		Required             []string                  `json:"required"`
		AdditionalProperties bool                      `json:"additionalProperties"`
		Properties           map[string]map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if strings.Join(schema.Required, ",") != "cluster_id,group_name,topic_name" || schema.AdditionalProperties {
		t.Fatalf("필수·추가 속성 계약 불일치: %s", raw)
	}
	if _, ok := schema.Properties["limit"]; ok {
		t.Fatalf("대상 1건 도구에 limit 노출: %s", raw)
	}
	if fmt.Sprint(schema.Properties["range"]["enum"]) != "[5m 15m 1h 6h]" {
		t.Fatalf("range enum이 백엔드 지원 기간과 다름: %v", schema.Properties["range"])
	}
	for _, args := range []map[string]any{
		{"cluster_id": "c1", "group_name": "g1"},
		{"cluster_id": "c1", "topic_name": "t2"},
		{"group_name": "g1", "topic_name": "t2"},
		{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2", "range": "7d"},
		{"cluster_id": "c1", "group_name": "g1", "topic_name": " t2"},
		{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2", "limit": 10},
		{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2", "query": "up"},
	} {
		if !call(t, cs, "get_consumer_target_throughput", args).IsError {
			t.Errorf("잘못된 인자를 허용함: %v", args)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("검증 실패 후 REST 요청 발생: %d", requests.Load())
	}
}

func TestTargetThroughputExactScope(t *testing.T) {
	var requests atomic.Int32
	var queries []string
	backend := throughputBackend{
		current: currentReport(recoveringTarget),
		history: historyBody("15m", series("msg/s", 31), series("msg/s", 31), series("messages", 31)),
		lag:     lagBody(`,"rebalanceCorrelation":{"state":"REBALANCE_RECOVERY","reason":"재조정 종료 관측 뒤 회복 중","sinceEndSec":120}`),
	}
	cs, _ := connect(t, backend.handler(t, &requests, &queries))
	res := call(t, cs, "get_consumer_target_throughput", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2"})
	raw, _ := json.Marshal(res.StructuredContent)
	out := decoded[tools.TargetThroughputData](t, res)
	if res.IsError || out.Status != "ok" || requests.Load() != 3 {
		t.Fatalf("정상 조회 실패(요청 %d): %s", requests.Load(), raw)
	}
	if !strings.Contains(strings.Join(queries, " "), "range=15m") {
		t.Fatalf("기본 기간 15m 미전달: %v", queries)
	}
	c := out.Data.Current
	if c == nil || c.Group != "g1" || c.Topic != "t2" || c.CurrentLag != 120 || c.ProcessingState != "RECOVERING" || *c.ProcessingRatio.Value != 1.5 || *c.RecoveryEtaSec.Value != 24 || *c.LagGrowthRate.Value != -5 {
		t.Fatalf("정확한 대상·백엔드 판정 보존 실패: %+v", c)
	}
	if out.Data.History.Group != "g1" || out.Data.History.Topic != "t2" || len(out.Data.History.Produce.Points) != 31 || out.Data.History.ExpectedPoints != 31 {
		t.Fatalf("시계열 범위 불일치: %+v", out.Data.History)
	}
	rb := out.Data.Rebalance
	if rb == nil || rb.Correlation.State != "REBALANCE_RECOVERY" || *rb.Correlation.SinceEndSec != 120 || rb.GroupRebalance.State != "STABLE" || rb.Semantics != "correlation_hint_not_causation" {
		t.Fatalf("다른 대상의 재조정 힌트 혼입 또는 누락: %+v", rb)
	}
	for _, leak := range []string{"999", "777", "REBALANCE_REPEATED", "private-marker", "10.0.0.1", "query"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("범위 밖 데이터 또는 내부 정보 노출(%s): %s", leak, raw)
		}
	}
	joined := strings.Join(out.Limitations, " ")
	if !strings.Contains(joined, "원인 판정이 아닙니다") || strings.Contains(joined, "때문에") {
		t.Fatalf("상관 의미 제한 누락 또는 인과 표현: %v", out.Limitations)
	}
}

func TestTargetThroughputCurrentStatuses(t *testing.T) {
	full := historyBody("15m", series("msg/s", 31), series("msg/s", 31), series("messages", 31))
	lag := lagBody(`,"rebalanceCorrelation":{"state":"NONE","reason":"최근 재조정 없음"}`)
	for _, tc := range []struct {
		name, target, status string
	}{
		{"no_data", `{"group":"g1","topic":"t2","currentLag":5,"lagObservation":"OBSERVED","produceRate":{"status":"NO_DATA","reason":"표본 0건: private-marker"},"consumeRate":` + measured(3) + `,"lagGrowthRate":{"status":"NOT_APPLICABLE"},"processingRatio":{"status":"NOT_APPLICABLE"},"recoveryEtaSec":{"status":"NOT_APPLICABLE"},"processingState":"UNOBSERVED","processingReason":"private-marker"}`, "partial"},
		{"query_failed", `{"group":"g1","topic":"t2","currentLag":5,"lagObservation":"OBSERVED","produceRate":{"status":"QUERY_FAILED","reason":"dial tcp private-marker"},"consumeRate":{"status":"QUERY_FAILED","reason":"private-marker"},"lagGrowthRate":{"status":"NOT_APPLICABLE"},"processingRatio":{"status":"NOT_APPLICABLE"},"recoveryEtaSec":{"status":"NOT_APPLICABLE"},"processingState":"UNOBSERVED"}`, "partial"},
		{"lag_partial", strings.Replace(recoveringTarget, `"lagObservation":"OBSERVED"`, `"lagObservation":"PARTIAL"`, 1), "partial"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			cs, _ := connect(t, throughputBackend{current: currentReport(tc.target), history: full, lag: lag}.handler(t, &requests, nil))
			res := call(t, cs, "get_consumer_target_throughput", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2"})
			raw, _ := json.Marshal(res.StructuredContent)
			out := decoded[tools.TargetThroughputData](t, res)
			if out.Status != tc.status || res.IsError || strings.Contains(string(raw), "private-marker") {
				t.Fatalf("현재값 상태 보존 실패: %s", raw)
			}
			c := out.Data.Current
			if c.ProduceRate.Status != "MEASURED" && c.ProduceRate.Value != nil {
				t.Fatalf("미수집에 값 생성: %+v", c.ProduceRate)
			}
			if tc.name != "lag_partial" && (c.ProcessingState != "UNOBSERVED" || c.ProcessingReason == "") {
				t.Fatalf("UNOBSERVED 판정 재계산 또는 사유 누락: %+v", c)
			}
		})
	}
}

func TestTargetThroughputHistoryStatuses(t *testing.T) {
	current := currentReport(recoveringTarget)
	lag := lagBody(`,"rebalanceCorrelation":{"state":"NONE","reason":"없음"}`)
	for _, tc := range []struct {
		name, history, want string
	}{
		{"gap", historyBody("1h", series("msg/s", 40), series("msg/s", 61), series("messages", 61)), "partial"},
		{"not_collected", historyBody("1h", unmeasuredSeries("NOT_COLLECTED", "msg/s"), unmeasuredSeries("NOT_COLLECTED", "msg/s"), unmeasuredSeries("NOT_COLLECTED", "messages")), "partial"},
		{"one_series_failed", historyBody("1h", series("msg/s", 61), unmeasuredSeries("QUERY_FAILED", "msg/s"), series("messages", 61)), "partial"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			cs, _ := connect(t, throughputBackend{current: current, history: tc.history, lag: lag}.handler(t, &requests, nil))
			res := call(t, cs, "get_consumer_target_throughput", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2", "range": "1h"})
			raw, _ := json.Marshal(res.StructuredContent)
			out := decoded[tools.TargetThroughputData](t, res)
			if out.Status != tc.want || strings.Contains(string(raw), "private-marker") {
				t.Fatalf("시계열 상태 보존 실패: %s", raw)
			}
			h := out.Data.History
			for _, s := range []ableops.ThroughputSeries{h.Produce, h.Consume, h.Lag} {
				if s.Status != "MEASURED" && (len(s.Points) != 0 || s.Reason == "") {
					t.Fatalf("미수집 계열에 점 생성 또는 사유 누락: %+v", s)
				}
			}
			if tc.name == "gap" && len(h.Produce.Points) != 40 {
				t.Fatalf("빈 구간을 채움: %d", len(h.Produce.Points))
			}
		})
	}
	t.Run("range_mismatch", func(t *testing.T) {
		var requests atomic.Int32
		// 백엔드는 미지원 기간을 15m으로 바꾼다. 요청과 다른 기간이면 결과로 쓰지 않는다.
		body := historyBody("15m", series("msg/s", 31), series("msg/s", 31), series("messages", 31))
		cs, _ := connect(t, throughputBackend{current: current, history: body, lag: lag}.handler(t, &requests, nil))
		out := decoded[tools.TargetThroughputData](t, call(t, cs, "get_consumer_target_throughput", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2", "range": "6h"}))
		if out.Status != "partial" || out.Data.History != nil || out.Errors[0].Component != "throughput_history" {
			t.Fatalf("기간 불일치 시계열 수용: %+v", out)
		}
	})
}

func TestTargetThroughputRebalanceAndPartial(t *testing.T) {
	full := historyBody("15m", series("msg/s", 31), series("msg/s", 31), series("messages", 31))
	for _, state := range []string{"REBALANCE_ACTIVE", "REBALANCE_RECENT", "REBALANCE_RECOVERY", "REBALANCE_PERSISTENT_IMPACT", "REBALANCE_REPEATED"} {
		var requests atomic.Int32
		cs, _ := connect(t, throughputBackend{current: currentReport(recoveringTarget), history: full, lag: lagBody(`,"rebalanceCorrelation":{"state":"` + state + `","reason":"상관 힌트"}`)}.handler(t, &requests, nil))
		out := decoded[tools.TargetThroughputData](t, call(t, cs, "get_consumer_target_throughput", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2"}))
		if out.Status != "ok" || out.Data.Rebalance.Correlation.State != state {
			t.Fatalf("%s 보존 실패: %+v", state, out)
		}
	}
	for _, tc := range []struct{ name, correlation string }{
		{"unobserved", `,"rebalanceCorrelation":{"state":"UNOBSERVED","reason":"그룹 상태 미확인"}`},
		{"not_combined", ``},
	} {
		var requests atomic.Int32
		cs, _ := connect(t, throughputBackend{current: currentReport(recoveringTarget), history: full, lag: lagBody(tc.correlation)}.handler(t, &requests, nil))
		out := decoded[tools.TargetThroughputData](t, call(t, cs, "get_consumer_target_throughput", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2"}))
		if out.Status != "partial" || !strings.Contains(strings.Join(out.Limitations, " "), "재조정 없음으로 해석하지 마세요") {
			t.Fatalf("%s: 미관측을 재조정 없음으로 처리: %+v", tc.name, out)
		}
	}
	t.Run("history_failed", func(t *testing.T) {
		var requests atomic.Int32
		cs, _ := connect(t, throughputBackend{current: currentReport(recoveringTarget), lag: lagBody(`,"rebalanceCorrelation":{"state":"NONE","reason":"없음"}`)}.handler(t, &requests, nil))
		res := call(t, cs, "get_consumer_target_throughput", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2"})
		raw, _ := json.Marshal(res.StructuredContent)
		out := decoded[tools.TargetThroughputData](t, res)
		if res.IsError || out.Status != "partial" || out.Data.Current == nil || out.Data.History != nil || out.Data.Rebalance == nil || requests.Load() != 3 || strings.Contains(string(raw), "private-marker") {
			t.Fatalf("부분 실패 보존 실패: %s", raw)
		}
		if fmt.Sprint(out.Data.Sources) != "[{current ok} {history error} {rebalance ok}]" {
			t.Fatalf("출처별 결과 불일치: %v", out.Data.Sources)
		}
	})
	t.Run("target_missing_everywhere", func(t *testing.T) {
		var requests atomic.Int32
		cs, _ := connect(t, throughputBackend{current: currentReport(strings.Replace(recoveringTarget, `"topic":"t2"`, `"topic":"t9"`, 1))}.handler(t, &requests, nil))
		res := call(t, cs, "get_consumer_target_throughput", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2"})
		out := decoded[tools.TargetThroughputData](t, res)
		if !res.IsError || out.Status != "error" || out.Data != nil || out.Errors[0].Code != "target_not_found" || requests.Load() != 3 {
			t.Fatalf("대상 없음·전체 실패 판정 실패: %+v", out)
		}
	})
}

func TestTargetThroughputPropagatesCancellation(t *testing.T) {
	canceled := make(chan struct{}, 3)
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			canceled <- struct{}{}
		case <-time.After(5 * time.Second):
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _ = cs.CallTool(ctx, &mcp.CallToolParams{Name: "get_consumer_target_throughput", Arguments: map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2"}})
	select {
	case <-canceled:
	case <-time.After(4 * time.Second):
		t.Fatal("MCP 요청 취소가 REST 호출에 전달되지 않음")
	}
}

func TestGroupLagTopicFilter(t *testing.T) {
	var partitions []string
	for i := 0; i < 60; i++ {
		partitions = append(partitions, fmt.Sprintf(`{"topic":"t1","partition":%d,"lag":1,"status":"OK"}`, i))
	}
	for i := 0; i < 3; i++ {
		partitions = append(partitions, fmt.Sprintf(`{"topic":"t2","partition":%d,"lag":%d,"status":"OK"}`, i, 10+i))
	}
	body := `{"group":"g1","found":true,"status":"OK","state":"Stable","totalLag":93,"totalPartitions":63,"countedPartitions":63,` +
		`"topicLag":[{"topic":"t1","lag":60,"partitions":60},{"topic":"t2","lag":33,"partitions":3}],"partitions":[` + strings.Join(partitions, ",") + `]}`
	var requests atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/api/clusters/c1/consumer-groups/missing/lag" {
			io.WriteString(w, `{"group":"missing","found":false,"code":"NOT_FOUND","status":"OK"}`)
			return
		}
		if r.URL.Path != "/api/clusters/c1/consumer-groups/g1/lag" || r.URL.RawQuery != "" {
			t.Errorf("기존 백엔드 계약 변경: %s", r.URL.RequestURI())
		}
		io.WriteString(w, body)
	})
	// limit 2보다 먼저 필터한다 — 앞 60건을 차지한 t1 때문에 t2가 잘리지 않아야 한다.
	out := decoded[ableops.ConsumerGroupLagView](t, call(t, cs, "get_consumer_group_lag", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t2", "limit": 2}))
	if out.Status != "ok" || out.Data.TopicFilter != "t2" || len(out.Data.Partitions) != 2 || !out.Truncated || len(out.Data.TopicLag) != 1 || out.Data.TopicLag[0].Lag != 33 {
		t.Fatalf("Topic 필터·limit 순서 실패: %+v", out)
	}
	for _, p := range out.Data.Partitions {
		if p.Topic != "t2" {
			t.Fatalf("다른 Topic 파티션 포함: %+v", p)
		}
	}
	if !strings.Contains(strings.Join(out.Limitations, " "), "그룹 전체 값") {
		t.Fatalf("그룹 집계 범위 제한 누락: %v", out.Limitations)
	}
	missingTopic := decoded[ableops.ConsumerGroupLagView](t, call(t, cs, "get_consumer_group_lag", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": "t9"}))
	missingGroup := decoded[ableops.ConsumerGroupLagView](t, call(t, cs, "get_consumer_group_lag", map[string]any{"cluster_id": "c1", "group_name": "missing", "topic_name": "t2"}))
	if missingTopic.Errors[0].Code != "topic_not_found" || missingGroup.Errors[0].Code != "not_found" || len(missingTopic.Data.Partitions) != 0 {
		t.Fatalf("Topic 미존재와 그룹 미존재 구분 실패: %+v / %+v", missingTopic.Errors, missingGroup.Errors)
	}
	// topic_name을 생략하면 기존 결과(그룹 전체 · limit 기본 50)와 같다.
	all := decoded[ableops.ConsumerGroupLagView](t, call(t, cs, "get_consumer_group_lag", map[string]any{"cluster_id": "c1", "group_name": "g1"}))
	if all.Status != "ok" || all.Data.TopicFilter != "" || len(all.Data.Partitions) != 50 || len(all.Data.TopicLag) != 2 || all.Data.TotalLag != 93 || len(all.Limitations) != 1 {
		t.Fatalf("topic_name 생략 시 기존 결과 변경: %+v", all)
	}
	if !call(t, cs, "get_consumer_group_lag", map[string]any{"cluster_id": "c1", "group_name": "g1", "topic_name": " t2"}).IsError {
		t.Fatal("공백 topic_name 허용")
	}
}
