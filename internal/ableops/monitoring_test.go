package ableops

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMonitoringCancellationAndOneRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-r.Context().Done()
	}))
	defer server.Close()
	client := mockClient(t, server)
	client.timeout = 40 * time.Millisecond
	ctx, cancel := client.BeginOperation(context.Background())
	defer cancel()
	_, err := client.MetricSeries(ctx, "c1", "consumer-lag", "1h")
	requireErrorCode(t, err, "timeout")
	if calls.Load() != 1 {
		t.Fatal("시계열 timeout 후 자동 재시도가 발생했습니다")
	}
	canceled, stop := client.BeginOperation(context.Background())
	stop()
	_, err = client.ClusterStorage(canceled, "c1")
	requireErrorCode(t, err, "canceled")
	if calls.Load() != 1 {
		t.Fatal("취소된 모니터링 요청이 전송되었습니다")
	}
}

func TestMonitoringScopedResponseAndAmbiguousPath(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, `{"clusterId":"c1","scope":"CLUSTER","stored":{"clusterId":"other","scope":"CLUSTER"},"layers":{}}`)
	}))
	defer server.Close()
	client := mockClient(t, server)
	_, err := client.ConsumerLagPolicy(context.Background(), "c1", "", "")
	requireErrorCode(t, err, "invalid_response")
	for _, cluster := range []string{"", "c/1", "c;1", "c,1"} {
		_, err = client.ClusterStorage(context.Background(), cluster)
		if err == nil {
			t.Fatal("모호한 클러스터의 저장량 API 호출을 허용했습니다")
		}
		_, err = client.PartitionReassignments(context.Background(), cluster)
		if err == nil {
			t.Fatal("모호한 클러스터의 재배치 API 호출을 허용했습니다")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("명시하지 않은 클러스터를 기본값으로 바꿨거나 모호한 경로를 호출했습니다")
	}
}
