package ableops

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTopicMemberEndpointContracts(t *testing.T) {
	for _, tc := range []struct {
		name, path, body string
		call             func(context.Context, *Client) (any, error)
	}{
		{"topic", "/api/clusters/c1/topics/orders", `{"name":"orders","partitions":3,"replicationFactor":3,"owner":"합성 소유자","configs":{"retention.ms":"86400000","min.insync.replicas":"2","sasl.password":"private-value","ssl.keystore.password":"private-value"},"messages":["private-value"]}`, func(ctx context.Context, c *Client) (any, error) { return c.TopicDetail(ctx, "c1", "orders") }},
		{"partitions", "/api/clusters/c1/topics/orders/partitions", `{"topic":"orders","internal":false,"partitions":[{"partition":0,"leader":1,"leaderEpoch":2,"replicas":[1,2],"isr":[1],"offlineReplicas":[],"state":"URP"}],"health":{"status":"WARN","replicationFactor":2,"minIsr":1,"underReplicated":1,"reasons":["합성 복제 지연"]},"authConfig":"private-value"}`, func(ctx context.Context, c *Client) (any, error) { return c.TopicPartitions(ctx, "c1", "orders") }},
		{"members", "/api/clusters/c1/consumer-groups/readers/members", `[{"memberId":"m1","clientId":"client1","clientHost":"synthetic-host","instanceId":"instance1","lastSeenAt":"2026-09-16T00:00:00Z","assignments":[{"topic":"orders","partitions":[0]}],"password":"private-value"}]`, func(ctx context.Context, c *Client) (any, error) { return c.ConsumerGroupMembers(ctx, "c1", "readers") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.path || r.URL.RawQuery != "" {
					t.Errorf("예상하지 않은 경로 또는 필터: %s", r.URL.RequestURI())
				}
				io.WriteString(w, tc.body)
			}))
			defer server.Close()
			out, err := tc.call(context.Background(), mockClient(t, server))
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(out)
			if strings.Contains(string(raw), "private-value") {
				t.Fatal("허용하지 않은 필드 노출")
			}
			if tc.name == "topic" {
				detail := out.(TopicDetail)
				if detail.Name != "orders" || detail.Configs == nil || detail.Configs.RetentionMS == nil || *detail.Configs.RetentionMS != "86400000" {
					t.Fatal("토픽 기본 정보와 허용 설정 누락")
				}
			}
		})
	}
}

func TestTopicMemberResponseValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		call       func(context.Context, *Client) (any, error)
	}{
		{"wrong_topic", `{"name":"other"}`, func(ctx context.Context, c *Client) (any, error) { return c.TopicDetail(ctx, "c1", "orders") }},
		{"wrong_partition_topic", `{"topic":"other","partitions":[],"health":{"status":"OK"}}`, func(ctx context.Context, c *Client) (any, error) { return c.TopicPartitions(ctx, "c1", "orders") }},
		{"missing_health", `{"topic":"orders","partitions":[]}`, func(ctx context.Context, c *Client) (any, error) { return c.TopicPartitions(ctx, "c1", "orders") }},
		{"member_business_failure", `{"status":"FORBIDDEN","error":"private-value"}`, func(ctx context.Context, c *Client) (any, error) { return c.ConsumerGroupMembers(ctx, "c1", "readers") }},
		{"missing_member_id", `[{"clientId":"client1"}]`, func(ctx context.Context, c *Client) (any, error) { return c.ConsumerGroupMembers(ctx, "c1", "readers") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) }))
			defer server.Close()
			_, err := tc.call(context.Background(), mockClient(t, server))
			requireErrorCode(t, err, "invalid_response")
		})
	}
}

func TestTopicMemberInputAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("잘못된 인자 또는 취소 후 백엔드 요청")
	}))
	defer server.Close()
	c := mockClient(t, server)
	for _, call := range []func(context.Context, string, string) error{
		func(ctx context.Context, cluster, name string) error {
			_, err := c.TopicDetail(ctx, cluster, name)
			return err
		},
		func(ctx context.Context, cluster, name string) error {
			_, err := c.TopicPartitions(ctx, cluster, name)
			return err
		},
		func(ctx context.Context, cluster, name string) error {
			_, err := c.ConsumerGroupMembers(ctx, cluster, name)
			return err
		},
	} {
		requireErrorCode(t, call(context.Background(), "", "orders"), "invalid_request")
		requireErrorCode(t, call(context.Background(), "c1", " "), "invalid_request")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		requireErrorCode(t, call(ctx, "c1", "orders"), "canceled")
	}
}

func TestConsumerGroupMembersRejectsAmbiguousPathBeforeHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("디코딩을 보장할 수 없는 경로의 멤버 API 호출")
	}))
	defer server.Close()
	c := mockClient(t, server)
	for _, name := range []string{"readers/team", "/readers", "readers/", "readers;team", "readers,team"} {
		members, err := c.ConsumerGroupMembers(context.Background(), "c1", name)
		requireErrorCode(t, err, "unsupported")
		if members != nil {
			t.Fatal("대상 일치를 확인할 수 없는 그룹의 멤버 반환")
		}
	}
	_, err := c.ConsumerGroupMembers(context.Background(), "c/1", "readers")
	requireErrorCode(t, err, "unsupported")
}

func TestConsumerGroupMembersAllowsCanonicalEscaping(t *testing.T) {
	for _, name := range []string{"합성 그룹", "readers%2Fteam", "readers?team", "readers#team"} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.RawPath != "" || r.URL.Path != "/api/clusters/c1/consumer-groups/"+name+"/members" {
					t.Error("백엔드 경로 인코딩과 그룹 식별자 불일치")
				}
				io.WriteString(w, `[]`)
			}))
			defer server.Close()
			_, err := mockClient(t, server).ConsumerGroupMembers(context.Background(), "c1", name)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
