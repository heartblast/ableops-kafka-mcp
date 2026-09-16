package tools_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

func TestTopicMemberRequiredInputs(t *testing.T) {
	var requests atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1); io.WriteString(w, `{}`) })
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{"get_topic_detail", map[string]any{"cluster_id": "c1"}},
		{"get_topic_detail", map[string]any{"cluster_id": " ", "topic_name": "orders"}},
		{"get_topic_detail", map[string]any{"cluster_id": "c1", "topic_name": " "}},
		{"get_topic_detail", map[string]any{"cluster_id": "c1", "topic_name": "orders", "include": []string{"messages"}}},
		{"get_topic_detail", map[string]any{"cluster_id": "c1", "topic_name": "orders", "include": []string{"partitions", "partitions"}}},
		{"get_topic_detail", map[string]any{"cluster_id": "c1", "topic_name": "orders", "unexpected": true}},
		{"get_consumer_group_members", map[string]any{"cluster_id": "c1"}},
		{"get_consumer_group_members", map[string]any{"cluster_id": "c1", "group_name": " "}},
		{"get_consumer_group_members", map[string]any{"cluster_id": "c1", "group_name": "readers", "limit": 101}},
	} {
		if !call(t, cs, tc.name, tc.args).IsError {
			t.Errorf("잘못된 입력 허용: %s", tc.name)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("잘못된 입력이 백엔드에 전달됨: %d", requests.Load())
	}
}

func TestTopicDetailIncludesAndSafeFields(t *testing.T) {
	var requests atomic.Int32
	cs, logs := connect(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.RawQuery != "" {
			t.Error("백엔드에 없는 필터 전달")
		}
		switch r.URL.Path {
		case "/api/clusters/c1/topics/orders":
			io.WriteString(w, `{"name":"orders","partitions":2,"replicationFactor":2,"department":"합성 부서","owner":"합성 담당자","configs":{"retention.ms":"86400000","sasl.password":"private-marker"},"messages":["private-marker"]}`)
		case "/api/clusters/c1/topics/orders/partitions":
			io.WriteString(w, `{"topic":"orders","partitions":[{"partition":0,"leader":1,"replicas":[1,2],"isr":[1],"offlineReplicas":[],"state":"URP"},{"partition":1,"leader":2,"replicas":[1,2],"isr":[1,2],"offlineReplicas":[],"state":"정상"}],"health":{"status":"WARN","underReplicated":1,"reasons":["합성 복제 지연"]}}`)
		default:
			t.Errorf("예상하지 않은 API: %s", r.URL.Path)
		}
	})
	basic := decoded[tools.TopicDetailData](t, call(t, cs, "get_topic_detail", map[string]any{"cluster_id": "c1", "topic_name": "orders"}))
	if basic.Status != "ok" || basic.Data.Topic.Configs != nil || basic.Data.Partitions != nil || requests.Load() != 1 {
		t.Fatalf("기본 상세의 추가 호출 또는 설정 노출: %+v", basic)
	}
	res := call(t, cs, "get_topic_detail", map[string]any{"cluster_id": "c1", "topic_name": "orders", "include": []string{"configs", "partitions"}, "limit": 1})
	out := decoded[tools.TopicDetailData](t, res)
	if out.Status != "ok" || !out.Truncated || requests.Load() != 3 || out.Data.Topic.Configs == nil || *out.Data.Topic.Configs.RetentionMS != "86400000" || out.Data.Partitions.Health.Status != "WARN" || out.Data.Partitions.Items.Received != 2 || out.Data.Partitions.Items.Returned != 1 || len(out.Data.Partitions.Items.Items[0].Replicas) != 1 {
		t.Fatalf("선택 조회·설정·복제 상태 또는 제한 실패: %+v", out)
	}
	raw, _ := json.Marshal(res)
	if strings.Contains(string(raw), "private-marker") || strings.Contains(logs.String(), "private-marker") || strings.Contains(logs.String(), "orders") {
		t.Fatal("민감 필드 또는 본문 로그 노출")
	}
}

func TestTopicDetailRequiredAndOptionalFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		required   bool
		httpStatus int
		body       string
		wantCode   string
	}{
		{"required_forbidden", true, 403, `{}`, "access_denied"},
		{"required_missing", true, 404, `{}`, "not_found"},
		{"required_wrong_target", true, 200, `{"name":"other"}`, "invalid_response"},
		{"optional_forbidden", false, 403, `{}`, "access_denied"},
		{"optional_unavailable", false, 503, `{"error":"private-marker"}`, "backend_unavailable"},
		{"optional_wrong_target", false, 200, `{"topic":"other","partitions":[],"health":{"status":"OK"}}`, "invalid_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if tc.required || strings.HasSuffix(r.URL.Path, "/partitions") {
					w.WriteHeader(tc.httpStatus)
					io.WriteString(w, tc.body)
					return
				}
				io.WriteString(w, `{"name":"orders"}`)
			})
			res := call(t, cs, "get_topic_detail", map[string]any{"cluster_id": "c1", "topic_name": "orders", "include": []string{"partitions"}})
			out := decoded[tools.TopicDetailData](t, res)
			if res.IsError != tc.required || out.Errors[0].Code != tc.wantCode {
				t.Fatalf("필수·선택 실패 구분 실패: %+v", out)
			}
			if tc.required {
				if out.Data != nil || requests.Load() != 1 {
					t.Fatal("필수 조회 실패 후 데이터 또는 부가 조회 노출")
				}
			} else if out.Status != "partial" || out.Data.Topic.Name != "orders" || out.Data.Partitions != nil || requests.Load() != 2 {
				t.Fatalf("선택 실패 시 기본 상세 소실: %+v", out)
			}
		})
	}
}

func TestConsumerGroupMembersEmptyAndOptionalFields(t *testing.T) {
	for _, empty := range []bool{true, false} {
		t.Run(map[bool]string{true: "empty", false: "optional_omitted"}[empty], func(t *testing.T) {
			var requests atomic.Int32
			cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path != "/api/clusters/c1/consumer-groups/readers/members" || r.URL.RawQuery != "" {
					t.Errorf("멤버 API 경로 또는 필터 오류: %s", r.URL.RequestURI())
				}
				if empty {
					io.WriteString(w, `[]`)
				} else {
					io.WriteString(w, `[{"memberId":"m1","assignments":[{"topic":"orders","partitions":[0,1]}],"password":"private-marker"}]`)
				}
			})
			res := call(t, cs, "get_consumer_group_members", map[string]any{"cluster_id": "c1", "group_name": "readers"})
			out := decoded[tools.ConsumerGroupMembersData](t, res)
			if res.IsError || out.Status != "ok" || out.Data.GroupName != "readers" || requests.Load() != 1 || len(out.Limitations) == 0 {
				t.Fatalf("멤버 조회 실패 또는 빈 멤버 장애 추정: %+v", out)
			}
			if empty && out.Data.Members.Returned != 0 {
				t.Fatal("빈 멤버 계약 불일치")
			}
			if !empty && (out.Data.Members.Items[0].ClientID != "" || len(out.Data.Members.Items[0].Assignments[0].Partitions) != 2) {
				t.Fatal("선택 필드 누락 또는 할당 정보 손실")
			}
			raw, _ := json.Marshal(res)
			if strings.Contains(string(raw), "private-marker") {
				t.Fatal("민감 멤버 필드 노출")
			}
		})
	}
}

func TestConsumerGroupMembersSlashIsUnsupported(t *testing.T) {
	var requests atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		io.WriteString(w, `[]`)
	})
	res := call(t, cs, "get_consumer_group_members", map[string]any{"cluster_id": "c1", "group_name": "readers/team"})
	out := decoded[tools.ConsumerGroupMembersData](t, res)
	if !res.IsError || out.Status != "error" || out.Data != nil || len(out.Errors) != 1 || out.Errors[0].Code != "unsupported" || requests.Load() != 0 {
		t.Fatalf("슬래시 그룹 미지원 또는 사전 호출 차단 실패: %+v", out)
	}
}

func TestConsumerGroupMembersFailureAndBounds(t *testing.T) {
	for _, status := range []int{403, 404, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				io.WriteString(w, `{"error":"private-marker"}`)
			})
			res := call(t, cs, "get_consumer_group_members", map[string]any{"cluster_id": "c1", "group_name": "readers"})
			out := decoded[tools.ConsumerGroupMembersData](t, res)
			if !res.IsError || out.Data != nil || out.Errors[0].HTTPStatus != status {
				t.Fatalf("멤버 필수 조회 오류: %+v", out)
			}
		})
	}
	t.Run("nested_count_and_bytes", func(t *testing.T) {
		members := make([]ableops.ConsumerGroupMember, 80)
		for i := range members {
			members[i] = ableops.ConsumerGroupMember{MemberID: "synthetic-member", ClientID: strings.Repeat(`"\\`, 1500), Assignments: []ableops.ConsumerGroupAssignment{{Topic: "orders", Partitions: []int32{0, 1, 2}}, {Topic: "other", Partitions: []int32{0}}}}
		}
		cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) { jsonReply(t, w, members) })
		res := call(t, cs, "get_consumer_group_members", map[string]any{"cluster_id": "c1", "group_name": "readers", "limit": 2})
		out := decoded[tools.ConsumerGroupMembersData](t, res)
		if res.IsError || !out.Truncated || out.Data.Members.Received != 80 || out.Data.Members.Returned > 2 || len(out.Data.Members.Items[0].Assignments[0].Partitions) != 2 {
			t.Fatalf("멤버 출력 제한 실패: %+v", out)
		}
		structured, _ := json.Marshal(res.StructuredContent)
		wire, _ := json.Marshal(res)
		if len(structured) > tools.MaxOutputBytes || len(wire) > tools.MaxResultBytes {
			t.Fatal("MCP 출력 바이트 제한 초과")
		}
	})
}
