package tools_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

const graphFixture = `{"clusterId":"c1","center":"TOPIC:orders","view":"full","depth":1,"syncedAt":"2026-09-16T00:00:00Z","nodes":[{"id":"TOPIC:orders","type":"TOPIC","label":"orders"},{"id":"PRINCIPAL:User:reader","type":"PRINCIPAL","label":"reader","attrs":{"hasCredential":true,"credentialLocked":true,"credentials":[{"password":"secret-marker"}],"config":"secret-marker"}}],"edges":[{"id":"permission","source":"PRINCIPAL:User:reader","target":"TOPIC:orders","label":"READ","kind":"PREFIX","attrs":{"principal":"User:reader","operations":["READ"],"aclCount":1,"acls":[{"secret":"secret-marker"}]}}],"summary":{"shownNodes":2,"shownEdges":1,"hiddenNodes":3,"acls":1,"relations":1}}`

func TestAssetImpactGraphAndOptionalFailure(t *testing.T) {
	var calls atomic.Int32
	cs, logs := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/api/clusters/c1/asset-graph":
			io.WriteString(w, graphFixture)
		case "/api/clusters/c1/asset-graph/impact":
			http.Error(w, "secret-marker", http.StatusForbidden)
		default:
			t.Errorf("예상하지 않은 API: %s", r.URL.Path)
		}
	})
	args := map[string]any{"cluster_id": "c1", "asset_type": "TOPIC", "asset_key": "orders"}
	res := call(t, cs, "get_asset_impact", args)
	out := decoded[tools.AssetImpactData](t, res)
	if out.Status != "ok" || out.Data.Impact != nil || !out.Truncated || calls.Load() != 1 {
		t.Fatalf("기본 그래프 결과: %+v calls=%d", out, calls.Load())
	}
	edge := out.Data.Graph.Edges[0]
	if edge.Source != "PRINCIPAL:User:reader" || edge.Target != "TOPIC:orders" || edge.Kind != "PREFIX" || edge.Attrs.Operations[0] != "READ" {
		t.Fatalf("관계 방향/종류 변경: %+v", edge)
	}
	raw, _ := json.Marshal(res)
	if strings.Contains(string(raw), "secret-marker") || strings.Contains(logs.String(), "orders") {
		t.Fatal("민감 속성 또는 대상 본문 로그 노출")
	}
	args["include"] = []string{"impact"}
	out = decoded[tools.AssetImpactData](t, call(t, cs, "get_asset_impact", args))
	if out.Status != "partial" || out.Data == nil || len(out.Errors) != 1 || out.Errors[0].Code != "access_denied" || calls.Load() != 3 {
		t.Fatalf("선택 조회 부분 실패/호출 상한: %+v calls=%d", out, calls.Load())
	}
}

func TestAssetImpactGraphMismatchStopsFollowup(t *testing.T) {
	for _, body := range []string{
		strings.Replace(graphFixture, `"clusterId":"c1"`, `"clusterId":"other"`, 1),
		`{"clusterId":"c1","center":"TOPIC:orders","view":"full","depth":1,"nodes":[{"id":"TOPIC:other","type":"TOPIC","label":"other"}],"edges":[]}`,
	} {
		var calls atomic.Int32
		cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			io.WriteString(w, body)
		})
		out := decoded[tools.AssetImpactData](t, call(t, cs, "get_asset_impact", map[string]any{"cluster_id": "c1", "asset_type": "TOPIC", "asset_key": "orders", "include": []string{"impact"}}))
		if out.Status != "error" || out.Data != nil || calls.Load() != 1 {
			t.Fatalf("클러스터/중심 불일치 후 상세/추가 호출 발생: %+v calls=%d", out, calls.Load())
		}
	}
}

func TestAssetImpactOutputTruncationAndSummary(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.HasSuffix(r.URL.Path, "/impact") {
			if r.URL.Query().Get("type") != "TOPIC" || r.URL.Query().Get("key") != "orders" {
				t.Error("영향 요약 쿼리 계약 위반")
			}
			io.WriteString(w, `{"target":{"id":"TOPIC:orders","type":"TOPIC","label":"orders"},"producers":["User:one","User:two"],"notes":["간접 영향"]}`)
			return
		}
		io.WriteString(w, graphFixture)
	})
	out := decoded[tools.AssetImpactData](t, call(t, cs, "get_asset_impact", map[string]any{"cluster_id": "c1", "asset_type": "TOPIC", "asset_key": "orders", "include": []string{"impact"}, "limit": 1}))
	if out.Status != "ok" || !out.Truncated || len(out.Data.Graph.Nodes) != 1 || len(out.Data.Graph.Edges) != 0 || out.Data.Graph.Nodes[0].ID != "TOPIC:orders" || len(out.Data.Impact.Producers) != 1 || calls.Load() != 2 {
		t.Fatalf("잘림/중심 노드/호출 상한: %+v calls=%d", out, calls.Load())
	}
}

func TestAssetImpactEmptyAndPartialCollection(t *testing.T) {
	for _, body := range []string{
		`{"clusterId":"c1","center":"TOPIC:orders","view":"full","depth":1,"syncedAt":"2026-09-16T00:00:00Z","nodes":[],"edges":[]}`,
		strings.Replace(graphFixture, `"summary":`, `"errors":["secret-marker password"],"summary":`, 1),
	} {
		cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) })
		res := call(t, cs, "get_asset_impact", map[string]any{"cluster_id": "c1", "asset_type": "TOPIC", "asset_key": "orders"})
		out := decoded[tools.AssetImpactData](t, res)
		if out.Status != "partial" || out.Data == nil {
			t.Fatalf("빈 그래프/수집 실패가 완전한 성공으로 해석됨: %+v", out)
		}
		raw, _ := json.Marshal(res)
		if strings.Contains(string(raw), "secret-marker") {
			t.Fatal("수집 오류 원문 노출")
		}
	}
}

func TestRequestStatusStateHistoryAndPayload(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, `{"requestId":"r1","clusterId":"c1","type":"TOPIC_CREATE","status":"APPROVED","payload":{"topicName":"orders","password":"secret-marker","message":"secret-marker"},"policyResult":{"passed":true,"riskLevel":"LOW","violations":[]},"history":[{"time":"2026-09-15T00:00:00Z","state":"REQUESTED","actor":"requester","comment":"secret-marker"},{"time":"2026-09-16T00:00:00Z","state":"APPROVED","actor":"operator","comment":"secret-marker"}],"updatedAt":"2026-09-16T00:00:00Z"}`)
	})
	args := map[string]any{"cluster_id": "c1", "request_id": "r1"}
	out := decoded[tools.RequestStatusData](t, call(t, cs, "get_request_status", args))
	if out.Status != "ok" || out.Data.Request.Status != "APPROVED" || out.Data.History != nil || len(out.Data.Request.History) != 0 {
		t.Fatalf("승인/반영 구분 또는 기본 이력 생략 실패: %+v", out)
	}
	args["include"], args["limit"] = []string{"history"}, 1
	res := call(t, cs, "get_request_status", args)
	out = decoded[tools.RequestStatusData](t, res)
	if !out.Truncated || out.Data.History.Received != 2 || out.Data.History.Returned != 1 || out.Data.History.Items[0].State != "APPROVED" || calls.Load() != 2 {
		t.Fatalf("최근 이력 제한 실패: %+v", out)
	}
	raw, _ := json.Marshal(res)
	if strings.Contains(string(raw), "secret-marker") {
		t.Fatal("payload 또는 이력 의견 노출")
	}
}

func TestImpactRequestRequiredFailuresAndValidation(t *testing.T) {
	for _, name := range []string{"get_asset_impact", "get_request_status"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `{"error":"secret-marker"}`)
			})
			args := map[string]any{"cluster_id": "c1"}
			if name == "get_asset_impact" {
				args["asset_type"], args["asset_key"] = "TOPIC", "orders"
			} else {
				args["request_id"] = "r1"
			}
			args["include"] = []string{"unknown"}
			if !call(t, cs, name, args).IsError || calls.Load() != 0 {
				t.Fatal("미지원 include가 백엔드 호출을 유발함")
			}
			delete(args, "include")
			res := call(t, cs, name, args)
			if !res.IsError || calls.Load() != 1 {
				t.Fatal("필수 조회 권한 거부가 실패로 전달되지 않음")
			}
			args["extra"] = true
			if !call(t, cs, name, args).IsError || calls.Load() != 1 {
				t.Fatal("알 수 없는 필드가 백엔드 호출을 유발함")
			}
		})
	}
}

func TestRequestStatusMismatchAndNotFound(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound} {
		var calls atomic.Int32
		cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(status)
			io.WriteString(w, `{"requestId":"r1","clusterId":"other","status":"APPLIED","payload":{"topicName":"private"}}`)
		})
		out := decoded[tools.RequestStatusData](t, call(t, cs, "get_request_status", map[string]any{"cluster_id": "c1", "request_id": "r1", "include": []string{"history"}}))
		if out.Status != "error" || out.Data != nil || calls.Load() != 1 {
			t.Fatalf("클러스터 불일치/없음 처리 실패: %+v calls=%d", out, calls.Load())
		}
	}
}

func TestRequestStatusRetainsWorkflowStatesAnd200Failure(t *testing.T) {
	for _, state := range []string{"APPROVED", "READY_TO_APPLY", "APPLIED", "VERIFIED", "APPLY_FAILED", "REJECTED", "CANCELED", "DISCARDED", "FAILED"} {
		t.Run(state, func(t *testing.T) {
			cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"requestId":"r1","clusterId":"c1","type":"TOPIC_CREATE","status":%q,"payload":{"topicName":"orders"}}`, state)
			})
			out := decoded[tools.RequestStatusData](t, call(t, cs, "get_request_status", map[string]any{"cluster_id": "c1", "request_id": "r1"}))
			if state == "FAILED" {
				if out.Status != "error" || out.Data != nil {
					t.Fatal("HTTP 200 미정의 업무 실패를 성공으로 해석함")
				}
			} else if out.Status != "ok" || out.Data.Request.Status != state {
				t.Fatalf("업무 상태가 변경됨: %+v", out)
			}
		})
	}
}

func TestAssetImpactEmptyTargetSkipsSummary(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, `{"clusterId":"c1","center":"TOPIC:orders","view":"full","depth":1,"nodes":[],"edges":[]}`)
	})
	out := decoded[tools.AssetImpactData](t, call(t, cs, "get_asset_impact", map[string]any{"cluster_id": "c1", "asset_type": "TOPIC", "asset_key": "orders", "include": []string{"impact"}}))
	if out.Status != "partial" || out.Data.ImpactStatus != "skipped" || out.Data.Impact != nil || calls.Load() != 1 {
		t.Fatalf("대상 없는 그래프에서 영향 요약 호출: %+v calls=%d", out, calls.Load())
	}
}
