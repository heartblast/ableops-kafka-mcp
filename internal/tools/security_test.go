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

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

const identityToolFixture = `{"principal":"User:reader","identityKind":"SERVICE","identityKindSource":"inferred","managed":true,"status":"ACTIVE","lifecycle":{"stage":"REVIEW_DUE","severity":"WARN","reasons":[]},"credential":{"has":true,"supported":true,"locked":true,"items":[{"username":"reader","mechanism":"SCRAM-SHA-512","iterations":8192,"locked":true,"password":"excluded-secret"}]},"entitlement":{"aclCount":3},"findings":[],"severity":"WARN","recentAudit":[{"before":"excluded-secret"}],"grants":[{"detail":"excluded-secret"}]}`

func TestSecurityIdentityMeaningAndLimits(t *testing.T) {
	var calls atomic.Int32
	cs, logs := connect(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/api/clusters/c1/identities":
			if r.URL.Query().Get("stage") != "REVIEW_DUE" {
				t.Error("단계 필터 누락")
			}
			fmt.Fprintf(w, `{"clusterId":"c1","items":[%s,%s],"summary":{"total":8}}`, identityToolFixture, identityToolFixture)
		case "/api/clusters/c1/identities/User:reader":
			fmt.Fprintf(w, `{"clusterId":"c1","items":%s}`, identityToolFixture)
		default:
			t.Errorf("미예상 조회: %s", r.URL.Path)
		}
	})
	res := call(t, cs, "list_identities", map[string]any{"cluster_id": "c1", "stage": "REVIEW_DUE", "limit": 1})
	out := decoded[tools.IdentityInventoryData](t, res)
	if out.Status != "partial" || out.Data == nil || !out.Truncated || out.Data.Returned != 1 || out.Data.Received != 2 || out.Data.Summary.Total != 8 {
		t.Fatalf("목록 제한/전체 요약 구분: %+v", out)
	}
	detail := decoded[ableops.Identity](t, call(t, cs, "get_identity_detail", map[string]any{"cluster_id": "c1", "principal": "User:reader"}))
	if detail.Status != "partial" || detail.Data == nil || detail.Data.Status != "ACTIVE" || detail.Data.Lifecycle.Stage != "REVIEW_DUE" || detail.Data.IdentityKindSource != "inferred" || !detail.Data.Credential.Locked {
		t.Fatalf("선언/계산/추론/잠금 구분 누락: %+v", detail)
	}
	raw, _ := json.Marshal(detail)
	if strings.Contains(string(raw), "excluded-secret") || strings.Contains(logs.String(), "User:reader") || calls.Load() != 2 {
		t.Fatal("제외 필드/로그 노출 또는 부가 조회")
	}
}

func TestSecurityACLPrincipalScopeAndUnknownSnapshot(t *testing.T) {
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/clusters/c1/principals/User:reader/acls" || r.URL.RawQuery != "" {
			t.Errorf("조건별 ACL 경로: %s", r.URL.RequestURI())
		}
		io.WriteString(w, `{"clusterId":"c1","items":[{"principal":"User:reader","resourceType":"Topic","resourceName":"orders","operation":"Read","password":"excluded-secret"}]}`)
	})
	out := decoded[tools.SnapshotData[ableops.SecurityACL]](t, call(t, cs, "list_acls", map[string]any{"cluster_id": "c1", "principal": "User:reader"}))
	if out.Status != "partial" || out.Data == nil || out.Data.SyncedAt != nil || out.Data.Items[0].Operation != "Read" {
		t.Fatalf("시각 미상 ACL 스냅샷: %+v", out)
	}
}

func TestSecurityDiagnosisBodyFailureAndUnsupported(t *testing.T) {
	for _, tc := range []struct {
		tool, status         string
		supported            bool
		wantStatus, wantCode string
	}{
		{"get_acl_risk", "FORBIDDEN", true, "error", "access_denied"},
		{"get_acl_risk", "WARN", true, "partial", "observation_completeness_unknown"},
		{"get_scram_audit", "UNAVAILABLE", true, "error", "backend_unavailable"},
		{"get_scram_audit", "OK", false, "partial", "unsupported"},
		{"get_scram_audit", "CRITICAL", true, "partial", "observation_completeness_unknown"},
	} {
		t.Run(tc.tool+"/"+tc.status, func(t *testing.T) {
			cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"clusterId":"c1","status":%q,"supported":%t,"findings":[],"principals":[],"generatedAt":"2026-09-16T00:00:00Z","note":"excluded-secret"}`, tc.status, tc.supported)
			})
			res := call(t, cs, tc.tool, map[string]any{"cluster_id": "c1"})
			out := decoded[map[string]any](t, res)
			if out.Status != tc.wantStatus || out.Data == nil || (*out.Data)["status"] != tc.status || len(out.Errors) != 1 || out.Errors[0].Code != tc.wantCode || res.IsError != (tc.wantStatus == "error") {
				t.Fatalf("본문 실패/미지원 구분: %+v", out)
			}
			raw, _ := json.Marshal(res)
			if strings.Contains(string(raw), "excluded-secret") {
				t.Fatal("내부 실패 원문 노출")
			}
		})
	}
}

func TestSecurityRequestsUseObjectAuthorizationPath(t *testing.T) {
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/requests" || r.URL.Query().Get("cluster") != "c1" || len(r.URL.Query()) != 1 {
			t.Errorf("객체 권한이 부족한 경로 사용: %s", r.URL.RequestURI())
		}
		io.WriteString(w, `[{"requestId":"r1","clusterId":"c1","status":"APPROVED","payload":{"topicName":"orders","password":"excluded-secret"}},{"requestId":"r2","clusterId":"c1","status":"VERIFIED","payload":{}}]`)
	})
	out := decoded[tools.ListData[ableops.RequestListItem]](t, call(t, cs, "list_requests", map[string]any{"cluster_id": "c1", "limit": 1}))
	if out.Status != "partial" || len(out.Errors) != 1 || out.Errors[0].Code != "observation_completeness_unknown" || !out.Truncated || out.Data == nil || out.Data.Returned != 1 || out.Data.Received != 2 || out.Data.Items[0].Status != "APPROVED" {
		t.Fatalf("권한 목록/상태 계약: %+v", out)
	}
}

func TestSecurityRequestsEmptyDoesNotHideStoreFailure(t *testing.T) {
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/requests" {
			t.Errorf("신청 범위 이외 호출: %s", r.URL.Path)
		}
		// 실제 Backend는 저장소 실패와 정상 빈 목록 모두 이 응답으로 반환한다.
		io.WriteString(w, `[]`)
	})
	result := call(t, cs, "list_requests", map[string]any{"cluster_id": "c1"})
	out := decoded[tools.ListData[ableops.RequestListItem]](t, result)
	if result.IsError || out.Status != "partial" || out.Data == nil || out.Data.Returned != 0 || len(out.Errors) != 1 || out.Errors[0].Component != "requests" || out.Errors[0].Code != "observation_completeness_unknown" {
		t.Fatalf("신청 저장소 실패를 정상 0건으로 해석: %+v", out)
	}
}

func TestSecuritySchemaAndAnnotations(t *testing.T) {
	var calls atomic.Int32
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, `[]`) })
	listed, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, tool := range listed.Tools {
		switch tool.Name {
		case "list_identities", "get_identity_detail", "list_acls", "get_acl_risk", "get_scram_audit":
			if tool.Annotations == nil || tool.Annotations.ReadOnlyHint || tool.Annotations.IdempotentHint {
				t.Fatalf("동기화/감사 부작용 누락: %s", tool.Name)
			}
		case "list_requests":
			if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint || !tool.Annotations.IdempotentHint {
				t.Fatal("신청 조회 annotation 불일치")
			}
		case "list_access_reviews", "search_audit_logs":
			t.Fatalf("권한 경계 미충족 도구 등록: %s", tool.Name)
		default:
			continue
		}
		seen[tool.Name] = true
		if tool.InputSchema == nil || tool.OutputSchema == nil {
			t.Fatalf("스키마 누락: %s", tool.Name)
		}
		if !call(t, cs, tool.Name, map[string]any{}).IsError {
			t.Fatalf("필수 클러스터 누락 허용: %s", tool.Name)
		}
	}
	if len(seen) != 6 {
		t.Fatalf("추가 보안 도구 발견 개수: %d", len(seen))
	}
	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"list_identities", map[string]any{"cluster_id": "c1", "stage": "invalid"}},
		{"list_requests", map[string]any{"cluster_id": "c1", "page": 1}},
		{"list_acls", map[string]any{"cluster_id": "c1", "resource_type": "Topic"}},
		{"get_identity_detail", map[string]any{"cluster_id": "c1", "principal": " "}},
		{"get_acl_risk", map[string]any{"cluster_id": "c1", "limit": 101}},
	} {
		if !call(t, cs, tc.tool, tc.args).IsError {
			t.Fatalf("미지원 입력 허용: %s", tc.tool)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("잘못된 입력으로 백엔드 호출")
	}
}

func TestSecurityLargeFindingsAreBounded(t *testing.T) {
	cs, _ := connect(t, func(w http.ResponseWriter, r *http.Request) {
		findings := make([]map[string]any, 90)
		for i := range findings {
			findings[i] = map[string]any{"code": "HOST_UNRESTRICTED", "status": "WARN", "target": strings.Repeat("a", 2000)}
		}
		jsonReply(t, w, map[string]any{"clusterId": "c1", "status": "WARN", "findings": findings, "principals": []any{}, "total": 90, "warn": 90, "generatedAt": "2026-09-16T00:00:00Z"})
	})
	res := call(t, cs, "get_acl_risk", map[string]any{"cluster_id": "c1", "limit": 100})
	out := decoded[ableops.ACLRiskReport](t, res)
	raw, _ := json.Marshal(res)
	if !out.Truncated || out.Data == nil || len(out.Data.Findings) >= 90 || out.Data.Total != 90 || len(raw) > tools.MaxResultBytes {
		t.Fatalf("진단 바이트 제한/집계 손실: bytes=%d %+v", len(raw), out)
	}
}
