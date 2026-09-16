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

func TestRequestStatusScopeAndSafeFields(t *testing.T) {
	for _, cluster := range []string{"c1", "other", ""} {
		t.Run("cluster="+cluster, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() != "/api/requests/request%2F1" || r.URL.RawQuery != "" {
					t.Errorf("요청 상세 경로 계약 위반: %s", r.URL.EscapedPath())
				}
				io.WriteString(w, `{"requestId":"request/1","clusterId":"`+cluster+`","type":"TOPIC_CHANGE","status":"APPROVED","payload":{"topicName":"orders","password":"secret-marker","after":{"sasl.jaas.config":"secret-marker"}},"summary":"secret-marker","history":[{"time":"2026-09-16T00:00:00Z","state":"APPROVED","actor":"operator","comment":"secret-marker"}],"policyResult":{"passed":true,"riskLevel":"LOW","violations":[{"code":"MIN_ISR","severity":"WARN","message":"secret-marker"}]}}`)
			}))
			defer server.Close()
			result, err := mockClient(t, server).RequestStatus(context.Background(), "c1", "request/1")
			if cluster != "c1" {
				requireErrorCode(t, err, "invalid_response")
				if result.RequestID != "" {
					t.Fatal("클러스터 불일치 상세 노출")
				}
				return
			}
			if err != nil || result.Status != "APPROVED" || result.Payload.TopicName != "orders" {
				t.Fatalf("요청 변환 실패: %+v, %v", result, err)
			}
			raw, _ := json.Marshal(result)
			if strings.Contains(string(raw), "secret-marker") || strings.Contains(string(raw), "APPLIED") {
				t.Fatal("민감정보 노출 또는 승인/반영 상태 혼동")
			}
		})
	}
}

func TestAssetRelationsContractAndFailureSanitization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.EscapedPath() != "/api/clusters/c%2F1/asset-graph" || q.Get("center") != "PRINCIPAL:User:a & b" || q.Get("depth") != "2" || q.Get("view") != "full" || q.Get("expandStructural") != "false" || len(q) != 4 {
			t.Errorf("관계 조회 계약 위반: %s", r.URL)
		}
		io.WriteString(w, `{"clusterId":"c/1","center":"PRINCIPAL:User:a & b","view":"full","depth":2,"nodes":[],"edges":[],"errors":["secret-marker sasl.password=hidden"]}`)
	}))
	defer server.Close()
	result, err := mockClient(t, server).AssetRelations(context.Background(), "c/1", "PRINCIPAL", "User:a & b", 2)
	if err != nil || len(result.Errors) != 1 || strings.Contains(result.Errors[0], "secret-marker") {
		t.Fatalf("부분 오류 안전 변환 실패: %+v %v", result, err)
	}
}

func TestImpactRequestRejectInvalidIdentityWithoutHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("잘못된 입력으로 백엔드 호출") }))
	defer server.Close()
	client := mockClient(t, server)
	for _, kind := range []string{"SCRAM_USER", "ACL", "unknown"} {
		_, err := client.AssetRelations(context.Background(), "c1", kind, "id", 1)
		requireErrorCode(t, err, "invalid_request")
	}
	_, err := client.AssetImpact(context.Background(), "c1", "PRINCIPAL", "group:User:prefix")
	requireErrorCode(t, err, "invalid_request")
	_, err = client.RequestStatus(context.Background(), "", "r1")
	requireErrorCode(t, err, "invalid_request")
}
