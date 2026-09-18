package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/dynamic"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Promotion한 두 도구의 계약 경로다. Static 구현이 쓰던 경로와 계약이 선언한 경로가 같다.
const (
	promoGroupsPath  = "/api/clusters/c1/consumer-groups"
	promoSummaryPath = "/api/events/summary"
)

// 백엔드 응답에는 Static이 공개하지 않는 필드를 섞어 둔다. Dynamic 경로로 실행해도
// 기존 공개 범위 밖 필드가 MCP 결과에 나오지 않아야 한다.
const promoSecret = "synthetic-not-public"

func promoGroupsBody(marker string) string {
	return `{"clusterId":"c1","clusterName":"` + marker + `","environment":"dev","syncedAt":"2026-09-17T00:00:00Z","items":[` +
		`{"name":"g1","state":"Stable","members":2,"totalLag":7,"topicLag":{"orders":7},"internalNote":"` + promoSecret + `"}],` +
		`"adapterError":"` + promoSecret + `"}`
}

func promoSummaryBody(marker string) string {
	return `{"unacknowledgedCritical":1,"openTotal":2,"securityEvents":0,"availabilityEvents":1,"resolvedLast24h":3,` +
		`"notificationFailures":0,"monitoredClusters":1,"coverageGapClusters":0,"attentionUrgent":1,"attentionReview":0,` +
		`"attentionObserve":0,"attentionUnevaluated":0,"syntheticExcluded":4,"generatedAt":"` + marker + `",` +
		`"scope":{"all":false,"clusterCount":1,"totalClusters":1,"clusterId":"c1","clusterDenied":false},` +
		`"collection":{"enabled":true,"intervalSec":60},"internalNote":"` + promoSecret + `"}`
}

// promoBackend는 계약과 데이터 응답을 함께 제공하고 데이터 요청만 기록한다.
// 계약 조회(/openapi.json)는 기록·오류 주입 대상이 아니다.
type promoBackend struct {
	mu       sync.Mutex
	spec     []byte
	etag     string
	requests []string
	bodies   map[string]string
	status   int
	delay    time.Duration
}

func (b *promoBackend) set(status int, delay time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status, b.delay = status, delay
}

func (b *promoBackend) setSpecBytes(spec []byte, etag string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.spec, b.etag = spec, etag
}

func (b *promoBackend) take() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.requests
	b.requests = nil
	return out
}

func (b *promoBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	if r.URL.Path == "/openapi.json" {
		spec, etag := b.spec, b.etag
		b.mu.Unlock()
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		w.Write(spec)
		return
	}
	b.requests = append(b.requests, r.URL.EscapedPath()+"?"+r.URL.RawQuery)
	status, delay, body := b.status, b.delay, b.bodies[r.URL.Path]
	b.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if status != 0 {
		http.Error(w, "synthetic", status)
		return
	}
	if body == "" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, body)
}

func newPromo(t *testing.T, timeout time.Duration) (*ableops.Client, *promoBackend) {
	t.Helper()
	spec, err := os.ReadFile("../openapi/testdata/ableops-openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	backend := &promoBackend{spec: spec, etag: `"promo-v1"`, bodies: map[string]string{
		promoGroupsPath:          promoGroupsBody("개발"),
		promoSummaryPath:         promoSummaryBody("2026-09-17T00:00:00Z"),
		promoGroupsPath + "-v2":  promoGroupsBody("v2"),
		promoSummaryPath + "-v2": promoSummaryBody("2026-09-18T00:00:00Z"),
	}}
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	client, err := ableops.NewClient(config.Config{BaseURL: base, Token: e2eToken, Timeout: timeout, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	return client, backend
}

// promoServer는 Dynamic 런타임을 연결한 기본 서버를 만들고 계약을 한 번 적재한다.
func promoServer(t *testing.T, client *ableops.Client) (*mcp.Server, *dynamic.Runtime) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	runtime := dynamic.NewRuntime(client, logger, openapi.NewLoader(client), dynamic.Options{StaticToolNames: tools.StaticNames()})
	server := New(client, logger, WithRuntime(runtime))
	if report := runtime.Refresh(context.Background()); report.Registry == nil {
		t.Fatalf("계약 적재 실패: %s", report.Code)
	}
	return server, runtime
}

// rewriteOperationPath는 계약에서 operationId의 REST 경로만 바꾼다.
func rewriteOperationPath(t *testing.T, spec []byte, operationID, newPath string) []byte {
	t.Helper()
	return editSpec(t, spec, operationID, func(doc map[string]any, path string, item any) {
		delete(doc["paths"].(map[string]any), path)
		doc["paths"].(map[string]any)[newPath] = item
	})
}

// removeOperation은 계약에서 operationId를 통째로 없앤다.
func removeOperation(t *testing.T, spec []byte, operationID string) []byte {
	t.Helper()
	return editSpec(t, spec, operationID, func(doc map[string]any, path string, _ any) {
		delete(doc["paths"].(map[string]any), path)
	})
}

func editSpec(t *testing.T, spec []byte, operationID string, edit func(doc map[string]any, path string, item any)) []byte {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(spec, &doc); err != nil {
		t.Fatal(err)
	}
	paths := doc["paths"].(map[string]any)
	found := ""
	for path, item := range paths {
		for _, op := range item.(map[string]any) {
			if fields, ok := op.(map[string]any); ok && fields["operationId"] == operationID {
				found = path
			}
		}
	}
	if found == "" {
		t.Fatalf("계약에 %s가 없습니다", operationID)
	}
	edit(doc, found, paths[found])
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestPromotedToolsKeepMCPContract는 Dynamic ON/OFF에서 두 도구의 MCP 계약이 같고
// 같은 이름의 Dynamic 도구가 따로 노출되지 않는지 확인한다.
func TestPromotedToolsKeepMCPContract(t *testing.T) {
	client, _ := newPromo(t, 2*time.Second)
	off := toolMap(t, session(t, New(client, nil)))
	server, _ := promoServer(t, client)
	listed, err := session(t, server).ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	on := map[string]*mcp.Tool{}
	for _, tool := range listed.Tools {
		seen[tool.Name]++
		on[tool.Name] = tool
	}
	for _, name := range []string{"get_event_summary", "list_consumer_groups"} {
		if seen[name] != 1 {
			t.Fatalf("%s 노출 수 %d: Static/Dynamic 중복 노출", name, seen[name])
		}
		if !reflect.DeepEqual(off[name], on[name]) {
			t.Fatalf("%s: Dynamic ON에서 MCP 도구 계약이 달라졌습니다", name)
		}
	}
	if len(listed.Tools) != staticCount() {
		t.Fatalf("노출 도구 수 %d, Static %d: Promotion이 도구 수를 바꾸면 안 됩니다", len(listed.Tools), staticCount())
	}
}

// TestPromotedToolsUseDynamicExecutor는 두 도구가 실제로 계약의 REST 경로로 실행되는지,
// cluster_id → id 파라미터 변환이 되는지, 결과가 기존 공개 범위인지 확인한다.
func TestPromotedToolsUseDynamicExecutor(t *testing.T) {
	client, backend := newPromo(t, 2*time.Second)
	server, _ := promoServer(t, client)
	cs := session(t, server)
	backend.take()

	_, text := invokeText(t, cs, "get_event_summary", map[string]any{"cluster_id": "c1"})
	summary := staticResult[ableops.EventSummary](t, text)
	if summary.Status != "ok" || summary.Data == nil || summary.Data.SyntheticExcluded != 4 {
		t.Fatalf("get_event_summary 결과: %s", text)
	}
	if got := backend.take(); len(got) != 1 || got[0] != promoSummaryPath+"?clusterId=c1" {
		t.Fatalf("계약 경로로 실행하지 않았습니다: %v", got)
	}
	if strings.Contains(text, promoSecret) {
		t.Fatal("Dynamic 경로가 기존 공개 범위 밖 필드를 내보냈습니다")
	}

	_, text = invokeText(t, cs, "list_consumer_groups", map[string]any{"cluster_id": "c1"})
	groups := staticResult[tools.SnapshotData[ableops.ConsumerGroup]](t, text)
	if groups.Status != "ok" || groups.Data == nil || len(groups.Data.Items) != 1 || groups.Data.Items[0].Name != "g1" {
		t.Fatalf("list_consumer_groups 결과: %s", text)
	}
	// cluster_id는 계약의 경로 파라미터 id로 바뀌어야 한다.
	if got := backend.take(); len(got) != 1 || got[0] != promoGroupsPath+"?" {
		t.Fatalf("cluster_id → id 변환이 되지 않았습니다: %v", got)
	}
	if strings.Contains(text, promoSecret) {
		t.Fatal("Dynamic 경로가 기존 공개 범위 밖 필드를 내보냈습니다")
	}
}

// TestPromotedToolsFollowContractPath는 계약의 REST 경로만 바뀌어도 operationId가 같으면
// MCP 계약은 그대로이고 새 경로로 조회하는지 확인한다.
func TestPromotedToolsFollowContractPath(t *testing.T) {
	client, backend := newPromo(t, 2*time.Second)
	server, runtime := promoServer(t, client)
	cs := session(t, server)
	before := toolMap(t, cs)

	spec := rewriteOperationPath(t, backend.spec, "listConsumerGroups", "/api/clusters/{id}/consumer-groups-v2")
	spec = rewriteOperationPath(t, spec, "getEventSummary", "/api/events/summary-v2")
	backend.setSpecBytes(spec, `"promo-v2"`)
	if report := runtime.Refresh(context.Background()); report.Outcome == dynamic.RefreshFailed {
		t.Fatalf("계약 갱신 실패: %s", report.Code)
	}
	backend.take()

	_, text := invokeText(t, cs, "get_event_summary", map[string]any{"cluster_id": "c1"})
	summary := staticResult[ableops.EventSummary](t, text)
	if summary.Status != "ok" || summary.Data == nil || summary.Data.GeneratedAt != "2026-09-18T00:00:00Z" {
		t.Fatalf("새 경로 결과가 아닙니다: %s", text)
	}
	_, text = invokeText(t, cs, "list_consumer_groups", map[string]any{"cluster_id": "c1"})
	groups := staticResult[tools.SnapshotData[ableops.ConsumerGroup]](t, text)
	if groups.Status != "ok" || groups.Data == nil || groups.Data.ClusterName != "v2" {
		t.Fatalf("새 경로 결과가 아닙니다: %s", text)
	}
	want := []string{promoSummaryPath + "-v2?clusterId=c1", promoGroupsPath + "-v2?"}
	if got := backend.take(); !reflect.DeepEqual(got, want) {
		t.Fatalf("REST 경로 변경이 반영되지 않았습니다: %v", got)
	}
	after := toolMap(t, cs)
	for _, name := range []string{"get_event_summary", "list_consumer_groups"} {
		if !reflect.DeepEqual(before[name], after[name]) {
			t.Fatalf("%s: REST 경로 변경이 MCP 계약을 바꿨습니다", name)
		}
	}
}

// TestPromotedToolsFallBackWhenOperationMissing은 필요한 operationId가 계약에 없으면
// 비슷한 이름을 추측하지 않고 기존 Static 경로를 그대로 쓰는지 확인한다.
func TestPromotedToolsFallBackWhenOperationMissing(t *testing.T) {
	client, backend := newPromo(t, 2*time.Second)
	spec := removeOperation(t, backend.spec, "listConsumerGroups")
	spec = removeOperation(t, spec, "getEventSummary")
	backend.setSpecBytes(spec, `"promo-missing"`)
	server, _ := promoServer(t, client)
	cs := session(t, server)
	backend.take()

	_, text := invokeText(t, cs, "get_event_summary", map[string]any{"cluster_id": "c1"})
	if summary := staticResult[ableops.EventSummary](t, text); summary.Status != "ok" {
		t.Fatalf("Static 경로 결과가 아닙니다: %s", text)
	}
	_, text = invokeText(t, cs, "list_consumer_groups", map[string]any{"cluster_id": "c1"})
	if groups := staticResult[tools.SnapshotData[ableops.ConsumerGroup]](t, text); groups.Status != "ok" {
		t.Fatalf("Static 경로 결과가 아닙니다: %s", text)
	}
	want := []string{promoSummaryPath + "?clusterId=c1", promoGroupsPath + "?"}
	if got := backend.take(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Static 경로를 쓰지 않았습니다: %v", got)
	}
}

// TestPromotedToolsDoNotRetryOnBackendFailure는 백엔드 4xx/5xx/timeout에서 Static 재호출이
// 없는지(호출 1회) 확인한다. Dynamic 인프라 실패가 아니므로 경로를 바꾸지 않는다.
func TestPromotedToolsDoNotRetryOnBackendFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		delay  time.Duration
		code   string
	}{
		{"401", http.StatusUnauthorized, 0, "authentication_required"},
		{"403", http.StatusForbidden, 0, "access_denied"},
		{"404", http.StatusNotFound, 0, "not_found"},
		{"500", http.StatusInternalServerError, 0, "backend_unavailable"},
		{"timeout", 0, 1500 * time.Millisecond, "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeout := 2 * time.Second
			if tc.delay > 0 {
				// 백엔드 응답보다 짧은 최소 허용 timeout이다.
				timeout = time.Second
			}
			client, backend := newPromo(t, timeout)
			server, _ := promoServer(t, client)
			cs := session(t, server)
			backend.set(tc.status, tc.delay)
			backend.take()
			for _, name := range []string{"get_event_summary", "list_consumer_groups"} {
				_, text := invokeText(t, cs, name, map[string]any{"cluster_id": "c1"})
				var out tools.Envelope[json.RawMessage]
				if err := json.Unmarshal([]byte(text), &out); err != nil {
					t.Fatal(err)
				}
				if out.Status != "error" || len(out.Errors) != 1 || out.Errors[0].Code != tc.code {
					t.Fatalf("%s: 오류 코드가 다릅니다(원함 %s): %s", name, tc.code, text)
				}
				if got := backend.take(); len(got) != 1 {
					t.Fatalf("%s: 백엔드 호출 %d회, 이중 호출입니다: %v", name, len(got), got)
				}
			}
		})
	}
}

// TestPromotedToolsUseStaticWhenDynamicOff는 Dynamic이 꺼진 서버가 기존 Static 경로를
// 그대로 쓰는지 확인한다.
func TestPromotedToolsUseStaticWhenDynamicOff(t *testing.T) {
	client, backend := newPromo(t, 2*time.Second)
	cs := session(t, New(client, nil))
	backend.take()
	invokeText(t, cs, "get_event_summary", map[string]any{"cluster_id": "c1"})
	invokeText(t, cs, "list_consumer_groups", map[string]any{"cluster_id": "c1"})
	want := []string{promoSummaryPath + "?clusterId=c1", promoGroupsPath + "?"}
	if got := backend.take(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Static 경로를 쓰지 않았습니다: %v", got)
	}
}
