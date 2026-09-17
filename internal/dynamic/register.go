package dynamic

import (
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// toolSlot은 서버에 등록한 도구 하나의 현재 실행 대상이다.
//
// SDK는 같은 serverTool 안에서 입력 스키마 검증과 핸들러를 함께 교체하므로, 정의가 바뀐
// 도구는 새 slot으로 다시 등록한다. 정의는 같고 실행 바인딩(explode·선언된 204)만 바뀐
// 계약 갱신은 이 값만 원자적으로 바꾼다. 진행 중인 호출은 시작할 때 읽은 도구로 끝난다.
type toolSlot struct {
	tool atomic.Pointer[Tool]
}

func newSlot(tool *Tool) *toolSlot {
	slot := &toolSlot{}
	slot.tool.Store(tool)
	return slot
}

// Register는 도구를 서버에 등록하고 실제로 등록한 이름을 돌려준다. 실행 중 교체가 없는
// 고정 등록 경로이며 비교용 서버가 사용한다. 기본 서버는 Runtime을 쓴다.
//
// SDK AddTool은 같은 이름의 도구를 조용히 교체한다. 그래서 reserved(이미 등록된 Static 도구)와
// 겹치는 이름은 Registry 배치와 무관하게 다시 한 번 거부한다. 등록 중 예외가 나도 서버 전체를
// 중단하지 않고 해당 도구만 제외한다.
func Register(server *mcp.Server, client *ableops.Client, logger *slog.Logger, list []*Tool, reserved []string) []string {
	executor := NewExecutor(client, logger)
	blocked := map[string]bool{}
	for _, name := range reserved {
		blocked[name] = true
	}
	var registered []string
	for _, tool := range list {
		if blocked[tool.Name] {
			executor.logger.Error("동적 도구 등록 거부", "tool", tool.Name, "operation_id", tool.Operation.ID, "code", "static_tool_conflict")
			continue
		}
		if err := add(server, executor, newSlot(tool)); err != nil {
			executor.logger.Error("동적 도구 등록 실패", "tool", tool.Name, "operation_id", tool.Operation.ID, "code", IssueRegisterFailed)
			continue
		}
		blocked[tool.Name] = true
		registered = append(registered, tool.Name)
	}
	return registered
}

func add(server *mcp.Server, executor *Executor, slot *toolSlot) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("도구 등록 중 예외")
		}
	}()
	mcp.AddTool(server, slot.tool.Load().definition(), executor.handler(slot))
	return nil
}

// LogSummary는 적재 결과를 원문 없이 stderr 로그로 남긴다.
func LogSummary(logger *slog.Logger, registry *Registry) {
	stats := registry.Stats()
	logger.Info("동적 도구 계약 적재",
		"discovered", stats.Discovered,
		"mcp_enabled", stats.MCPEnabled,
		"registrable_get", stats.Registrable,
		"selected", stats.Selected,
		"exposed", stats.Exposed,
		"shadow", stats.Shadow,
		"blocked", stats.Blocked,
		"static_conflicts", stats.StaticConflicts,
		"skipped", stats.Skipped,
		"etag_present", registry.ETag() != "")
	for _, issue := range registry.Skipped() {
		logger.Warn("동적 도구 제외", "operation_id", issue.OperationID, "method", issue.Method, "path", issue.Path, "code", issue.Code, "reason", issue.Message)
	}
	for _, id := range registry.UnknownSelections() {
		logger.Warn("선택한 operationId를 등록할 수 없음", "operation_id", id, "code", "unknown_selection")
	}
	for _, entry := range registry.Entries() {
		if entry.Placement == PlacementNotSelected || entry.Placement == PlacementExposed {
			continue
		}
		code := "static_tool_conflict"
		if !entry.StaticConflict || entry.Exposure == ExposureBlocked {
			code = "exposure_" + string(entry.Exposure)
		}
		// 선택했지만 기본 서버에 노출하지 않은 이유를 남긴다. 사유는 고정 문구다.
		logger.Warn("선택한 동적 도구를 노출하지 않음",
			"tool", entry.Tool.Name,
			"operation_id", entry.Tool.Operation.ID,
			"placement", string(entry.Placement),
			"exposure", string(entry.Exposure),
			"code", code,
			"reason", entry.ExposureReason)
	}
}
