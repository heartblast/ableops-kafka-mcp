package extension

// slog → extensionv1.Logger 다리.
//
// internal/app 은 *slog.Logger 를 받고 SDK 는 extensionv1.Logger 를 준다. 어느 한쪽을 바꾸는
// 대신 얇은 Handler 하나로 잇는다 — 그래야 MCP Runtime 의 로그가 Core 가 수집하는 Extension
// 로그 스트림(확장 ID 가 붙은 stderr)에 그대로 섞인다.

import (
	"context"
	"log/slog"

	extv1 "github.com/heartblast/ableops-sdk/extension/v1"
)

// hostHandler는 slog 레코드를 HostContext 의 Logger 로 넘긴다.
type hostHandler struct {
	log   extv1.Logger
	attrs []any
	level slog.Level
}

// newHostLogger는 SDK Logger 를 감싼 *slog.Logger 를 만든다. log 가 nil 이면 아무것도 기록하지 않는다.
func newHostLogger(log extv1.Logger) *slog.Logger {
	if log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(&hostHandler{log: log, level: slog.LevelInfo})
}

func (h *hostHandler) Enabled(_ context.Context, level slog.Level) bool { return level >= h.level }

// Handle은 레코드를 key/value 쌍으로 펼쳐 SDK Logger 로 넘긴다(SDK 가 slog 관례를 그대로 쓴다).
func (h *hostHandler) Handle(_ context.Context, record slog.Record) error {
	args := make([]any, 0, len(h.attrs)+record.NumAttrs()*2)
	args = append(args, h.attrs...)
	record.Attrs(func(a slog.Attr) bool {
		args = append(args, a.Key, a.Value.Any())
		return true
	})
	switch {
	case record.Level >= slog.LevelError:
		h.log.Error(record.Message, args...)
	case record.Level >= slog.LevelWarn:
		h.log.Warn(record.Message, args...)
	default:
		h.log.Info(record.Message, args...)
	}
	return nil
}

func (h *hostHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := &hostHandler{log: h.log, level: h.level, attrs: append([]any(nil), h.attrs...)}
	for _, a := range attrs {
		next.attrs = append(next.attrs, a.Key, a.Value.Any())
	}
	return next
}

// WithGroup은 그룹을 쓰지 않는다. MCP Runtime 의 로그는 평평한 key/value 뿐이라 그룹을 접어도
// 잃는 정보가 없고, 접두 규칙을 새로 만들면 Core 로그에서 키 이름이 두 가지가 된다.
func (h *hostHandler) WithGroup(string) slog.Handler { return h }
