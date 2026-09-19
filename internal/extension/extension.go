package extension

// extensionv1.Extension 생애주기 구현.
//
// # 이 파일이 하지 않는 것
//
// MCP 서버·도구·인증·위임 발급을 만들지 않는다. 전부 internal/app.Runtime 과 internal/mcpserver
// 가 이미 가진 것이고, 여기서는 그것을 Extension 계약에 **잇기만** 한다.
//
// # 보안 경계
//
//	Core
//	  │ X-Ableops-Call-Token          ← extserver 가 상수시간 비교로 검증(모든 경로)
//	  ▼
//	extserver 인증
//	  │
//	  ▼
//	Managed wrapper                    ← 외부가 보낸 내부 비밀 헤더를 **지우고** 프로세스 비밀을 주입
//	  │
//	  ▼
//	기존 MCP internal delegation handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/app"
	extv1 "github.com/heartblast/ableops-sdk/extension/v1"
	"github.com/heartblast/ableops-sdk/extension/v1/extserver"
)

// MCP 경로. mcpserver 가 이 경로들만 알아보므로 값이 어긋나면 404 가 된다.
const (
	// mcpPath는 MCP JSON-RPC 경로다. 위임 Bearer 인증을 통과해야 한다.
	mcpPath = "/mcp"
	// delegationPath는 서버간 위임 발급 경로다(mcpserver.internalDelegationPath 와 같은 값).
	delegationPath = "/internal/delegations"
	// internalSecretHeader는 mcpserver 의 서버간 공유 비밀 헤더다(같은 이름이어야 한다).
	//
	// ⚠ 외부에서 들어온 이 헤더는 **신뢰하지 않는다**. wrapper 가 지우고 다시 넣는다.
	internalSecretHeader = "X-AbleOps-Internal-Secret"
)

// initialRefreshWait는 기동 시 Dynamic 최초 적재를 기다리는 상한이다.
//
// Core 의 기동 핸드셰이크 대기(15초)보다 충분히 짧아야 한다. Backend 가 느리거나 멈춰 있을 때
// Start 가 그 대기를 전부 써 버리면 Core 는 "기동 신호가 오지 않았다"로 판정해 프로세스를 죽인다.
// Dynamic 적재 실패는 기동 실패가 아니다 — 시간 안에 못 끝내면 Static 도구로 뜨고, 적재는
// 백그라운드에서 계속된다(주기 갱신이 이어받는다).
const initialRefreshWait = 5 * time.Second

// Extension은 AbleOps Kafka MCP 의 Managed Process Extension 이다.
//
// 프로세스당 1개이며 여러 요청이 동시에 핸들러를 호출하므로 상태는 뮤텍스로 보호한다.
type Extension struct {
	mu        sync.RWMutex
	runtime   *app.Runtime
	handler   http.Handler
	secret    string
	started   bool
	startedAt time.Time
}

// 컴파일 타임 계약 확인 — 인터페이스 메서드 누락을 빌드에서 잡는다.
var _ extv1.Extension = (*Extension)(nil)

// New는 Extension 인스턴스를 만든다. 아직 아무것도 조립하지 않는다.
func New() *Extension { return &Extension{} }

// Manifest는 이 Extension 의 선언을 돌려준다(호출마다 동일).
//
// 파싱 실패 시 빈 Manifest 를 돌려준다. 패닉을 내지 않는 이유는 extserver.Run 의 검증이
// 한국어 오류로 거부하고 Core 가 그 메시지를 그대로 기록하기 때문이다.
func (e *Extension) Manifest() extv1.Manifest {
	m, err := LoadManifest()
	if err != nil {
		return extv1.Manifest{}
	}
	return m
}

// Start는 MCP Runtime 을 조립하고 시작한다.
//
// ctx 는 extserver 의 실행 컨텍스트다(종료 신호까지 산다). 주기 갱신이 이 컨텍스트에 매이므로
// 짧은 파생 컨텍스트를 넘기지 않는다.
func (e *Extension) Start(ctx context.Context, host extv1.HostContext) error {
	manifest, err := LoadManifest()
	if err != nil {
		return fmt.Errorf("MCP Extension Manifest 로딩 실패: %w", err)
	}
	// Core 가 준 기동 환경을 다시 읽는다. HostContext 에는 Host API 주소가 없고,
	// 이 검증(확장 ID 교차 확인·프로토콜 협상)은 같은 결과를 돌려주므로 반복해도 안전하다.
	env, err := extserver.LoadEnvironment(manifest)
	if err != nil {
		return err
	}
	cfg, err := managedServerConfig(env.HostURL)
	if err != nil {
		return err
	}
	secret, err := newInternalSecret()
	if err != nil {
		return err
	}
	logger := newHostLogger(host.Log)
	runtime, err := app.NewWithOptions(cfg, logger, app.Options{Managed: true, InternalSecret: secret})
	if err != nil {
		// 조립 오류 문구는 이 저장소가 만든 고정 문구다(외부 입력·비밀을 담지 않는다).
		var failure *app.Error
		if errors.As(err, &failure) {
			return fmt.Errorf("MCP Runtime 조립 실패: %s", failure.Message)
		}
		return errors.New("MCP Runtime 조립 실패")
	}
	handler, err := runtime.NewHTTPHandler()
	if err != nil {
		runtime.Close()
		return fmt.Errorf("MCP HTTP 핸들러 조립 실패: %w", err)
	}

	e.mu.Lock()
	e.runtime, e.handler, e.secret = runtime, handler, secret
	e.started, e.startedAt = true, time.Now()
	e.mu.Unlock()

	// Dynamic 최초 적재는 기다리되 기동을 볼모로 잡지 않는다(위 initialRefreshWait 주석).
	done := make(chan struct{})
	go func() {
		defer close(done)
		runtime.Start(ctx)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	case <-time.After(initialRefreshWait):
		logger.Info("동적 도구 최초 적재를 기다리지 않고 기동한다", "timeout", initialRefreshWait.String())
	}

	// Backend 주소는 origin 뿐이며 토큰·비밀은 어떤 필드에도 넣지 않는다.
	logger.Info("AbleOps Kafka MCP Extension 시작", "extensionId", ID, "backend", cfg.Backend.BaseURL.String())
	return nil
}

// Stop은 Runtime 을 정리한다. 여러 번 불러도 안전하다(멱등).
func (e *Extension) Stop(context.Context) error {
	e.mu.Lock()
	if !e.started {
		e.mu.Unlock()
		return nil
	}
	runtime := e.runtime
	// 정지 후 남은 요청이 핸들러에 닿지 않게 비운다. 비밀도 메모리에서 지운다.
	e.runtime, e.handler, e.secret = nil, nil, ""
	e.started, e.startedAt = false, time.Time{}
	e.mu.Unlock()

	if runtime != nil {
		runtime.Close()
	}
	return nil
}

// Health는 현재 상태를 돌려준다. 외부 호출을 하지 않는다(SDK 계약: 빠르게 반환).
//
// Dynamic 적재 여부는 보지 않는다 — Dynamic 실패는 Static 도구로 계속 동작한다는 뜻이며,
// 그것을 비정상으로 보고하면 Core 가 정상 동작 중인 MCP 를 재시작한다.
func (e *Extension) Health(context.Context) extv1.Health {
	now := time.Now()
	if _, err := LoadManifest(); err != nil {
		return extv1.Health{OK: false, Message: "Manifest 를 읽지 못했습니다(패키지 자산 손상)", CheckedAt: now}
	}
	e.mu.RLock()
	started, startedAt, ready := e.started, e.startedAt, e.handler != nil
	e.mu.RUnlock()
	if !started || !ready {
		return extv1.Health{OK: false, Message: "MCP Runtime 이 아직 시작되지 않았습니다", CheckedAt: now}
	}
	return extv1.Health{
		OK:        true,
		Message:   fmt.Sprintf("정상 동작 중입니다(시작 시각 %s)", startedAt.Format(time.RFC3339)),
		CheckedAt: now,
	}
}

// Routes는 마운트할 핸들러를 돌려준다. MCP 전용 경로 둘뿐이다.
//
// Permission 을 비워 둔다 — Manifest 가 공개 라우트를 선언하지 않으므로 Core 공개 프록시에는
// 이 경로가 없고, 호출자는 Core 내부(호출 토큰 보유자)뿐이다.
func (e *Extension) Routes() []extv1.RouteHandler {
	return []extv1.RouteHandler{
		{Method: http.MethodPost, Pattern: mcpPath, Handler: http.HandlerFunc(e.serveMCP)},
		{Method: http.MethodPost, Pattern: delegationPath, Handler: http.HandlerFunc(e.serveDelegation)},
	}
}

// serveMCP는 `/mcp` 를 기존 MCP 핸들러로 넘긴다. 인증은 그 핸들러가 위임 Bearer 로 수행한다.
func (e *Extension) serveMCP(w http.ResponseWriter, r *http.Request) {
	handler, _ := e.current()
	if handler == nil {
		unavailable(w)
		return
	}
	handler.ServeHTTP(w, r)
}

// serveDelegation은 `/internal/delegations` 를 기존 MCP 핸들러로 넘기되, 서버간 공유 비밀을
// **이 프로세스가 만든 값으로 바꾼다**.
//
// 요청이 여기 도달했다는 것은 extserver 의 호출 토큰 검증을 이미 통과했다는 뜻이다. 그 경계가
// Managed 모드의 서버간 인증이며, 기존 mcpserver 의 비밀 검사는 그 뒤에서 계약을 유지한다.
// 외부가 보낸 비밀 헤더는 값이 무엇이든 신뢰하지 않으므로 먼저 **전부 지운다**.
func (e *Extension) serveDelegation(w http.ResponseWriter, r *http.Request) {
	handler, secret := e.current()
	if handler == nil || secret == "" {
		unavailable(w)
		return
	}
	// 원 요청의 헤더 맵을 건드리지 않도록 사본에 주입한다.
	proxied := r.Clone(r.Context())
	proxied.Header.Del(internalSecretHeader)
	proxied.Header.Set(internalSecretHeader, secret)
	handler.ServeHTTP(w, proxied)
}

// current는 현재 핸들러와 내부 비밀 사본을 돌려준다(락 보유 시간을 최소화한다).
func (e *Extension) current() (http.Handler, string) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.handler, e.secret
}

// unavailable은 아직 시작되지 않았거나 이미 정지한 상태의 응답이다.
// 본문에 내부 상태·주소·비밀을 담지 않는다.
func unavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"error":"MCP 확장 기능이 준비되지 않았습니다"}` + "\n"))
}
