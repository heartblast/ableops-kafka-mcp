// Package app은 MCP Runtime 조립 책임을 실행 진입점과 분리한다.
//
// CLI(standalone)와 향후 AbleOps 외부 프로세스 Extension이 **같은** Runtime을 쓴다.
// 이 패키지는 다음만 책임진다.
//
//   - Backend REST Client 생성
//   - Dynamic Runtime 생성과 lifecycle(최초 Refresh·주기 Refresh·정지)
//   - MCP Server 생성
//   - Web Delegation / 로컬 인증 저장소 조립
//   - listener 없는 MCP HTTP Handler 생성
//   - 종료 시 Dynamic Runtime 정리와 REST Client idle connection 정리
//
// flag 처리, 설정 적재, transport 선택, signal 처리, listener bind, exit code,
// 사용자용 시작/종료 로그는 **호출자(진입점)의 책임**으로 남긴다.
package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/dynamic"
	"github.com/heartblast/ableops-kafka-mcp/internal/localauth"
	"github.com/heartblast/ableops-kafka-mcp/internal/mcpserver"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
	"github.com/heartblast/ableops-kafka-mcp/internal/webdelegation"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 조립 실패 코드다. 진입점은 이 코드만 로그에 남기고 원문은 남기지 않는다.
const (
	// CodeBackendClient는 REST 클라이언트 초기화 실패다. 설정 검증 오류라 Detail이 채워진다.
	CodeBackendClient = "backend_client_error"
	// CodeAuthConfiguration은 로컬 인증 저장소 초기화 실패다.
	CodeAuthConfiguration = "auth_configuration_error"
	// CodeDelegationConfiguration은 Web 위임 저장소 초기화 실패다.
	CodeDelegationConfiguration = "delegation_configuration_error"
	// CodeManagedConfiguration은 Managed 모드 조립 인자가 계약을 만족하지 못한 실패다.
	CodeManagedConfiguration = "managed_configuration_error"
)

// Error는 Runtime 조립 실패다. Message는 이 리포지토리가 만든 고정 문구이며 외부 입력을 담지 않는다.
// Detail은 그대로 로그에 남겨도 되는 경우에만 채운다(비어 있으면 Code만 남긴다).
type Error struct {
	Code    string
	Message string
	Detail  string
	err     error
}

func (e *Error) Error() string { return e.Message }
func (e *Error) Unwrap() error { return e.err }

// Runtime은 전송 방식과 무관한 MCP 실행 단위다. New로 조립하고 Start로 시작하며 Close로 정리한다.
type Runtime struct {
	cfg    config.ServerConfig
	logger *slog.Logger

	client  *ableops.Client
	server  *mcp.Server
	dynamic *dynamic.Runtime

	// httpOptions는 Transport가 "http"일 때만 채운다. listener 주소는 호출자가 바꿀 수 있다.
	httpOptions mcpserver.HTTPOptions

	startOnce   sync.Once
	closeOnce   sync.Once
	stopRefresh context.CancelFunc
	refreshDone chan struct{}
}

// Options는 조립 시점의 **추가** 선택지다. 제로값이 지금까지의 standalone 조립이므로
// New(cfg, logger) 와 NewWithOptions(cfg, logger, Options{}) 는 같은 Runtime을 만든다.
//
// 새 실행 방식(AbleOps Managed Process Extension)이 인증 경계만 다르게 필요로 하므로,
// Runtime 을 한 벌 더 만들지 않고 이 한 지점에서 갈라낸다.
type Options struct {
	// Managed가 참이면 AbleOps Core 가 관리하는 Extension 프로세스로 조립한다.
	//
	// 차이는 인증 경계 하나뿐이다. 로컬 인증 저장소(localauth)를 만들지 않고 `/mcp` 는
	// **Web 위임 토큰만** 받는다 — Managed 모드에는 Claude Desktop·Codex 가 쓰는 로컬 토큰
	// 파일이 존재하지 않으므로, 저장소를 만들면 "아무도 발급할 수 없는 인증 경로"만 남는다.
	Managed bool
	// InternalSecret은 서버간 위임 발급(`/internal/delegations`)의 공유 비밀이다.
	//
	// Managed 모드에서만 쓴다. 호출자(Extension)가 기동 시 프로세스 메모리에만 두는 임시 비밀을
	// 넘기며, 설정 파일·환경변수로 받지 않는다. 비어 있으면 Managed 조립은 실패한다.
	// ⚠ 시크릿이다 — 로그·오류·Health 어디에도 넣지 않는다.
	InternalSecret string
}

// New는 설정으로 MCP Runtime을 조립한다. 계약 조회·전송 시작·listener bind는 하지 않는다.
// logger가 nil이면 아무것도 기록하지 않는다.
func New(cfg config.ServerConfig, logger *slog.Logger) (*Runtime, error) {
	return NewWithOptions(cfg, logger, Options{})
}

// NewWithOptions는 New에 조립 선택지를 더한 형태다. opts 제로값은 New와 완전히 같다.
func NewWithOptions(cfg config.ServerConfig, logger *slog.Logger, opts Options) (*Runtime, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if opts.Managed && cfg.Transport != "http" {
		return nil, &Error{Code: CodeManagedConfiguration, Message: "Managed 모드는 http 전송에서만 조립할 수 있습니다"}
	}
	client, err := ableops.NewClient(cfg.Backend)
	if err != nil {
		return nil, &Error{Code: CodeBackendClient, Message: "REST 클라이언트 초기화 실패", Detail: err.Error(), err: err}
	}
	r := &Runtime{cfg: cfg, logger: logger, client: client}
	var options []mcpserver.Option
	if cfg.Dynamic.Enabled {
		r.dynamic = newDynamicRuntime(client, cfg.Dynamic, logger)
		options = append(options, mcpserver.WithRuntime(r.dynamic))
	}
	r.server = mcpserver.New(client, logger, options...)
	if cfg.Transport == "http" {
		if err := r.buildHTTPOptions(opts); err != nil {
			client.CloseIdleConnections()
			return nil, err
		}
	}
	return r, nil
}

// buildHTTPOptions는 HTTP 전송의 인증 경계를 조립한다. listener는 만들지 않는다.
func (r *Runtime) buildHTTPOptions(options Options) error {
	if options.Managed {
		return r.buildManagedHTTPOptions(options)
	}
	store, err := localauth.NewStore(r.cfg.AuthStore, verifyBackend(r.client))
	if err != nil {
		return &Error{Code: CodeAuthConfiguration, Message: "로컬 인증 저장소 초기화 실패", err: err}
	}
	opts := mcpserver.HTTPOptions{
		Address:        r.cfg.HTTPAddress,
		AllowedOrigins: r.cfg.AllowedOrigins,
		Authenticate:   store.Authenticate,
		Logger:         r.logger,
	}
	if r.cfg.WebDelegation.Enabled {
		delegations, delegationErr := webdelegation.NewStore(verifyBackend(r.client), r.cfg.WebDelegation.TTL)
		if delegationErr != nil {
			return &Error{Code: CodeDelegationConfiguration, Message: "Web 위임 저장소 초기화 실패", err: delegationErr}
		}
		// 인증 Provider 는 **교체가 아니라 추가**다. 위임 토큰 접두사만 새 저장소로 보내고
		// 나머지(Claude Desktop·Codex 등 기존 local 토큰)는 그대로 localauth 로 간다.
		opts.Authenticate = func(ctx context.Context, token string) (requestctx.Principal, error) {
			if webdelegation.ValidToken(token) {
				return delegations.Authenticate(ctx, token)
			}
			return store.Authenticate(ctx, token)
		}
		opts.InternalSecret = r.cfg.WebDelegation.Secret
		opts.IssueDelegation = func(ctx context.Context, backendToken string) (mcpserver.DelegationGrant, error) {
			grant, err := delegations.Issue(ctx, backendToken)
			return mcpserver.DelegationGrant{Token: grant.Token, UserID: grant.UserID, ExpiresAt: grant.ExpiresAt}, err
		}
		// TTL 만 남긴다 — 비밀·토큰은 어떤 수준에서도 로그에 넣지 않는다.
		r.logger.Info("Web 세션 위임 발급 활성", "ttl", r.cfg.WebDelegation.TTL.String())
	}
	r.httpOptions = opts
	return nil
}

// buildManagedHTTPOptions는 Managed Extension 의 인증 경계를 조립한다.
//
// standalone 과 다른 점은 **두 가지뿐**이다.
//  1. localauth 저장소를 만들지 않는다(Managed 프로세스에는 로컬 토큰 파일이 없다).
//  2. 서버간 공유 비밀을 설정이 아니라 호출자가 넘긴 프로세스 내부 비밀로 쓴다.
//
// 사용자별 권한 검사는 그대로다 — `/mcp` 는 여전히 단기 위임 토큰을 Backend 세션으로 검증하고,
// 도구 호출은 그 사용자의 Backend 권한으로 나간다.
func (r *Runtime) buildManagedHTTPOptions(options Options) error {
	if !r.cfg.WebDelegation.Enabled {
		return &Error{Code: CodeManagedConfiguration, Message: "Managed 모드는 Web 위임 발급이 필요합니다"}
	}
	// 비밀은 오류 메시지에도 싣지 않는다. 길이 검증은 mcpserver 가 한 번 더 한다.
	if options.InternalSecret == "" {
		return &Error{Code: CodeManagedConfiguration, Message: "Managed 모드는 프로세스 내부 공유 비밀이 필요합니다"}
	}
	delegations, err := webdelegation.NewStore(verifyBackend(r.client), r.cfg.WebDelegation.TTL)
	if err != nil {
		return &Error{Code: CodeDelegationConfiguration, Message: "Web 위임 저장소 초기화 실패", err: err}
	}
	r.httpOptions = mcpserver.HTTPOptions{
		Address: r.cfg.HTTPAddress,
		// 브라우저는 Managed MCP 에 직접 붙지 않는다. 허용 Origin 을 비워 CORS 경로를 닫는다.
		AllowedOrigins: nil,
		// 위임 토큰 **하나만** 받는다. 형식이 다른 토큰은 저장소에 묻지도 않는다 —
		// 없는 저장소로 흘려보내면 인증 실패 사유가 두 벌이 된다.
		Authenticate: func(ctx context.Context, token string) (requestctx.Principal, error) {
			if !webdelegation.ValidToken(token) {
				return requestctx.Principal{}, errManagedToken
			}
			return delegations.Authenticate(ctx, token)
		},
		InternalSecret: options.InternalSecret,
		IssueDelegation: func(ctx context.Context, backendToken string) (mcpserver.DelegationGrant, error) {
			grant, err := delegations.Issue(ctx, backendToken)
			return mcpserver.DelegationGrant{Token: grant.Token, UserID: grant.UserID, ExpiresAt: grant.ExpiresAt}, err
		},
		Logger: r.logger,
	}
	// TTL 만 남긴다 — 비밀·토큰은 어떤 수준에서도 로그에 넣지 않는다.
	r.logger.Info("Managed MCP 위임 인증 활성", "ttl", r.cfg.WebDelegation.TTL.String())
	return nil
}

// errManagedToken은 Managed `/mcp` 에 위임 토큰이 아닌 값이 온 경우다.
// AuthCode 를 달지 않으므로 mcpserver 는 기본 401(authentication_required)로 응답한다 —
// 어떤 토큰 형식이 기대되는지 응답으로 알려 주지 않는다.
var errManagedToken = errors.New("위임 토큰이 아닙니다")

// Server는 전송 계층에 넘길 MCP 서버다.
func (r *Runtime) Server() *mcp.Server { return r.server }

// Client는 조립한 Backend REST 클라이언트다.
func (r *Runtime) Client() *ableops.Client { return r.client }

// Dynamic은 Dynamic Runtime이다. 설정이 꺼져 있으면 nil이며 호출자가 lifecycle을 직접 제어할 수 있다.
func (r *Runtime) Dynamic() *dynamic.Runtime { return r.dynamic }

// HTTPOptions는 HTTP 전송 설정 사본이다. Transport가 "http"가 아니면 빈 값이다.
func (r *Runtime) HTTPOptions() mcpserver.HTTPOptions { return r.httpOptions }

// NewHTTPHandler는 listener 없이 MCP HTTP Handler를 만든다. Extension처럼 자체 수신 계층을
// 가진 호출자가 쓴다. 새 전송 계층을 만들지 않고 mcpserver.NewHTTPHandler를 그대로 재사용한다.
func (r *Runtime) NewHTTPHandler() (http.Handler, error) {
	return mcpserver.NewHTTPHandler(r.server, r.httpOptions)
}

// Start는 Dynamic 계약을 한 번 적재하고 주기 갱신을 시작한다. Dynamic이 꺼져 있으면 아무것도 하지 않는다.
// 두 번째 호출부터는 무시한다.
//
// 최초 적재는 전송을 시작하기 전에 끝내 첫 tools/list에 반영한다. 실패해도 Static 도구로 기동한다.
func (r *Runtime) Start(ctx context.Context) {
	r.startOnce.Do(func() {
		if r.dynamic == nil {
			return
		}
		r.dynamic.Refresh(ctx)
		refreshCtx, stopRefresh := context.WithCancel(ctx)
		done := make(chan struct{})
		interval := r.cfg.Dynamic.RefreshInterval
		go func() {
			defer close(done)
			r.dynamic.Run(refreshCtx, interval)
		}()
		r.stopRefresh, r.refreshDone = stopRefresh, done
		r.logger.Info("동적 도구 계약 주기 갱신 시작", "interval", interval.String())
	})
}

// Close는 주기 갱신을 멈추고 끝날 때까지 기다린 뒤 REST 클라이언트의 idle connection을 정리한다.
// 여러 번 불러도 안전하다.
func (r *Runtime) Close() {
	r.closeOnce.Do(func() {
		if r.stopRefresh != nil {
			r.stopRefresh()
			<-r.refreshDone
		}
		r.client.CloseIdleConnections()
	})
}

// verifyBackend는 기존 REST Client 로 Backend 세션을 확인하는 검사기를 만든다.
// localauth 와 webdelegation 이 **같은** 검사기를 쓴다 — 세션 유효성 판정이 두 벌이 되면
// 한쪽에서만 로그아웃이 반영되는 상태가 생긴다.
func verifyBackend(client *ableops.Client) func(context.Context, string) (string, error) {
	return func(ctx context.Context, token string) (string, error) {
		p, _ := requestctx.FromContext(ctx)
		p.BackendToken = token
		return client.CurrentUserID(requestctx.WithPrincipal(ctx, p))
	}
}

// newDynamicRuntime은 계약 조회기와 Registry 구성을 묶은 런타임을 만든다. 계약은 아직 조회하지 않는다.
// 설정이 꺼져 있으면 호출하지 않으므로 /openapi.json 조회도, 주기 갱신도 없다.
func newDynamicRuntime(client *ableops.Client, cfg config.DynamicConfig, logger *slog.Logger) *dynamic.Runtime {
	return dynamic.NewRuntime(client, logger, openapi.NewLoader(client), dynamic.Options{
		StaticToolNames: tools.StaticNames(),
		Operations:      cfg.Operations,
	})
}
