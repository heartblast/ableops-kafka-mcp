package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/dynamic"
	"github.com/heartblast/ableops-kafka-mcp/internal/localauth"
	"github.com/heartblast/ableops-kafka-mcp/internal/mcpserver"
	"github.com/heartblast/ableops-kafka-mcp/internal/openapi"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
	"github.com/heartblast/ableops-kafka-mcp/internal/tools"
)

func main() {
	os.Exit(run())
}

var errArguments = errors.New("실행 인자가 올바르지 않습니다")

// loadServerConfig는 명시한 플래그만 설정 파일보다 우선하도록 전달한다.
func loadServerConfig(args []string, getenv func(string) string) (config.ServerConfig, error) {
	flags := flag.NewFlagSet("ableops-kafka-mcp", flag.ContinueOnError)
	// 플래그 오류에 포함될 수 있는 입력값과 파일 경로는 출력하지 않는다.
	flags.SetOutput(io.Discard)
	path := flags.String("config", "", "명시적으로 읽을 YAML 설정 파일")
	transport := flags.String("transport", "stdio", "stdio 또는 http")
	address := flags.String("http-address", "127.0.0.1:8081", "로컬 HTTP 수신 주소")
	origins := flags.String("allowed-origins", "", "브라우저 Origin 허용 목록(쉼표 구분)")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return config.ServerConfig{}, errArguments
	}
	var overrides config.ServerOverrides
	invalid := false
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "config":
			invalid = strings.TrimSpace(*path) == ""
		case "transport":
			overrides.Transport = transport
		case "http-address":
			overrides.HTTPAddress = address
		case "allowed-origins":
			allowed := []string{}
			if *origins != "" {
				for _, origin := range strings.Split(*origins, ",") {
					allowed = append(allowed, strings.TrimSpace(origin))
				}
			}
			overrides.AllowedOrigins = &allowed
		}
	})
	if invalid || (overrides.Transport != nil && *transport != "stdio" && *transport != "http") {
		return config.ServerConfig{}, errArguments
	}
	return config.LoadServer(*path, getenv, overrides)
}

func run() int {
	// 설정 오류에는 입력값을 넣지 않으며 모든 로그는 stderr로만 출력한다.
	startup := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runtimeConfig, err := loadServerConfig(os.Args[1:], os.Getenv)
	if errors.Is(err, errArguments) {
		startup.Error("실행 인자 오류", "usage", "--config=<YAML 파일> --transport=stdio|http --http-address=127.0.0.1:8081 --allowed-origins=<origin,...>")
		return 1
	}
	if err != nil {
		startup.Error("설정 검증 실패", "error", err.Error())
		return 1
	}
	cfg := runtimeConfig.Backend
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	client, err := ableops.NewClient(cfg)
	if err != nil {
		logger.Error("REST 클라이언트 초기화 실패", "error", err.Error())
		return 1
	}
	defer client.CloseIdleConnections()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var options []mcpserver.Option
	var runtime *dynamic.Runtime
	if runtimeConfig.Dynamic.Enabled {
		runtime = newDynamicRuntime(client, runtimeConfig.Dynamic, logger)
		options = append(options, mcpserver.WithRuntime(runtime))
	}
	server := mcpserver.New(client, logger, options...)
	if runtime != nil {
		// 최초 적재는 전송을 시작하기 전에 끝내 첫 tools/list에 반영한다. 실패해도 Static 도구로 기동한다.
		runtime.Refresh(ctx)
		refreshCtx, stopRefresh := context.WithCancel(ctx)
		refreshDone := make(chan struct{})
		go func() {
			defer close(refreshDone)
			runtime.Run(refreshCtx, runtimeConfig.Dynamic.RefreshInterval)
		}()
		defer func() {
			stopRefresh()
			<-refreshDone
		}()
		logger.Info("동적 도구 계약 주기 갱신 시작", "interval", runtimeConfig.Dynamic.RefreshInterval.String())
	}
	if runtimeConfig.Transport == "http" {
		store, storeErr := localauth.NewStore(runtimeConfig.AuthStore, func(ctx context.Context, token string) (string, error) {
			p, _ := requestctx.FromContext(ctx)
			p.BackendToken = token
			return client.CurrentUserID(requestctx.WithPrincipal(ctx, p))
		})
		if storeErr != nil {
			logger.Error("로컬 인증 저장소 초기화 실패", "code", "auth_configuration_error")
			return 1
		}
		logger.Info("MCP 로컬 HTTP 서버 시작", "version", "0.1.0")
		err = mcpserver.RunHTTP(ctx, server, mcpserver.HTTPOptions{Address: runtimeConfig.HTTPAddress, AllowedOrigins: runtimeConfig.AllowedOrigins, Authenticate: store.Authenticate, Logger: logger})
	} else {
		logger.Info("MCP stdio 서버 시작", "version", "0.1.0")
		err = mcpserver.RunStdio(ctx, server)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		// SDK 오류에 원본 입력이 포함될 수 있어 원문은 로그에 남기지 않는다.
		logger.Error("MCP 연결 종료", "code", "protocol_error")
		return 1
	}
	logger.Info("MCP 서버 종료")
	return 0
}

// newDynamicRuntime은 계약 조회기와 Registry 구성을 묶은 런타임을 만든다. 계약은 아직 조회하지 않는다.
// 설정이 꺼져 있으면 호출하지 않으므로 /openapi.json 조회도, 주기 갱신도 없다.
func newDynamicRuntime(client *ableops.Client, cfg config.DynamicConfig, logger *slog.Logger) *dynamic.Runtime {
	return dynamic.NewRuntime(client, logger, openapi.NewLoader(client), dynamic.Options{
		StaticToolNames: tools.StaticNames(),
		Operations:      cfg.Operations,
	})
}
