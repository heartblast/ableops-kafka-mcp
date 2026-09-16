package mcpserver

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RunStdio는 프로세스 종료를 진행 중인 요청에 전파하며 stdio 세션을 실행한다.
// 프로세스 수명에 연결한 미들웨어를 등록하므로 서버 인스턴스당 한 번 호출한다.
func RunStdio(ctx context.Context, server *mcp.Server) error {
	return run(ctx, server, &mcp.StdioTransport{MaxLineLength: 64 * 1024})
}

func run(processCtx context.Context, server *mcp.Server, transport mcp.Transport) error {
	// SDK의 stdio 요청은 연결 context의 취소를 기본적으로 상속하지 않는다.
	// 개별 요청 취소와 프로세스 종료를 결합해 정상 종료 시 REST 요청도 중단한다.
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(requestCtx context.Context, method string, request mcp.Request) (mcp.Result, error) {
			ctx, cancel := context.WithCancel(requestCtx)
			stop := context.AfterFunc(processCtx, cancel)
			defer stop()
			defer cancel()
			if processCtx.Err() != nil {
				cancel()
			}
			return next(ctx, method, request)
		}
	})
	return server.Run(processCtx, transport)
}
