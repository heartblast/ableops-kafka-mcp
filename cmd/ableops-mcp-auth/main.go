// ableops-mcp-auth는 로컬 관리자만 실행하는 개발용 자격증명 등록·폐기 명령이다.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/config"
	"github.com/heartblast/ableops-kafka-mcp/internal/localauth"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

func main() { os.Exit(run(os.Args[1:], os.Getenv, os.Stderr)) }

func run(args []string, getenv func(string) string, stderr io.Writer) int {
	fail := func(code string) int { fmt.Fprintln(stderr, "로컬 인증 관리 실패:", code); return 1 }
	if len(args) == 0 {
		return fail("enroll 또는 revoke 명령이 필요합니다")
	}
	flags := flag.NewFlagSet("ableops-mcp-auth", flag.ContinueOnError)
	// flag 기본 오류에는 입력값이 포함될 수 있어 출력하지 않는다.
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "명시적으로 읽을 YAML 설정 파일")
	clientID := flags.String("client-id", "", "등록할 클라이언트 식별자")
	ttl := flags.Duration("ttl", localauth.DefaultTTL, "MCP 토큰 유효기간(최대 8h)")
	output := flags.String("token-output", "", "새 MCP 토큰을 기록할 소유자 전용 파일")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
		return fail("명령 인자가 올바르지 않습니다")
	}
	invalidConfig := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "config" && strings.TrimSpace(*configPath) == "" {
			invalidConfig = true
		}
	})
	if invalidConfig {
		return fail("명령 인자가 올바르지 않습니다")
	}
	switch args[0] {
	case "enroll":
		cfg, path, err := config.LoadEnrollment(*configPath, getenv)
		if err != nil {
			// 설정 검증 오류는 실제 설정값을 포함하지 않는 고정 문구만 반환한다.
			return fail("백엔드 연결 설정이 올바르지 않습니다: " + err.Error())
		}
		client, err := ableops.NewClient(cfg)
		if err != nil {
			// 클라이언트 생성 오류도 인증서 경로나 내용을 포함하지 않는다.
			return fail("백엔드 클라이언트를 생성할 수 없습니다: " + err.Error())
		}
		defer client.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout+time.Second)
		defer cancel()
		verify := func(ctx context.Context, token string) (string, error) {
			return client.CurrentUserID(requestctx.WithPrincipal(ctx, requestctx.Principal{BackendToken: token}))
		}
		_, err = localauth.Enroll(ctx, path, *clientID, cfg.Token, *ttl, *output, verify)
		if err != nil {
			return fail(err.Error())
		}
		fmt.Fprintln(stderr, "등록 완료. MCP 토큰은 지정한 비공개 파일에 저장했습니다.")
	case "revoke":
		path, err := config.LoadAuthStore(*configPath, getenv)
		if err != nil {
			return fail("인증 저장소 설정이 올바르지 않습니다: " + err.Error())
		}
		if err := localauth.Revoke(path, *clientID); err != nil {
			return fail(err.Error())
		}
		fmt.Fprintln(stderr, "폐기 완료. 다음 요청부터 적용됩니다.")
	default:
		return fail("지원하지 않는 명령입니다")
	}
	return 0
}
