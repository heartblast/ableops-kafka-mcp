// Command ableops-kafka-mcp-extension은 AbleOps Kafka MCP 의 Managed Process Extension
// 실행 진입점이다.
//
// 이 바이너리는 **AbleOps Core 가 실행한다**. Core 는 패키지의 bin/<GOOS>-<GOARCH>/ 아래에서
// 이 파일을 찾아 환경변수(호출 토큰·Host API 주소 등)를 주입해 기동하고, 프로세스가 stdout 에
// 출력하는 기동 핸드셰이크 한 줄로 실제 리스닝 주소를 알아낸다.
//
// 직접 실행하면 SDK 가 필수 환경변수 누락을 감지해 한국어 안내와 함께 종료한다. standalone MCP
// 가 필요하면 기존 `ableops-kafka-mcp` 를 쓴다 — 그쪽 실행 방식은 이 바이너리가 바꾸지 않는다.
//
// main 이 하는 일은 extserver.Run 호출 하나뿐이다. HTTP listener·핸드셰이크·호출 토큰 검증·
// 신원 전달·graceful shutdown 은 SDK 런타임의 책임이고, MCP 서버·도구·인증은
// internal/app.Runtime 의 책임이다.
package main

import (
	"fmt"
	"os"

	"github.com/heartblast/ableops-kafka-mcp/internal/extension"
	"github.com/heartblast/ableops-sdk/extension/v1/extserver"
)

func main() {
	if err := extserver.Run(extension.New()); err != nil {
		// 오류는 stderr 로 낸다 — stdout 은 기동 핸드셰이크 전용이며, Core 는 stderr 를
		// 확장 ID 와 함께 로그로 수집해 설치 이력·최근 오류에 보여 준다.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
