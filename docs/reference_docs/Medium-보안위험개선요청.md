# AbleOps Kafka MCP Medium 보안위험 개선 요청

현재 기준으로, 기존 보안 점검에서 확인된 **Medium 위험도 2개 항목만 최소 범위로 보완**해줘.

대상은 다음 두 가지다.

1. **HTTP 요청 Rate Limit 보강**

   * 현재 global/user 동시성 제한은 유지한다.
   * `/mcp` 인증 실패 및 정상 요청에 대해 과도한 반복 호출을 제한할 수 있도록 lightweight rate limit을 추가한다.
   * 가능하면 IP와 인증된 Principal 기준을 구분해 적용한다.
   * 메모리 증가를 막기 위한 entry 상한/정리 정책을 포함한다.
   * 정상 MCP Stream/SSE 사용이나 기존 동시성 제한과 충돌하지 않도록 한다.
   * 별도 외부 보안 제품이나 무거운 신규 의존성은 도입하지 않는다.

2. **Reverse Proxy를 통한 의도치 않은 외부 노출 방지**

   * MCP HTTP 서버의 기존 **loopback-only 정책은 절대 완화하지 않는다.**
   * 프록시를 통해 외부 요청이 `127.0.0.1:8081`로 전달되는 경우를 방어할 수 있는 fail-closed 검증을 추가한다.
   * `Forwarded`, `X-Forwarded-*` 등 프록시 관련 헤더가 들어오는 요청을 기본적으로 신뢰하지 말고, 외부 프록시 경유 가능성이 있는 요청은 거부하도록 검토한다.
   * `/internal/delegations`는 기존보다 약해지지 않도록 별도 서버간 보안 경계를 유지한다.
   * Managed MCP, standalone HTTP, Claude Desktop/Codex 등 기존 사용 방식의 호환성을 깨지 않는다.

구현 전 현재 `internal/mcpserver/http.go`, 인증/Delegation 테스트와 실제 요청 흐름을 먼저 확인하고, 불필요한 구조 변경 없이 최소 수정으로 처리해줘.

반드시 추가/보완할 테스트:

* 짧은 시간 내 반복 인증 실패 요청 차단
* 인증된 동일 사용자 반복 요청 제한
* 다른 사용자 간 rate limit 독립성
* 제한 해제/회복 동작
* rate-limit 상태 메모리 상한
* `Forwarded` / `X-Forwarded-For` / `X-Forwarded-Host` 등 프록시 헤더를 이용한 우회 시도 차단
* 기존 loopback 직접 호출 정상 동작
* Managed MCP 및 `/internal/delegations` 기존 테스트 회귀 없음

완료 후 아래를 수행해줘.

```bash
go test ./...
go vet ./...
```

가능하면 `go test -race ./...`도 수행하고, 실패 시 제품 코드 문제인지 환경/테스트 문제인지 구분해줘.

최종 보고에는 **수정 파일, 두 위험의 기존 공격 가능 경로, 적용한 방어 방식, 테스트 결과, 남은 제약사항**만 간략히 정리해줘.
