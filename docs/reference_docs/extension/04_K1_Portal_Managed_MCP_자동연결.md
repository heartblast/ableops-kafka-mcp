# AbleOps Kafka — Phase K1: Managed MCP Extension 자동 연결

## 대상 저장소

`heartblast/ableops-kafka`

현재 GitHub `main` 최신 소스를 기준으로 작업한다.

확인 시점 기준 v1.8.0 병합 이후 구조를 전제로 하지만, 작업 시작 시 반드시 현재 HEAD를 다시 확인한다.

## 선행조건

`heartblast/ableops-kafka-mcp`에 다음 ID의 Process Extension이 구현되어 있다고 가정한다.

```text
ableops-kafka-mcp
```

Extension 내부 MCP 경로:

```text
POST /internal/delegations
POST /mcp
```

두 경로는 AbleOps Extension `X-Ableops-Call-Token` 보안 경계를 먼저 통과한다.

## 목적

현재 별도 MCP 서비스에 의존하는 구조:

```text
AbleOps Kafka
  → MCP_DELEGATION_URL
  → MCP_DELEGATION_SECRET
  → 별도 MCP localhost:8081
```

를 Managed Extension이 설치된 경우:

```text
AbleOps Kafka
  → ExtensionHost에서 현재 MCP child process 조회
  → 현재 random loopback address
  → Extension Call Token
  → /internal/delegations
  → /mcp
```

방식으로 자동 연결한다.

기존 External MCP 방식은 backward compatibility를 위해 제거하지 않는다.

---

## 우선 확인할 파일

전체 저장소를 다시 광범위하게 스캔하지 않는다.

먼저 아래만 확인한다.

- `internal/extensionhost/process.go`
- `internal/extensionhost/registry.go`
- `internal/extensionhost/proxy.go`
- `internal/extensionhost/token.go`
- `internal/mcpdelegation/service.go`
- `internal/server/mcp_delegation.go`
- `internal/aichat/mcpclient.go`
- `internal/aichat/service.go`
- `internal/aiconfig/*`
- `internal/server/ai_settings.go`

직접 의존성이 필요한 경우에만 추가 파일을 확인한다.

---

## 1. Extension Process Target Resolver

`internal/extensionhost`에 Core 내부에서만 사용하는 Process Target 조회 기능을 추가한다.

개념적 반환값:

```go
type ProcessTarget struct {
    ExtensionID string
    BaseURL     string
    CallToken   string
    State       State
}
```

실제 타입/위치는 현재 코드 스타일에 맞춘다.

### 요구사항

Target은 다음 조건을 만족할 때만 반환한다.

- Extension ID 일치
- installed
- enabled
- backend kind=process
- 현재 process instance 존재
- RUNNING 상태
- 현재 loopback address 존재
- 현재 Call Token 존재

다음 값은 절대 반환하지 않는다.

- Host API Token
- DB credential
- Kafka credential
- Extension config 전체
- 사용자 session token

REST API로 노출하지 않는다.

### Cache 금지

Extension 재기동 시:

- port
- process instance
- Call Token

이 바뀔 수 있다.

따라서 caller가 Target을 장시간 캐시하는 구조를 만들지 않는다.

새 Chat 요청/새 Delegation 발급 시 최신 Target을 해소할 수 있어야 한다.

---

## 2. MCP Delegation Provider 분리

현재 `mcpdelegation.Service`는 고정 URL + Internal Shared Secret 구조다.

이를 작은 인터페이스 또는 Provider 경계로 분리한다.

두 구현이 존재해야 한다.

### External MCP Provider

현재 동작 그대로 유지한다.

```text
fixed URL
+ X-AbleOps-Internal-Secret
```

기존 설정/테스트/오류 의미를 깨지 않는다.

### Managed Extension Provider

매 요청 시 ExtensionHost에서 최신 Target을 조회한다.

호출:

```text
POST {target.BaseURL}/internal/delegations
```

Header:

```text
X-Ableops-Call-Token: <current call token>
X-Ableops-Extension-Id: ableops-kafka-mcp
```

Backend Web Session은 기존 request body 계약대로 전달한다.

Managed mode에서는 별도의 MCP Delegation Shared Secret을 사용하지 않는다.

### 기존 보안 계약 유지

다음은 그대로 유지한다.

- 현재 포털 로그인 사용자
- `mw.Token(r)`
- `mw.UserFrom(r.Context())`
- MCP가 확인한 userId와 포털 userId 교차 검증
- Delegation Token을 Browser에 반환하지 않음
- Session Token/Delegation Token 로그 금지

---

## 3. AI Chat MCP Client에 Managed Target 지원

현재 AI Chat에서 수행하는:

```text
tools/list
tools/call
```

도 Managed Extension Target을 사용할 수 있게 한다.

Managed MCP 호출:

```text
POST {current-target}/mcp
```

Header:

```text
X-Ableops-Call-Token: <target call token>
X-Ableops-Extension-Id: ableops-kafka-mcp
Authorization: Bearer <delegation-token>
```

기존 MCP HTTP client의:

- timeout
- cancellation
- stream cancellation
- JSON-RPC 처리
- 오류 안전화
- credential 비노출

계약은 유지한다.

### Extension 재시작 처리

새 Chat 요청은 항상 최신 Target을 해소한다.

진행 중 Tool Call 도중 MCP Extension이 재시작되어 연결이 끊긴 경우:

- 새 Target으로 같은 `tools/call`을 자동 재실행하지 않는다.
- 중복 실행 가능성이 있으므로 현재 요청은 안전한 MCP unavailable 계열 오류로 종료한다.

조회 Tool이라도 공통 정책을 단순하게 유지한다.

---

## 4. Provider 선택 규칙

다음 순서를 적용한다.

1. `ableops-kafka-mcp` Extension이 설치 + enabled + RUNNING
   → Managed MCP Provider 사용

2. Managed Extension 사용 불가
   + 기존 External MCP 설정 정상
   → External MCP Provider 사용

3. 둘 다 사용 불가
   → 기존 `unconfigured / disabled / misconfigured` 의미에 맞는 상태 반환

외부 MCP를 이미 사용하는 운영 환경을 깨지 않는다.

---

## 5. 상태 판정

현재 AI status/setting test에서 다음 상태를 정확하게 구분한다.

- MCP Extension 미설치
- Extension disabled
- Extension STARTING
- Extension FAILED
- Extension RUNNING
- Managed MCP protocol 연결 실패
- External MCP fallback 사용
- 사용자 Web Session 문제
- Delegation 문제

특히 MCP Extension 장애를:

```text
로그인 세션 만료
```

로 오분류하지 않는다.

사용자가 재로그인해도 해결되지 않는 MCP 설정/기동 문제는 별도 오류로 표시한다.

---

## 보안 불변식

다음을 반드시 유지한다.

- Browser → MCP 직접 연결 금지
- Web Session Token의 Browser 외 추가 노출 금지
- MCP Delegation Token Browser 노출 금지
- Extension Call Token Browser 노출 금지
- Extension Call Token 로그 금지
- MCP Tool 원본 응답 Browser 노출 금지
- 기존 사용자별 Backend 권한 검사 유지
- Extension public proxy를 MCP transport로 사용하지 않음
- 임의 외부 URL 생성 금지
- redirect를 통한 Token 전달 금지

---

## 테스트

### Extension Target

- RUNNING Extension Target 조회 성공
- disabled Extension 거부
- FAILED Extension 거부
- STARTING 상태 처리
- process restart 후 새로운 address 조회
- process restart 후 새로운 Call Token 조회
- stale Target cache가 없음

### Delegation

- Managed Delegation 정상
- Call Token 누락 거부
- Call Token 오류 거부
- user ID mismatch 거부
- session token 비노출
- delegation token 비노출

### MCP

- Managed `tools/list`
- Managed `tools/call`
- cancellation propagation
- MCP process 종료 시 안전한 오류
- 자동 tool retry 없음

### Regression

- 기존 External MCP 정상
- External MCP 설정만 있는 환경 정상
- Managed MCP 우선선택
- Managed MCP 없을 때 External fallback
- 기존 AI Chat tests 통과
- 기존 Extension tests 통과

---

## 테스트 실행 전략

먼저 변경 영역만 수행한다.

```bash
go test ./internal/extensionhost ./internal/mcpdelegation ./internal/aichat ./internal/server
```

필요하면 race:

```bash
go test -race ./internal/extensionhost ./internal/mcpdelegation ./internal/aichat ./internal/server
```

마지막에 전체 검증을 1회만 수행한다.

저장소의 기존 `scripts/verify.sh` 또는 현재 최신 검증 절차를 사용한다.

---

## 이번 Phase에서 하지 않을 것

- AI 설정 UI 대규모 변경
- Extension 설치 UI 변경
- Extension package 제작
- MCP Tool 추가/삭제
- 새로운 인증 체계
- OAuth/OIDC 도입
- Browser 직접 MCP
- 별도 process supervisor 구현
- 실제 Kafka/DB 접속
- 원격 push

## 완료 조건

1. Managed MCP Extension이 있으면 Portal이 자동으로 현재 process target을 사용한다.
2. 별도 고정 MCP port가 필요 없다.
3. Managed mode에서는 별도 MCP Delegation URL/Secret이 필요 없다.
4. 기존 External MCP 방식은 계속 동작한다.
5. Extension restart 이후 다음 Chat 요청이 자동으로 새 process target을 사용한다.
6. 사용자 권한/위임 보안 모델이 유지된다.

## 완료 보고

1. 변경 파일
2. Managed Target Resolver 구조
3. Managed/External Provider 선택 규칙
4. 제거 가능해진 운영 설정
5. 보안 경계
6. 테스트 결과
7. K2에서 처리할 UI/배포 항목
