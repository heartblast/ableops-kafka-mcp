# AbleOps Kafka MCP — Phase M2: Managed Process Extension 구현

## 대상 저장소

'/home/netbee/sharedisk/dev/workspace/ableops-kafka-mcp'


## 현재 작업 상태 — 반드시 보존

Phase M1이 완료되어 **현재 working tree에 아직 commit되지 않은 변경사항이 존재한다.**

Phase M1에서 다음이 이미 구현되어 있으므로 다시 구현하거나 되돌리지 않는다.

```text
internal/app/app.go
internal/app/app_test.go
cmd/ableops-kafka-mcp/main.go
```

확정된 재사용 API:

```go
rt, err := app.New(cfg config.ServerConfig, logger *slog.Logger)

rt.Start(ctx)           // Dynamic 최초 Refresh + 주기 갱신, 멱등
rt.NewHTTPHandler()     // listener 없는 MCP HTTP Handler
rt.HTTPOptions()
rt.Server()
rt.Client()
rt.Dynamic()
rt.Close()              // lifecycle 정리, 멱등
```

Phase M1에서 다음 책임은 이미 `internal/app.Runtime`으로 이동했다.

* Backend REST Client 생성
* Dynamic Runtime 생성/Refresh/lifecycle
* MCP Server 생성
* localauth/webdelegation 조립
* MCP HTTP Handler 생성
* Client connection 정리

### 중요

현재 working tree의 Phase M1 변경은 의도된 선행 작업이다.

다음을 하지 않는다.

```text
git reset
git checkout .
git restore .
Phase M1 코드 재구현
Phase M1 구조 원복
```

작업 시작 시 현재 diff를 확인하고 **Phase M1 위에 이어서 구현한다.**

---

# 목표

기존 MCP Runtime을 그대로 사용하여 AbleOps Kafka가 관리하는 **외부 Process Extension 실행 방식**을 추가한다.

최종적으로 두 실행 방식을 지원한다.

```text
cmd/ableops-kafka-mcp
    └─ 기존 standalone MCP

cmd/ableops-kafka-mcp-extension
    └─ AbleOps Kafka Managed Process Extension
```

두 실행 방식은 동일한:

```text
internal/app.Runtime
MCP Tool Registry
Dynamic Runtime
Backend REST Client
MCP Server
```

를 공유해야 한다.

MCP 기능을 새로 복사해 만들지 않는다.

---

# 참고 저장소

필요한 범위에서만 최신 소스를 확인한다.

```text
heartblast/ableops-sdk
heartblast/ableops-kafka
```

우선 확인:

```text
ableops-sdk/extension/v1
ableops-sdk/extension/v1/extserver

ableops-kafka/extensions/sample-process
```

전체 저장소를 다시 광범위하게 분석하지 않는다.

---

# 1. SDK 의존성

다음을 정식 Go module dependency로 사용한다.

```text
github.com/heartblast/ableops-sdk/extension/v1
github.com/heartblast/ableops-sdk/extension/v1/extserver
```

금지:

```text
local replace
go.work
ableops-kafka/internal/... import
Core 소스 복사
```

현재 MCP의 Go 버전과 최신 AbleOps SDK `go.mod`를 확인한다.

SDK가 Go 1.27 이상을 요구한다면 다음을 일관되게 맞춘다.

```text
go.mod
CI Go matrix
build/test script
관련 현행 문서
```

과거 이력 문서는 불필요하게 수정하지 않는다.

---

# 2. Extension Manifest

Extension ID:

```text
ableops-kafka-mcp
```

기본 구조:

```yaml
apiVersion: ableops.io/extension/v1
id: ableops-kafka-mcp
name: AbleOps Kafka MCP

backend:
  enabled: true
  kind: process

frontend:
  enabled: false
```

현재 Core/SDK 계약을 확인하여 적절한 `requires.core` 범위를 선언한다.

MCP는 일반 사용자 화면 Extension이 아니므로 이번 Phase에서는:

```text
메뉴 없음
Frontend 없음
일반 Browser용 API 없음
```

을 기본으로 한다.

capability/permission도 SDK validation이 허용하는 최소 범위로 한다.

사용하지 않는 capability를 편의상 선언하지 않는다.

---

# 3. Managed Extension 전용 Runtime 조립 seam

Phase M1의 `app.New()`를 복제하지 않는다.

현재 `app.New()`는 standalone HTTP 모드를 위해:

```text
localauth
webdelegation
HTTPOptions
```

까지 조립한다.

Managed Extension에서는 인증 구조가 다르므로 `internal/app`에 **최소한의 additive seam**만 추가한다.

예를 들면 다음 중 현재 코드에 가장 작은 방식을 선택한다.

```go
app.NewWithOptions(...)
```

또는:

```go
app.NewManaged(...)
```

또는 이에 준하는 작은 Option 패턴.

### 반드시 지킬 것

기존:

```go
app.New(cfg, logger)
```

의 의미와 standalone 동작은 변경하지 않는다.

Managed Mode 때문에 standalone Runtime을 다시 설계하지 않는다.

---

# 4. Managed Mode 인증 구성

Managed Extension에서는 `localauth`를 사용하지 않는다.

`/mcp` 인증은 **Managed Web Delegation Token만** 허용한다.

즉:

```text
Portal Web Session
      ↓
/internal/delegations
      ↓
short-lived MCP delegation token
      ↓
/mcp Authorization Bearer
```

구조를 유지한다.

Standalone에서 사용하는:

```text
local auth store
Claude Desktop/Codex용 MCP token
```

구조는 기존 그대로 둔다.

---

# 5. Backend Base URL 자동 구성

Managed Extension에서는 운영자가 별도로:

```text
ABLEOPS_BASE_URL
```

을 설정하지 않는다.

SDK의:

```go
extserver.LoadEnvironment(manifest)
```

또는 현재 최신 동등 계약에서 받은 `HostURL`을 사용한다.

예:

```text
http://127.0.0.1:8080/api/extensions/_host
```

에서 URL 구조를 이용해 origin만 추출한다.

결과:

```text
http://127.0.0.1:8080
```

규칙:

* 문자열 replace 금지
* `net/url` 사용
* scheme + host만 사용
* query/fragment/UserInfo 금지
* HTTP는 loopback만 허용
* loopback HTTP인 경우에만 `AllowHTTP=true`

Managed Mode는 shared Backend Token을 갖지 않는다.

기존 MCP HTTP mode처럼 사용자별 request credential을 사용한다.

---

# 6. 별도 MCP Listener 금지

Extension 내부에서:

```go
mcpserver.RunHTTP(...)
```

를 호출하지 않는다.

다음 M1 결과를 그대로 사용한다.

```go
rt.NewHTTPHandler()
```

이는 listener 없는 Handler다.

AbleOps SDK `extserver`가:

```text
127.0.0.1:0
```

으로 실제 listener를 하나만 열고:

```text
ABLEOPS_EXT_READY
```

핸드셰이크를 수행하게 한다.

### HTTPOptions.Address

현재 `NewHTTPHandler()` 내부 validation에서 loopback Address가 필요하더라도 이를 이유로 `mcpserver/http.go`의 계약을 다시 리팩터링하지 않는다.

필요하면 Managed Mode의 Handler 설정값으로:

```text
127.0.0.1:0
```

과 같은 유효한 loopback placeholder를 사용한다.

실제 listener bind는 extserver만 수행한다.

---

# 7. Extension Route

Managed Extension의 MCP Handler를 다음 경로에 연결한다.

```text
POST /mcp
POST /internal/delegations
```

두 route가 동일한 기존 MCP Handler를 사용할 수 있으면 재사용한다.

새 MCP protocol handler를 만들지 않는다.

Extension의:

```text
/health
```

는 SDK `extserver`가 제공하는 기존 Health 계약을 사용한다.

---

# 8. Extension Call Token 보안 경계

모든 Extension 요청은 기존 extserver의:

```text
X-Ableops-Call-Token
```

검증을 가장 먼저 통과해야 한다.

별도의 Core↔Extension 인증 체계를 만들지 않는다.

---

# 9. `/internal/delegations` 내부 secret 처리

현재 standalone MCP의 `/internal/delegations`는:

```text
X-AbleOps-Internal-Secret
```

을 요구한다.

Managed Mode에서는 사용자가:

```text
ABLEOPS_WEB_DELEGATION_SECRET
```

을 별도로 설정하지 않게 한다.

기존 `mcpserver` 보안 계약을 크게 변경하지 말고 다음 방식으로 처리한다.

1. Extension 시작 시 `crypto/rand`로 충분히 긴 임시 secret 생성
2. 프로세스 메모리에만 보관
3. 기존 `HTTPOptions.InternalSecret`으로 전달
4. Extension Call Token 검증을 통과한 `/internal/delegations` 요청만 wrapper 처리
5. wrapper에서 외부 요청의 Internal Secret header를 제거
6. 내부 Handler로 넘기기 직전에 프로세스 내부 secret을 주입
7. secret은 로그/오류/Health/응답/Manifest에 절대 포함하지 않음

보안 경계:

```text
Core
  │
  │ X-Ableops-Call-Token
  ▼
extserver 인증
  │
  ▼
managed wrapper
  │ 내부 ephemeral secret 주입
  ▼
기존 MCP internal delegation handler
```

기존 `internal/mcpserver/internal_delegation.go`의 핵심 인증/요청 검증 로직을 복사하지 않는다.

---

# 10. `/mcp` 인증

`/mcp`는 기존 Delegation Bearer 인증을 그대로 사용한다.

```text
Authorization: Bearer <short-lived-delegation-token>
```

Managed Extension이라고 해서 사용자별 권한 검사를 제거하지 않는다.

MCP Tool → AbleOps REST 호출도 현재 사용자의 Backend Session 권한으로 수행한다.

---

# 11. Extension Lifecycle

`extensionv1.Extension` 계약에 맞게 구현한다.

## Start

* Extension 환경 확인
* Core REST origin 생성
* Managed `config.ServerConfig` 또는 동등 Runtime 설정 생성
* Managed 인증 Provider 준비
* `internal/app.Runtime` 생성
* `rt.Start(ctx)`
* MCP HTTP Handler 준비

이미 M1에서 구현한 Runtime lifecycle을 복제하지 않는다.

## Stop

가능하면 핵심은:

```go
rt.Close()
```

를 재사용한다.

추가 resource가 있다면 함께 정리한다.

Stop은 멱등이어야 한다.

## Health

Health마다 Backend/Kafka REST 호출을 하지 않는다.

빠르게 반환 가능한 내부 상태만 사용한다.

예:

```text
Runtime 생성 여부
Start 완료 여부
종료 여부
```

Dynamic 초기 Refresh 실패를 Extension 기동 실패로 새롭게 정의하지 않는다.

현재 MCP 계약은 최초 Dynamic 적재 실패 시 Static Tool로 계속 기동하므로 이 의미를 유지한다.

따라서 M1 후속사항의:

```text
Runtime.Start()가 RefreshReport를 반환하도록 변경
```

은 이번 Phase에서 하지 않는다.

## Routes

MCP 전용 route만 제공한다.

---

# 12. M1 후속사항 중 이번 Phase에서 하지 않을 것

M1 완료 보고의 다음 항목은 이번 작업과 직접 관계없으므로 구현하지 않는다.

### HTTPOptions Address validation 대규모 리팩터링

extserver가 TCP loopback `:0`을 사용하므로 현재 요구에는 필요 없다.

### Runtime.Start RefreshReport 반환

Dynamic failure는 기존대로 Static fallback으로 처리한다.

### ableops-mcp-auth 통합

이번 Extension 동작과 무관하다.

---

# 13. Standalone 호환성

반드시 기존 동작을 보존한다.

```text
stdio MCP
standalone HTTP MCP
localauth
standalone web_delegation
Claude Desktop/Codex 사용
Dynamic Tool
33개 Static Tool
기존 환경변수
기존 YAML
```

Managed Mode를 위해 standalone 설정 의미를 변경하지 않는다.

---

# 테스트

## Phase M1 회귀

```bash
go test ./internal/app
go test ./cmd/ableops-kafka-mcp
```

## Managed Extension targeted test

최소 다음을 검증한다.

### Manifest

* Manifest validation
* ID mismatch 거부

### Backend origin

* HostURL → origin
* loopback HTTP
* 잘못된 URL 거부

### Runtime

* Managed Runtime 생성
* localauth 없이 동작
* `rt.Start()`/`rt.Close()` lifecycle
* Stop 멱등성

### Extension

* Start
* Stop
* Health
* `/mcp`
* `/internal/delegations`

### 인증

* Call Token 없음 → 거부
* 잘못된 Call Token → 거부
* 외부에서 Internal Secret을 주입해도 신뢰하지 않음
* delegation token 없음 → `/mcp` 거부
* 정상 delegation 후 MCP 요청 성공
* 사용자 ID mismatch 거부

### Dynamic

Dynamic 최초 Refresh 실패 시:

```text
Extension 자체는 기동
Static Tool 사용 가능
```

해야 한다.

## 전체 검증

마지막에 한 번만 실행한다.

```bash
go test ./...
go vet ./...
go build ./cmd/ableops-kafka-mcp
go build ./cmd/ableops-kafka-mcp-extension
go mod verify
```

실제 Kafka/DB에는 접속하지 않는다.

`httptest`와 synthetic 데이터만 사용한다.

---

# 금지

* M1 구조 원복
* app Runtime 복제
* 새 MCP Tool 추가
* Tool Schema 변경
* `ableops-kafka` 저장소 수정
* Core internal package import
* 두 번째 MCP listener 생성
* 고정 8081 포트 생성
* local `replace`
* `go.work`
* 실제 Kafka/DB 접속
* OAuth/OIDC 신규 구현
* Browser 직접 MCP 연결
* 원격 push

범위 밖 개선은 구현하지 말고 후속사항으로만 남긴다.

---

# 완료 조건

1. 기존 standalone MCP가 그대로 동작한다.
2. `ableops-kafka-mcp-extension` binary가 생성된다.
3. Managed Extension과 standalone이 같은 `internal/app.Runtime`을 사용한다.
4. Extension은 extserver의 단일 listener만 사용한다.
5. Managed Mode에서 별도 `ABLEOPS_BASE_URL`이 필요 없다.
6. Managed Mode에서 별도 Web Delegation Shared Secret 설정이 필요 없다.
7. 사용자별 MCP Delegation 보안 모델은 유지된다.
8. Dynamic 초기 실패 시 Static Tool로 정상 기동한다.
9. 전체 테스트가 통과한다.

# 완료 보고

1. 변경 파일
2. `internal/app`에 추가한 Managed Mode seam
3. Extension 진입점
4. Runtime 재사용 구조
5. 인증 구조
6. Backend origin 결정 방식
7. Go/SDK 버전 변경
8. 테스트 결과
9. Phase M3/K1에 전달할 실제 계약
