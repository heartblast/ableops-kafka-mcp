# AbleOps Kafka MCP — Phase M1: MCP Runtime 분리

## 대상 저장소

'/home/netbee/sharedisk/dev/workspace/ableops-kafka-mcp'


## 목적

AbleOps Kafka MCP를 향후 AbleOps Kafka의 **외부 프로세스 Extension**으로 실행할 수 있도록 준비한다.

이번 Phase에서는 Extension을 실제 구현하지 않는다.  
현재 `cmd/ableops-kafka-mcp/main.go`에 집중되어 있는 MCP Runtime 조립 로직을 분리하여:

- 기존 standalone 실행
- 향후 AbleOps Extension 실행

두 방식이 동일한 MCP Runtime을 재사용할 수 있게 만드는 것이 목표다.

## 구현 범위

현재 `cmd/ableops-kafka-mcp/main.go`에 있는 다음 조립 책임을 재사용 가능한 내부 패키지로 분리한다.

- Backend REST Client 생성
- MCP Server 생성
- Dynamic Runtime 생성
- Dynamic Runtime 최초 Refresh
- Dynamic Tool 주기 Refresh
- Web Delegation Store 조립
- MCP HTTP Handler 생성
- 종료 시 Dynamic Runtime 정리
- REST Client idle connection 정리

가능하면 현재 저장소 구조에 맞춰 `internal/app`, `internal/runtime` 등 자연스러운 위치를 사용한다.

불필요한 범용 프레임워크나 과도한 추상화는 만들지 않는다.

## CLI에 남겨야 할 책임

다음은 기존 CLI의 책임으로 유지한다.

- flag 처리
- env/YAML 설정 적재
- stdio/http transport 선택
- signal 처리
- standalone HTTP listener bind
- 프로세스 exit code 결정
- 사용자용 시작/종료 로그

즉, CLI는 얇은 조립 계층이 되고 MCP Runtime 자체는 다른 실행 진입점에서도 재사용할 수 있어야 한다.

## 반드시 보존할 기존 계약

다음 기능과 외부 계약은 변경하지 않는다.

- 기존 MCP Tool 전체
- Static Tool 계약
- Dynamic Tool 계약
- Dynamic Hot Reload
- `localauth`
- `webdelegation`
- `/internal/delegations`
- `/mcp`
- stdio 모드
- standalone HTTP 모드
- 인증/인가 동작
- request timeout
- response/request 크기 제한
- concurrency 제한
- stdout은 MCP 프로토콜 전용
- 로그는 stderr 전용
- 기존 설정 우선순위
- 기존 환경변수명
- 기존 REST API 기반 사용자 권한 판정

특히 `internal/mcpserver/http.go`의 `NewHTTPHandler()`는 이미 listener와 Handler가 분리되어 있으므로 새 HTTP 전송 계층을 만들지 말고 재사용한다.

## 우선 확인할 파일

전체 저장소를 처음부터 다시 광범위하게 읽지 않는다.

먼저 아래 파일만 확인하고, 직접 의존성이 필요한 경우에만 추가 파일을 읽는다.

- `AGENTS.md`
- `cmd/ableops-kafka-mcp/main.go`
- `internal/mcpserver/http.go`
- `internal/mcpserver/internal_delegation.go`
- `internal/config/config.go`
- `internal/config/webdelegation.go`
- `internal/dynamic/*`
- `internal/webdelegation/*`

## 테스트 전략

클라우드/모델 사용량과 실행 시간을 줄이기 위해 다음 순서로 테스트한다.

### 1. 변경 패키지 중심 테스트

```bash
go test ./internal/... ./cmd/ableops-kafka-mcp
```

### 2. 정적 검증

```bash
go vet ./...
go build ./cmd/ableops-kafka-mcp
```

### 3. 마지막에 전체 검증 1회

실행 가능한 환경이면:

```powershell
./scripts/test.ps1
```

기존 standalone 관련 테스트는 가능하면 수정 없이 통과시킨다.

## 금지사항

이번 Phase에서는 다음을 하지 않는다.

- `ableops-kafka` 저장소 수정
- `ableops-sdk` 의존성 추가
- Extension 구현
- MCP Tool 추가/삭제
- Tool Schema 변경
- 인증 구조 변경
- `local replace`
- `go.work`
- AbleOps Kafka Core의 `internal` 패키지 import
- 실제 Kafka 접속
- 실제 DB 접속
- 기존 기능과 무관한 리팩터링
- 원격 push

범위 밖에서 발견한 개선사항은 구현하지 말고 마지막 결과에 `후속사항`으로만 기록한다.

## 완료 조건

다음이 성립해야 한다.

1. 기존 standalone 실행 방식이 유지된다.
2. MCP Runtime 생성/시작/종료 로직을 다른 진입점에서도 호출할 수 있다.
3. HTTP Handler를 listener 없이 생성할 수 있다.
4. Dynamic Runtime lifecycle을 외부 호출자가 제어할 수 있다.
5. 기존 Tool/인증/설정 계약이 바뀌지 않는다.
6. 테스트와 빌드가 통과한다.

## 완료 보고 형식

아래 항목만 간단히 보고한다.

1. 변경 파일
2. 분리한 Runtime 책임
3. standalone 호환성 여부
4. 수행한 테스트와 결과
5. Phase M2에서 사용할 재사용 진입점/API
6. 구현하지 않고 남긴 후속사항
