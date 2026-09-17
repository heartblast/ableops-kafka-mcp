# ableops-kafka-mcp Dynamic MCP 2차 — Hot Reload · Registry 교체 · 노출 안전성 개발

`heartblast/ableops-kafka-mcp`의 1차 Dynamic MCP 구현 결과를 기반으로 **실행 중 OpenAPI 변경을 안전하게 반영할 수 있는 운영 수준의 Dynamic Tool Registry**를 구현해줘.

이번 작업은 Claude Max 5x의 **1회 5시간 사용량 창에서 완료 가능한 범위**로 제한한다.

이번 단계의 핵심은 Static Tool 전환이 아니다.

목표는 다음이다.

```text
AbleOps Kafka
/openapi.json
      │
      │ ETag
      ▼
Contract Refresher
      │
      ▼
새 계약 검증
      │
      ▼
새 Dynamic Registry 생성
      │
      ▼
Atomic Swap
      │
      ├─ 성공 → tools/list_changed
      │
      └─ 실패 → Last Known Good 유지
```

---

# 현재 구현 상태

1차 개발에서 다음이 완료되어 있다.

* OpenAPI Loader
* `x-mcp-enabled` 파싱
* Dynamic Tool Compiler
* Dynamic Tool Registry
* Generic GET REST Executor
* Static/Dynamic 병행
* Shadow Registry
* ETag 저장
* `If-None-Match`
* `304 Not Modified`
* Last Known Good 계약 유지
* 실제 MCP `tools/list` / `tools/call` E2E 검증
* Dynamic 기능은 기본 OFF

현재 설정:

```text
ABLEOPS_DYNAMIC_TOOLS=true
```

또는:

```yaml
dynamic_tools:
  enabled: true
```

로 활성화한다.

---

# 현재 확인된 중요한 제약

이번 개발에서 아래 문제를 반드시 고려한다.

## 1. Static과 Dynamic의 공개 범위 차이

Static Tool은 일부 정보를 의도적으로 제거한다.

예:

```text
Cluster:
- SASL 사용자명
- TLS 경로

Topic:
- configs

Event:
- evidence
```

Dynamic Tool은 Backend/OpenAPI Response를 그대로 반환하므로 해당 값이 LLM에 전달될 수 있다.

따라서 **Dynamic Tool 전체 자동 노출 확대를 금지한다.**

---

## 2. 입력 이름 차이

예:

```text
Static:
cluster_id

Dynamic:
id
```

Tool 이름이 같다고 자동 교체하면 기존 LLM/Client 호출과 호환되지 않을 수 있다.

이번 단계에서도 Static Tool 자동 교체는 하지 않는다.

---

## 3. Static의 int64 문제

기존 Static Tool은 SDK 후처리 과정에서 `2^53`을 초과하는 Lag/Offset이 반올림되는 문제가 확인됐다.

이번 작업에서 이 문제를 Dynamic 결과까지 따라 하도록 만들지 않는다.

Dynamic 결과는 현재처럼 원문 JSON 숫자를 보존한다.

Static 버그 수정도 이번 범위에서는 제외한다.

---

## 4. Backend 의미를 MCP가 추측하지 않는다

예:

```text
200 + []
syncedAt = null
partial
status
```

등을 MCP가 임의로:

```text
정상
이상 없음
success=true
```

등으로 바꾸지 않는다.

---

# Phase 1 — Runtime Contract Refresher

## 목표

MCP 서버 실행 중 OpenAPI 계약 변경을 감지한다.

현재 구현된:

```text
ETag
If-None-Match
304
Last Known Good
```

기능을 이용한다.

## 설정 추가

기존 설정 체계를 따르되 예를 들어:

```yaml
dynamic_tools:
  enabled: true
  refresh_interval: 5m
```

또는 환경변수:

```text
ABLEOPS_DYNAMIC_REFRESH_INTERVAL
```

을 지원한다.

기본값은 보수적으로 설정한다.

권장:

```text
5m
```

최소 허용값도 두어 지나친 polling을 막는다.

예:

```text
최소 1m
```

### 동작

```text
Timer
  ↓
GET /openapi.json
If-None-Match: "<etag>"
  ↓
304
→ 아무 작업 안 함

200
→ 새 계약 parse
→ 검증
→ 새로운 Dynamic Registry 생성

오류
→ 기존 Registry 유지
```

### 중요

Refresh 실패로 현재 Dynamic Tool이 사라지면 안 된다.

항상:

```text
Last Known Good
```

를 유지한다.

---

# Phase 2 — Atomic Registry Swap

## 목표

새로운 OpenAPI 계약이 정상인 경우에만 현재 Registry를 교체한다.

다음과 같은 중간 상태가 사용자에게 보여서는 안 된다.

```text
Registry 재생성 중
→ Tool 일부만 존재
```

따라서:

```text
Current Registry
       │
       │ 계속 서비스
       │
New Contract
       ↓
New Registry Build
       ↓
Validation
       ↓
Atomic Swap
```

형태로 구현한다.

Go에서 현재 구조에 맞게:

* immutable registry snapshot
* mutex
* atomic pointer

중 가장 단순하고 안전한 방식을 선택한다.

과도한 동시성 구조는 만들지 않는다.

---

# Phase 3 — Tool Set Diff + list_changed

## 목표

Registry가 바뀌었을 때 실제 Tool 목록 변화가 있는지를 계산한다.

다음 Diff를 구한다.

```text
Added
Removed
Changed
Unchanged
```

Changed 판단 최소 기준:

* Tool name
* description
* inputSchema
* operationId
* method/path

단순 ETag 변경만으로 Tool 변경 알림을 보내지 않는다.

예:

```text
OpenAPI 설명 내부 변경
       ↓
Tool definition 동일
       ↓
알림 없음
```

Tool Definition이 실제 바뀐 경우에만 MCP SDK가 지원하는 공식 방식으로:

```text
notifications/tools/list_changed
```

를 전송한다.

SDK v1.8.0의 실제 API를 확인하고 구현한다.

API 이름을 추측해서 만들지 않는다.

---

# Phase 4 — Dynamic Exposure Safety Gate

## 목표

Dynamic Tool을 확대하기 전에 **LLM에 노출해도 되는 Operation만 Registry에 올라가도록 보호 계층을 추가한다.**

현재 다음 문제가 확인됐다.

```text
OpenAPI x-mcp-enabled=true
        ≠
현재 Static MCP와 동일한 공개 범위
```

따라서 이번 단계에서는 `x-mcp-enabled=true` 하나만으로 기존 Static Tool을 자동 대체하지 않는다.

## Exposure 분류

Dynamic Operation을 내부적으로 최소 다음 상태로 나눈다.

```text
SAFE
SHADOW
BLOCKED
```

### SAFE

현재 Dynamic Tool로 노출 가능한 Operation.

### SHADOW

Dynamic 실행은 가능하지만 Static과:

* input
* output
* 공개 범위
* 의미

중 하나가 달라 자동 교체하면 안 되는 Operation.

### BLOCKED

민감정보 또는 명확한 안전 문제가 있어 LLM에 직접 노출하면 안 되는 Operation.

---

## 현재 알려진 위험 반영

최소 다음은 자동 SAFE로 승격하지 않는다.

```text
getCluster
getTopic
getEvent
```

등 Static이 민감 필드를 제거하는 Tool과 충돌하는 Operation.

실제 코드를 분석해서 정확한 목록을 결정한다.

민감 필드 때문에 문제가 있는 Operation은:

```text
SHADOW
```

또는 필요하면:

```text
BLOCKED
```

로 둔다.

이번 단계에서 복잡한 범용 Response Sanitizer를 만들지 않는다.

**잘못된 자동 마스킹보다 노출하지 않는 편을 선택한다.**

---

# Phase 5 — Compatibility 분석 기능

## 목표

Static Tool을 향후 Dynamic으로 전환할 수 있는지 기계적으로 판단할 수 있도록 한다.

Static과 이름이 충돌하는 현재 8개 Tool에 대해 최소 다음을 비교한다.

```text
Tool name
Input parameter 이름
required/optional
Schema type
Output 공개 범위
Operation 의미
```

결과를 코드 또는 테스트에서 다음처럼 분류할 수 있게 한다.

```text
COMPATIBLE
INPUT_MISMATCH
OUTPUT_MISMATCH
SEMANTIC_MISMATCH
SECURITY_MISMATCH
```

예:

```text
list_clusters
→ input 없음
→ output 공개 범위 비교

list_topics
Static: cluster_id
Dynamic: id
→ INPUT_MISMATCH
```

이번 Phase의 목적은 **교체가 아니라 교체 가능 여부를 객관적으로 확인하는 것**이다.

---

# Phase 6 — 실제 Backend 연동 검증

1차 구현에서는 실제 Backend 연동이 미검증이었다.

이번 단계에서는 가능한 경우 실제 실행 중인 `ableops-kafka`의:

```text
/openapi.json
```

을 이용하여 Dynamic Registry가 동작하는지 확인한다.

최소 확인:

```text
OpenAPI 조회
ETag 수신
304
Tool Registry 생성
tools/list
Dynamic tools/call
인증 전달
Backend 실제 JSON 반환
```

실제 Backend 접근이 불가능하면 억지로 수행하지 말고:

```text
미검증 사유
```

를 정확히 보고한다.

테스트 fixture를 실제 검증으로 가장하지 않는다.

---

# Hot Reload 실패 시나리오 테스트

반드시 다음을 테스트한다.

## 정상 갱신

```text
Contract A
→ Registry A

Contract B
→ Registry B

Tool 변경 감지
→ list_changed
```

## 304

```text
Registry A
→ OpenAPI 304
→ Registry A 유지
→ 알림 없음
```

## 잘못된 OpenAPI

```text
Registry A

새 OpenAPI parse 실패
        ↓
Registry A 유지
        ↓
알림 없음
```

## 일부 Unsupported Operation

```text
지원 Tool → 유지

문제 Operation
→ skip + warning
```

## Tool 삭제

```text
Registry A:
tool_a
tool_b

Registry B:
tool_a

→ tool_b removed
→ atomic swap
→ list_changed
```

## Concurrent Call

Registry 교체 중 기존 Dynamic Tool 호출이 진행 중이어도:

* panic 없음
* partial registry 없음
* data race 없음

을 확인한다.

가능하면 Linux CI의 `-race` 대상에 포함한다.

---

# 이번 단계에서 Static → Dynamic 교체 금지

매우 중요하다.

이번 작업에서는 기존 Static Tool을 제거하거나 Dynamic으로 교체하지 않는다.

```text
Static Tool
   │
   └── 그대로 유지

Dynamic Tool
   │
   └── runtime refresh 가능
```

상태까지만 만든다.

다음 조건을 만족한 Tool만 **후속 Phase의 전환 후보**로 보고한다.

```text
입력 호환
출력 공개범위 호환
보안 호환
업무 의미 호환
E2E 검증
```

---

# OpenAPI Extension 확대

이번 작업 중 현재 `x-mcp-enabled`만으로 노출 안전성을 표현하기 부족하다고 판단되면 필요한 Metadata를 **제안만 한다.**

예:

```yaml
x-mcp:
  enabled: true
  risk: low

  parameterAliases:
    id: cluster_id

  response:
    exclude:
      - saslUsername
      - tls.caFile
```

하지만 이번 MCP 저장소 작업에서 Upstream 계약을 임의로 바꾸거나 자체 규격을 확정하지 않는다.

필요 Metadata와 사용 목적을 최종 보고에 정리한다.

그 후 별도로 `ableops-kafka`에서 계약을 확장하는 것이 원칙이다.

---

# 테스트

최소 다음 테스트를 추가한다.

## Refresh

```text
200 new contract
304
timeout
500
invalid JSON
invalid contract
```

## Registry Swap

```text
add
remove
change
same
failed build
```

## Notifications

```text
tool set changed → 1회 알림
304 → 알림 없음
실패 → 알림 없음
ETag만 변경 → Tool 동일이면 알림 없음
```

## Safety Gate

```text
SAFE → 노출
SHADOW → 기본 Tool 목록 비노출 또는 기존 정책 유지
BLOCKED → 노출 금지
```

현재 서버 동작과 충돌하지 않는 방식으로 적용한다.

---

# 설정 호환성

현재:

```text
ABLEOPS_DYNAMIC_TOOLS
ABLEOPS_DYNAMIC_OPERATIONS
```

및 YAML 설정 호환성을 유지한다.

기존 설정 파일을 수정하지 않아도 서버가 기동되어야 한다.

Dynamic 기본값:

```text
OFF
```

도 유지한다.

Refresh 기능은 Dynamic이 OFF이면 동작하지 않아야 한다.

불필요한 `/openapi.json` polling도 없어야 한다.

---

# 이번 작업에서 하지 않을 것

명시적으로 제외:

```text
Static Tool 삭제
전체 28개 Dynamic Tool 노출
POST/PUT/PATCH/DELETE Dynamic 지원
Backend의 6개 알려진 결함 수정
Static int64 반올림 버그 수정
범용 Response Sanitizer
자체 OpenAPI 규격 확대 확정
```

---

# 작업량 관리

이번 5시간 창 기준:

```text
Phase 1 Runtime Refresh        20%
Phase 2 Atomic Swap            15%
Phase 3 Tool Diff/Notification 20%
Phase 4 Safety Gate            20%
Phase 5 Compatibility 분석     15%
Phase 6 E2E/검증               10%
```

사용량이 부족하면 우선순위:

```text
1. Runtime Refresh
2. Atomic Registry Swap
3. Last Known Good
4. Tool Diff
5. list_changed
6. Safety Gate
7. Compatibility Report
```

Static 전환은 절대 이번 창에서 억지로 진행하지 않는다.

---

# 종료 기준

다음 흐름이 테스트로 입증되면 이번 작업을 완료한다.

```text
MCP Server
    │
    ├── Registry A 서비스
    │
    ▼
OpenAPI ETag 변경
    │
    ▼
새 Contract 조회
    │
    ▼
검증
    │
    ▼
Registry B 생성
    │
    ▼
Atomic Swap
    │
    ▼
Tool Diff
    │
    ▼
tools/list_changed
```

그리고 잘못된 새 계약에서는:

```text
Bad Contract
    ↓
검증 실패
    ↓
Registry A 유지
    ↓
기존 Tool 정상 호출
```

이 반드시 보장되어야 한다.

---

# 최종 보고 형식

```text
완료 Phase:
- Phase 1:
- Phase 2:
- Phase 3:
- Phase 4:
- Phase 5:
- Phase 6:

Runtime Refresh:
- interval:
- ETag:
- 304:
- 오류 시 동작:

Registry:
- atomic swap 방식:
- Last Known Good:
- concurrent call 영향:

Tool Diff:
- added:
- removed:
- changed 판단 기준:

MCP Notification:
- tools/list_changed:
- 사용 SDK API:
- 검증:

Exposure Safety:
- SAFE:
- SHADOW:
- BLOCKED:
- 위험 Operation:

Static/Dynamic Compatibility:
- COMPATIBLE:
- INPUT_MISMATCH:
- OUTPUT_MISMATCH:
- SECURITY_MISMATCH:
- SEMANTIC_MISMATCH:

실제 Backend 검증:
- ...

기존 기능 영향:
- Static Tool:
- stdio:
- HTTP:
- 인증:

Race/Concurrency:
- ...

검증:
- go test:
- go vet:
- go build:
- test.ps1:
- race:

발견한 문제:
- ...

Upstream OpenAPI에 추가가 필요한 Metadata:
- ...

다음 Phase에서 Dynamic 전환 가능한 Static Tool:
- ...

아직 전환하면 안 되는 Tool:
- ...
```

중요: 이번 개발 결과만 보고 **“Dynamic이 Static보다 최신이므로 교체 가능”이라고 판단하지 않는다.**

Static과 Dynamic의 입력·출력·보안·업무 의미가 모두 일치하거나 명확한 호환 계층이 마련된 Tool만 후속 전환 대상으로 선정해줘.
