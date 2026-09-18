# ableops-kafka-mcp Dynamic MCP 3차 - 2개 Tool 선택적 Promotion

대상:
- repo: `heartblast/ableops-kafka-mcp`
- branch: `feat/dynamic-mcp-openapi`
- model: Claude Opus 5

이번 세션은 사용량을 아끼기 위해 **2개 Tool만 확실히 Dynamic Promotion하고 전체 테스트까지 완료**한다.

대상 Tool:
1. `get_event_summary`
2. `list_consumer_groups`

## 현재 구현 재사용

이미 구현된 다음 기능은 다시 만들지 말 것.

- OpenAPI Loader
- ETag / 304
- Last Known Good
- Dynamic Registry / Executor
- Runtime Refresh / Atomic Swap
- `tools/list_changed`
- SAFE / SHADOW / BLOCKED
- Static/Dynamic Compatibility 분석

관련 파일만 좁게 확인한다.

```text
internal/dynamic/
internal/tools/
internal/ableops/
internal/mcpserver/
docs/dynamic-mcp.md
````

## 목표 구조

기존 MCP 계약은 유지하고 내부 실행만 Dynamic으로 바꾼다.

```text
기존 MCP Tool
    ↓
Stable Adapter
    ↓
operationId
    ↓
Dynamic Registry
    ↓
Generic REST Executor
    ↓
AbleOps REST API
```

REST URL은 Adapter에 하드코딩하지 말고 OpenAPI Registry에서 가져온다.

---

## 1. get_event_summary

연결:

```text
get_event_summary
→ getEventSummary
```

기존 Static Tool의 다음 계약을 유지한다.

* `cluster_id` 입력
* 기존 Input/Output Schema
* `collection.enabled=false` 처리
* `partial`, `collection_disabled`
* 기존 공개 필드와 제한 설명

Raw Dynamic 응답을 그대로 반환하지 말고 기존 Static 공개 범위로 Projection한다.

---

## 2. list_consumer_groups

연결:

```text
list_consumer_groups
→ listConsumerGroups
```

기존 MCP 입력:

```text
cluster_id
```

OpenAPI 입력:

```text
id
```

이므로 내부에서만:

```text
cluster_id → id
```

로 변환한다.

기존 Static Tool의 다음 의미를 유지한다.

* clusterId 대상 확인
* `syncedAt=null` partial 처리
* topicLag limit
* 기존 결과 봉투/제한 설명

---

## 3. Dynamic ON/OFF

Dynamic OFF:

* 기존 Static Tool 그대로 사용

Dynamic ON:

* 동일한 MCP Tool 이름과 Input Schema 유지
* 내부 실행만 Stable Adapter + Dynamic Registry 사용
* Static/Dynamic 중복 Tool 노출 금지

Backend의 4xx/5xx/timeout은 Static으로 재호출하지 않는다.

OpenAPI 적재 실패나 필요한 operationId 부재 등 **Dynamic 인프라 사용 불가 시에만 기존 Static 유지**.

---

## 4. 핵심 테스트

반드시 다음만 확인한다.

### 계약 유지

* Tool 이름 동일
* Input Schema 동일
* 기존 공개 필드보다 추가 노출 없음

### Parameter mapping

* `cluster_id → id`

### Dynamic 실행

* 두 Tool 모두 실제 Dynamic Executor 경로 사용

### REST path 변경 대응

테스트 계약에서 path만 바꿔도:

```text
operationId 유지
→ MCP Tool 계약 유지
→ 새 REST path 사용
```

되는지 검증한다.

### operationId 없음

비슷한 이름을 추측하지 말고 Dynamic Promotion을 사용하지 않는다.

### 오류

401 / 403 / 404 / 5xx / timeout에서 이중 호출이 없어야 한다.

---

## 5. 전체 검증

2개 Tool 구현 완료 후 다른 기능을 추가하지 말고 전체 테스트한다.

```powershell
.\scripts\test.ps1
```

그리고 가능하면:

```bash
go test -count=1 ./...
go vet ./...
go mod verify
go build ./cmd/ableops-kafka-mcp
go build ./cmd/ableops-mcp-auth
```

기존 Dynamic Hot Reload / Registry / Static Tool 테스트를 약화시키지 않는다.

---

## 이번 세션에서 하지 않을 것

* `list_topics` Promotion
* 세 번째 Tool 전환
* Static Tool 삭제
* BLOCKED Operation 전환
* Curated Tool 변경
* POST/PUT/PATCH/DELETE Dynamic 지원
* 범용 Sanitizer
* `ableops-kafka` 수정
* 대규모 리팩터링
* 전체 int64 문제 수정

두 Tool이 끝나도 남은 사용량으로 새 Tool을 추가하지 말고 테스트와 회귀 확인에 사용한다.

---

## 종료 기준

다음 2개가 모두:

```text
기존 MCP 계약
→ Stable Adapter
→ operationId
→ Dynamic Registry
→ REST 호출
→ 기존 MCP 결과 계약
```

으로 동작하고 전체 테스트가 통과하면 종료한다.

최종 보고:

```text
완료:
- get_event_summary
- list_consumer_groups

각 Tool:
- operationId
- input mapping
- response projection
- semantic 유지 내용

Dynamic OFF:
- 기존 Static 유지 여부

Dynamic ON:
- Dynamic 실행 여부
- 중복 Tool 여부

REST path 변경:
- MCP 계약 영향 여부

오류/Fallback:
- 동작 요약

검증:
- test.ps1
- go test
- go vet
- go build

미검증/후속:
- ...
```

중요:
**이번 세션은 Opus 5로 2개 Tool Promotion + 전체 테스트까지만 수행하고 종료한다.**
