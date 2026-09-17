# ableops-kafka-mcp OpenAPI 기반 Dynamic MCP 1차 구현 요청

`heartblast/ableops-kafka-mcp`를 현재 `ableops-kafka`의 OpenAPI 계약에 연동하여 **OpenAPI → MCP Tool → REST 호출**이 실제로 동작하는 Dynamic MCP 기반을 구현해줘.

이번 작업은 Claude Max 5x의 **1회 5시간 사용량 창 안에서 완료 가능한 범위**로 제한한다.

전체 Static Tool 33개를 교체하거나 전체 OpenAPI 기능을 구현하는 것이 아니라, **Dynamic MCP의 핵심 실행 경로를 완성하고 소수 Tool로 검증하는 것**이 목표다.

---

# 선행 상태

`ableops-kafka`에는 다음 작업이 완료되어 있다.

* `GET /openapi.json`

  * 미인증
  * ETag 지원
  * `If-None-Match` → `304`
* OpenAPI 등록 Operation 31개
* 이 중 `x-mcp-enabled: true` 28개
* 모든 Operation에 `x-mcp-enabled` 존재
* 안정적인 `operationId`
* Request/Response Schema
* OpenAPI ↔ 실제 Route Contract Guard
* 파괴적 변경 CI Guard
* `/docs` 내장 API Viewer
* `servers[0].url = "/"`
* `.claude/context/OpenAPI계약.md`에 MCP 인계 계약 정리

현재 MCP 허용 API의 대표 operationId:

```text
listClusters
getCluster
getClusterHealth
getClusterPartitionHealth

listDefaultClusterTopics
listTopics
getTopic
getTopicPartitions

listConsumerGroups
listConsumerGroupAlerts
getConsumerGroupState
getConsumerGroupLag
getConsumerGroupMembers
getConsumerLagOverview

listEvents
getEventSummary
getEvent
listEventOccurrences
getEventIssue
listClusterEvents
getEventPlaybook

getAssetGraph
getAssetImpact
searchAssetGraph

listRequests
getRequest

getCurrentUser
getBranding
```

다음은 `x-mcp-enabled: false`이므로 Dynamic Tool로 노출하지 않는다.

```text
login
getClusterStorage
getAssetGraphReport
```

---

# 매우 중요한 계약 해석 원칙

OpenAPI 계약을 과도하게 해석하지 않는다.

Upstream이 명시적으로 보장하지 않는 것은 MCP에서도 추측하지 않는다.

특히:

1. OpenAPI는 전체 REST API 전수 명세가 아니다.
2. Response Schema는 계약이며 Backend Runtime Validator는 아니다.
3. 일부 enum은 실제 서버 동작 때문에 의도적으로 열린 형태다.
4. nullable 가능성이 있는 값을 강제로 non-null로 해석하지 않는다.
5. HTTP 200이 반드시 정상 조회를 의미하지 않는다.
6. 상태·partial·판별 필드가 존재하면 함께 보존한다.
7. 빈 배열 `[]`을 무조건 "이상 없음"으로 해석하지 않는다.
8. Backend의 현재 오류 의미를 MCP에서 임의 수정하지 않는다.

특히 현재 알려진 Backend 결함:

* `failedBrokers` nil 가능
* storage 실패 사유 손실 가능
* Consumer Lag 전면 관측 실패가 `200 + []`로 보일 수 있음
* Event 저장소 오류가 404로 접힐 수 있음
* `getCluster` RBAC 비대칭
* Request memory/sql 정렬 차이

이번 MCP 작업에서 이러한 Backend 문제를 **보정하거나 숨기지 않는다.**

필요한 경우 원본 상태 정보를 보존하여 호출자가 판단할 수 있게 한다.

---

# 기존 구조 보호 원칙

현재 `ableops-kafka-mcp`는 33개의 Static/Curated Tool을 제공한다.

이번 작업에서:

* 기존 Tool 제거 금지
* Tool 이름 변경 금지
* 기존 인증 구조 변경 금지
* Kafka/DB 직접 접근 금지
* 로그인 대행 금지
* 기존 stdio/HTTP transport 동작 변경 금지
* 기존 테스트를 대량 수정해서 통과시키는 방식 금지

Dynamic 기능은 기존 구조 옆에 **추가**한다.

목표:

```text
ableops-kafka-mcp
│
├─ Existing Static / Curated Tools
│
├─ OpenAPI Client
│
├─ Dynamic Tool Registry
│
└─ Generic REST Executor
```

---

# Phase 1 — OpenAPI Loader + Contract Model

## 목표

Backend `/openapi.json`을 읽고 MCP 허용 Operation을 내부 모델로 변환한다.

기존 저장소 구조를 먼저 좁게 분석하고 적절한 위치에 구현한다.

예상 역할:

```text
internal/openapi/
  loader.go
  parser.go
  operation.go
```

파일명은 현재 구조에 맞춰 조정해도 된다.

## 구현

Backend Base URL 기준:

```text
GET {ABLEOPS_BASE_URL}/openapi.json
```

을 호출한다.

기존 Backend HTTP Client 설정을 최대한 재사용한다.

반드시 재사용할 것:

* timeout
* TLS 설정
* custom CA
* base URL validation
* logging 정책

OpenAPI 조회 자체는 인증 Token 없이 수행한다.

### 추출 정보

최소 다음을 내부 Operation 모델로 만든다.

```text
operationId
method
path
summary
description
parameters
requestBody
responses
x-mcp-enabled
```

`x-mcp-enabled == true`만 Dynamic 후보로 만든다.

### 검증

다음은 오류 처리한다.

* OpenAPI JSON parse 실패
* operationId 없음
* operationId 중복
* 잘못된 path parameter
* 지원하지 않는 Parameter Schema
* malformed x-mcp-enabled

다만 일부 Operation 하나가 unsupported라고 해서 전체 MCP 서버를 죽이지 않는다.

가능하면:

```text
지원 Operation → Registry 등록

Unsupported Operation
        ↓
skip + warning
```

형태로 격리한다.

---

# Phase 2 — Dynamic MCP Tool Compiler / Registry

## 목표

OpenAPI Operation을 MCP Tool 정의로 변환한다.

이번 구현 범위는 우선:

```text
GET
+
x-mcp-enabled=true
```

로 제한한다.

POST/PUT/PATCH/DELETE는 실행하지 않는다.

## Tool 이름

기본적으로 operationId를 snake_case로 변환한다.

예:

```text
listClusters
        ↓
list_clusters

getConsumerGroupLag
        ↓
get_consumer_group_lag

listEventOccurrences
        ↓
list_event_occurrences
```

변환 규칙을 하나의 공통 함수로 구현하고 테스트한다.

## Input Schema 변환

이번 범위에서 지원:

* string
* integer
* number
* boolean
* enum
* array
* nullable
* required / optional
* path parameter
* query parameter

`$ref`가 사용되면 현재 31개 계약을 처리하는 데 필요한 범위까지만 정상 resolve한다.

범용 OpenAPI Engine을 새로 만들려고 하지 않는다.

## Tool Description

가능하면 다음을 조합한다.

```text
summary
+
description
```

OpenAPI에 없는 업무 의미를 MCP에서 임의 생성하지 않는다.

---

# Phase 3 — Generic REST Executor

## 목표

Dynamic Tool 호출을 실제 AbleOps Backend REST 호출로 변환한다.

예:

```text
MCP

list_topics {
    id: "prod01"
}

        ↓

OpenAPI Registry

GET
/api/clusters/{id}/topics

        ↓

Generic Executor

GET /api/clusters/prod01/topics
Authorization: 기존 사용자 Backend Token
```

## 반드시 재사용

현재 MCP 서버의:

* Backend Token 처리
* 사용자 인증 Mapping
* HTTP Client
* timeout
* TLS/CA
* 오류 처리 기반

을 재사용한다.

새 인증체계를 만들지 않는다.

### Token 정책

Token은:

* MCP Tool argument에 포함하지 않는다.
* LLM에 노출하지 않는다.
* OpenAPI에 넣지 않는다.
* 로그에 출력하지 않는다.

기존 사용자별 Backend Session Token을 Executor가 사용한다.

---

## Request Binding

최소:

```text
path parameter
query parameter
```

를 지원한다.

path parameter는 URL escaping을 정확히 적용한다.

특히:

```text
topic name
consumer group name
principal
event id
```

등을 문자열 결합으로 직접 붙이지 말고 안전하게 처리한다.

---

## Response 처리

이번 단계에서는 Backend JSON을 과도하게 DTO로 재구성하지 않는다.

기본적으로 **구조를 보존해서 MCP Result로 반환**한다.

특히 다음 정보를 버리면 안 된다.

```text
status
partial
source
isSynthetic
error
warnings
availability/관측 상태 관련 필드
```

존재하는 경우 그대로 유지한다.

### 중요한 원칙

```text
HTTP 200
```

이라고 해서 Executor가:

```text
success=true
정상
이상 없음
```

같은 의미를 추가하지 않는다.

Backend 결과를 사실대로 전달한다.

---

# Phase 4 — Static/Dynamic 병행 검증

이번 사용량 창에서 가장 중요한 Phase다.

전체 28개 Tool을 Dynamic 전환하지 않는다.

우선 **단순 조회 Tool 5개 이내**를 선정해 실제 End-to-End 검증한다.

우선 후보:

```text
list_clusters
list_topics
get_topic
list_consumer_groups
list_events
```

실제 기존 Static Tool 이름과 충돌하는 경우 기존 이름을 덮어쓰지 않는다.

## 충돌 정책

초기에는:

```text
Static Tool 존재
       ↓
Static Tool 유지

Dynamic Tool
       ↓
shadow registry에 보관
```

형태가 좋다.

테스트 또는 debug 경로를 통해 두 구현을 비교한다.

동일 이름을 가진 Dynamic Tool로 Static Tool을 자동 교체하지 않는다.

---

# Dynamic Tool의 1차 범위

이번 작업의 최우선 성공 기준은 다음 Tool을 가능한 많이 처리하는 것이 아니라:

```text
OpenAPI
  ↓
Dynamic Tool 생성
  ↓
tools/list
  ↓
tools/call
  ↓
REST API
  ↓
MCP Result
```

이 경로를 **실제로 완성하는 것**이다.

최소 3개, 목표 5개 조회 Tool로 End-to-End 테스트한다.

---

# Phase 5 — ETag 기반 계약 변경 감지 기반만 추가

Upstream은 이미:

```text
ETag
If-None-Match
304 Not Modified
```

를 보장한다.

따라서 이번 작업에서는 복잡한 Hot Reload까지 만들지 말고 ETag 저장 기반만 준비한다.

Loader가 마지막 성공 응답의:

```text
ETag
```

를 보관할 수 있도록 한다.

재조회 시:

```text
If-None-Match
```

를 보낼 수 있도록 API를 설계한다.

결과:

```text
304
→ 기존 Registry 유지

200 + 새로운 ETag
→ 새로운 Contract 반환
```

까지 구현 가능하면 구현한다.

다만 이번 Phase에서는:

* background polling
* Registry live swap
* MCP tool list changed notification

까지 확장하지 않는다.

이 부분은 다음 사용량 창으로 남긴다.

---

# Last Known Good 정책

OpenAPI 장애 때문에 기존 MCP 서비스를 죽이면 안 된다.

다음 정책을 적용한다.

```text
서버 시작
   │
   ├─ OpenAPI 성공
   │      ↓
   │   Dynamic Registry 생성
   │
   └─ OpenAPI 실패
          ↓
       Dynamic 비활성
          ↓
       기존 Static Tool 정상 제공
```

실행 중 추후 갱신 기능을 추가할 때도:

```text
새 Contract 검증 실패
       ↓
현재 정상 Registry 유지
```

원칙을 지킬 수 있도록 구조를 설계한다.

---

# 이번 작업에서 하지 않을 것

다음은 명시적으로 제외한다.

### 1. 전체 28개 Dynamic Tool 전환

하지 않는다.

### 2. Static Tool 삭제

하지 않는다.

### 3. Write API

다음 Method는 Dynamic Executor가 실행하지 않는다.

```text
POST
PUT
PATCH
DELETE
```

### 4. Hot Reload 완성

이번에는 하지 않는다.

```text
주기 polling
runtime Registry atomic swap
notifications/tools/list_changed
```

은 후속 작업이다.

### 5. Backend 결함 수정

`ableops-kafka`의 알려진 6개 서버 결함은 이번 저장소에서 수정하거나 우회하지 않는다.

### 6. 범용 OpenAPI Gateway 개발

현재 AbleOps OpenAPI 계약을 안전하게 처리하는 데 필요한 수준까지만 구현한다.

---

# 테스트

실제 Backend 없이도 가능한 테스트를 충분히 만든다.

`httptest`를 이용하여 최소 다음을 검증한다.

## Loader

```text
200 + OpenAPI
304
invalid JSON
중복 operationId
x-mcp-enabled false
unsupported Operation
```

## Compiler

```text
camelCase → snake_case
path parameter
query parameter
required
optional
nullable
array
enum
```

## Executor

```text
path binding
query encoding
Authorization 전달
401
403
404
500
timeout
JSON response
```

## 안전성

```text
POST Operation 실행 거부
DELETE Operation 실행 거부
x-mcp-enabled=false 등록 거부
Token 로그/Result 노출 없음
```

---

# 검증

마무리 시 기존 검증 체계를 그대로 실행한다.

```powershell
.\scripts\test.ps1
```

또는 동등하게:

```text
go test ./...
go vet ./...
go build ./cmd/ableops-kafka-mcp
```

기존 stdio child-process MCP 통합 테스트가 있다면 반드시 유지한다.

---

# 사용량 관리

이번 5시간 창의 작업 비중:

```text
Phase 1 Loader/Parser           20%
Phase 2 Registry/Compiler       20%
Phase 3 Generic Executor        30%
Phase 4 E2E/Shadow 검증         20%
Phase 5 ETag 기반               10%
```

시간 또는 사용량이 부족하면 우선순위:

```text
1. Loader
2. Dynamic Registry
3. Generic GET Executor
4. 3개 이상 E2E Tool
5. 테스트
6. ETag/304
```

ETag 기능보다 End-to-End Dynamic Tool 동작을 우선한다.

---

# 이번 작업 종료 기준

다음 흐름이 실제 테스트로 입증되면 이번 작업을 종료한다.

```text
ableops-kafka
/openapi.json
       ↓

OpenAPI Loader
       ↓

x-mcp-enabled=true GET 발견
       ↓

Dynamic Tool Compiler
       ↓

MCP Tool Registry
       ↓

tools/list
       ↓

tools/call
       ↓

Generic REST Executor
       ↓

AbleOps REST API
       ↓

MCP Result
```

최소 3개, 목표 5개의 실제 AbleOps 조회 Operation으로 이를 검증한다.

기존 Static Tool은 그대로 정상 동작해야 한다.

---

# 최종 보고 형식

```text
완료 Phase:
- Phase 1:
- Phase 2:
- Phase 3:
- Phase 4:
- Phase 5:

OpenAPI:
- 조회:
- ETag/304:
- 지원 Schema:
- Unsupported 처리:

Dynamic Registry:
- 발견 Operation 수:
- 등록 가능 GET 수:
- 충돌 Static Tool 수:

E2E 검증:
- operationId / MCP Tool:
- ...

Executor:
- 인증:
- path/query binding:
- HTTP 오류:
- response 보존:

기존 기능 영향:
- Static Tool:
- stdio:
- HTTP:
- 인증:

의도적으로 하지 않은 작업:
- ...

발견한 문제:
- ...

검증:
- go test:
- go vet:
- go build:

다음 Phase 권장:
- ETag 기반 Contract refresh
- Atomic Registry swap
- MCP tool list changed notification
- 검증된 Static Tool의 단계적 Dynamic 전환
- x-mcp Metadata 확장 필요성 재평가
```

중요: 구현 과정에서 upstream OpenAPI 계약과 기존 MCP Static Tool이 다르게 동작하는 부분을 발견하면 **Dynamic 쪽에서 임의로 맞추지 말고 차이를 기록하고 원인을 분석한 뒤 보고**해줘.
