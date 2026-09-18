# OpenAPI 기반 Dynamic 도구

AbleOps Backend의 `GET /openapi.json` 계약에서 조회 도구를 만들어 기존 Static 도구 **옆에 추가**하는 기능이다.

- **1차**: `OpenAPI → Dynamic Tool 생성 → tools/list → tools/call → REST → MCP Result` 경로를 완성했다. [요청 문서](reference_docs/ableops-kafka-mcp-OpenAPI기반DynamicMCP-1차구현.md)
- **2차**: 실행 중 계약 갱신(Hot Reload), Registry 원자적 교체, 도구 변경 알림(`tools/list_changed`), 노출 안전 게이트, Static/Dynamic 호환성 판정을 더했다. [요청 문서](reference_docs/DynamicMCP2차운영안정화개발.md)
- **3차**: `get_event_summary`·`list_consumer_groups` 2개를 Stable Adapter로 Promotion했다. MCP 계약은 그대로 두고 REST 경로만 계약에서 가져온다. [요청 문서](reference_docs/DynamicMCP-3차.md)

핵심 규칙은 다음과 같다.

- **기본값은 비활성**이다. 켜지 않으면 기존 11개 Static 도구만 제공한다. 계약 조회도 주기 갱신도 하지 않는다.
- 켜면 기동할 때 계약을 한 번 읽고, 이후 `refresh_interval`(기본 5분)마다 ETag로 변경을 확인한다.
- `x-mcp-enabled: true`인 GET만 도구로 만든다. 그중 **설정에서 선택했고, 노출 안전 게이트가 SAFE로 분류한 것만** 기본 서버에 노출한다.
- Static 도구는 교체하거나 삭제하지 않는다. 같은 이름의 Dynamic 도구는 노출하지 않는다.
- 새 계약이 검증을 통과했을 때만 도구 목록을 바꾼다. 조회·검증에 실패하면 **마지막 정상 도구 목록(Last Known Good)**을 유지한다.

## 구조

```text
cmd/ableops-kafka-mcp      newDynamicRuntime → mcpserver.New(WithRuntime) → Refresh(최초) → go Run(주기)
internal/ableops           FetchOpenAPI: 무인증 계약 조회, ETag·304
                           GetRaw: 기존 GET 전송 경계 + 원문 보존(최상위 null·선언된 204 허용)
internal/openapi           Parse: 계약 모델·Operation 단위 Issue
                           Loader.LoadValidated: If-None-Match, 호출자 검증 뒤에만 계약·ETag 커밋
internal/dynamic           ToolName → Compile(정의·바인딩 지문) → ClassifyExposure → Build(불변 Registry)
                           Runtime: Refresh·Run·apply(Diff·원자적 교체)·tools/list 직렬화 미들웨어
                           Executor(원문 전달) · AssessCompatibility(Static 전환 판정)
internal/tools             DynamicSource 포트: 기존 도구가 operationId로 실행하는 Stable Adapter 경계
internal/mcpserver         New(..., WithRuntime(rt) | WithDynamic(registry)) / NewDynamicComparison(비교용)
                           Runtime.Adapter()를 tools.Register에 주입(Promotion)
```

의존 방향은 `mcpserver → dynamic → openapi → ableops` 한 방향이다. Dynamic 경로도 기존 REST Client를 그대로 쓴다. 따라서 다음이 모두 Static 경로와 같다.

- base URL 검증, TLS·사설 CA, timeout, 리다이렉트 차단
- 동시 호출 4개, 도구당 호출 예산
- 요청별 위임 토큰, 응답 안 토큰 검사

새 인증 체계는 없다.

## 설정

| YAML | 환경변수 | 기본값·의미 |
| --- | --- | --- |
| `dynamic_tools.enabled` | `ABLEOPS_DYNAMIC_TOOLS` | `false`. `true`일 때만 계약을 조회하고 주기 갱신을 시작한다. 환경변수는 `true`/`false`만 허용한다 |
| `dynamic_tools.operations` | `ABLEOPS_DYNAMIC_OPERATIONS`(쉼표 구분) | 생략하면 1차 파일럿 `listClusters, listTopics, getTopic, listConsumerGroups, listEvents`를 선택한다. YAML `[]`는 "선택 없음"이다. 최대 64개, lowerCamelCase, 중복 금지 |
| `dynamic_tools.refresh_interval` | `ABLEOPS_DYNAMIC_REFRESH_INTERVAL` | `5m`. Go duration 문자열, 허용 범위 `1m`~`24h`. 지나친 폴링을 막으려고 1분 미만은 거부한다 |

우선순위는 기존과 같다: 비어 있지 않은 환경변수 > YAML > 기본값. 세 환경변수 이름은 `auth.token_env`로 지정할 수 없다. 설정은 기동할 때 한 번만 읽는다. 갱신 주기를 포함한 세 값은 Dynamic이 꺼져 있어도 형식을 검증하므로, 잘못된 값이 있으면 기동하지 않는다(기존 `operations` 검증과 같다). 오류 문구에는 입력값을 넣지 않는다.

**기본 파일럿 선택으로는 기본 서버에 추가되는 도구가 없다.** 1차에서는 `get_topic`·`list_events`가 노출되었지만, 2차 노출 안전 게이트에서 5개가 모두 SHADOW 또는 BLOCKED로 분류되기 때문이다. 기존 설정 파일은 수정하지 않아도 기동하며, 선택하고도 노출하지 않은 도구는 stderr에 사유를 남긴다(`선택한 동적 도구를 노출하지 않음`). 현재 노출 가능한 Operation은 SAFE 3개이며, 그중 `getEventSummary`는 v0.2.0 Static 도구와 이름이 같아 실제로 추가되는 것은 `getBranding`·`getTopicPartitions` 2개다.

```powershell
$env:ABLEOPS_DYNAMIC_TOOLS = 'true'
# SAFE로 분류된 Operation만 기본 서버에 추가된다.
$env:ABLEOPS_DYNAMIC_OPERATIONS = 'getTopicPartitions,getEventSummary,getBranding'
# 선택 사항: 계약 변경 확인 주기(1m~24h)
$env:ABLEOPS_DYNAMIC_REFRESH_INTERVAL = '10m'
```

`x-mcp-enabled: false`인 Operation(`login`·`getClusterStorage`·`getAssetGraphReport`)을 선택해도 등록하지 않는다. 대신 stderr에 `unknown_selection` 경고를 남긴다.

## 계약 적재와 실행 중 갱신

- `GET {ABLEOPS_BASE_URL}/openapi.json`을 **Authorization 없이** 보낸다. HTTP 모드처럼 공용 토큰이 없어도 적재된다.
- 본문 상한은 4 MiB다(업무 응답 2 MiB와 별개이며, 현재 계약은 약 340 KiB). 3xx는 차단한다. 조건 없이 온 304와 200 이외의 2xx는 오류다. stdio 세션 토큰이 본문에 들어 있으면 거부한다.
- 최초 적재는 전송을 시작하기 **전에** 끝낸다. 그래서 첫 `tools/list`부터 반영된다. 적재는 기동을 최대 `ABLEOPS_REQUEST_TIMEOUT`(기본 15초)만큼 늦출 수 있다.
- 이후 `Runtime.Run`이 주기마다 `Runtime.Refresh`를 부른다. 갱신은 한 번에 하나만 실행한다.
  - 주기는 **이전 갱신이 끝난 시점부터** 센다. Backend가 제한시간까지 응답하지 않아도 조회를 쉬지 않고 이어 보내지 않는다. 계약 조회도 업무 GET과 같은 동시 호출 슬롯(4개)을 쓰기 때문이다.
  - 지터와 실패 백오프는 없다(주기 자체가 1분 이상이다).

```text
Timer → GET /openapi.json (If-None-Match: 서비스 중인 계약의 ETag)
  304            → 아무것도 바꾸지 않음(outcome=not_modified), 알림 없음
  200            → Parse(문서 검증) → Build(새 불변 Registry) → Diff → 원자적 교체
                     정의 변화 있음 → outcome=updated, tools/list_changed
                     정의 변화 없음 → outcome=unchanged(ETag만 바뀐 경우 포함), 알림 없음
  전송·검증 실패 → Last Known Good 유지(outcome=failed, code 기록), 알림 없음
```

### Last Known Good와 ETag 커밋

`Loader.LoadValidated`는 새 계약을 문서 검증과 호출자 검증(Registry 생성)이 **모두 성공한 뒤에만** 보관한다.

- 먼저 보관하면 안 되는 이유: Registry 생성에 실패한 계약의 ETag를 먼저 보관하면, 다음 조회가 그 ETag를 보내 304를 받는다. 그러면 고쳐지지 않은 계약에 영구히 머물고, 서비스 중인 도구와 보관한 계약이 어긋난다.
- 실패한 다음 조회는 **서비스 중인 계약의 ETag**로 확인하므로, 같은 새 계약을 다시 받아 검증한다.
- ETag가 없는 계약은 매번 전체를 받는다. 도구 정의가 같으면 알리지 않는다.

### 실패와 경계 사례

| 상황 | 동작 |
| --- | --- |
| 5xx·timeout·네트워크 오류 | 기존 도구 유지. `code`(`backend_unavailable`·`timeout` 등)만 기록한다 |
| JSON 오류·`servers` 위반·`paths` 없음·Operation 0개 | 계약 전체 거부(아래 표). 기존 도구 유지 |
| 일부 Operation이 지원되지 않음 | 그 Operation만 제외하고 경고한다(skip + warning). 나머지는 반영한다 |
| 전에 노출하던 Operation이 새 계약에서 지원되지 않게 됨 | **옛 정의로 남기지 않고 제거한다.** 옛 정의로 새 Backend를 호출하는 것은 추측이기 때문이다 |
| upstream이 Operation의 `x-mcp-enabled`를 `false`로 바꿈(전부 바꾼 경우 포함) | 회수로 보고 해당 도구를 제거한다. 계약이 구조상 유효하면 Last Known Good으로 노출을 붙잡지 않는다 |
| 최초 적재 실패 | Static 도구만으로 기동하고 다음 주기에 다시 시도한다. 성공하면 도구를 추가하고 알린다 |
| Registry 생성 실패 | 기존 도구와 ETag를 유지한다. 이 계약을 다음 주기에 다시 받는다 |

로그(stderr)에는 결과(`outcome`), 오류 코드, 추가·삭제·변경된 **도구 이름**, `etag_changed`만 남긴다. 계약 본문·ETag 값·응답 원문은 기록하지 않는다.

- 실패 로그: `동적 도구 계약 갱신 실패: 마지막 정상 도구 목록을 유지합니다`
- 최초 실패 로그: `동적 도구 계약 적재 실패: Static 도구만 제공하며 다음 갱신 주기에 다시 시도합니다`
- 304 로그: debug 수준

### 계약 전체를 거부하는 경우

다음 경우에는 계약 전체를 거부한다. 빈 계약으로 기존 도구를 비우지 않는다.

- JSON 오류, 뒤에 붙은 추가 JSON 값
- OpenAPI 3.0.x/3.1.x가 아닌 문서
- `servers[0].url`이 `"/"`가 아닌 경우(설정 밖 호스트로 토큰이 가지 않게 한다)
- `paths` 없음·빈 `paths`
- 사용할 수 있는 Operation이 0개인 경우

### Operation 하나만 제외하는 경우 (skip + 경고)

| 코드 | 조건 |
| --- | --- |
| `missing_operation_id` / `invalid_operation_id` / `duplicate_operation_id` | ID 없음 / `^[a-z][A-Za-z0-9]*$` 위반 / 중복. 중복이면 같은 ID를 **모두** 제외한다 |
| `missing_x_mcp_enabled` / `malformed_x_mcp_enabled` | 키 없음 / JSON 불리언이 아님(`"true"` 포함). 노출하지 않는다 |
| `unsupported_path` / `invalid_path_parameter` | `/api/` 밖 경로, 허용 외 문자 / 템플릿 변수와 path 파라미터 불일치, `required=false`, `v{n}` 같은 부분 변수 |
| `unsupported_parameter` / `unsupported_parameter_schema` / `duplicate_parameter` | header·cookie, `content`, form·simple 이외 style, `allowReserved` / 지원하지 않는 스키마 키워드·`oneOf`·object·해석 불가 `$ref` / 같은 위치의 중복 선언, path와 query의 동명 파라미터 |
| `unsupported_servers_override` / `unsupported_response` / `unsupported_path_item` | Operation·경로 단위 `servers` / `responses` 없음 / 경로 항목 `$ref` |
| `write_method_rejected` / `unsupported_method` | POST·PUT·PATCH·DELETE / HEAD 등 GET 이외 |
| `unsupported_request_body` / `sensitive_parameter` | 요청 본문이 있는 GET / `token`·`password`·`secret`·`credential`·`authorization`·`apikey`가 들어간 파라미터 이름 |
| `duplicate_tool_name` | 서로 다른 operationId가 같은 snake_case 이름이 됨(둘 다 제외) |
| `register_failed` | MCP SDK 등록 사전검사 실패. 버리는 서버에 먼저 등록해 보고, 실제 교체 중 panic으로 목록이 일부만 바뀌는 일을 막는다 |

2026-09-17 upstream 계약의 결과는 다음과 같다.

- 발견 31, `x-mcp-enabled=true` 28, 등록 가능 GET 28, 제외 0
- Static 이름 충돌 11: `get_asset_impact`, `get_cluster_health`, `get_consumer_group_lag`, `get_consumer_group_members`, `get_consumer_lag_overview`, `get_event_summary`, `list_cluster_events`, `list_clusters`, `list_consumer_groups`, `list_requests`, `list_topics`
  (v0.2.0에서 Static 도구가 33개로 늘며 `get_consumer_lag_overview`·`get_event_summary`·`list_requests` 3개가 새로 겹쳤다)

## Registry 원자적 교체

`Registry`는 계약 하나로 만든 **불변 스냅샷**이다. 서비스 중인 값은 `atomic.Pointer`로 읽는다.

MCP SDK v1.8.0에는 여러 도구를 한 번에 바꾸는 API가 없다. `AddTool`·`RemoveTools`는 호출마다 따로 잠근다. 그래서 다음 두 장치를 둔다.

- **`tools/list` 직렬화**: `Runtime`이 수신 미들웨어로 `tools/list`에 읽기 잠금을 건다. 도구 집합 교체 구간은 쓰기 잠금 안에서 실행한다. 따라서 `tools/list`는 **교체 전 또는 교체 후의 완전한 목록**만 본다. 일부 도구만 바뀐 목록은 보이지 않는다. 교체 구간은 네트워크 없이 SDK 등록만 하므로 짧다.
- **적용 순서**: 추가·정의 변경(`AddTool`, 같은 이름은 제자리 교체)을 먼저, 삭제(`RemoveTools` 한 번)를 나중에 한다. 두 계약에 모두 있는 도구가 교체 중 사라지는 순간이 없다.

`tools/call`은 잠그지 않는다. 다음 이유로 부분 상태나 data race가 생기지 않는다.

- SDK는 도구 하나의 입력 스키마와 핸들러를 한 항목으로 함께 교체한다.
- 핸들러는 호출을 시작할 때 읽은 도구로 끝까지 바인딩한다. 따라서 **교체 중 진행 중이던 호출은 이전 정의로 정상 종료**하고, 다음 호출부터 새 정의를 쓴다.
- 삭제된 도구를 부르면 SDK가 `unknown tool` 오류를 돌려준다.
- 두 계약에 모두 있는 도구는 교체 중에도 계속 호출된다(`TestRuntimeConcurrentSwap`이 `-race` 대상에서 확인한다).

정의가 같고 실행 바인딩만 바뀐 도구는 SDK를 건드리지 않고 실행 대상만 원자적으로 바꾼다. 실행 바인딩은 query `explode`·파라미터 위치·선언된 204처럼 `tools/list`에 드러나지 않는 값이다. 이 경우 알림이 없다(`Diff.Rebound`).

`Refresh`·`Install`·`Bind`는 뮤텍스로 직렬화한다. 더 복잡한 동시성 구조(복수 버전 보관, RCU 등)는 두지 않았다.

## 도구 변경 감지와 `tools/list_changed`

`Diff`는 기본 서버에 노출한 Dynamic 도구 집합을 비교한다.

- **Added / Removed**: 이름 기준
- **Changed**: `tools/list`로 보이는 정의 전체가 다른 경우다. 정의 전체는 이름, 설명, 입력 스키마, 출력 스키마, 주석, `_meta`의 operationId·메서드·경로를 정규화한 JSON이다.
- **Unchanged**: 위 정의가 같은 경우다. 그중 실행 바인딩만 바뀐 것은 `Rebound`로 따로 표시한다.

ETag, `info`, `x-mcp-note`, 응답 설명처럼 도구 정의에 쓰이지 않는 부분만 바뀌면 Changed가 아니다. 이때는 알리지 않는다.

**사용한 SDK API**: SDK v1.8.0에는 알림만 보내는 공개 API가 없다. 공식 경로는 `mcp.AddTool`(내부적으로 `(*Server).AddTool`)과 `(*Server).RemoveTools`다.

- 두 호출은 `changeAndNotify`를 거쳐 연결된 세션에 `notifications/tools/list_changed`(`ToolListChangedParams`)를 보낸다.
- `AddTool`은 같은 정의로 다시 넣어도 알림을 보낸다. 그래서 정의가 같은 도구는 다시 등록하지 않는다. 변경이 없으면(304·실패·ETag만 변경·정의 동일) 알림은 **0회**다.
- 교체 한 번은 추가·변경 도구마다 `AddTool` 한 번, 삭제가 있으면 `RemoveTools` 한 번을 부른다. SDK는 10ms 안의 연속 호출을 한 알림으로 묶으므로, **도구 목록이 바뀐 교체는 정상 조건에서 알림 1회**다.
- 1회는 **최선 노력**이다. SDK에 여러 도구를 한 번에 바꾸는 API가 없기 때문이다. CPU 경합으로 호출 사이가 10ms를 넘으면 알림이 여러 번 갈 수 있다. 상한은 그 교체의 SDK 변경 호출 수이고, 알림이 한 번도 가지 않는 경우는 없다. 클라이언트는 알림을 "목록을 다시 읽으라"는 신호로만 쓰면 되므로 중복 알림에도 결과는 같다. 테스트는 변경 시 "1회 이상, 호출 수 이하", 무변경 시 "0회"를 단언한다.
- 서버 capabilities는 SDK 기본값(`logging`)을 유지하면서 `tools.listChanged: true`를 명시한다.

전송·클라이언트별 전달 범위는 다음과 같다(SDK 소스와 실측 확인).

| 전송 | 클라이언트 | 알림 |
| --- | --- | --- |
| stdio | 2025-11-25 이하(legacy) | 구독 없이 받는다 |
| stdio | 2026-07-28(SDK 기본) | `ToolListChangedHandler`가 있으면 SDK가 `subscriptions/listen`으로 자동 구독한다. 연결 시점에 서버가 `tools.listChanged`를 광고해야 한다 |
| Streamable HTTP(stateless) | legacy | **받을 수 없다**(standalone GET 스트림 없음). 다음 `tools/list`에서 반영을 확인한다 |
| Streamable HTTP(stateless) | 2026-07-28 | `subscriptions/listen` 요청이 열린 동안만 받는다. 이 서버의 요청 제한 시간(기본 30초)이 지나면 끊기고 SDK 클라이언트는 다시 구독하지 않는다 |

HTTP 모드의 알림은 **최선 노력**이다. 클라이언트는 도구 호출이 `unknown tool`로 실패하거나 주기적으로 `tools/list`를 다시 조회해 반영한다. 요청 처리 중 교체가 일어나도 진행 중 응답은 깨지지 않는다(`TestDynamicRuntimeOverHTTP`).

SDK의 알려진 한계도 있다.

- legacy `initialize`에 `2026-07-28`을 요청한 클라이언트는 신규 세션으로 분류되어 알림을 받지 못한다.
- 디바운스 타이머와 변경이 경합하면 드물게 알림이 한 번 더 갈 수 있다.

## 노출 안전 게이트

upstream의 `x-mcp-enabled=true`는 "도구로 쓸 가치가 있다"는 판단이다. "원문 그대로 LLM에 넘겨도 안전하다"는 보장이 아니다. 실제로 upstream `x-mcp-note` 스스로 `listClusters`·`getCluster`에 "노출 측에서 username과 TLS 파일 경로 마스킹을 전제로 한다"고 적었다. Dynamic 도구는 응답을 마스킹·재구성하지 않는다. 그래서 노출 여부는 이 저장소의 명시적 정책표(`internal/dynamic/exposure.go`)로 따로 정한다. **잘못된 자동 마스킹보다 노출하지 않는 편을 택했고, 범용 Response Sanitizer는 만들지 않았다.**

| 분류 | 의미 | 배치 |
| --- | --- | --- |
| `SAFE` | 응답 필드 전체가 Static 공개 범위 안이고, 오류 원문·권한 우회·기본 클러스터 대체가 없다 | 선택했고 Static 이름 충돌이 없으면 기본 서버에 노출(`exposed`) |
| `SHADOW` | 실행은 가능하지만 Static 도구나 MCP 원칙과 입력·출력·공개 범위·의미가 다르다 | 기본 서버 비노출. 비교용 서버(`NewDynamicComparison`)에서만 실행(`shadow`) |
| `BLOCKED` | 민감정보 또는 명확한 안전 문제가 있다 | 어떤 서버에도 등록하지 않음(`blocked`) |

배치 우선순위는 `BLOCKED` > (Static 이름 충돌 또는 SAFE 아님 → `shadow`) > `exposed`다. 다음 경우도 노출하지 않는다.

- **정책표에 없는 Operation**: `SHADOW`(미분류)다. upstream이 새 Operation을 `x-mcp-enabled=true`로 추가해도, 이 저장소가 응답 공개 범위를 확인해 정책표에 넣기 전까지는 노출하지 않는다.
- **분류 값이 잘못된 경우**: `BLOCKED`로 본다.
- **SAFE 검토 이후 계약이 바뀐 경우**: SAFE 항목은 검토 당시의 GET 경로 템플릿과 파라미터 구성(위치·이름)을 함께 고정한다. 실행 중 갱신으로 둘 중 하나라도 달라지면 다시 검토할 때까지 `SHADOW`(`reason`: 검토 시점과 다름)로 내린다. 새 파라미터가 응답 공개 범위를 넓힐 수 있기 때문이다. 설명·제약·스키마 세부 변경은 SAFE를 유지하고 Changed로 알린다.

`TestExposurePolicyCoversUpstreamContract`는 픽스처의 28개가 모두 분류되었는지 강제한다.

분류 근거는 upstream 픽스처의 응답 스키마, kadmin 핸들러의 실제 직렬화(읽기 전용 확인), Static DTO가 버리는 필드를 대조해 정했다(2026-09-17).

### BLOCKED (7)

| operationId | 근거 |
| --- | --- |
| `listClusters` | `ClusterView`에 SASL `username`과 포털 서버의 TLS 파일 경로 4종(`tlsCaFile`·`tlsCertFile`·`tlsKeyFile`·`tlsTrustStoreFile`)이 평문으로 실린다. Static은 이 값을 버린다. 클러스터 ID 탐색은 Static `list_clusters`로 대체된다 |
| `getCluster` | 클러스터별 RBAC 게이트가 없다(인증만 통과하면 ID를 아는 누구나 조회). 응답은 `listClusters`와 같은 인증 설정 식별자를 싣는다 |
| `getClusterHealth` | 연결 실패 시 `error`에 어댑터 생성 오류 원문이 실린다. TLS 설정 오류이면 서버 파일 경로가 포함된다. Static은 고정 문구로 바꾼다 |
| `listDefaultClusterTopics` | 클러스터를 지정할 수 없는 기본 클러스터 대체 경로이고, 클러스터별 권한 판정도 없다(AGENTS.md의 `cluster_id` 필수 원칙 위반) |
| `getAssetGraph` | Principal 노드에 SCRAM 사용자명·자격증명 요약, 원본 ACL·Host, Kafka 오류 원문이 실린다. Static은 허용 속성만 남긴다 |
| `listRequests`, `getRequest` | 반영 실패 이력 `comment`에 어댑터 오류 원문(서버 TLS 파일 경로 포함 가능)과 신청 `payload` 전체가 실린다. 레거시 신청은 서버가 `default` 클러스터로 판정한다 |

### SHADOW (18)

| operationId | 근거 |
| --- | --- |
| `listTopics`, `getTopic` | Static이 버리거나 허용 목록 6개 키로 제한하는 토픽 `configs` 전체 맵을 기본으로 내보낸다 |
| `listConsumerGroups` | 같은 이름의 Static 도구가 있고, 입력 이름·출력 봉투·`syncedAt=null` 판정이 다르다 |
| `getConsumerGroupLag`, `getClusterPartitionHealth`, `getConsumerGroupState`, `getConsumerLagOverview` | 실패 상태의 `reasons`(·`partitions[].error`)에 Kafka·저장소 오류 원문이 실린다. Static은 내부 접속 정보가 섞일 수 있어 고정 문구로 바꾼다(Lag는 `clientHost`도 추가 노출) |
| `getConsumerGroupMembers` | Static은 경로 특수문자(`/ ; ,`)가 든 그룹명을 호출 전에 거부한다. 응답에 그룹 식별자가 없어 Dynamic은 대상 일치를 검증할 수 없다 |
| `getAssetImpact` | ACL 수집 실패가 응답에 드러나지 않는다. 그래서 '회수 안전' 같은 잘못된 안전 신호가 나갈 수 있다. Static은 그래프 오류를 먼저 확인한다 |
| `listConsumerGroupAlerts` | 관측 실패·권한 부족도 `200 + []`이고 관측 시각이 없어 '경보 없음'과 구분되지 않는다 |
| `searchAssetGraph` | 부분 문자열로 클러스터의 Principal과 SCRAM 자격증명 보유 계정을 열거할 수 있다. Static은 지정한 자산의 관계만 공개한다 |
| `listEvents`, `getEvent`, `listEventOccurrences`, `listClusterEvents` | Static이 허용 목록으로 줄이는 `evidence`와 실행 링크(`runbookUrl`·`deepLink`)·담당자 필드를 원문으로 내보낸다. 클러스터 소속 대조도 없다. Alertmanager 연동 설치에서는 `evidence`에 인프라 주소가 실릴 수 있다 |
| `getEventIssue` | Static이 버리는 Issue 멤버·조치 이력·운영자 자유 입력을 원문으로 내보낸다 |
| `getEventPlaybook` | Static이 제외하는 자리표시자 실행 명령·포털 경로·참고 링크를 내보낸다. 셸 권한이 있는 에이전트가 명령을 실행 지시로 오인할 수 있다 |
| `getCurrentUser` | 이메일·이름·부서 등 개인정보를 원문으로 내보낸다. 권한 목록이 클러스터별 부여를 반영하지 않아 사전 권한 판단을 오도한다 |

### SAFE (3)

| operationId | 근거 |
| --- | --- |
| `getBranding` | 미인증 공개 정보이며 자격증명·클러스터·자산 데이터가 없다 |
| `getEventSummary`(Static 이름 충돌로 비노출) | 집계 숫자와 조회 범위 메타만 있다. 권한 밖 필터(`scope.clusterDenied`)와 미수집(`collection.enabled`)을 본문에 명시한다. `clusterId`를 생략하면 권한 범위 전체 합산이며, 이 사실이 본문에 드러난다(기본 클러스터 대체 아님) |
| `getTopicPartitions` | 응답 필드가 Static `get_topic_detail`의 공개 범위 안이고 오류 원문이 없다. 클러스터별 권한 게이트가 적용된다. 호출마다 Kafka Metadata를 조회하므로 반복 호출을 절제한다 |

판단이 갈릴 수 있는 곳도 적어 둔다.

- `searchAssetGraph`: 필드 수준만 보면 SAFE 후보지만, 열거 가능성 때문에 SHADOW로 두었다.
- `getTopicPartitions`: Static이 이 호출을 옵트인으로 둔다는 점만으로는 공개 범위 차이로 보지 않았다.
- `getEventSummary`: `cluster_id` 필수 원칙을 모든 도구에 엄격히 적용한다면 SHADOW로 내려야 한다.

SAFE로 올리거나 내릴 때는 정책표, 이 표, `TestExposurePolicyCoversUpstreamContract`, 관련 E2E 단언을 함께 고친다.

## 도구 정의

- **이름**: 공통 함수 `ToolName`이 operationId를 snake_case로 바꾼다(`getConsumerGroupLag` → `get_consumer_group_lag`, `getHTTPStatus` → `get_http_status`).
- **설명**: OpenAPI `summary`와 `description`만 조합하고 최대 8 KiB로 자른다. 끝에 출처(`operationId · METHOD path`)와 "HTTP 200은 조회 성공·정상 판정이 아니다"라는 고정 문구 한 줄만 붙인다. `x-mcp-note`는 설명에 넣지 않는다.
- **입력 스키마**: path·query 파라미터를 하나의 객체로 모은다(`additionalProperties: false`). 인자 이름은 OpenAPI 이름을 그대로 쓴다(`id`, `name`, `clusterId`, `pageSize`).
  - 지원: string, integer, number, boolean, enum, 스칼라 원소 배열, nullable(3.1 `type` 배열과 3.0 `nullable`), required/optional, default, min/max, 길이, pattern, `minItems`/`maxItems`/`uniqueItems`.
  - `$ref`는 문서 내부 `components`만 해석한다. 참조한 쪽의 형제 키워드가 값을 덮어쓰며, 깊이 16에서 순환을 끊는다.
  - path 문자열에는 `minLength: 1`을 보탠다(빈 조각은 다른 경로를 호출하기 때문이다).
  - SDK와 같은 옵션으로 미리 Resolve한다. 실패(예: RE2가 지원하지 않는 pattern)하면 등록 시 panic 대신 그 Operation만 제외한다.
- **출력 스키마**: 아래 결과 봉투의 스키마다. `body`는 OpenAPI 응답 스키마로 제약하지 않는다. 응답 스키마는 계약 문서일 뿐 런타임 검증기가 아니다. nil 슬라이스나 열린 enum이 섞인 정상 응답이 클라이언트 검증에서 거부되면 안 된다.
- **메타**: `readOnlyHint`·`idempotentHint`를 붙이고, `_meta["ableops/openapi"]`에 operationId·method·path를 넣는다. `readOnlyHint`는 안내일 뿐 접근 통제가 아니다. 권한은 Backend가 매 요청 확인한다.

## 실행과 결과

1. SDK가 입력 스키마로 인자를 검증한다. 선언 밖 인자, 타입 오류, enum 밖 값은 REST 호출 없이 오류가 된다.
2. Executor는 SDK가 default를 채운 값이 아니라 **원본 인자**만 바인딩한다. 사용자가 주지 않은 default(`page`, `pageSize`, `sort` 등)는 보내지 않는다.
3. path 값은 **조각 단위로** 넘기고 Client가 각각 `PathEscape`한다. 그래서 토픽명·그룹명·Principal·이벤트 ID의 `/ ? % # 공백`도 한 조각으로 남는다. 공백만 있는 값, `.`·`..`, 개행은 호출 전에 거부한다.
4. query는 form 스타일로 보낸다. `explode: true`(기본)는 `a=1&a=2`, `false`는 `a=1,2`로 보내며, 이때 값에 콤마가 있으면 거부한다. 빈 배열과 `null`은 보내지 않는다.
5. GET을 한 번만 보내고 자동 재시도는 하지 않는다. 도구 전체 timeout과 호출 예산은 Static 도구와 같다.

결과(`structuredContent`와 같은 JSON의 text):

| 필드 | 의미 |
| --- | --- |
| `operation_id`, `method`, `path_template` | 호출한 계약. 경로에는 인자 값을 넣지 않는다 |
| `http_status` | 백엔드 HTTP 상태. 전송 전 실패나 네트워크 오류이면 없다 |
| `queried_at` | MCP 조회 완료 시각(원본 관측 시각이 아니다) |
| `body` | **백엔드 JSON 원문**(키 순서·큰 정수·`null`·`[]` 그대로). 재구성·보정하지 않는다 |
| `body_bytes` | 압축한 원문 크기 |
| `no_content` | 계약이 선언한 204(예: `getEventIssue`) |
| `error` | `code`·`message`·`http_status`. 기존 Static 도구와 같은 코드 체계 |
| `notes` | 해석 주의 고정 문구 |

- **HTTP 2xx이면 `isError=false`**다. 이것은 MCP 실행 성공일 뿐이다. `success`·`ok`·`healthy` 같은 판정 필드를 만들지 않는다. `status`·`partial`·`source`·`isSynthetic`·`error`·`warnings`·`failedBrokers`·`items: null`은 `body` 안에 있는 그대로 남는다.
- 백엔드 오류 상태는 `isError=true`와 아래 코드로 전달한다.
  - 401·403·404·429·5xx: `authentication_required`·`access_denied`·`not_found`·`rate_limited`·`backend_unavailable`
  - 400 등 그 밖의 상태: `backend_error`
  - 전송 실패: `timeout`·`canceled`·`backend_unavailable`
  - 원문이나 응답 결함: `invalid_response`·`response_too_large`
- **Backend 오류 본문 원문은 전달하지 않는다**(기존 경계). Backend가 404로 접은 오류도 그대로 404다.
- 최상위 `null`은 `body: null`로 보존한다.
- 계약이 204를 선언하지 않은 Operation에서 204가 오면 `invalid_response`다.
- 결과가 64 KiB(구조화)나 128 KiB(text 포함)를 넘으면 `body`를 **통째로 생략**하고 `output_too_large`를 반환한다. 원문을 잘라 부분 결과를 만들지 않는다. 필터·페이지 인자로 범위를 줄인다.
- stderr 로그에는 도구·operationId·클러스터(`/api/clusters/{x}`의 x 또는 `clusterId`)·사용자·요청 ID·소요 시간·HTTP 상태·결과 코드만 남긴다. 인자 원문과 응답은 기록하지 않는다.
- Static int64 반올림 결함(아래)과 달리 Dynamic 결과는 원문 JSON 숫자를 그대로 유지한다. 다만 SDK 클라이언트는 `structuredContent`를 `any`(float64)로 해석하므로 큰 정수가 **클라이언트 쪽에서** 반올림될 수 있다. 같은 JSON의 `text`에는 원문이 남는다.

## Stable Adapter Promotion (3차)

노출 판정(위 절)은 **Dynamic 도구를 그대로 LLM에 보여도 되는가**를 묻는다. Promotion은 다른 질문이다. **기존 MCP 도구 계약은 그대로 두고 내부 실행만 계약 기반으로 바꾼다.**

```text
기존 MCP Tool (이름·Input Schema·결과 봉투 그대로)
    ↓
Stable Adapter (internal/tools)
    ↓ operationId
Dynamic Registry (internal/dynamic)
    ↓ REST 경로·파라미터
Generic REST Executor (ableops.Client.GetRaw)
    ↓
AbleOps REST API
    ↓ 기존 공개 계약으로 Projection (ableops.Decode*)
기존 MCP 결과 계약
```

REST 경로는 어댑터에 하드코딩하지 않고 Registry에서 가져온다. 응답 원문은 그대로 내보내지 않고 Static과 **같은** 투영 함수를 지난다. 같은 이름의 Dynamic 도구를 따로 등록하지 않으므로 중복 노출도 없다.

의존 방향은 그대로다. `internal/tools`는 포트(`tools.DynamicSource`·`tools.ErrDynamicUnavailable`)만 정의하고 operationId만 알며 REST 경로는 모른다. 구현(`dynamic.Adapter`)은 `mcpserver.New`가 주입한다.

### 전환한 도구 (2개)

| MCP 도구 | operationId | 입력 변환 | 응답 투영 | 유지한 의미 |
| --- | --- | --- | --- | --- |
| `get_event_summary` | `getEventSummary` | `cluster_id` → query `clusterId` | `ableops.DecodeEventSummary` | `cluster_id` 필수(권한 범위 합산으로 넓히지 않음), `scope.clusterId` 대조, `clusterDenied`·허용 범위 0 → `access_denied`, `monitoredClusters=0` → `not_found`, 음수 집계 거부, `collection.enabled=false` → `partial`·`collection_disabled`, 기존 공개 필드와 제한 설명 |
| `list_consumer_groups` | `listConsumerGroups` | `cluster_id` → path `id` | `ableops.DecodeConsumerGroups` | `clusterId` 대조, `syncedAt=null` → `partial`, `topicLag` limit 절단, 기존 스냅샷 봉투·제한 설명 |

Registry 조회는 `LookupOperation`으로 하며 노출 선택 목록(`operations` 설정)이나 배치와 무관하다. Promotion은 도구를 새로 노출하지 않기 때문이다. 다만 노출 안전 게이트가 `BLOCKED`로 분류한 Operation은 어떤 경로로도 실행하지 않는다.

### Dynamic ON/OFF와 Fallback

| 상황 | 동작 |
| --- | --- |
| Dynamic OFF(`dynamic.enabled=false`) | 기존 Static 경로 그대로 |
| Dynamic ON, 계약 적재 성공 | 계약의 REST 경로로 실행 |
| 계약 미적재·`operationId` 부재·계약 파라미터 불일치·`BLOCKED` | Dynamic 인프라 사용 불가로 보고 기존 Static 경로 사용 |
| 백엔드 4xx / 5xx / timeout | 그대로 오류 반환. **Static으로 재호출하지 않는다**(호출 1회) |

비슷한 이름의 operationId를 추측하지 않는다. 계약에 없으면 Promotion을 쓰지 않는다.

REST 경로가 계약에서 바뀌어도 `operationId`가 같으면 MCP 도구 계약(이름·Input Schema·설명·주석)은 그대로이고 다음 조회부터 새 경로를 쓴다. Hot Reload 경로(`Runtime.Refresh`)를 그대로 쓰므로 별도 재기동이 필요 없다.

### 검증

`internal/mcpserver/promotion_test.go`

- `TestPromotedToolsKeepMCPContract`: Dynamic ON/OFF의 도구 정의가 완전히 같고, 도구 수가 Static과 같으며 중복 노출이 없다
- `TestPromotedToolsUseDynamicExecutor`: 계약 경로로 실행, `cluster_id` → `id` 변환, 공개 범위 밖 필드 미노출
- `TestPromotedToolsFollowContractPath`: 계약의 경로만 바꿔도 MCP 계약은 그대로이고 새 경로로 조회
- `TestPromotedToolsFallBackWhenOperationMissing`: `operationId` 부재 시 Static 경로
- `TestPromotedToolsDoNotRetryOnBackendFailure`: 401·403·404·500·timeout에서 백엔드 호출 1회
- `TestPromotedToolsUseStaticWhenDynamicOff`: Dynamic OFF에서 Static 경로

`internal/dynamic/adapter_test.go`는 계약 미적재·미상 operationId·`BLOCKED` 거부를 본다.

## Static/Dynamic 호환성 (전환 판정)

`dynamic.AssessCompatibility`는 Static 도구와 같은 이름의 Dynamic 도구를 비교한다. **교체는 하지 않고 교체 가능 여부만 판정한다.** 비교 방식은 두 가지다.

- **기계 비교**
  - 입력 스키마의 속성 이름·필수 여부·`type`, SDK가 검증에 쓰는 제약(`enum`·`const`·범위·배타 범위·`multipleOf`·길이·`pattern`·배열 제약), 추가 속성 허용. 설명·예시·`format`·기본값은 입력 검증 결과를 바꾸지 않아 비교하지 않는다.
  - 결과 봉투의 최상위 필드
  - Static이 호출하는 Operation 구성
  - `BLOCKED` 여부
- **선언 비교**: 공개 범위(Static이 제거·치환·절단하는 필드), Static이 붙이는 판정·검증, 보안 처리를 코드 근거에 따라 선언(`StaticProfiles`)한다. 선언이 없으면 의미 동등성을 추측하지 않고 `SEMANTIC_MISMATCH`로 둔다.

`TestStaticDynamicCompatibility`가 결과를 고정한다.

| 도구 | 판정 | 주요 차이 |
| --- | --- | --- |
| `list_clusters` | INPUT · OUTPUT · SEMANTIC · SECURITY | Static `limit` 입력 / 8개 필드만 공개 / BLOCKED(SASL 사용자명·TLS 경로) |
| `get_cluster_health` | INPUT · OUTPUT · SEMANTIC · SECURITY | `cluster_id`↔`id` / Static은 `getClusterHealth`+`getClusterPartitionHealth` 합성 / 오류 원문 치환, BLOCKED |
| `list_topics` | INPUT · OUTPUT · SEMANTIC | `cluster_id`·`limit`↔`id` / `configs` 제거 / `syncedAt=null`→`partial`, clusterId 대조 |
| `list_consumer_groups` | INPUT · OUTPUT · SEMANTIC | `cluster_id`↔`id` / `topicLag` 절단·봉투 / `syncedAt=null` 판정 (Static 큰 정수 반올림 결함 별도) |
| `get_consumer_group_lag` | INPUT · OUTPUT · SEMANTIC · SECURITY | `group_name`↔`name` / `clientHost` 제거 / `found=false`·FORBIDDEN 판정 / Kafka 오류 원문 치환 |
| `get_consumer_group_members` | INPUT · OUTPUT · SEMANTIC · SECURITY | 입력 이름 / 절단·해석 제한 / null·빈 memberId 거부 / 경로 특수문자 사전 거부 |
| `list_cluster_events` | INPUT · OUTPUT · SEMANTIC · SECURITY | `page_size`↔`pageSize`, Dynamic에만 있는 무시되는 `clusterId` 쿼리 등 / `evidence`·실행 링크 제거 / 항목 clusterId 대조 |
| `get_asset_impact` | INPUT · OUTPUT · SEMANTIC | `asset_type`·`asset_key`↔`type`·`key` / Static은 `getAssetGraph` 선조회 후 영향 요약 옵트인 / 그래프 오류로 `partial` 판정 |
| `get_event_summary` | INPUT · OUTPUT · SEMANTIC | Static은 `cluster_id` 필수 / EventSummary 허용 필드 / 수집 비활성을 `partial`로 판정 |
| `get_consumer_lag_overview` | INPUT · OUTPUT · SEMANTIC · SECURITY | 입력 이름·`limit` / 허용 필드·절단 / 관측·경보 상태로 `partial` 판정 / 실패 `reasons` 원문 치환 |
| `list_requests` | INPUT · OUTPUT · SEMANTIC · SECURITY | Static은 `cluster_id` 필수(Dynamic은 `cluster` 선택) / payload는 대상 식별자만 / 완전성 미확정 표시 / BLOCKED |

**현재 COMPATIBLE은 0개다**(충돌 11개 전부). 후속 단계에서 Dynamic으로 전환할 수 있는 Static 도구는 없다. 전환 후보가 되려면 다음이 모두 필요하다.

- 입력 호환: 이름 별칭 계층 또는 upstream 메타데이터
- 출력 공개 범위 호환: upstream 제외 필드 선언 또는 검증된 필터
- 보안 호환
- 업무 의미 호환: 판정 필드의 위치 선언
- 실제 Backend E2E

## Static과 Dynamic의 차이 (비교 서버 결과)

`mcpserver.NewDynamicComparison`은 선택한 Dynamic 도구 중 exposed + shadow만 등록한 비교용 서버다. BLOCKED는 등록하지 않는다. `internal/mcpserver/dynamic_test.go`의 `TestDynamicShadowComparison`은 같은 합성 Backend에 Static 서버와 이 서버를 함께 붙인다. 두 구현이 **같은 REST 경로를 같은 인증으로** 호출하는지 보고, 아래 차이를 단언으로 고정한다. 요청서 원칙대로 차이를 Dynamic 쪽에서 맞추지 않았다.

| 항목 | Static 도구 | Dynamic 도구 | 원인·판단 |
| --- | --- | --- | --- |
| 입력 이름 | `cluster_id`, `group_name` | `id`, `name` 등 OpenAPI 이름 | Dynamic은 계약 이름을 그대로 쓴다. 같은 도구 이름이라도 입력 계약이 달라 자동 교체할 수 없다 |
| 공개 범위 | DTO 허용 필드만(인증 설정·토픽 `configs`·이벤트 `evidence` 제외) | upstream 응답 원문 전체 | 원문 전달 자체는 유지하고, 민감 필드가 있는 Operation은 노출 안전 게이트로 막는다. `listClusters`는 BLOCKED라 비교 서버에도 없다(실행기로 원문에 SASL 사용자명이 실리는지만 확인) |
| `syncedAt: null` | `status=partial`과 제한 설명 추가 | 판정 없이 `null` 보존 | Dynamic은 업무 의미를 만들지 않는다 |
| 응답 검증 | 클러스터 ID 대조, ProbeStatus enum 검증 후 불일치면 `invalid_response` | 검증하지 않음(원문 전달) | 응답 스키마는 검증기가 아니라는 계약 해석 원칙을 따른다 |
| 큰 목록 | `limit`·`truncated`로 항목을 줄임 | 64 KiB를 넘으면 `output_too_large` | 원문 구조를 재구성하지 않는다 |
| 2^53을 넘는 정수 | **반올림된다**(예: Lag 9007199254740993 → …992) | 원문 유지 | 기존 결함이며 이번 범위에서 수정하지 않았다. SDK 제네릭 `AddTool`이 출력 스키마 default를 적용하면서 결과를 float64로 해석했다가 다시 직렬화한다 |
| 경로 특수문자 | 멤버 도구는 `/`·`;`·`,`를 호출 전에 거부 | 인코딩해서 전송 | Backend의 percent-decoding 계약이 확정되지 않았다([roadmap](roadmap.md)) |
| 오류 본문 | 미전달 | 미전달 | 기존 경계를 유지한다 |

## 실제 Backend 검증

`scripts/verify-backend.ps1`은 기존 `TestBackendLive`와 함께 `TestDynamicBackendLive`를 실행한다(`ABLEOPS_VERIFY_BACKEND=1`일 때만). 확인 항목은 다음과 같다. 응답 원문·토큰·실제 식별자는 출력하지 않는다.

- 무인증 `/openapi.json` 조회와 검증, ETag 수신, `If-None-Match` 304
- 정책표에 없는 upstream Operation과 SAFE 검토 고정값이 어긋난 Operation(operationId만 기록)
- SAFE 전체를 선택한 Runtime의 최초 적재와 재조회 304
- `tools/list`(Static 11 + 게시한 Dynamic)
- `tools/call`: `get_branding`, `get_event_summary`(Dynamic 비노출이므로 Static 도구가 응답), `get_topic_partitions`(`ABLEOPS_VERIFY_CLUSTER_ID`·`ABLEOPS_VERIFY_TOPIC_NAME` 필요)
- 인증 전달: 토큰이 없으면 합성 무효 토큰으로 401까지 확인한다

2026-09-17 결과는 [verification.md](verification.md#dynamic-mcp-2차-검증)에 있다.

## upstream OpenAPI에 필요한 메타데이터 (제안)

이 저장소에서 규격을 확정하지 않았다. 아래는 제안이며, upstream(`ableops-kafka`)이 계약을 확장한 뒤에 반영한다. 목적은 정책표의 수작업 판정을 기계 판독으로 바꾸고, SHADOW 일부를 호환 계층과 함께 전환 후보로 올리는 것이다.

```yaml
x-mcp:
  enabled: true
  exposure: safe | restricted | blocked      # 원문 그대로 LLM에 넘겨도 되는지(현재 x-mcp-enabled와 분리)
  response:
    exclude: [username, tlsCaFile, ...]       # 원문 전달 시 반드시 뺄 필드 경로
    rawErrorFields: [reasons, partitions[].error]  # 오류 원문이 실릴 수 있는 필드
    status: {field: status, partial: partial}  # 판별 필드 위치(200 ≠ 정상)
    emptyMeansUnknown: true                    # 빈 배열이 '없음'을 보장하지 않음
    observedAt: checkedAt                      # 원본 관측 시각 필드
  scope:
    cluster: path:id | query:clusterId | none   # 클러스터 대상 인자와 기본 클러스터 대체 여부
    rbac: cluster | global | none              # 권한 게이트 수준(getCluster 비대칭 표시)
  sideEffects: [lazySync, liveKafka, auditOnDeny]
  parameterAliases: {id: cluster_id, name: group_name}   # Static 입력과의 호환 별칭
```

## 운영 주의

- Dynamic을 켜는 것은 SAFE Operation의 응답 원문 공개를 받아들인다는 뜻이다. 선택 목록은 필요한 Operation으로 좁힌다.
- 계약 변경은 다음 갱신 주기(기본 5분) 안에 반영된다. 설정(`dynamic_tools.*`) 변경은 여전히 재시작해야 반영된다.
- upstream이 새 Operation을 추가해도 정책표에 분류하기 전까지는 노출되지 않는다(`exposure_SHADOW`, `reason`에 미분류 표시).
- 픽스처(`internal/openapi/testdata/ableops-openapi.json`)를 다시 만들면 개수 단언과 정책표 테스트를 함께 확인한다([api-mapping.md](api-mapping.md#테스트-픽스처)).
- stdout은 여전히 JSON-RPC 전용이다. 적재 요약·제외·노출 제외·갱신 결과 로그는 stderr에 남는다.
