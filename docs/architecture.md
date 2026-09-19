# 아키텍처

```text
개인 MCP Client -- stdio --------------------> MCP Server --> AbleOps Backend
로컬 MCP Client -- HTTP / MCP Bearer --> 사용자 매핑 검증 -- 사용자별 Backend Bearer
                                                                  └ 세션·RBAC·Kafka·DB
```

## 역할과 의존성

- `cmd/ableops-kafka-mcp`: 환경 검증, stderr 구조화 로그, 시그널 취소와 stdio 수명 관리.
- `internal/config`: 명시적으로 지정한 YAML·기존 환경변수·CLI 덮어쓰기를 조합하고 origin URL, 명시적 loopback HTTP, timeout, 토큰 형식, 로그 수준 검증.
- `internal/ableops`: 공통 HTTP 클라이언트, 안전한 경로 인코딩, 인증 전달, 타입을 명시한 공개 DTO, HTTP 오류 분류. Static 도구용 응답은 DTO로만 해석하며 통째로 중계하지 않는다. 무인증 계약 조회(`FetchOpenAPI`)와 Dynamic 도구용 원문 GET(`GetRaw`)도 같은 전송 경계에 있다.
- `internal/openapi`: `/openapi.json`의 내부 모델·검증(Operation 단위 제외)과 ETag·Last Known Good 로더. 새 계약은 호출자 검증 뒤에만 커밋한다.
- `internal/dynamic`: operationId→도구 이름, 입력 스키마 컴파일, 노출 안전 게이트(SAFE/SHADOW/BLOCKED)와 배치(불변 Registry), 원문 전달 Executor, 실행 중 갱신·원자적 교체(Runtime), Static 전환 판정(compat).
- `internal/tools`: 도구별 입력/출력 Schema, DTO 변환, 부분 실패·잘림·출처·조회시각 표현, 도구 실행 로그.
- `internal/mcpserver`: 도구를 등록한 전송 독립 서버 생성. stdio 연결은 실행 진입점이 담당한다.
- `internal/mcpserver/stdio.go`: stdio 전송 실행과 프로세스 종료 context를 개별 요청에 연결한다. SDK 기본 stdio 수명만으로 진행 중 요청이 취소된다고 가정하지 않는다.

공식 MCP Go SDK v1.8.0을 고정하고, Managed Process Extension 이 쓰는 AbleOps SDK `extension/v1` v1.0.0 이 요구하는 Go 1.27.0을 사용한다. 기존 프로젝트의 모듈, Kafka 클라이언트, DB 드라이버, 정책 엔진을 가져오지 않는다. 로컬 `replace`, `go.work`, 별도 저장소, 공유 캐시, 수집 워커가 없다.

## OpenAPI 기반 Dynamic 도구

기본 비활성인 선택 기능이다. 켜면 흐름은 다음과 같다.

1. `cmd`가 `dynamic.Runtime`을 만들고, `mcpserver.New(WithRuntime)`가 Static 도구를 먼저 등록한 뒤 Runtime을 서버에 연결한다.
2. 전송을 시작하기 전에 최초 적재를 끝낸다.
3. 이후 goroutine이 주기적으로 갱신한다. 프로세스 종료 시 취소하고 끝날 때까지 기다린다.

SDK `AddTool`은 같은 이름을 조용히 교체한다. 그래서 Static 도구 이름은 Registry 배치와 등록 경계 두 곳에서 예약한다. 같은 이름의 Dynamic 도구와 SHADOW 분류 도구는 `NewDynamicComparison` 비교 서버에서만 호출할 수 있다. BLOCKED는 어디에도 등록하지 않는다.

```text
/openapi.json ─(무인증, If-None-Match)→ openapi.Loader.LoadValidated ─Parse→ Contract
    → dynamic.Build(Static 이름, 선택 목록, 노출 정책) → Registry{exposed, shadow, blocked, not_selected, skipped}
    → 검증 성공 시에만 계약·ETag 커밋 → Runtime.apply: Diff → [tools/list 쓰기 잠금] AddTool·RemoveTools → list_changed
tools/list → Runtime 미들웨어(읽기 잠금) → SDK 목록(교체 전 또는 후의 완전한 목록)
tools/call → SDK 입력 검증 → Executor(시작 시점 도구, 원본 인자 바인딩) → Client.GetRaw → Result{body 원문}
```

Dynamic 결과는 DTO 원칙의 **의도된 예외**다. Backend 응답을 해석·보정하지 않고 `body`에 원문으로 싣는다.

- **노출 결정**: upstream의 `x-mcp-enabled`, 운영자의 선택 목록, 이 저장소의 노출 정책표(SAFE만 기본 노출)가 함께 결정한다.
- **Static과 같은 경계**: 토큰 검사, 크기 제한, 오류 본문 비노출, 리다이렉트 차단, 호출 예산.
- **쓰기 메서드**: 컴파일과 실행 두 단계에서 거부한다.
- **적재·갱신 실패**: 마지막 정상 도구 목록을 유지한다. 최초 적재 실패이면 Static만으로 서비스하고 다음 주기에 다시 시도한다.

상세 규칙과 Static과의 차이는 [dynamic-mcp.md](dynamic-mcp.md)에 있다.

## 실행 설정

`--config`를 명시한 경우에만 YAML 파일을 읽는다. 기존 명시 CLI 플래그, 비어 있지 않은 환경변수, YAML, 기본값 순으로 메모리에서 조합하며 프로세스 환경은 바꾸지 않는다. YAML의 경로는 파일 디렉터리 기준이고 환경변수에서 온 경로는 기존 실행 디렉터리 기준이다. HTTP 서버와 등록/폐기 CLI는 같은 공개 설정 계약을 사용하며 token_env는 stdio/등록의 비밀 환경변수 참조다. HTTP에서는 공용 Backend 토큰을 읽지 않는다.

YAML 파서는 [공식 YAML Go 패키지](https://pkg.go.dev/go.yaml.in/yaml/v3)를 명시적 버전으로 고정하여 사용한다. 파일 64 KiB 상한과 엄격한 키·타입·단일 문서 검사를 적용하고 파서 원문을 외부 오류로 전달하지 않는다. 토큰 원문과 인증 매핑은 YAML에 포함하지 않으며 기존 소유자 전용 인증 저장소를 유지한다. 설정 변경은 프로세스 재시작 후 적용하고, 사용자별 인증 저장소의 매 요청 재조회와는 별개다. Dynamic 도구의 upstream 계약 변경은 설정이 아니므로 재시작 없이 갱신 주기에 따라 반영된다.

## 사용자와 클러스터 권한

stdio 프로세스는 한 사용자의 Backend 세션 토큰을 환경에서 읽는다. HTTP 프로세스는 공용 토큰을 읽지 않고 요청 context의 사용자별 위임 토큰만 사용한다. 도구 인자로 토큰을 받지 않으며 자동 로그인을 하지 않는다. 권한·세션이 바뀌면 다음 REST 요청에서 Backend가 다시 검사한다.

클러스터 대상 도구는 `cluster_id`가 필수다. 클러스터 목록 캐시를 권한 증명으로 사용하지 않는다. `readOnlyHint`는 클라이언트 안내 메타데이터이며 접근 통제가 아니다. 실제 통제는 Backend의 Bearer 인증 및 도구별 `topic.view`/`event.view`/`acl.view`, 신청의 `canSeeRequest` 객체 권한 검사다. 전역 이벤트·신청 상세는 Backend 객체 접근 통제를 확인한 경로만 사용하고 응답의 소속 클러스터를 추가 대조한다. 불일치하면 상세와 후속 조회를 차단한다. Backend 접근 권한 밖의 결과를 얻기 위한 기본 클러스터 API 우회는 없다.

일반 조회는 GET을 사용하고 ACL·Flink ACL·DDL 미리보기만 확인된 세 POST 경로를 사용한다. 임의 메서드·URL을 받지 않으며 저장·신청·반영·실행 API로 대체하지 않는다. 기존 Backend는 인증에 따라 세션 idle 시각을 갱신하고 목록 조회에서 lazy sync를 수행할 수 있다. 따라서 MCP의 조회 전용 범위는 Kafka 변경·신청·승인·명시적 동기화를 제공하지 않는다는 의미다. 기존 Backend의 내부 저장 부작용까지 없다고 보장하지 않는다.

## REST 통신 경계

`ABLEOPS_BASE_URL`은 `/api`가 없는 origin이며 클라이언트가 검증된 API 세그먼트를 붙인다. 사용자 입력은 각 경로 세그먼트로 인코딩한다. 임의 URL·메서드·헤더·API 경로 입력 도구는 없다. HTTPS와 인증서 검증을 기본으로 하고 사설 PEM CA를 추가할 수 있다. HTTP는 명시적 허용을 받은 loopback 주소에만 사용한다. 모든 3xx는 차단하여 리다이렉트 대상에 토큰을 전달하지 않는다.

REST 응답은 2 MiB, 동시 호출은 4개, timeout은 1~120초다. `ABLEOPS_REQUEST_TIMEOUT`(기본 15초)은 개별 REST와 여러 REST를 합친 도구 전체에 적용한다. 공통 실행 context는 하위 REST 호출 최대 8회의 안전 상한도 적용하며 각 도구는 문서에 정한 더 작은 고정 호출 수를 따른다. HTTP 요청 자체의 마감 시각이 더 빠르면 그 시각을 따른다. 요청 취소를 HTTP까지 전파하고 자동 재시도는 하지 않는다. 오류에는 안전한 코드·설명·HTTP 상태만 싣고 원문 응답/네트워크 오류는 노출하지 않는다. 백엔드 응답에 세션 토큰이 포함되면 응답을 거부한다. SDK 입력 오류가 잘못된 값을 인용할 수 있으므로 도구 이름·인자에 포함된 설정 토큰도 스키마 검증 전에 차단한다. 공개 DTO는 허용한 측정·설정 필드만 선언하며 임의 evidence/config/payload를 중계하지 않는다.

## 결과 의미와 크기

SDK 표준 `structuredContent`와 같은 내용의 text를 함께 반환한다. `queried_at`은 MCP 조회 시각으로만 기록한다. 원본의 관측 시각이 없는 결과에 임의 시각을 추가하지 않는다. 저장 스냅샷, Backend 실시간 어댑터 조회, 이벤트 저장소를 구분하며 실제 Kafka인지 mock인지는 백엔드가 제공하는 필드를 따른다.

HTTP 200의 FORBIDDEN/UNAVAILABLE/NOT_FOUND 등도 확인한다. 건강 상태는 두 API를 합치므로 한쪽만 실패하면 `partial`, 전체가 실패하면 `error`다. 조회 실패를 정상·0건·Lag 0으로 바꾸지 않으며 원인이나 Lag 임계값을 새로 판정하지 않는다. 그룹·토픽 스냅샷에 동기화 시각이 없으면 Backend가 초기 동기화 실패를 숨길 수 있다는 한계를 표시한다.

REST 수신 제한과 MCP 출력 제한은 독립이다. 기본 목록 50개/최대100개, 구조화 JSON 64 KiB, text 포함 `CallToolResult` 128 KiB를 적용한다. 항목이나 바이트 한계로 줄이면 `truncated`를 표시하며 원본 전체 목록을 받지 못한 것을 MCP 잘림과 혼동하지 않는다. 이벤트의 전체 건수와 페이지는 백엔드 값을 따른다.

## 로그와 감사

stdout에는 JSON-RPC만 기록한다. stderr JSON 로그에는 도구명, 클러스터, 소요 시간과 결과 코드를 남긴다. 원본 입력/응답, Authorization 헤더, 세션 토큰은 기록하지 않는다. SDK의 상세 프로토콜 로그는 활성화하지 않는다.

이 로그는 MCP 프로세스의 실행 진단용이다. 기존 Backend의 변경 신청·승인·반영 감사 기록과 별개이며 중앙 감사 저장소나 누락 없는 보안 감사로 간주할 수 없다. 조회마다 기존 Backend가 어떤 감사를 기록하는지는 Backend 구현에 따른다.

## 로컬 HTTP와 사용자 위임

공식 SDK v1.8.0의 `NewStreamableHTTPHandler`를 stateless로 사용한다. 초기화·버전 협상과 MCP 처리는 SDK가 담당하며 기존 도구 등록을 재사용한다. 인증용 세션 ID를 발급하지 않아 다른 사용자의 MCP 세션을 재사용하는 경로도 없다. HTTP 인증, Origin/Host, 크기·동시 호출·시간 제한과 종료 수명은 `internal/mcpserver/http.go`가 담당한다.

`internal/localauth`는 로컬 관리자 파일의 MCP 토큰 해시·audience·만료·폐기를 매 요청 확인한다. Backend용 opaque 세션은 별도 저장하며 `/api/me`로 매 요청 현재 사용자 매핑을 재검증한다. 사용자 ID나 `X-User`를 신뢰하지 않는다. Backend 클러스터 권한은 실제 조회 요청에서 최종 적용한다. 파일과 사용자/권한 결과를 장기 캐시하지 않는다. 갱신·폐기는 다음 요청부터 적용되며 이미 진행 중인 요청을 소급 취소하지 않는다.

`internal/requestctx`의 값은 요청에만 속한다. 공유 REST Client의 토큰을 덮어쓰지 않는다. `Config.RequireRequestCredentials`에서는 요청 자격증명 누락과 공용 토큰 설정을 거부한다. 토큰 해시의 audience는 이 MCP에 한정되며 MCP 토큰을 Backend로 전달하지 않는다. 로컬 인증 저장소의 Backend 세션은 평문 비밀이므로 OS 파일 권한이 보안 경계다.

HTTP 요청마다 새 추적 ID를 발급하고 응답 및 Backend 요청의 `X-Request-ID`에 연결한다. 도구 로그에는 검증된 사용자, 로컬 등록에 바인딩된 클라이언트 별칭, 도구, 클러스터, 시간, 결과 코드를 기록한다. 클라이언트 별칭은 토큰 등록 단위 식별자이며 소프트웨어의 신뢰성 증명이 아니다. Backend가 헤더를 감사 저장소에 기록하는 기능은 추가하지 않았다.

별도 인증 서버가 제공되지 않았으므로 OAuth discovery·protected resource metadata·issuer/audience/scope 검증은 구현했다고 주장하지 않는다. 공개 등록 API가 없는 로컬 검증 인증이며 임의 Bearer 설정 클라이언트만 지원한다. 교체 경계는 Authenticate와 Backend 자격증명 해석 함수다. 공용 서비스에는 표준 OAuth 보호 리소스와 사용자 동의·위임/교환, TLS, 중앙 감사 검증이 필요하다. [MCP 전송 규격](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)과 [인증 규격](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization)을 참조한다.

## 운영·보안 도구 확장

등록 도구는 총 33개다. 새 기능군 계약은 [모니터링](monitoring-tools.md)·[보안](security-tools.md)·[이벤트](event-tools.md)·[데이터](data-tools.md)로 분리한다. 전역 API의 클러스터 권한이 부족한 도구는 등록하지 않으며 사후 필터로 보완하지 않는다. 저장소 실패를 숨기는 API의 결과는 partial과 명시적 불확실성으로 보존한다.

샘플 정책은 Client 생성 시 복사한 불변의 허용 목록과 별도 기능 플래그다. 사용자 권한과 별개의 추가 제한이며 요청 context의 위임 인증을 계속 적용한다. 위치 메타데이터만 공개하고 응답 전송 256 KiB·5초의 추가 상한을 둔다. 일반 상세 GET은 null을 거부하지만 샘플 고정 경로의 null은 불확실한 빈 표본이다. DDL 생성 요청은 명시적 컬럼만 사용하며 반환된 컬럼 선언을 대조한 뒤 WITH 옵션 전체를 공개하지 않는다.

신규 도구의 감사·최초 스냅샷 저장은 readOnlyHint=false/idempotentHint=false로 표시한다. annotation은 서버 권한 검사 대신 사용할 수 없다. 기존 이벤트 상세의 무제한 Issue 확장은 이제 호출 없이 unsupported를 반환한다.

기존 `list_topics`, `list_consumer_groups`, `get_topic_detail`도 최초 자산 스냅샷 동기화·저장 가능성이 있어 v0.2.0에서 readOnly/idempotent hint를 false로 보완했습니다. 실제 업무 권한과 조회 동작은 유지합니다. 근거는 Backend `internal/server/clusters.go`, `cluster_scope.go`, `internal/clusters/sync.go`입니다.
