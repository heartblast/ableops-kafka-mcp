# 구현 검증 기록

## PowerShell 스크립트 인코딩 규칙 고정

검증일: 2026-09-22. 환경은 **Windows 11, Go 1.27.1, Windows PowerShell 5.1**이다.

`.\scripts\build-extension.ps1` 실행이 구문 오류로 실패했다. 원인은 스크립트 로직이 아니라 파일 인코딩이다. 해당 파일이 BOM 없는 UTF-8로 저장되어 있었고, Windows PowerShell 5.1은 BOM이 없는 `.ps1`을 시스템 ANSI 코드페이지(이 환경은 cp949)로 읽는다. 한글 주석·문자열의 바이트가 깨지면서 그 안에 `(`, `)`, `"`가 섞여 들어갔고 파서가 문자열 종결자를 찾지 못해 연쇄 구문 오류가 발생했다. 같은 문제는 `auth-init.ps1`에서 이미 한 번 확인된 적이 있다.

비ASCII 문자가 있는 `scripts/build-extension.ps1`, `scripts/build.ps1`, `scripts/verify-backend.ps1`에 UTF-8 BOM을 추가했다. `scripts/test.ps1`은 ASCII 전용이라 변경하지 않았다. 재발 방지를 위해 세 곳에 규칙을 고정했다.

- `.editorconfig`: `[*.ps1]`에 `charset = utf-8-bom`, `end_of_line = crlf`를 지정해 편집기 저장 시점에 BOM을 유지한다.
- `.gitattributes`: `*.ps1 text eol=crlf`로 줄바꿈만 고정한다. 인코딩 변환(`working-tree-encoding`)은 걸지 않았다 — Git 2.53에서 `UTF-8BOM` 지정 시 `failed to encode ... from UTF-8BOM to UTF-8`로 add 자체가 실패했다. 변환 없이 두면 BOM 바이트가 내용 그대로 보존된다.
- `.github/workflows/ci.yml`: checkout 직후 Linux 러너에서 추적 중인 `.ps1` 중 비ASCII 문자가 있는데 BOM이 없는 파일을 찾아 실패시킨다.

| 검증 | 이번 실행 결과 |
| --- | --- |
| `Parser::ParseFile`(`scripts/build-extension.ps1`) | 구문 오류 0건 |
| `.\scripts\build-extension.ps1` | 통과. linux/amd64·windows/amd64 교차 컴파일, 스테이지 규칙·패키지 검증까지 완료 |
| BOM 라운드트립(`git add` → 삭제 → `git checkout`) | index blob과 체크아웃 결과 모두 `efbbbf` 유지 |
| CI 검사 스크립트 로직 | 현재 트리에서 통과. `build.ps1`의 BOM을 임시로 제거하면 해당 파일을 지목하며 실패 |
| `go test ./...`, `go vet ./...`, `go build ./cmd/ableops-kafka-mcp` | 통과 |

Go 소스는 변경하지 않았다. 패키징 산출물 `dist/extensions/ableops-kafka-mcp_0.6.0.ableops-ext`는 이번 실행으로 다시 생성되었다.

## v0.2.0 운영·보안·데이터 확장 검증

검증일: 2026-09-16. 현재 환경은 **macOS arm64, Go 1.26.5**다. 아래 Windows/다른 Go 버전 결과는 이전 작업의 이력이며 이번 실행 결과로 해석하지 않는다.

[요구 문서](reference_docs/운영보안-데이터연계용MCP도구추가.md)를 기준으로 도구 22개를 추가해 총 33개를 등록했다. 기존 이벤트 목록 필터를 확장하고, 기존 이벤트 상세의 무제한 Issue 확장을 호출 없이 unsupported로 변경했다. 스냅샷 저장 가능성이 있는 기존 토픽 목록·그룹 목록·토픽 상세의 annotation도 보완했다. Backend 참조 경로·HEAD·변경 여부는 [API 매핑](api-mapping.md)에 기록했다.

| 검증 | 이번 실행 결과 |
| --- | --- |
| `go test ./...` | 통과. 공식 SDK의 전체 33개 발견·엄격한 입력/출력 Schema, 기존 stdio 자식 프로세스·YAML·HTTP·인증 회귀 포함 |
| `go vet ./...` | 통과 |
| `go build ./cmd/ableops-kafka-mcp` | 통과 |
| `go test -race ./internal/ableops ./internal/tools ./internal/config ./internal/localauth ./internal/mcpserver` | 통과. 공유 REST 클라이언트·사용자별 자격증명·도구·설정·인증 동시성 검증 |
| 기능군별 httptest·SDK | Lag/모니터링 6개, 보안/신청 6개, 이벤트 5개, 데이터 5개 통과 |
| HTTP 공식 SDK | 33개 발견, 기존 11개와 신규 GET/POST 대표 4개 호출 및 요청별 위임 인증·추적 ID·토큰 비노출 검증 |
| 샘플·설정 | 기본 비활성·정확 허용 토픽·최대 100건·256 KiB·취소·위치 정밀도·빈 null·본문/키/헤더 제외·YAML/환경 우선순위 통과 |
| 미리보기 | 고정된 POST 3경로만 호출, 변경/신청/복원/배포/SQL 실행 미호출, ACL 미관측·정책 실패, DDL 인증 옵션 제외와 정적 오류 상태 보존 통과 |
| 결과 의미 | HTTP200 업무 실패·부분 실패·demo·unknown·단위·시각·상속 false/0·미평가 주의도·페이지/출력 잘림·권한 거부 0건·다른 객체 소속 차단 통과 |
| 저장소 오류 은닉 | 신청·백업·정책 등 오류 표지가 부족한 결과를 partial로 유지. 빈 목록을 정상 0건으로 확정하지 않는 회귀 통과 |
| 포맷·패치 | `gofmt -l cmd internal` 출력 없음, `git diff --check` 통과 |

처음에는 샌드박스의 Go 모듈 캐시·로컬 포트 바인딩 제한으로 테스트가 실행되지 않았다. 의존성 다운로드와 로컬 httptest 실행을 허용받고 `GOCACHE=/private/tmp/ableops-mcp-go-cache`로 위 검증을 완료했다. 테스트 실패 과정에서 기존 HTTP 테스트의 11개 도구 가정도 현재 목록·대표 호출에 맞게 갱신했다.

추가 CI 설정은 Windows/Linux에 macOS를 더하고 race 대상에 도구·설정 패키지를 포함한다. 원격 CI 결과와 로컬 결과는 구분한다. 배포 스크립트나 실제 서비스 재시작·배포는 이번 작업에서 실행하지 않았다.

### 실연동·보류·클라이언트 후속

`ABLEOPS_VERIFY_BACKEND`, 실제 `ABLEOPS_API_TOKEN`, 대상 `ABLEOPS_VERIFY_CLUSTER_ID`가 제공되지 않아 운영 Backend 실연동은 수행하지 않았다. 기본 opt-in 통합 테스트는 SKIP이며 합성 httptest 성공을 실제 Kafka/DB 연동 성공으로 보고하지 않는다. Backend 저장소는 종료 확인 시에도 같은 HEAD와 clean 상태다.

Backend 계약 때문에 보류한 12개는 접근 재검토·감사검색, 카탈로그·거버넌스 리포트, Flink 작업 목록/상세·파이프라인 목록/상세·ES 검사, Issue 상세·알림 발송 이력, 기존 복원 계획 조회다. 상세 근거와 재개 요건은 [보안](security-tools.md), [이벤트](event-tools.md), [데이터](data-tools.md)에 있다. 전역 API 사후 필터, 새 복원 계획 생성, 기본 클러스터 대체로 우회하지 않았다.

MCP 클라이언트는 신규 22개 도구 허용목록, 이벤트 필터, 선택 Issue 미지원 처리와 입력 Schema를 갱신해야 한다. `partial/errors/limitations/truncated`, 원본 업무 상태·단위·관측 시각·demo/unknown과 실제 annotation을 보존해야 하며 메시지 원문·실행 가능한 전체 DDL을 기대하면 안 된다. 해당 클라이언트는 수정하지 않았다.

## 이전 검증 이력

검증일: 2026-09-16. 환경: Windows amd64 / PowerShell, Git 2.53.0.windows.1.

## OpenAPI 기반 Dynamic MCP 1차 검증

검증일: 2026-09-17. 환경: Windows amd64, Go 1.27.1(go.mod 최소 1.25.0), MCP Go SDK v1.8.0. 요청 문서는 [reference_docs](reference_docs/ableops-kafka-mcp-OpenAPI기반DynamicMCP-1차구현.md), 설계와 차이 기록은 [dynamic-mcp.md](dynamic-mcp.md)에 있다.

> 이 절은 1차 당시의 기록이다. 2차에서 노출 안전 게이트가 생겨 기본 파일럿의 노출 수(2→0)와 E2E 대상 도구가 바뀌었다. 현재 기준은 아래 [2차 검증](#dynamic-mcp-2차-검증)을 본다.

upstream 계약은 kadmin 로컬 작업본(HEAD `6aead42`, OpenAPI 구현 미커밋)에서 만든 `/openapi.json` 스냅샷을 픽스처로 썼다. 출처와 생성 방법은 [api-mapping.md](api-mapping.md#openapi-계약과-dynamic-도구)에 적었다. kadmin의 소스·설정·Git 상태는 바꾸지 않았고, 테스트와 서비스도 실행하지 않았다. 실제 Backend·Kafka·DB에는 접속하지 않았다.

| 검증 대상 | 합성 `httptest`·공식 SDK 결과 |
| --- | --- |
| 계약 조회 | 통과. 무인증 GET, ETag 보관, `If-None-Match`→304, 조건 없는 304·204·3xx·401·5xx·크기 초과·토큰 포함·잘못된 ETag 거부, timeout, HTTP 모드(공용 토큰 없음) 적재 |
| Loader | 통과. 200→304→새 ETag 200→검증 실패·JSON 오류·5xx 때 Last Known Good 유지→복구 후 304. ETag 없는 계약은 매번 무조건 조회 |
| 계약 파서 | 통과. 실제 계약 31/28/제외 0. 문서 수준 거부(JSON·버전·servers·paths·Operation 0), Operation 단위 제외 20종(중복 ID는 모두 제외), 3.0/3.1 nullable, `$ref` 형제 덮어쓰기·순환·외부 참조 |
| Compiler·이름 | 통과. snake_case 변환(약어·숫자), path/query·required/optional·nullable·array·enum·default, 실제 JSON Schema 검증기 동작, POST·PUT·PATCH·DELETE·HEAD·본문·비JSON·토큰 이름·RE2 불가 pattern·잘못된 default 거부, 실제 28개 전부 컴파일 |
| Registry | 통과. 실제 계약 기준 노출 2·shadow 3·Static 충돌 8, 선택 목록·빈 목록·`x-mcp-enabled=false` 선택 경고, 이름 충돌 제외, 등록 경계에서 Static 이름 재차 거부 |
| Executor | 통과. 조각 단위 인코딩(공백·`/`·`?`·`%`·`#`·한글·`;`·`,`·Principal), `..` 거부, 반복 query·주지 않은 default 미전송, Authorization·위임 토큰, 401/403/404/400/429/500/503, timeout, 원문 보존(키 순서·2^53 초과 정수·`null`·판정 필드 미추가), 선언된 204만 허용, 128 KiB 초과 시 본문 전체 생략, 응답 안 토큰 거부, 로그 비노출 |
| E2E(in-memory) | 통과. `/openapi.json`→Registry→`tools/list`(Static 11 정의 불변 + 2)→`tools/call`(`get_topic`, `list_events`)→REST→Result. 인자 토큰 차단, 적재 실패 3종에서 Static 11 유지 |
| Static/Dynamic 병행 | 통과. 비교 서버에서 `list_clusters`·`list_topics`·`list_consumer_groups`·`get_topic`·`list_events` 5개 호출. 같은 REST 경로·인증 확인, 차이(입력 이름·공개 범위·`partial` 판정·큰 정수)를 단언으로 고정 |
| HTTP 전송 | 통과. SDK Streamable HTTP로 사용자 A/B 동시 호출, 요청별 위임 토큰·추적 ID, 권한 거부 분리, 로그 비노출 |
| 실제 stdio 자식 프로세스 | 통과. `ABLEOPS_DYNAMIC_TOOLS=true`로 13개 발견·호출, `ABLEOPS_DYNAMIC_OPERATIONS`로 12개, 계약 503일 때 경고 후 11개. stdout JSON-RPC 전용, 토큰·응답 원문 로그 비노출 |
| 설정 | 통과. 기본 비활성, 환경변수>YAML>기본값, 빈 목록 의미, 잘못된 값 거부(값 비노출), 토큰 변수 이름 예약, 등록 CLI 호환, 공개 예제 YAML 로드 |

새 테스트 함수는 47개다. 기존 테스트 파일은 수정하지 않았다(기존 stdio·HTTP·도구 수 11개 단언 그대로 통과).

`go test -count=1 ./...`, `go vet ./...`, `go mod verify`, `go build ./cmd/ableops-kafka-mcp`, `go build ./cmd/ableops-mcp-auth`, `scripts/test.ps1`이 모두 통과했다. gofmt 미적용 파일은 없다. CI의 `-race` 대상에 `internal/openapi`, `internal/dynamic`을 추가했다. 다만 이 Windows 환경에는 cgo용 gcc가 없어 race 검사는 **로컬에서 실행하지 못했다**(CI Linux에서 실행된다). 배포 빌드 스크립트(`build.ps1`/`build.sh`)는 이번에 다시 실행하지 않았다. 루트의 `ableops-kafka-mcp.exe`·`ableops-mcp-auth.exe`는 검증 빌드로 갱신되었고, 기본 설정에서는 Dynamic이 꺼져 있다.

미검증: 실제 AbleOps Backend의 `/openapi.json`과 Dynamic 도구 실연동(토큰·대상 미제공), 대형 클러스터에서의 64 KiB 초과 빈도, 인코딩된 `/`가 든 경로의 Backend 해석.

## Dynamic MCP 2차 검증

검증일: 2026-09-17(구현·검증) / 2026-09-18(v0.2.0 통합 후 재검증). 환경: Windows amd64, Go 1.27.1(go.mod 최소 1.25.0), MCP Go SDK v1.8.0. 요청 문서는 [reference_docs](reference_docs/DynamicMCP2차운영안정화개발.md), 설계는 [dynamic-mcp.md](dynamic-mcp.md)에 있다. kadmin의 소스·설정·Git 상태는 바꾸지 않았고 kadmin 서비스를 실행하지 않았다.

이 작업은 Static 도구 11개 시점에 구현했고, 이후 원격 `main`의 v0.2.0(운영·보안·데이터 도구 22개 추가, 총 33개) 위로 rebase해 재검증했다. 통합에서 바뀐 것은 다음과 같다.

- Static 이름 충돌 8 → 11(`get_consumer_lag_overview`·`get_event_summary`·`list_requests` 추가). 세 도구의 호환성 선언을 새로 작성했다.
- SAFE 3개 중 `getEventSummary`가 Static 이름과 겹쳐 기본 노출은 2개(`get_branding`·`get_topic_partitions`)다.
- 도구 이름 예약은 v0.2.0이 도입한 `registerWithAnnotations` 한 곳에서 모으므로 신규 22개도 자동으로 보호된다.
- v0.2.0의 일부 Static 도구는 스냅샷 부작용 때문에 `readOnlyHint=false`다. 프로세스 테스트의 도구 정의 점검을 Dynamic 도구에만 적용하도록 고쳤다.

| 검증 대상 | 결과 |
| --- | --- |
| Runtime Refresh | 통과. 200 새 계약, 304(Registry 포인터 유지·재생성 없음), ETag만 변경(알림 없음), timeout, 500, invalid JSON, invalid contract(`servers`), 빈 `paths`, 빌드 실패. 실패 뒤에도 서비스 중 계약의 ETag로 조건부 조회. ETag 없는 계약은 매번 전체 조회. 최초 적재 실패 후 다음 갱신에서 복구 |
| Registry Swap | 통과. 추가·삭제·정의 변경·동일·빌드 실패. 일부 Operation 미지원 시 그 Operation만 제외. upstream 회수(`x-mcp-enabled=false`) 시 제거. 정의는 같고 바인딩만 바뀐 경우 실제 REST 호출이 새 바인딩을 따름(`s=a&s=b`→`s=a%2Cb`, 선언된 204 처리) |
| Notifications | 통과. 도구 목록 변경 시 `notifications/tools/list_changed` 수신(1회 이상, SDK 변경 호출 수 이하), 304·실패·ETag만 변경·정의 동일 시 0회. SDK 기본(2026-07-28, `subscriptions/listen` 자동 구독)과 legacy(2025-11-25) 클라이언트 모두 |
| Concurrent Call | 통과. 교체 40회 동안 `tools/list` 4개·`tools/call` 6개 고루틴이 동시에 실행돼도 부분 목록·A/B 혼합 정의가 보이지 않았다. 두 계약에 모두 있는 도구 호출은 실패하지 않았고 panic도 없었다. CI Linux `-race` 대상(`internal/dynamic`·`internal/mcpserver`·`internal/openapi`)이다 |
| 서버 수준 종료 기준 | 통과. Static 33개와 함께 Registry A(35개) → 304·잘못된 계약에서 A 유지·기존 도구 호출 → Registry B(34개, 회수·설명 변경) → 알림 → Static 정의 바이트 동일. HTTP(stateless)에서는 교체 중 호출이 깨지지 않고 다음 `tools/list`에 반영 |
| Safety Gate | 통과. upstream 28개 전부 분류(SAFE 3·SHADOW 18·BLOCKED 7, 배치는 노출 2·shadow 19·blocked 7). BLOCKED > Static 충돌 > SAFE 우선순위, 잘못된 분류 값→BLOCKED, 미분류→SHADOW, SAFE 검토 후 경로·파라미터 변경→SHADOW. 기본 서버는 SAFE만 등록하고 비교 서버에도 BLOCKED는 없음 |
| Compatibility | 통과. Static 이름 충돌 11개 모두 COMPATIBLE 아님(INPUT·OUTPUT·SEMANTIC 전부, SECURITY 7개). 입력 스키마 기계 비교(이름·필수·타입·enum·const·범위·배수·길이·pattern·배열·추가 속성), 결과 봉투 비교, 호출 Operation 구성 비교 |
| 설정 | 통과. `refresh_interval` 기본 5m, 환경변수>YAML>기본값, 공백 환경변수 무시, 1m 미만·24h 초과·0·음수·단위 없음·정수 YAML·null·중복 거부(값 비노출), 토큰 변수 이름 예약, 등록 CLI 호환, 공개 예제 로드 |
| Loader | 통과. 호출자 검증 실패 시 계약·ETag 미커밋, 304에서 검증 콜백 미호출, 다음 조회는 기존 ETag 사용 |
| 실제 stdio 자식 프로세스 | 통과. 기본 파일럿은 노출 0(33개)과 노출 제외 사유 로그, SAFE 선택 시 35개 발견·호출(경로 공백 인코딩·2^53 초과 정수 원문), 계약 503에서 경고 후 33개·stdin 종료로 정상 종료, 잘못된 갱신 주기에서 기동 거부. stdout JSON-RPC 전용 |

새 테스트 함수는 23개다.

- 수정한 1차 테스트는 노출 게이트 도입으로 기대값이 바뀐 것들이다: 기본 파일럿 노출 2→0, E2E 대상 `get_topic`·`list_events`→SAFE 도구, 비교 서버에서 `list_clusters` 제외, 등록 경계 테스트 입력을 Static 충돌 목록으로 명시.
- 실행기·선택 로직 테스트에는 게이트와 무관하게 확인하도록 "전부 SAFE" 분류를 주입했다.

`go test -count=1 ./...`(하위 테스트 포함 555개), `go vet ./...`, `go mod verify`, `go build ./cmd/ableops-kafka-mcp`, `go build ./cmd/ableops-mcp-auth`, `scripts/test.ps1`이 모두 통과했다. gofmt 미적용 파일은 없다.

### 리뷰와 회귀 방지 확인

5개 관점(동시성, LKG·ETag, 노출·보안, 설정·호환, 테스트 품질)으로 적대적 리뷰를 했고, 발견마다 반박 검증을 거쳤다.

- 반박된 항목은 설계 의도이거나 도달할 수 없는 오용 경로였다. 예를 들어 최초 적재가 기동을 timeout만큼 늦추는 것은 문서화된 절충이다.
- 확정되었거나 근거가 분명한 결함은 다음 네 가지이며 모두 고쳤다.
  - 304 뒤 Registry 유지 여부를 단언하지 않음
  - 알림 "정확히 1회" 단언이 고부하에서 흔들림 → "1회 이상·호출 수 이하"로 바꾸고 문서에 최선 노력으로 명시
  - 재바인딩 테스트가 실제 호출을 확인하지 않음
  - 동시성 테스트의 1초 timeout과 블로킹 알림 핸들러
- 비용이 작은 강화도 함께 넣었다.
  - SAFE 판정을 검토 시점의 경로·파라미터에 고정
  - 호환성 비교 키워드 보강
  - 주기를 갱신 완료 시점부터 계산

`go test -overlay`로 구현을 일부러 망가뜨린 변형 4종을 넣어 보았고, 새 테스트가 모두 실패로 잡았다.

| 변형 | 잡은 테스트 |
| --- | --- |
| 재바인딩 제거 | `TestRuntimeRebindsWithoutNotification` |
| 304에서 Registry 재생성 | `TestRuntimeHotReloadLifecycle` |
| SAFE 고정 해제 | `TestExposureSafePolicyDrift` |
| `const` 비교 제거 | `TestCompareInputSchemas` |

불안정성도 확인했다. 12 CPU에서 테스트 프로세스 14개를 동시에 돌리고(`-count=3`, `-cpu 1,4`), 따로 `-count=8`로 반복했다. Runtime·서버 수준 테스트가 모두 통과했다.

### 실제 Backend 검증

사용자가 로컬에서 실행 중이던 kadmin 개발 빌드(`http://localhost:8080`, `version: dev`)를 `scripts/verify-backend.ps1`로 확인했다. 사용자 토큰은 제공되지 않았다.

| 항목 | 결과 |
| --- | --- |
| `/healthz`·`/readyz` | HTTP 200 |
| `/openapi.json` 조회·검증 | 통과. 발견 31, `x-mcp-enabled=true` 28, 제외 0, 정책표 밖 Operation 없음 |
| ETag·`If-None-Match` 304 | 통과 |
| Registry 생성과 Runtime 재조회 | 통과. 등록 가능 28, SAFE 선택 시 노출 3. 두 번째 Refresh는 `not_modified` |
| `tools/list` | 통과. 14개(Static 11 + Dynamic 3) |
| Dynamic `tools/call` | `get_branding`(미인증 공개): HTTP 200, 원문 JSON 455바이트 |
| 인증 전달 | 합성 무효 토큰으로 Dynamic `get_event_summary`(당시 노출) 호출 시 Backend가 401 → `authentication_required`. Static `list_clusters`도 401 |
| 계약 대조 | 실제 계약(322,426바이트)과 픽스처는 Operation·메서드·경로·`x-mcp-enabled`가 같았다. 설명·note·일부 응답 선언은 이전 버전이었다(예: `getEventIssue`의 204 미선언). 보관한 실제 계약 사본으로 다시 분류해도 SAFE 3·SHADOW 18·BLOCKED 7이고, SAFE 고정값도 일치했다 |

**미검증**:

- 인증에 성공한 Dynamic 호출, 즉 사용자 토큰으로 `get_topic_partitions`가 실제 JSON을 반환하는 경로. 토큰과 대상 클러스터·토픽이 제공되지 않았다.
- 실제 Backend에서 계약이 바뀌는 순간의 Hot Reload.
- 사내 배포 환경(`edu-cluster1-04`). 이 PC에서 DNS가 조회되지 않았다.
- 리뷰 반영과 v0.2.0 통합 뒤의 실연동 재실행. 로컬 Backend가 종료되어 있었고, kadmin 서비스는 이 저장소 규칙상 직접 실행하지 않았다. 보관한 실제 계약 사본으로 분류만 다시 확인했다.

인증 경로는 다음과 같이 재현할 수 있다.

```powershell
$env:ABLEOPS_BASE_URL = 'http://localhost:8080'
$env:ABLEOPS_API_TOKEN = '<본인 세션 토큰>'
$env:ABLEOPS_VERIFY_CLUSTER_ID = '<클러스터 ID>'
$env:ABLEOPS_VERIFY_TOPIC_NAME = '<토픽 이름>'
.\scripts\verify-backend.ps1
```

`-race`는 이 Windows 환경에 cgo용 gcc가 없어 **로컬에서 실행하지 못했다**(CI Linux에서 실행된다). 배포 빌드 스크립트(`build.ps1`/`build.sh`)는 `dist/` 산출물을 덮어쓰므로 다시 실행하지 않았다. 루트의 `ableops-kafka-mcp.exe`는 `scripts/test.ps1`의 검증 빌드로 갱신되었다.

## YAML 실행 설정 추가 검증

[실행 프롬프트](reference_docs/ableops-kafka-mcp-YAML설정지원.md)를 작성하고 서버와 인증 관리 CLI에 명시적 `--config`를 구현했다. 로컬 `mcp-server-config.yaml`은 Backend `http://localhost:8080`, HTTP 수신 `127.0.0.1:8081`, 기존 Windows 사용자별 인증 저장소 경로를 지정한다. 실제 토큰을 YAML에 넣지 않으며 HTTP에서는 공용 Backend 토큰 환경변수를 읽지 않는다. 환경변수·플래그 방식도 유지한다.

| 검증 대상 | 결과 |
| --- | --- |
| CLI > 환경변수 > YAML > 기본값 | 통과. 명시 플래그 여부, false, 빈 Origin 목록, 환경변수 전용 실행, 자동 파일 탐색 없음 |
| YAML 입력 | 통과. 필수 version, 알 수 없는 키·중복·타입·null·다중 문서·anchor/alias/merge 거부, 64 KiB 제한·일반 파일·UTF-8 BOM |
| 경로·토큰 참조 | 통과. YAML 위치 기준 상대 경로, 변수 확장, 미설정 변수·토큰 경로 확장·예약 설정 변수명 충돌 거부, HTTP 토큰 미조회, stdio 저장소 미조회 |
| 인증 CLI | 통과. 동일 YAML로 합성 Backend 등록·폐기, 사용자 토큰 검증, 토큰 없이 폐기, 오류 원문 비노출 |
| 실제 HTTP 자식 프로세스 + 공식 SDK | 통과. 합성 사용자 A/B, 무인증 401, Origin·CLI 우선순위, 11개 도구 발견·조회, 다른 실행 디렉터리의 상대 경로 |
| 실제 stdio 자식 프로세스 + 공식 SDK | 통과. YAML과 별도 토큰 환경변수, HTTP→stdio 플래그 변경, 초기화·11개 도구 발견·조회, stdout·stderr 비밀값 비노출 |

`go test ./...`, `go vet ./...`, `go build ./cmd/ableops-kafka-mcp`, `go build ./cmd/ableops-mcp-auth`가 모두 통과했다. `gofmt` 미적용 파일은 없으며 PowerShell/Bash 빌드 스크립트 구문을 확인했다. `scripts/build.ps1 -TargetOS all -Arch amd64`로 Windows/Linux 서버·인증 CLI 배포본을 다시 생성하고 체크섬·공개 YAML 예제 사본·사용자 YAML 미포함을 확인했다. Linux 실행파일은 교차 빌드했으며 실제 Linux 실행은 검증하지 않았다.

현재 8081 수신 프로세스(PID 29552)는 작업 전후 동일하다. 루트 실행파일과 배포본은 새 구현으로 빌드했지만 실행 중인 프로세스에는 자동 반영되지 않는다. 기존 서버를 종료한 뒤 `.\ableops-kafka-mcp.exe --config .\mcp-server-config.yaml`로 다시 실행하면 적용된다. 테스트는 합성 Backend·임시 인증 저장소·임시 포트를 사용했으며 실제 토큰·인증 저장소 내용을 읽거나 Backend·Kafka·DB에 요청하지 않았다. 원격 배포와 실행 중 서비스 재시작은 수행하지 않았다. 아래 항목들은 이전 작업 당시의 검증 이력이다.

## 로컬 인증 등록 진단 수정

사용자가 토큰 입력 없이 Enter를 눌러 `auth-init2.ps1`의 인증 등록이 설정 검증 단계에서 실패했음을 확인했다. Backend 401이나 8080 서비스 장애가 아니라 빈 `ABLEOPS_API_TOKEN`이 원인이었다. 사용자 스크립트 `auth-init.ps1`, `auth-init2.ps1`은 BOM 없는 UTF-8이어서 Windows PowerShell에서 한글이 깨졌으며 UTF-8 BOM으로 저장했다.

`auth-init2.ps1`은 빈 토큰·공백/비ASCII·Bearer 접두어·MCP 토큰 오입력을 네트워크 요청 전에 고정 안내로 거부한다. 프로젝트 루트에 새로 빌드한 `ableops-mcp-auth.exe`를 사용하도록 변경했다. CLI도 설정 검증과 클라이언트 초기화 실패의 안전한 고정 원인을 출력하되 실제 토큰·설정값·인증서 경로를 노출하지 않는다. 실제 등록·사용자 인증 검사를 우회하지 않았다.

합성 CLI 테스트로 빈/공백/비ASCII 토큰, URL·HTTP·제한시간·로그 수준·CA 오류를 구분하고 비밀값 비노출과 실패 시 파일 미생성을 확인했다. 두 PowerShell 스크립트의 구문 및 BOM을 검사했고, 빈 SecureString을 반환하는 합성 `Read-Host`로 등록 스크립트의 한글 오류와 임시 토큰 환경변수 정리를 확인했다. 이 검증은 실제 Backend에 인증 요청을 보내지 않았다.

`go test ./...`, `go vet ./...`, `go build ./cmd/ableops-kafka-mcp`, `go build ./cmd/ableops-mcp-auth`가 모두 통과했다. 새 인증 CLI는 루트 실행파일이며 기존 `dist` 묶음은 다시 생성하지 않았다. 실제 사용자 인증 등록과 8081 서버 실행은 유효한 본인 세션 토큰 입력이 남아 있다.

## 운영 분석용 조회 도구 추가 검증

요구 문서는 실제 위치 [ableops-kafka-mcp운영분석용조회도구추가.md](reference_docs/ableops-kafka-mcp운영분석용조회도구추가.md)를 사용했다. 기존 6개에 토픽 상세·Consumer 멤버·이벤트 상세·자산 영향·신청 상태 도구를 추가하여 총 11개다. 기존 REST Client, 오류/출력 봉투, 사용자별 인증 context, stdio·HTTP 전송을 재사용했다. 도구 전체 timeout과 하위 API 최대 8회 안전 상한을 공통으로 적용했다.

Backend 참조는 `D:\golang\go-workspace\kadmin`, HEAD `6aead42e70be7dcdd1b701f225a00b5af6eafa4a`이며 작업 전후 clean과 동일 HEAD를 확인했다. MCP 디렉터리에는 `.git` 메타데이터가 없다. Backend 소스·설정·서비스 변경, Kafka/DB 직접 접근, 원격 저장소 생성·push·배포를 수행하지 않았다.

| 검증 대상 | 합성 `httptest`·공식 SDK 결과 |
| --- | --- |
| 11개 도구 발견·입력/출력 Schema·read-only | 통과. 필수 대상·공백·추가 인자·잘못된 enum/include/limit 거부 |
| 다섯 신규 도구 기본 및 선택 데이터 | 통과. 공개 DTO와 실제 경로·쿼리·호출 수 확인 |
| 필수 실패·권한 거부·없는 객체·객체 소속 | 통과. HTTP 403/404, HTTP 200의 부적합 업무 응답, cluster/ID/center 불일치 후 후속 조회 차단 |
| 부분 실패·미조회·빈 결과·미지원 | 통과. 선택 API 실패 시 기본 자료 보존, Issue 204, 빈 멤버, 이벤트 actions 미지원, 빈 그래프 후 영향 조회 생략 |
| 이력·관계 출력과 호출 상한 | 통과. 발생 최대 101건 단일 요청·최대100 출력, 멤버 중첩 목록, 그래프 중심 보존·끝점 제거·원본 집계 보존, 신청 최근 이력 |
| 민감정보 제외 | 통과. 토픽 임의 설정, evidence/플레이북 비공개 필드, 그래프 인증 attrs/수집 오류 원문, 신청 payload/의견/내부 실패 원문 제외 |
| 신청 업무 상태 | 통과. APPROVED/READY_TO_APPLY/APPLIED/VERIFIED와 실패·반려·철회·폐기를 원상태로 보존 |
| 취소·시간·요청 예산 | 통과. context 취소, 도구 전체 마감, 동시 하위 호출 최대8회, 다른 실행의 독립 예산, 기존 REST 전송 제한 |
| stdio·HTTP 및 기존 6개 도구 회귀 | 통과. 실제 자식 프로세스 stdio의 11개 발견·호출, SDK HTTP에서 11개 모두 호출, 기존 사용자별 인증/권한/격리 테스트 |

새 테스트는 `internal/ableops/{topic_detail,event_detail,impact_request,operation}_test.go`, `internal/tools/{topic_detail,event_detail,impact_request}_test.go`에 있다. 기존 통합·stdio·HTTP 도구 수 검증도 11개로 갱신했다. `/`, `;`, `,`가 포함된 경로의 멤버 조회는 응답에 대상 식별자가 없어 디코딩 불일치를 대조할 수 없으므로 HTTP 호출 전에 미지원으로 거부하는 회귀 테스트를 추가했다.

최종 실행 환경은 Go 1.27.1 / windows/amd64다.

| 명령 | 결과 |
| --- | --- |
| `go test ./...` | 통과 |
| `go vet ./...` | 통과 |
| `go build ./cmd/ableops-kafka-mcp` | 통과 |

이번 작업에서는 실제 사용자 토큰과 대상 환경변수가 없고 실연동 opt-in도 꺼져 있어 실제 Backend 요청을 보내지 않았다. 새 도구의 실제 연동은 **미검증**이며 아래 기존 HTTP 작업의 실연동 이력과 구분한다. `internal/integration/analysis_test.go`에 명시 opt-in 검증을 추가했으며 `ABLEOPS_API_TOKEN`, `ABLEOPS_VERIFY_CLUSTER_ID`와 도구별 `ABLEOPS_VERIFY_TOPIC_NAME`, `ABLEOPS_VERIFY_GROUP_NAME`, `ABLEOPS_VERIFY_EVENT_ID`, `ABLEOPS_VERIFY_ASSET_TYPE`/`ABLEOPS_VERIFY_ASSET_KEY`, `ABLEOPS_VERIFY_REQUEST_ID`가 있을 때만 지정 대상을 조회한다. 빠진 대상은 SKIP하고 임의 생성·검색하지 않는다. 실제 값/본문은 출력하지 않는다.

남는 Backend 요건은 제한된 이벤트 조치 이력, 발생 이력의 저장소 실패 표시, 요약 전용 Issue API, 토픽 설정 출처·동기화 시각, 멤버 존재/상태·경로 디코딩, 자산 영향의 수집 실패/관측 시각·서버 노드 제한, 신청의 안전한 실패 코드·재시도 정보와 서버 이력 제한이다. [API 매핑](api-mapping.md#운영-분석-조회-도구)에 구체적인 계약과 제한을 기록했다. 이번 변경으로 배포용 `dist` 묶음을 다시 생성하거나 배포하지 않았다.

## HTTP·사용자별 인증 추가 검증

요구 문서는 실제 위치 `docs/reference_docs/mcp검증및추가개발-1.md`를 사용했다. 이번 작업 디렉터리에는 `.git` 메타데이터가 없으므로 MCP 변경의 Git diff/status는 제공할 수 없다. Backend는 `D:\golang\go-workspace\kadmin`, 브랜치 `v1.7.2`, HEAD `6aead42e70be7dcdd1b701f225a00b5af6eafa4a`, 작업 시작 시 clean이었다. 아래 초기 구현의 HEAD와 미커밋 정보는 과거 검증 기록이다.

기존 도구와 stdio를 유지하고 SDK v1.8.0의 stateless Streamable HTTP, loopback 제한, 별도 로컬 MCP 토큰·사용자 Backend 세션 매핑을 추가했다. OAuth 인증 서버는 제공되지 않았으며 공용 서비스 준비 완료를 의미하지 않는다. Backend 파일 수정과 서비스 시작·종료·재시작, Kafka/DB 직접 접근, 운영 데이터 생성, 마이그레이션은 수행하지 않았다. Backend 변경·재시작은 필요 없다.

자동 검증은 합성 `httptest` Backend와 공식 SDK Client를 사용한다. HTTP 초기화·도구 목록·여섯 도구·종료, stdio 회귀, 무인증·잘못된 토큰·만료·미래 발급·폐기, audience·사용자 매핑 불일치, 사용자 A/B 동시 호출과 클러스터별 권한 거부, Backend 401/403·timeout·부분 실패를 확인한다. 실제 보호 파일→SDK HTTP→공유 REST 전송을 연결하여 서로 다른 토큰/데이터가 섞이지 않고 폐기와 Backend 로그아웃이 다음 요청에 반영되는지도 검증한다.

Origin/Host·CORS, 알려진 크기 및 chunked 입력의 상한, 전체/사용자별 동시 제한, 처리 deadline·클라이언트 취소·서버 종료, stateless 세션 비재사용, 원문/이스케이프 토큰 반사와 로그 비노출을 검사한다. Windows 보호 ACL과 생성 시 권한, MCP 원문 출력 파일 덮어쓰기 금지, 파일 손상·크기·TTL 검증, 관리자 CLI의 `/api/me` 등록 경로도 검사한다. 세션 ID는 발급하지 않으며 입력 세션 ID가 인증을 우회하지 못한다.

### 이번 실제 Backend 결과

`scripts/verify-backend.ps1`을 명시적으로 실행했다. 다음 결과는 실제 `http://localhost:8080` 요청이다.

| 시나리오 | 결과 |
| --- | --- |
| TCP 8080 연결 | 성공 |
| `GET /healthz`, `GET /readyz` | 각각 HTTP 200. 본문을 읽지 않았으므로 Kafka/저장소의 세부 준비 상태는 확인하지 않음 |
| 합성 무효 세션으로 SDK `list_clusters` | Backend HTTP 401 → MCP `authentication_required` 확인 |
| 유효 사용자 여섯 도구·실제 DTO/관측/스냅샷/빈 결과 | `ABLEOPS_API_TOKEN` 미제공으로 SKIP |
| 실제 사용자 A/B 권한·접근 불가·없는 대상 | 계정/대상 미제공으로 미검증 |
| 실제 만료 세션 | 별도 만료 토큰 미제공으로 SKIP. 합성 무효 토큰 결과로 대신하지 않음 |

실행에는 본인의 `ABLEOPS_API_TOKEN`, `ABLEOPS_VERIFY_CLUSTER_ID`, `ABLEOPS_VERIFY_GROUP_NAME`을 안전한 환경변수로 전달한다. 선택 시나리오는 `ABLEOPS_VERIFY_DENIED_CLUSTER_ID`, `ABLEOPS_VERIFY_MISSING_CLUSTER_ID`, `ABLEOPS_VERIFY_MISSING_GROUP_NAME`, `ABLEOPS_VERIFY_EXPIRED_TOKEN`이다. 실제 값과 응답 본문은 출력하지 않는다. 만료 토큰은 만료 사실을 사용자가 확인한 토큰이어야 하며 Backend 401만으로 만료와 무효의 원인을 구분하지 않는다.

```powershell
.\scripts\verify-backend.ps1
# 직접 실행할 경우 명시적 opt-in이 필요하다.
$env:ABLEOPS_VERIFY_BACKEND = '1'
go test ./internal/integration -run '^TestBackendLive$' -count=1 -v
Remove-Item Env:ABLEOPS_VERIFY_BACKEND
```

기본 `go test ./...`와 CI는 opt-in 없이 실환경 테스트를 SKIP하므로 운영 Backend에 의존하지 않는다. 실제 권한·만료 검증이 필요하면 사용자 준비 후 위 명령으로 재실행한다. 사내 OAuth·HTTPS 프록시, 실제 다중 사용자 서비스, Linux 실행 및 원격 CI 실행은 이번 로컬 검증과 별개다.

### 이번 자동 검증 실행 결과

| 실행 | 결과 |
| --- | --- |
| Go 1.27.1 `go test ./...` | 통과. 명시 opt-in 실연동은 기본 SKIP |
| `go vet ./...` | 통과 |
| `go build ./cmd/ableops-kafka-mcp` | 통과 |
| `scripts/test.ps1` | test/vet/build 전체 통과 |
| Go 1.25.0 `go test ./internal/localauth ./internal/mcpserver ./cmd/ableops-mcp-auth` | 통과. Windows 조회 중 폐기 파일 교체 포함 |
| `scripts/build.ps1 -TargetOS all -Arch amd64` | 서버·인증 CLI의 Windows/Linux amd64 빌드와 체크섬 확인 |
| 인증·동시성 패키지 `go test -race` | 실행 불가. `CGO_ENABLED=1`로 시도했으나 `gcc`가 PATH에 없어 race 빌드 실패 |
| PowerShell 검증 스크립트 구문 | 통과 |

race 검사는 Linux CI에 추가했지만 원격 CI를 실행하지 않았으므로 race 통과로 보고하지 않는다. 일반 동시 호출 테스트는 로컬에서 통과했다. Backend Git 상태는 작업 종료 시에도 clean이며 HEAD가 동일했다. Windows 비밀 저장소는 읽기 핸들의 삭제 공유와 Go 1.25 `os.Root.Rename`을 사용하여 조회 중에도 폐기 파일을 교체하고 다음 요청에서 적용한다.

아래는 초기 stdio 및 배포 스크립트 검증 당시의 이력이다.

## 배포 빌드 스크립트 추가

현재 배포 빌드의 출력은 `dist/<OS>-<아키텍처>/`다. 아래 초기 구현 기록의 `bin` 경로는 최초 검증 당시 산출물이다.

`scripts/build.ps1`과 `scripts/build.sh`는 기본 Linux/Windows amd64 및 선택적 arm64를 교차 빌드한다. 각 폴더는 실행파일, `.env.example`, 배포 안내서 `README.md`, 실행파일의 `SHA256SUMS`로 구성된다. `CGO_ENABLED=0`으로 빌드하며 Linux에서는 정적으로 링크된 ELF 파일이 생성된다. 빌드 스크립트는 실제 토큰이나 `.env`를 복사하지 않는다.

Windows PowerShell 및 Git Bash에서 스크립트를 실행하여 Linux/Windows × amd64/arm64 네 종류를 생성했다. 배포 파일과 SHA256SUMS, OS별 실행파일 형식을 확인했다. OS 선택 빌드, 저장소 밖에서 실행, PowerShell 환경변수·현재 경로 복원도 확인했다. 전체 `scripts/test.ps1`(test/vet/build)도 통과했다. Windows amd64 배포 실행파일은 합성 설정으로 시작하여 stdin 종료 후 정상 종료했고 stdout 로그 혼입과 토큰 노출이 없었다. 이 실행 확인은 Backend 네트워크 요청을 보내지 않았다.

Linux 실행 환경(WSL)이 없으므로 Linux 실행파일의 실제 실행은 미검증이다. Windows arm64도 교차 빌드 대상이며 현재 amd64 호스트에서 실행하지 않는다. CI에는 Windows PowerShell 및 Linux Bash 배포 빌드를 추가했지만 원격 CI는 실행하지 않았다.

## 실행 결과

| 검증 | Go 1.27.1 | Go 1.25.0 |
| --- | --- | --- |
| `go test ./...` | 통과 | 통과 |
| `go vet ./...` | 통과 | 통과 |
| `go build ./cmd/ableops-kafka-mcp` | 통과 | 통과(`-o .work/ableops-go1.25.exe`) |

`scripts/test.ps1`, `scripts/build.ps1`을 실제 PowerShell에서 실행했고 모두 성공했다. 배포용 실행파일은 `bin/ableops-kafka-mcp.exe`다. `go mod verify`도 통과했으며 `gofmt -l cmd internal`의 미포맷 파일은 없었다.

최소 버전 검증은 프로세스 환경에 `GOTOOLCHAIN=go1.25.0`을 설정하여 공식 Go toolchain으로 실행했다. 기존 프로젝트의 Go 버전·설정은 변경하지 않았다.

## 확인한 동작

- 실제 `.exe` 자식 프로세스에 공식 SDK MCP Client로 연결하여 초기화, 여섯 도구·스키마 조회, 도구 호출, stdin 종료와 정상 프로세스 종료를 확인했다.
- stdout의 모든 줄이 MCP JSON-RPC이며, 도구 로그는 stderr로 출력되고 토큰은 양쪽에 노출되지 않음을 검사했다.
- 필수 클러스터·그룹 입력, 정확한 GET 경로와 Bearer 전달, 특수문자 경로 인코딩, 다른 대상 응답 거부를 검증했다.
- 401·403·404·429·5xx, HTTP 200 본문의 실패·NOT_FOUND, 부분 성공과 전체 실패, 미정의 업무 상태 거부를 검증했다.
- timeout, 호출 취소, 프로세스 종료 시 진행 중 REST 취소, 비정상 JSON, 응답 크기 초과, 동시 호출 제한, 목록/응답 잘림을 검증했다.
- 다른 출처 및 같은 출처 리다이렉트 차단, 사설 CA 검증, 신뢰하지 않는 TLS 거부, 응답과 잘못된 SDK 인자의 토큰 반사 차단을 검증했다.
- 합성 DTO의 인증 설정·임의 configs/evidence 필드 제외, 스냅샷 시각 미확인과 원본 관측 시각 보존을 검증했다.

## 기존 소스 보존

분석 대상 `D:\golang\go-workspace\kadmin`의 브랜치는 `v1.7.2`, HEAD는 `fe0aeff661a6bdecf33ece5d443cd3d329dbb560`이다. 작업 전후 Git 상태는 `internal/server/event_cluster_filter_handler_test.go`의 기존 수정 1개였으며 binary diff가 동일함을 확인했다. 해당 파일 SHA256도 `2979F21A50A2F500000AF6C27E040C21F71A9FA34D367D55E71AE8045156E08D`로 동일했다.

기존 저장소의 소스·설정·Git 상태를 수정하지 않았고, 기존 서비스 실행·마이그레이션·운영 Kafka/DB 접속·원격 저장소 생성·push·배포를 수행하지 않았다.

## 초기 구현 당시 미검증 범위

초기 stdio 구현 당시에는 실제 Backend 연동 없이 `httptest` 합성 서버만 사용했다. 이후 이번 HTTP 작업에서 수행한 실제 연결·무효 토큰 검증과 사용자 매핑 구현은 문서 상단 기록을 따른다. 원격 CI·Linux 실행, 공용 OAuth, 변경 도구·LLM 채팅은 여전히 검증·구현 범위 밖이다.
