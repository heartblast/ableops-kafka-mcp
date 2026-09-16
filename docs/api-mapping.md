# 로컬 AbleOps API 계약과 MCP 매핑

## 분석 기준과 재현 범위

분석·재확인일은 2026-09-16이다. 계약의 기준은 **`D:\golang\go-workspace\kadmin` 로컬 작업본**이며 원격 최신 버전과 혼합하지 않았다.

- 브랜치: `v1.7.2`
- 현재 HEAD: `6aead42e70be7dcdd1b701f225a00b5af6eafa4a`
- 현재 Git 상태: `git status --short` 출력 없음. 관련 미커밋 변경 없음.
- 최초 여섯 도구 구현 때의 참조 HEAD는 `fe0aeff661a6bdecf33ece5d443cd3d329dbb560`이었다. 당시 `internal/server/event_cluster_filter_handler_test.go`의 미커밋 변경 1개를 포함해 읽었고, 현재 상태와 구분해 이력을 보존한다.
- 두 HEAD 사이에서 아래 여섯 도구의 라우트·핸들러, 인증·세션, RBAC·스냅샷, DTO, 관련 테스트를 좁혀 비교했다. 이 범위의 차이는 `event_cluster_filter_handler_test.go`가 발송 재시도 시각을 명시적으로 과거로 지정하는 테스트 수정뿐이다. 업무 API 경로·필터·응답·권한 계약 변경은 없었다.
- Backend 소스·설정·Git 상태를 변경하지 않았고 해당 저장소의 테스트를 실행하지 않았다. Backend 서비스를 시작·종료·재시작하지 않았다. Kafka·DB에 직접 접근하거나 마이그레이션을 실행하지 않았다.
- 기본 자동 테스트는 신규 프로젝트의 `httptest`와 공식 SDK 클라이언트만 사용한다. 명시적 실연동 실행 결과와 미검증 범위는 이 문서 끝에 별도로 기록한다.
- 기존 저장소의 비밀 설정 파일을 읽거나 복사하지 않았다.

실제로 확인한 로컬 파일은 다음과 같다. 이하 경로는 모두 위 `kadmin` 아래 상대 경로다.

| 목적 | 근거 파일 |
| --- | --- |
| 지침과 문서 비교 | `CLAUDE.md`, `.claude/context/불변식-체크리스트.md`, `.claude/context/API_계약.md`, `go.mod` |
| `/api`와 인증 그룹·현재 사용자 | `internal/server/server.go:369`, `internal/server/routes.go`, `internal/server/routes_auth.go:15`, `internal/server/auth_session.go:131`, `internal/middleware/middleware.go:54`, `internal/auth/session.go:185` |
| 실제 라우트 | `internal/server/routes_cluster.go`, `internal/server/routes_topic.go`, `internal/server/routes_event.go` |
| 클러스터·스냅샷 핸들러 | `internal/server/clusters.go`, `internal/server/cluster_scope.go` |
| 건강·Lag 핸들러 | `internal/server/partition_health.go`, `internal/server/consumer_group_health.go`, `internal/server/health.go` |
| 이벤트 핸들러·필터 | `internal/server/events.go` |
| 권한·데이터 출처 | `internal/clusters/rbac.go`, `internal/clusters/service.go`, `internal/clusters/sync.go` |
| 응답 DTO | `internal/domain/cluster.go`, `internal/domain/topic.go`, `internal/domain/consumer_group.go`, `internal/domain/groupview.go`, `internal/domain/partitionview.go`, `internal/domain/events.go` |
| 일부 판정 필드의 의미 | `internal/kafkaadmin/partitionhealth.go` |
| 계약 테스트 검토 | `internal/clusters/rbac_test.go`, `internal/server/topic_view_perm_test.go`, `internal/server/event_cluster_filter_handler_test.go`, `internal/server/routes_contract_test.go`, `internal/auth/session_test.go:241` |
| 기존 상태 확인 경로 | `internal/server/routes.go:92`, `internal/server/health.go` |
| Backend 접근 로그·감사 범위 | `internal/middleware/middleware.go:24`, `internal/server/clusters.go:295`, `internal/domain/audit.go:12` |

별도 MCP 프로젝트는 기존 `internal` 패키지를 import하지 않으며 권한·Lag 판정 로직도 복제하지 않는다. [DTO](../internal/ableops/dto.go)는 이 계약의 공개 허용 필드만 선언한다.

## 인증과 권한

모든 아래 요청은 `Authorization: Bearer <사용자 세션 토큰>`을 사용한다. `middleware.Auth`는 `auth.Manager.Resolve`로 서버 내 세션을 조회한다. 토큰 누락·무효·절대 만료·유휴 만료는 모두 HTTP 401이며 사유를 구분할 수 없다. 기본 세션 수명은 절대 8시간, 유휴 30분이고 사용 시 유휴 시각이 갱신된다. 인메모리 세션이므로 백엔드 재시작 후 유지되는 OAuth 토큰으로 간주해서는 안 된다.

`clusters.Can`은 전역 SystemAdmin 또는 해당 사용자·클러스터에 부여된 역할의 권한을 확인한다. MCP의 read-only annotation은 이 검사를 대체하지 않는다. 백엔드는 요청마다 최종 권한을 판단한다. 클러스터 목록에 노출되는 것은 어떤 역할이든 부여된 클러스터라는 의미이며, 그 목록에 있다는 이유만으로 `topic.view`나 `event.view`를 보유한다고 가정할 수 없다.

### HTTP 호출자와 Backend 사용자 연결

서버·인증 CLI는 선택적으로 `--config`로 실행 설정을 읽는다. YAML의 `auth.store_file`은 기존 매핑 파일 경로이고 `auth.token_env`는 stdio/등록의 환경변수 이름이다. 이 변경은 업무 REST 경로·DTO·사용자 권한 계약을 바꾸지 않는다. HTTP는 YAML 사용 시에도 공용 Backend 토큰을 사용하지 않으며 기존 `/api/me`와 요청별 위임 인증을 유지한다.

HTTP 모드는 로컬 관리자 절차로 등록한 **별도 MCP 접근 토큰 → 사용자별 Backend 세션 토큰** 매핑을 사용한다. stdio의 프로세스 전용 `ABLEOPS_API_TOKEN`을 HTTP 사용자 전체에게 적용하지 않는다. MCP 토큰은 Backend에 전송하지 않고, 공용 REST 클라이언트의 토큰을 덮어쓰지 않으며 요청 문맥의 Backend 자격증명으로 각 GET을 인증한다.

| 용도 | 확인한 기존 API | MCP가 해석하는 필드 |
| --- | --- | --- |
| 등록 시 사용자 식별, 매 HTTP 요청의 Backend 세션·사용자 재확인 | `GET /api/me` | 최상위 `id`만 사용 |

`routes_auth.go`의 `/me`는 `server.go`의 `/api` 인증 그룹 안에 등록된다. `auth_session.go`의 `meResponse`는 `domain.User`를 최상위에 포함하고 `permissions`를 추가한다. MCP는 이름·부서·서비스·역할·권한 목록 등 나머지 필드를 저장하거나 도구 응답으로 반환하지 않는다. 요청자가 보낸 `user_id`나 `X-User`를 신원으로 신뢰하지 않는다. 등록 시 `/api/me`에서 얻은 ID와 매 요청에 확인한 ID가 다르면 인증을 거부한다.

`auth.Manager.Resolve`는 매 요청마다 세션의 절대·유휴 만료를 확인한 뒤 현재 사용자 정보를 저장소에서 다시 읽는다. `TestLogoutAndRevoke`, `TestIdleExpiry`는 해당 기존 동작을 검증하는 Backend 테스트로 읽기만 했다. MCP도 `/api/me` 결과를 캐시하지 않으며 로그아웃·Backend 만료를 오래된 매핑 검증 결과로 우회하지 않는다. `/api/me` 성공은 특정 클러스터 권한을 보증하지 않으므로 업무 GET마다 기존 RBAC가 최종 판단한다.

이 경로는 신규 조회 도구가 아닌 내부 인증 확인이다. 별도 OAuth 인증 서버 설정이 제공되지 않은 이번 구현은 loopback 개발용 인증이다. 로컬 MCP 매핑의 audience는 `ableops-kafka-mcp`이며 Backend 토큰은 기존 Backend가 해석하는 opaque 세션이다. 이를 OAuth 토큰 교환이나 Backend의 audience claim 검증 구현으로 표현하지 않는다. Backend 수정과 신규 인증 API 추가가 없으므로 이 구현 때문에 Backend 재시작이 필요하지 않다.

### 여섯 도구의 업무 API

| MCP 도구 | 메서드와 실제 경로 | 백엔드 권한 |
| --- | --- | --- |
| `list_clusters` | `GET /api/clusters` | 인증 + `clusters.ListFor`의 사용자별 필터 |
| `get_cluster_health` | `GET /api/clusters/{id}/health`와 `GET /api/clusters/{id}/partition-health` | 각각 `clusterAdapterOr(..., topic.view)` |
| `list_topics` | `GET /api/clusters/{id}/topics` | `clusterAdapterOr(..., topic.view)` |
| `list_consumer_groups` | `GET /api/clusters/{id}/consumer-groups` | `clusterAdapterOr(..., topic.view)` |
| `get_consumer_group_lag` | `GET /api/clusters/{id}/consumer-groups/{name}/lag` | `clusterAdapterOr(..., topic.view)` |
| `list_cluster_events` | `GET /api/clusters/{id}/events` | `clusterAdapterOr(..., event.view)` |

`clusterAdapterOr`는 권한을 검사하지만 대상 존재를 일괄 검사하지 않는다. 따라서 미등록 ID의 응답은 역할과 엔드포인트에 따라 403, 200의 도달성 실패, 스냅샷 미제공, 빈 이벤트 목록 등으로 달라질 수 있다. MCP가 이를 추측하여 모두 404로 바꾸지 않는다. 전역 SystemAdmin은 `clusters.Can`에서 모든 ID에 허용되지만 그 자체로 ID의 존재를 보증하지 않는다.

BASE_URL에는 origin만 넣는다(예: `https://ableops.example.com`). `/api`는 REST 클라이언트가 붙인다. `{id}`와 `{name}`은 별도 경로 세그먼트로 인코딩한다. 클러스터 인자 누락 시 전역·기본 클러스터 엔드포인트로 대체하지 않는다.

경로 인코딩은 URL 경계를 보호하지만 기존 백엔드의 특수 이름 처리까지 보증하지 않는다. 특히 `/`가 들어간 그룹명은 `%2F`로 안전하게 전송되어도 chi의 RawPath 매칭과 핸들러의 미디코딩 `chi.URLParam` 때문에 백엔드가 인코딩 문자열 자체를 그룹명으로 조회할 수 있다. MCP는 응답 `group`이 요청 이름과 다르면 `invalid_response`로 거부하며 다른 이름의 결과나 오인된 NOT_FOUND를 전달하지 않는다. `%2F`를 경로 구분자로 풀어서 재시도하지 않는다. 이런 그룹을 지원하려면 백엔드의 경로 인자 디코딩 계약과 회귀 테스트가 먼저 필요하다.

## 응답과 출처

### `list_clusters`

백엔드 응답은 `clusters.View[]`이며 빈 목록은 `[]`이다. 백엔드가 마스킹한 접속 설정도 MCP에서는 다시 제외한다.

공개 필드는 `id`, `name`, `environment`, `mode`, `kafkaVersion`, `kraftMode`, `isActive`, `isDefault`이다. 등록 메타데이터이며 Kafka 실시간 상태가 아니다. 백엔드는 관측 시각을 제공하지 않는다. 목록 필터·페이지·정렬 쿼리는 구현되지 않았으므로 MCP는 보내지 않는다.

### `get_cluster_health`

`/health`의 실제 응답은 **`clusters.TestResult`**이다. `domain.ClusterHealth`(`/health/checks` 등)와 혼동하지 않는다.

```json
{
  "clusterId": "prod-a",
  "adapterName": "franz-go",
  "reachable": true,
  "cluster": {
    "clusterId": "Kafka가 반환한 ID",
    "controllerId": 1,
    "brokers": [{"nodeId": 1, "host": "broker.example", "port": 9093, "isController": true}],
    "topicCount": 2,
    "partitionCount": 6,
    "internalTopicCount": 1,
    "internalPartitionCount": 50,
    "underReplicatedPartitions": 0,
    "offlinePartitions": 0,
    "noLeaderPartitions": 0
  }
}
```

어댑터의 Ping과 DescribeCluster를 라이브 호출한다. mock 모드면 합성 어댑터 결과다. `cluster.clusterId`는 Kafka ID이며 외부 봉투의 등록 ID와 같다는 보장이 없다. 실패는 200 본문의 `reachable:false`, `error`로 나타날 수 있다. Ping 성공 후 DescribeCluster 실패는 `reachable:true`, `cluster` 누락, `error` 미제공으로도 나타난다. 메타데이터 누락은 정상 0개로 해석하면 안 된다. **이 응답에는 `checkedAt`이 없다.**

`/partition-health`는 다음 필드를 갖는다.

- 판정: `status`, `reasons`, `checkedAt`
- 측정: `totalTopics`, `totalPartitions`, `underReplicated`, `underMinIsr`, `offline`, `leaderless`, `unavailable`, `forbidden`, `downgraded`
- 상세: `issues[]`의 `topic`, `partition`, `state`, `status`, `leader`, `replicas`, `isr`, `offlineReplicas`, `minIsr`, `internal`, `reassigning`, `downgraded`, `reasons`
- 제한: `truncated`, `maxIssues`, `includeInternal`, `minIsrResolved`, `reassignApplied`

상태는 `OK`, `WARN`, `CRITICAL`, `UNAVAILABLE`, `FORBIDDEN`이다. HTTP 200의 `FORBIDDEN`은 Kafka 조회 권한 문제이며 포털 RBAC의 HTTP 403과 별개다. 파티션 번호 `-1`은 토픽 단위 조회 실패다. 이상 카운터는 독립 조건이라 합계를 이슈 개수로 해석하지 않는다. `minIsrResolved:false`는 관련 판정 미수행이며 대상 0개일 때도 발생할 수 있다. `reassignApplied:false`는 재배치 연계 판정 미적용이다. 재배치 조회 성공 후 대상 재배치 0건이면 이 값은 true다.

백엔드는 `includeInternal`과 `limit`을 지원하고 limit은 최대 2000이다. MVP는 두 쿼리를 노출하지 않고 백엔드 기본 범위로 조회하며 원본 필드와 서버 잘림을 보존한다. 이슈 카운터는 잘린 상세 목록의 길이로 다시 계산하지 않는다.

두 API를 조합할 때 한쪽 성공·한쪽 실패를 부분 실패로 반환한다. 둘 다 실패하면 도구 실행 실패다. 두 응답은 별도 호출이므로 단일 원자적 스냅샷을 의미하지 않는다.

### `list_topics`와 `list_consumer_groups`

공통 봉투는 아래와 같다.

```json
{
  "clusterId": "prod-a",
  "clusterName": "운영 A",
  "environment": "prod",
  "syncedAt": "2026-09-16T00:00:00Z",
  "items": []
}
```

토픽 항목은 `name`, `partitions`, `replicationFactor`, `cleanupPolicy`, `retentionMs`, `department`, `service`, `env`, `owner`를 공개한다. 백엔드의 임의 `configs` 맵은 공개하지 않는다. 소유 메타데이터는 데이터 카탈로그와 병합되므로 스냅샷 시각이 모든 카탈로그 필드의 갱신 시각을 뜻하지 않는다.

그룹 항목은 `name`, `state`, `members`, `totalLag`, `topicLag`(토픽명→정수)를 공개한다. **저장된 요약에는 파티션 조회 실패 여부가 없다.** 상세의 실패 상태 확인은 별도의 Lag 상세 API가 필요하다. 스냅샷 `totalLag:0`을 모든 파티션 관측 성공의 증거로 사용하지 않는다.

둘 다 `clusters/sync.go`의 자산 스냅샷에서 읽는다. 백엔드는 DB 또는 memory store로 구성될 수 있다. 스냅샷이 없으면 **GET 내부에서 lazy SyncAssets를 시도하여 저장소 스냅샷을 기록할 수 있다.** MCP가 직접 DB에 접근하거나 명시적 sync POST를 호출하지는 않는다. read-only 표시는 Kafka 변경 도구를 제공하지 않는다는 의미이며 백엔드 내부 저장소가 전혀 변하지 않는다는 보장은 아니다.

장애 중에는 이전 스냅샷이 반환될 수 있다. 스냅샷 취득·lazy sync 실패는 내부에서 흡수되어 `syncedAt:null`, `items:null`로 나타날 수 있다. 이 경우 MCP는 최신성·조회 성공을 확인할 수 없다는 제한을 표시해야 한다. `syncedAt`을 MCP 조회 시각으로 채우지 않는다. 기존 백엔드 개선 과제는 스냅샷 조회 성공 여부·실패 코드·부분 수집 여부를 응답에 명시하는 것이다.

서버 필터·페이지·정렬 쿼리는 구현되지 않았다. MCP의 `limit`은 이미 수신한 전체 목록의 출력 개수 제한이며 서버 페이지 처리가 아니다.

### `get_consumer_group_lag`

Kafka 어댑터로 라이브 조회한 후 백엔드의 대상별 Lag 정책으로 재평가한다. MCP는 임계치나 Lag 판정을 계산하지 않는다.

- 식별·판정: `group`, `found`, `code`, `status`, `reasons`, `state`, `protocolType`, `assignor`, `members`, `coordinator`
- 집계: `totalLag`, `maxPartitionLag`, `avgPartitionLag`, `totalPartitions`, `countedPartitions`, `errorPartitions`, `uncommittedPartitions`, `warnPartitions`, `criticalPartitions`
- 토픽별 집계: `topicLag[]`의 `topic`, `lag`, `partitions`, `errorPartitions`
- 파티션별 관측: `partitions[]`의 `topic`, `partition`, `commitOffset`, `startOffset`, `endOffset`, `lag`, `memberId`, `clientId`, `state`, `status`, `error`, `reasons`
- 원본 판정 시각·임계치: `checkedAt`, `thresholds`

`thresholds`는 `lagWarn`, `lagCritical`, `lagPartitionWarn`, `lagPartitionCritical`, `lagSkewWarn`, `lagSkewFloor`, `rebalanceWarnMinutes`, `warnOnIdleEmptyGroup`이다. 백엔드가 응답한 값을 그대로 전달한다.

HTTP 200에도 `FORBIDDEN`, `UNAVAILABLE`이 가능하며, 그룹 없음은 **`found:false`, `code:"NOT_FOUND"`**다(HTTP 404가 아님). 조회 실패 파티션의 `lag:-1`, 커밋 없음의 `commitOffset:-1`을 0으로 바꾸지 않는다. 합계는 실패 파티션을 제외한 값이므로 `errorPartitions`와 함께 해석한다. 어댑터 오류는 502, 정책 조회 실패는 500이 될 수 있다.

서버 페이지·정렬·필터는 없다. MCP 출력에서 상세를 자르더라도 원본 전체 집계를 유지하고 잘림을 명시한다.

### `list_cluster_events`

응답은 `{items: EventView[], total, page, pageSize}`이며 저장소 사건 기록이다. 이벤트 수집원을 이 호출이 직접 실행하지 않는다. 수집기 상태와 전체 수집 최신성은 이 API만으로 확인할 수 없다.

MCP가 지원하는 쿼리 부분집합은 다음과 같다.

| MCP 입력 | REST 쿼리 | 실제 계약 |
| --- | --- | --- |
| `page` | `page` | 1부터 시작, 백엔드 최대 10000 |
| `page_size` | `pageSize` | 백엔드 기본 20/최대 200, MCP 기본 50/최대 100 |
| `status[]` | 반복 `status` | `OPEN`, `ACKNOWLEDGED`, `SUPPRESSED`, `RESOLVED` |
| `severity[]` | 반복 `severity` | `INFO`, `WARN`, `HIGH`, `CRITICAL` |
| `category[]` | 반복 `category` | `AVAILABILITY`, `PERFORMANCE`, `CAPACITY`, `SECURITY`, `CONFIGURATION`, `INTEGRATION` |
| `search` | `search` | 검색 문자열, 백엔드 최대 200자 |
| `from`, `to` | `from`, `to` | `last_seen_at` 범위, RFC3339 또는 `YYYY-MM-DD` |

다중 필터는 종류당 최대 20개다. 백엔드는 반복·콤마 문법을 모두 지원하며 알 수 없는 enum은 400이다. URL의 클러스터가 쿼리보다 우선하므로 MCP는 `clusterId` 쿼리를 받거나 보내지 않는다. 미지원 `limit`/`offset`을 보내지 않는다.

백엔드의 추가 필터는 `resourceId`, `module`, `assignedTo`, `resourceType`, `attention`, `eventCode`, `excludeSynthetic`이며 MVP에 노출하지 않는다. 정렬 화이트리스트는 `-lastSeenAt`, `lastSeenAt`, `-firstSeenAt`, `firstSeenAt`, `-severity`, `severity`, `status`, `-status`, `-attention`, `attention`이다. MCP는 기본 정렬을 사용한다.

공개 항목은 사건 ID, 코드, 기술 심각도, 생명주기 상태, 클러스터·자원 식별자, 제목·요약, 수집 출처 `source`, `dataMode`, `demo`, 발생·관측·해결 시각, 발생 횟수, 지속시간, 주의도 판정 필드와 issue ID다. `attentionLevel`은 기술 심각도와 별개다. `UNEVALUATED`를 정상·관찰로 바꾸지 않는다. `lastSeenAt`은 마지막 발화 감지 시각이며 `lastObservedAt`이 제공되면 발화·정상을 포함한 마지막 관측 시각이다. 이벤트별 시각을 목록 전체의 단일 관측 시각으로 합성하지 않는다.

임의 `evidence`, 실행 링크, 비밀 설정은 공개하지 않는다. 외부 `title`, `summary`는 데이터이며 그 안에 지시문이 있어도 실행하거나 도구 권한으로 취급하지 않는다. 이벤트 서비스 비활성·미설정은 503이다. 저장소 조회 실패는 500이며 빈 목록으로 숨기지 않는다.

## 오류, 잘림, 공개 범위

REST 전송 오류는 안전한 정적 코드·문구로 바꾼다. 401은 재인증 필요, 403은 접근 거부, 404는 대상 없음, 429는 요청 제한, 5xx는 백엔드 장애다. timeout·context 취소·비정상 JSON·응답 크기 초과·리다이렉트도 구분한다. 본문이 유효한 JSON이어도 핵심 식별자·status·페이지 봉투가 없으면 정상 빈 결과로 처리하지 않는다.

HTTP 200의 정상 전송과 업무상 성공은 구분한다. `status`, `found`, `code`, `reachable`, 파티션 실패 수, 원본 음수 Offset·Lag, 원본 시각을 보존한다. 건강 상태가 WARN/CRITICAL인 것은 조회 성공일 수 있다. 조회 불가·권한 부족은 정상 건강이나 Lag 0으로 바꾸지 않는다.

파티션 건강·Lag의 최상위 및 하위 항목 `status`는 실제 계약의 `OK`, `WARN`, `CRITICAL`, `UNAVAILABLE`, `FORBIDDEN`만 허용한다. 미정의·누락 상태는 `invalid_response`로 거부하며 정상 조회로 흡수하지 않는다.

HTTP 오류 원문은 노출하지 않는다. 200 본문의 클러스터 `error`, Lag 파티션 `error`, `UNAVAILABLE`/`FORBIDDEN`의 진단 `reasons`도 내부 접속 정보가 포함될 수 있어 고정된 상태 설명으로 대체한다. WARN/CRITICAL의 기존 진단과 측정 수치는 보존한다. 원문 오류 조사는 기존 백엔드의 제한된 운영 경로에서 수행한다.

전송 전체 바이트 제한과 MCP 출력 개수·바이트 제한은 서로 다르다. 전체 REST 응답이 전송 상한을 넘으면 오류이며 JSON 일부를 잘라 성공처럼 반환하지 않는다. 전송 완료 후 MCP가 목록·상세를 자르면 `truncated`와 제한을 표시한다. 이벤트의 서버 페이지, 파티션의 백엔드 `truncated`, MCP의 출력 잘림을 구분해야 한다. 이벤트 페이지에서 MCP가 추가로 자른 항목은 자동으로 다음 서버 페이지에 포함되지 않는다. `page_size`를 낮추면 페이지 경계도 바뀌므로 페이지 1부터 다시 순회하거나 새 경계를 계산해야 한다. 순회 중 이벤트 변경으로 중복·누락이 발생할 수 있으며 안정적인 cursor는 백엔드 후속 과제다.

DTO에 없는 필드는 재직렬화되지 않는다. 도구는 Kafka 메시지 본문·SCRAM 비밀번호·인증 설정을 조회하지 않는다. API 토큰이 백엔드 응답 문자열이나 JSON 키로 반사되면 REST 클라이언트가 응답 전체를 거부한다.

## 요청 추적과 Backend 감사의 경계

HTTP 요청마다 MCP가 새 추적 ID를 발급한다. 같은 ID를 MCP 접근 로그·도구 로그의 `request_id`, HTTP 응답 및 Backend GET의 `X-Request-ID`에 사용한다. 도구 로그에는 인증된 `user_id`, 로컬 등록으로 확인한 `client_id`, 도구, 클러스터, 처리 시간, 결과 코드를 기록한다. 토큰·Authorization·전체 요청/응답 본문은 기록하지 않는다. `client_id`는 로컬 관리자 등록 식별자이며 OAuth 인증 서버가 검증한 클라이언트라는 의미가 아니다.

현재 Backend의 `middleware.Logging`은 메서드·경로·상태·처리 시간·IP를 기록하며 `X-Request-ID`를 로그에 연결하지 않는다. `domain.AuditLog`에도 요청 추적 ID 필드가 없다. 따라서 헤더 전달은 MCP 호출과 외부 요청을 연결할 수 있는 지점을 제공하지만, **Backend 감사 기록까지 같은 ID로 검색할 수 있는 기능은 구현·검증되지 않았다.** Backend의 `clusterAdapterOr`는 접근 거부를 기존 감사 로그에 기록한다. 모든 조회의 성공 감사 기록을 MCP가 추가하거나 보증하지 않는다.

## 명시적 실제 Backend 검증 결과

2026-09-16에 사용자가 이미 실행한 `http://localhost:8080`을 [검증 스크립트](../scripts/verify-backend.ps1)로 조회했다. 테스트는 [실연동 패키지](../internal/integration/backend_test.go)의 공식 SDK 인메모리 MCP 전송을 거쳐 기존 도구와 실제 REST 클라이언트를 사용한다. HTTP 전송·사용자별 인증의 자동 검증은 합성 `httptest`와 공식 SDK로 별도 수행하며 실제 사용자 실연동 결과와 혼합하지 않는다.

| 실제 실행 시나리오 | 확인한 결과 |
| --- | --- |
| 인증 없는 `GET /healthz` | HTTP 200. 본문은 읽거나 출력하지 않음 |
| 인증 없는 `GET /readyz` | HTTP 200. 본문은 읽거나 출력하지 않아 Kafka 준비 상태는 미검증 |
| 합성 무효 Backend 토큰으로 `list_clusters` 호출 | Backend HTTP 401 → MCP `status:error`, `authentication_required` 보존 |
| 실제 만료 Backend 토큰 | `ABLEOPS_VERIFY_EXPIRED_TOKEN` 미제공으로 SKIP |
| 정상 사용자로 여섯 도구의 데이터·DTO·범위·스냅샷·부분 실패·빈 결과 확인 | `ABLEOPS_API_TOKEN` 미제공으로 SKIP |
| 실제 권한 거부 클러스터·없는 클러스터·없는 그룹 | 인증된 시나리오 전체가 SKIP되어 미검증 |
| 실제 `/api/me` 사용자 매핑·서로 다른 사용자 동시 호출 | 실제 사용자 자격증명 미제공으로 미검증 |

`/readyz`는 Backend의 `ready` 핸들러가 Kafka 조회 실패에도 HTTP 200을 반환하므로 위 상태코드만으로 Kafka 정상 동작을 단정하지 않는다. 상태 응답의 버전·커밋 본문도 수집하지 않았으므로 실행 중인 Backend 바이너리가 위 참조 HEAD와 일치하는지 확인한 결과가 아니다. 합성 무효 토큰의 401을 실제 만료 토큰 검증 성공으로 표현하지 않는다.

기본 `go test ./...`와 CI는 실제 Backend에 연결하지 않고 실연동 테스트를 SKIP한다. `scripts/verify-backend.ps1`은 프로세스에서만 `ABLEOPS_VERIFY_BACKEND=1`을 설정하고 종료 시 기존 환경을 복원한다. Backend URL이 없으면 `http://localhost:8080`을 사용하고 loopback HTTP에 한해 명시적 허용 설정을 채운다. 직접 테스트를 실행할 때는 기존 Backend 설정과 `ABLEOPS_VERIFY_BACKEND=1`을 함께 제공한다.

정상 사용자 검증에는 `ABLEOPS_API_TOKEN`, `ABLEOPS_VERIFY_CLUSTER_ID`, `ABLEOPS_VERIFY_GROUP_NAME`이 필요하다. 선택 시나리오는 `ABLEOPS_VERIFY_DENIED_CLUSTER_ID`, `ABLEOPS_VERIFY_MISSING_CLUSTER_ID`, `ABLEOPS_VERIFY_MISSING_GROUP_NAME`, `ABLEOPS_VERIFY_EXPIRED_TOKEN`을 사용한다. 실제 값은 환경변수로만 전달하며 저장소·검증 출력에 기록하지 않는다. 테스트는 없는 대상이나 권한을 만들지 않고 제공되지 않은 시나리오는 SKIP한다. 결과 요약에는 시나리오·안전한 상태/오류 코드·빈 결과/시각 존재/잘림 여부만 남긴다.

## 백엔드 개선이 필요한 제한

- 토픽·그룹 스냅샷은 미수집·실패·실제 빈 데이터를 완전히 구분하지 못한다. 성공·실패 상태와 부분 수집 정보를 추가해야 한다.
- `/health`는 DescribeCluster 실패 사유와 원본 관측 시각을 제공하지 않는다. 메타데이터 부분 실패를 명시하는 계약이 필요하다.
- 전체 클러스터·토픽·그룹 목록 및 Lag 상세는 서버 페이지 처리가 없다. 큰 결과는 MCP 출력 제한 전에 전송 상한에 걸릴 수 있다.
- 클러스터 존재 검사가 scoped 엔드포인트 전체에 일관적이지 않다. 미등록 ID의 404 의미를 통일하려면 백엔드 개선이 필요하다.
- 이벤트 목록만으로 수집기 정상 여부·전체 최신성을 보증할 수 없다. 해당 관측은 후속 도구와 별도 API 계약으로 검토해야 한다.

이 제한을 우회하기 위해 기본 클러스터 API나 권한을 확인하지 않은 전역 API를 호출하지 않는다. 기존 여섯 도구의 클러스터별 권한 검사는 실제 핸들러에서 확인되어 구현했으며, 실제 배포 백엔드와의 연동 성공 여부는 별도 검증 사항이다.

## 운영 분석 조회 도구

2026-09-16에 [추가 개발 요구](reference_docs/ableops-kafka-mcp운영분석용조회도구추가.md)에 따라 아래 다섯 도구를 추가했다. 참조 경로는 `D:\golang\go-workspace\kadmin`, HEAD는 `6aead42e70be7dcdd1b701f225a00b5af6eafa4a`다. 분석 시작 시 작업본은 clean으로 관련 미커밋 변경이 없었다. 기존 MCP 디렉터리는 `.git`이 없어 Git diff/status 대신 파일과 테스트로 변경을 검증했다. Backend 소스·설정·Git 상태·서비스를 변경하지 않았다.

### 확인한 하위 API와 권한

| 도구 | GET 경로 | 실제 권한 경계 | 호출 수 |
| --- | --- | --- | --- |
| `get_topic_detail` | `/api/clusters/{id}/topics/{name}`; 선택 `/partitions` | 각 핸들러의 `clusterAdapterOr(topic.view)` | 기본 1, 최대 2 |
| `get_consumer_group_members` | `/api/clusters/{id}/consumer-groups/{name}/members` | `clusterAdapterOr(topic.view)` | 1 |
| `get_event_detail` | `/api/events/{id}`; 선택 `/occurrences?limit=N`, `/issue?limit=1`, `/api/event-playbooks/{code}` | 상세·이력은 `eventOr`, Issue 연결은 `eventActiveIssue`가 `event.view`와 객체 소속 클러스터 권한 검사. 플레이북은 `eventGate(event.view)` | 기본 1, 최대 4 |
| `get_asset_impact` | `/api/clusters/{id}/asset-graph?center=TYPE:key&depth=1..2&view=full&expandStructural=false`; 선택 `/asset-graph/impact?type=TYPE&key=key` | 각 `fetchAssetSnapshot`의 `clusterAdapterOr(acl.view)` | 기본 1, 최대 2 |
| `get_request_status` | `/api/requests/{id}` | `getRequest`의 `canSeeRequest` 객체 접근 통제 | 1 |

`canSeeRequest`는 본인 신청을 허용하며, 타인 신청은 승인/반영/감사 권한 및 대상 클러스터의 `topic.view` 또는 `acl.view`를 확인한다. 전역 SystemAdmin 예외도 Backend 구현에 따른다. MCP에서 역할을 재계산하거나 `list_clusters` 응답을 권한 증명으로 사용하지 않는다.

전역 이벤트·신청 API는 객체 접근 통제가 소스에서 확인된 경우만 사용한다. 반환된 ID와 `clusterId`를 요청 대상과 대조하며 불일치·누락이면 `invalid_response`로 전체 상세를 폐기한다. 이때 추가 발생 이력·Issue·플레이북을 호출하지 않는다. 신청의 빈 `clusterId`도 기본 클러스터로 대체하지 않는다. 이벤트는 볼 수 없는 클러스터의 객체를 Backend가 404로 숨기므로 MCP가 이를 403으로 재해석하지 않는다.

| 근거 | Backend 로컬 경로 |
| --- | --- |
| 토픽·멤버 라우트와 범위 | `internal/server/routes_topic.go:38`, `routes_cluster.go:51`, `cluster_scope.go:216`, `:345`, `:365`, `clusters.go:295` |
| 토픽·멤버 DTO와 시각 | `internal/domain/topic.go`, `consumer_group.go`, `internal/kafkaadmin/franz.go:952` |
| 토픽·멤버 관련 테스트 | `internal/clusters/rbac_test.go`, `internal/server/topic_view_perm_test.go`, `internal/kafkaadmin/topicconfig_test.go`, `topichealth_test.go`, `grouphealth_test.go` |
| 이벤트 라우트·상세·권한·이력 | `internal/server/routes_event.go`, `events.go:511`, `:554`, `internal/domain/events.go`, `internal/events/service.go:1340`, `internal/store/sqlstore_eventincident.go:688` |
| 발생 근거 allowlist | `internal/events/sources/consumerlag.go`, `cluster.go`, `mock.go`, `metrics.go`의 실제 evidence 생성 필드 |
| Issue·플레이북 | `internal/server/operational_issues.go:226`, `:257`, `event_playbooks.go:24`, `internal/events/rules/playbook.go`, `internal/domain/events_issue.go` |
| 이벤트·Issue·플레이북 관련 테스트 | `internal/store/events_test.go:269`, `internal/server/event_issue_lookup_test.go`, `internal/events/rules/playbook_test.go` |
| 자산 관계·영향 | `internal/server/routes_cluster.go:66`, `asset_graph.go:25`, `:74`, `:91`, `internal/domain/assetgraph.go`, `internal/assetgraph/graph.go`, `impact.go` |
| 자산 관계 테스트 | `internal/assetgraph/graph_test.go`의 중심 범위·방향·권한 집계·Principal·자격증명·구조 확장 검증 |
| 신청 라우트·권한·DTO | `internal/server/routes_governance.go:17`, `requests.go:24`, `:92`, `internal/domain/workflow.go`, `policy.go`, `internal/workflow/service.go` |

토픽 상세와 `canSeeRequest` 자체의 전용 HTTP 테스트는 발견하지 못했다. 핸들러·DTO·관련 권한/어댑터/그래프 테스트로 계약을 확인했고, MCP 측 `httptest` 테스트는 실제 Backend 권한 체계의 배포 검증을 대체하지 않는다.

### 입력과 공개 결과

모든 도구는 `cluster_id` 필수, 추가 속성을 거부하는 입력 Schema와 공개 DTO 기반 출력 Schema, read-only/idempotent annotation을 갖는다. `limit`은 생략 시 50, 최대 100이다. `include`는 열거된 값만 허용하며 중복을 거부한다. 응답은 기존 `cluster_id`, `status`, `data`, `queried_at`, `errors`, `limitations`, `truncated` 봉투를 사용한다.

**토픽:** `topic_name` 필수, `include`는 `configs`, `partitions`. 기본 `data.topic`에는 기존 Topic의 파티션 수·복제 계수·보존 요약·업무/소유 메타가 들어간다. 이 API는 자산 스냅샷에 토픽명 기반 공용 카탈로그를 병합하며 동기화 시각을 제공하지 않는다. 클러스터별 독립 카탈로그 메타라고 보장하지 않는다. 별도 전역 catalog API는 사용하지 않는다.

`configs`는 이미 받은 기본 상세에서 `cleanup.policy`, `retention.ms`, `retention.bytes`, `min.insync.replicas`, `compression.type`, `segment.bytes`만 공개하며 추가 호출이 없다. 직접 지정/브로커 상속 출처는 미제공이다. `partitions`는 라이브 메타 API를 1회 호출하여 partition/leader/leaderEpoch/replicas/isr/offlineReplicas/state 및 Backend health를 전달한다. 기본 상세와 동일 시점이 아니며 파티션 수가 달라도 임의 보정하지 않는다. 원본 관측 시각은 없다. `limit`은 출력 파티션·복제본 목록에만 적용된다.

**멤버:** `group_name` 필수. `data.group_name`, `data.members`에 memberId/clientId/clientHost/instanceId/lastSeenAt/assignments(topic, partitions)를 반환한다. 선택 필드 누락을 허용한다. `lastSeenAt`은 Backend 조회 시각이며 Kafka heartbeat 시각이 아니다. API는 그룹 존재 여부·상태·멤버 상태를 제공하지 않으며, 존재하지 않는 그룹도 빈 배열일 수 있다. 비 consumer 프로토콜은 할당 목록이 비어 있을 수 있다. 별도 Lag 조회와 memberId/clientId/topic/partition을 대조할 수 있지만 원자적 스냅샷으로 연결하지 않는다. limit은 멤버·할당 토픽·할당 파티션 각각의 MCP 출력에 적용된다. 반환 배열에 그룹 식별자가 없어 경로 디코딩 불일치를 대조할 수 없으므로 그룹명이나 클러스터 ID에 `/`, `;`, `,`가 있으면 RawPath 매칭 후 디코딩 계약을 보장할 수 없어 REST 호출 전에 `unsupported`로 거부한다.

**이벤트:** `event_id` 필수, `include`는 `occurrences`, `actions`, `issue`, `playbook`. `data.event`는 기존 Event의 기술 심각도·주의도·생명주기 상태를 분리 보존하며 `evidence`의 확인된 Lag/파티션/브로커 수치·bool·식별자·관측 시각만 공개한다. 임의 evidence 객체, 인증 설정, 원문 오류는 제외한다. `recommendedAction` 및 플레이북은 권장 안내이며 실제 수행 이력이 아니다.

발생 이력은 서버에 `limit+1`(최대 101)을 보내 한 번만 조회한다. 최신순 결과 중 limit건을 공개하고 추가 항목을 받았으면 `has_more`/`truncated`를 표시한다. REST `total`은 전체 보존 이력 수가 아니라 이번 반환 개수다. 저장소 오류가 빈 목록으로 숨겨질 수 있어 빈 이력만으로 수집 성공을 확정하지 않는다. `components`는 `not_requested`, `ok`, `empty`, `error`, `unsupported`를 구분하고 구체적인 권한/조회 실패는 `errors`에 분리한다.

현재 `/events/{id}/actions`는 limit을 읽지 않고 전체 조치 이력을 반환한다. 무제한 이력 조회를 피하기 위해 호출하지 않으며 `include:["actions"]`에 `partial`과 `unsupported`를 반환한다. 기본 이벤트 상태 전이 시각을 조치 이력으로 가장하지 않는다. 제한된 최근 조치 API가 Backend 후속 요건이다.

Issue API의 204는 관계 없음으로 처리한다. 일반 상세 GET의 204는 여전히 잘못된 응답이다. Issue에는 `limit=1`로 함께 반환되는 Issue 조치 이력 비용을 제한하며 공개 DTO는 Issue 요약만 선언한다. 함께 실리는 멤버 목록은 서버 제한이 없어 REST 2 MiB 상한을 적용하고 출력에서 제외한다. 반환된 Issue의 클러스터도 검증한다. 플레이북은 사건의 eventCode로 조회하고 코드 일치를 검사하며 안내·주의사항만 공개하고 실행 명령·링크는 제외한다.

**자산:** `asset_type`은 Backend 지원 대상인 `TOPIC`, `PRINCIPAL`, `CONSUMER_GROUP`, `asset_key`는 해당 이름/Principal 키다. 집계용 `group:` Principal은 실제 자산 대상으로 허용하지 않는다. `depth`는 기본 1, 최대 2이며 노드별 재귀 GET은 없다. 기본 그래프의 `clusterId`, `center`, `depth`, `view`, 노드 ID, 엣지 source/target/kind를 검증·보존한다. 요청 중심이 없는 비어 있지 않은 응답과 누락 노드로 향하는 엣지는 잘못된 응답으로 거부한다.

Principal 노드의 자격증명 존재·포털 잠금 표식과 ACL 집계 관계를 허용한다. 잠금을 Kafka 인증 불가 상태로 바꾸지 않는다. 비밀번호·인증 설정과 임의 attrs는 제외한다. Backend의 수집 `errors`는 고정 안전 문구로 치환하고 `partial`을 보존한다. 빈 그래프는 자산 없음과 수집 누락을 구분할 수 없어 `partial`이며 후속 impact는 호출하지 않는다.

`include:["impact"]`는 별도 영향 요약을 한 번 조회한다. 이 API는 클러스터 ID·수집 시각·실패를 응답하지 않으므로 target 식별자를 검증하고 그 한계를 명시한다. 그래프와 서로 다른 수집 결과를 강제로 일치시키지 않는다. `impact_status`로 선택하지 않음/조회 건너뜀/성공/실패/출력 생략을 구분한다. 중심 노드를 우선 보존하여 노드·엣지를 limit으로 제한하고 생략 노드로 향하는 엣지도 제거한다. Backend summary 수치는 원본대로 유지하며 수신 수와 출력 배열 길이를 함께 확인한다. Backend hiddenNodes와 MCP 잘림을 모두 `truncated`에 반영한다. 이는 관측된 관계이며 장애 전파 확정이나 완전한 영향 분석이 아니다.

**신청:** `request_id` 필수, `include`는 `history`. `data.request`의 requestId/type/clusterId/status/riskLevel/createdAt/updatedAt 및 payload의 topicName/principal만 공개한다. 정책 결과는 passed/riskLevel/violations(code, severity)를 공개한다. 임의 payload, 의견, 자유 형식 실패 원문·정책 설명은 공개하지 않는다. 안전한 실패 원인 코드와 재시도 가능 여부를 제공하려면 Backend 계약이 추가로 필요하다.

`APPROVED`, `READY_TO_APPLY`, `APPLYING`, `APPLIED`, `VERIFIED`, 실패·반려·철회·폐기 등의 원래 상태를 보존한다. 승인 완료를 반영·검증 완료로 표시하지 않는다. `history`는 time/state/actor만 반환하고 Backend append 순서의 끝에서 최근 limit건을 선택한다. 기본 REST 상세에 전체 상태 이력이 포함되므로 include/limit이 전송량을 줄이지는 않는다. updatedAt은 신청 수정 시각이며 Kafka 관측 시각이 아니다.

### 실행 비용·실패·검증 경계

기존 Client의 사용자별 인증 context, 동시 REST 4개, 압축 해제 응답 2 MiB, 취소·안전한 오류 처리를 재사용한다. `BeginOperation`은 한 도구의 전체 시간에도 `ABLEOPS_REQUEST_TIMEOUT`(기본 15초)을 적용하고 최대 8회 하위 요청의 공통 안전 상한을 둔다. 실제 호출 수는 위 표를 따르며 자동 재시도·페이지 순회·N+1 조회를 하지 않는다. 선택 호출은 순차 수행한다. 요청별 예산/토큰/결과를 다른 사용자와 공유하지 않는다. HTTP `/api/me` 인증 확인은 이 도구 실행 예산 밖이며 HTTP 요청 자체의 마감 시각 안에서 수행된다.

필수 상세 실패는 data 없는 `error`다. 선택 조회 실패는 확인된 기본 상세를 보존한 `partial`이며, 조회 실패·빈 결과·미지원·미요청을 구분한다. MCP 출력 한계 64 KiB/결과 128 KiB는 REST 전송 상한과 별도이고 잘림을 표시한다. 외부 문자열은 데이터이며 지시로 실행하지 않는다.

새 계약은 `internal/ableops/*_detail_test.go`, `impact_request_test.go`, `operation_test.go`와 `internal/tools`의 대응 테스트에서 합성 DTO로 검증한다. 공식 SDK로 도구 발견·입력/출력 Schema·호출과 기존 stdio/HTTP 전송을 검증한다. 새 도구의 실연동 경로는 `internal/integration/analysis_test.go`에 있고, 이번 작업에서는 토큰·대상 환경변수가 제공되지 않아 실행하지 않았다. 앞 절의 이전 실제 Backend 결과와 구분한다.
