# Lag·모니터링 도구

모든 도구는 `cluster_id`를 필수로 받는다. `get_consumer_target_throughput`만 REST 3회, 나머지는 지정한 클러스터의 REST API를 한 번 호출한다. 백엔드가 매 요청의 사용자·클러스터 권한을 검사하며 MCP는 기본 클러스터나 전역 레거시 API로 우회하지 않는다. 임의 경로·URL·PromQL·해상도 입력은 지원하지 않는다.

| 도구 | REST 계약 | 백엔드 권한 | 질문·인자 예시 |
| --- | --- | --- | --- |
| `get_consumer_lag_overview` | `GET /api/clusters/{id}/consumer-lag?refresh=true` (`refresh` 선택) | 대상 `topic.view` | “합성 클러스터의 그룹별·토픽별 Lag 판정과 경보를 보여줘.” `{"cluster_id":"sample-a","limit":20}` |
| `get_consumer_lag_policy` | `GET /api/clusters/{id}/consumer-lag/policy?topic=&group=` | 대상 `topic.view` | “합성 orders 토픽을 읽는 billing 그룹의 적용 임계치와 상속 근거는?” `{"cluster_id":"sample-a","topic_name":"orders","group_name":"billing"}` |
| `get_metric_series` | `GET /api/clusters/{id}/charts/{metric}?range=` | 대상 `topic.view` | “합성 클러스터의 최근 한 시간 Lag 추이를 보여줘.” `{"cluster_id":"sample-a","metric":"consumer-lag","range":"1h","max_points":30}` |
| `get_cluster_storage` | `GET /api/clusters/{id}/storage` | 대상 `topic.view` | “합성 클러스터의 브로커별 Kafka 저장량과 분포는?” `{"cluster_id":"sample-a","limit":20}` |
| `get_cluster_config_audit` | `GET /api/clusters/{id}/config-audit` | 대상 `acl.view` | “합성 클러스터의 토픽 설정 정책 위반을 보여줘.” `{"cluster_id":"sample-a","limit":20}` |
| `get_partition_reassignments` | `GET /api/clusters/{id}/reassignments` | 대상 `topic.view` | “합성 클러스터에서 진행 중인 파티션 재배치는?” `{"cluster_id":"sample-a","limit":20}` |
| `get_consumer_target_throughput` | `GET /api/clusters/{id}/consumer-lag/throughput` · `GET …/consumer-lag/throughput/history?group=&topic=&range=` · `GET …/consumer-lag` | 대상 `topic.view`(세 경로 모두) | “합성 billing 그룹이 orders 토픽을 유입보다 빨리 처리하고 있나?” `{"cluster_id":"sample-a","group_name":"billing","topic_name":"orders","range":"15m"}` |

예시는 모두 합성 식별자다. 정책에서 토픽·그룹을 생략하면 클러스터 정책, 토픽만 지정하면 토픽 정책, 둘을 지정하면 그룹×토픽 정책이다. 그룹만 지정할 수 없다. API가 트리 계층을 모두 반환하므로 후속 N+1 호출은 하지 않는다.

## 해석과 제한

- Lag의 Kafka `state`, 현재 `status`, 발생 경보 `alert.phase`는 구분한다. `PENDING`은 지속 조건 확인 중이고 `recovering=true`는 경보가 열려 있으나 정상 회복 확인 중임을 뜻한다. 다른 그룹의 Lag 합계는 토픽 미처리 메시지 수가 아니다.
- 정책 `stored/layers`는 저장 설정이고 `inherited/effective`는 적용 결과다. `INHERIT`, `SET`, `DISABLED`와 감시 비활성을 보존한다. 지속/회복 `value=0`과 값 누락은 다르며 `ruleBased`는 이벤트 규칙 사용을 뜻한다. 정책 API에는 이벤트 규칙의 실제 횟수·시간이 없으므로 필요하면 overview의 `eventRule`을 별도로 확인한다.
- `checkedAt`은 Lag 관측 시각이고 `topicsSyncedAt`은 “소비 그룹 없음”에 사용한 자산 스냅샷 시각이다. 자산 시각 미제공은 `partial`로 표시한다. 업무명은 토픽 이름 기준의 클러스터 공통 카탈로그 메타다.
- Lag 경보 조회가 `DISABLED` 또는 `UNAVAILABLE`이면 `partial`과 오류를 함께 반환한다. 이를 경보 0건으로 표시하지 않는다. 백엔드는 활성 경보를 최대 50페이지까지 읽지만 잘림 플래그를 주지 않으므로 경보 집계 완전성을 보장하지 않는다.
- 저장량의 `totalSize/avgSize`는 성공 브로커의 Kafka 로그 bytes다. OS 디스크 사용률(%)이나 메시지 수가 아니다. `maxOffsetLag/futureMaxOffsetLag`는 offset 차이이며 시간 지연이 아니다. `partial`, `failedBrokers`, `skewSuppressed`, `skewEvaluated`, `leaders.evaluated`를 보존한다.
- 설정 진단 `total`은 실제로 진단한 토픽 수다. 백엔드 상한은 기본 200개이며 운영 설정으로 바뀔 수 있다. 백엔드와 MCP의 `truncated`를 보존한다. 정책 예외는 백엔드의 기본 클러스터에서만 적용되며 MCP가 그 클러스터로 대체하지 않는다.
- 재배치 `observedSince/observedMinutes`는 포털 관측 기준 추정치다. `progressPct`는 추가 복제본의 ISR 진입 비율이며 이관 bytes 비율이 아니다. 실행·중단 API는 호출하지 않는다.
- Lag·저장량·설정 진단·재배치 응답에는 어댑터의 실측/mock 구분이 없다. 출처를 `backend_*`로 표시하고 구분 불가를 제약으로 제공하며 성공 응답을 실측·정상으로 단정하지 않는다.

## 시계열

`metric`은 `consumer-lag`, `topic-throughput`, `cluster-stability`만 허용한다. 기간 생략 시 `1h`다. 임의 시작·종료 시각이나 해상도를 백엔드에 전달하지 않는다.

| 기간 | 고정 해상도 | 백엔드 요청 포인트 예산 |
| --- | --- | --- |
| `15m` | 60초 | 15 |
| `1h` | 60초 | 60 |
| `6h` | 300초 | 72 |
| `24h` | 900초 | 96 |
| `7d` | 7,200초 | 84 |

`max_points`는 기본 기간별 예산, 최대 96이다. 기간별 예산보다 크면 그 예산을 적용한다. 최신 시점부터 최대 개수만 남기고 원래의 시간순을 보존한다. 다운샘플링은 하지 않는다. `step_seconds`, `expected_points`, `received_points`, `returned_points`, `point_limit`, `truncated`를 함께 반환한다. 시리즈는 최대 6개이며 각 포인트에서 공개된 시리즈의 값만 반환한다.

`clusterScoped=false`인 응답은 전역 Prometheus 데이터를 포함할 수 있으므로 수치를 폐기하고 `unsupported`로 응답한다. 선택 클러스터 이름만으로 전역 값을 해당 클러스터 관측처럼 노출하지 않는다.

`demo=true`는 `data_mode=demo`, `partial`로 반환한다. 백엔드의 “Kafka 현재값 + 합성 과거 추이”도 데모에 포함한다. 합성값을 실측 분석에 사용하면 안 된다. 처리량 실측은 `bytes/s`, 데모는 `msg/s`, Lag는 백엔드 단위 `messages`(커밋 offset 차이), 복제 안정성은 `partitions`다.

백엔드 `TimeSeries`에는 표본별 품질·부분 오류 정보가 없다. 특히 `pointsToSeries`는 다중 시리즈에서 누락된 표본을 0으로 채우며 일부 시리즈 조회 실패도 숨길 수 있다. 따라서 다중 시리즈 실측은 `partial`, `quality_verified=false`로 반환하고 0을 실측 0이나 정상으로 확정하지 않는다. 빈 응답·예산보다 짧은 응답도 `partial`로 반환하며 미수집·장애·수집 시작을 추측하지 않는다. `points.ts`는 원본 시각이고 `queried_at`은 MCP 조회 완료 시각이다.

## Consumer Group × Topic 처리량 (`get_consumer_target_throughput`, v0.6.2)

`cluster_id`·`group_name`·`topic_name`이 모두 필수이고 `range`만 선택이다(`5m`·`15m`·`1h`·`6h`, 기본 `15m` — 포털 대상 상세 추이 카드와 백엔드의 기본값). `limit`·임의 기간·해상도·PromQL은 받지 않으며 `additionalProperties=false`다. 백엔드는 미지원 기간을 조용히 `15m`으로 바꾸므로 MCP가 먼저 거부하고, 응답 `range`가 요청과 다르면 시계열을 쓰지 않는다.

**정확한 스코프**: 세 REST 응답 모두에서 `group`과 `topic`이 **정확히 일치하는 대상 1건만** 남긴다. 클러스터 리포트의 다른 그룹·다른 Topic 행과 요약·Topic 목록은 반환하지 않고, 일치 대상이 없으면 `target_not_found`로 알리며 다른 대상으로 대체하지 않는다. 클러스터 단위 `get_metric_series`를 이 대상의 추이처럼 쓰지 않는다.

| 출처(`data.sources[].source`) | REST | 반환 |
| --- | --- | --- |
| `current` | `/consumer-lag/throughput` | `produceRate`·`consumeRate`·`lagGrowthRate`·`processingRatio`·`recoveryEtaSec`(각 `status`·`value`·`windowSec`), `currentLag`, `lagObservation`, `processingState`·`processingReason`, `checkedAt`, 수집 경로 `produceSource`/`consumeSource`(`kind`·`label`·`windowSec`·`warning`) |
| `history` | `/consumer-lag/throughput/history` | `produce`·`consume`(msg/s)·`lag`(messages) 계열별 `status`·`unit`·`windowSec`·`points`, `range`·`stepSec`·`from`·`to`, `expected_points` |
| `rebalance` | `/consumer-lag` | 대상 행의 `rebalanceCorrelation`(`state`·`reason`·`sinceEndSec`), 그룹 행의 재조정 관측(`state`·관측 시각·`observedDurationSec`·`recentCount`·`prolonged`), `semantics="correlation_hint_not_causation"` |

**의미 보존**

- 값은 `status=MEASURED`일 때만 `value`가 있다. `NO_DATA`·`QUERY_FAILED`·`FORBIDDEN`·`NOT_COLLECTED`·`NOT_APPLICABLE`를 0으로 바꾸지 않는다. 시계열의 빈 구간에 점을 만들지 않고 보간하지 않는다.
- `lagGrowthRate = produceRate − consumeRate`(양수가 Lag 증가), `processingRatio = consumeRate/produceRate`(1.0으로 자르지 않음), `recoveryEtaSec`은 처리 > 유입이고 Lag > 0일 때만 백엔드가 계산한다.
- `processingState`(`HEALTHY`·`DEGRADING`·`RECOVERING`·`STALLED`·`UNOBSERVED`)는 백엔드 판정을 그대로 옮긴다. MCP가 재계산하지 않고, Lag 위험도(`OK/WARN/CRITICAL`)와 대응시키지 않는다(`CRITICAL`+`RECOVERING`은 정상적인 조합).
- 재조정 상관(`NONE`·`REBALANCE_ACTIVE`·`REBALANCE_RECENT`·`REBALANCE_RECOVERY`·`REBALANCE_PERSISTENT_IMPACT`·`REBALANCE_REPEATED`·`UNOBSERVED`)은 **상관 힌트이지 원인 판정이 아니다.** 관측 시각·지속시간은 포털 관측 기준 하한값이다. 백엔드가 상관을 결합하지 않은 경로(필드 없음)와 `UNOBSERVED`는 “재조정 없음”이 아니다.

**Partial**: 세 출처는 서로 독립적으로 성공·실패한다. 자동 재시도·페이지 순회는 없다. 한 출처라도 REST 실패·대상 없음이거나, 성공했어도 유입/처리 현재값이 실측이 아님·Lag 관측 `PARTIAL`·시계열 계열 미실측 또는 기대 버킷보다 짧음·재조정 미판정이면 `status=partial`이다. 세 출처 모두 실패하면 `status=error`이고 `data`를 싣지 않는다. 출처별 결과는 `data.sources`(`ok`·`partial`·`not_found`·`error`)와 `errors[].component`(`throughput_current`·`throughput_history`·`rebalance`)로 구분한다. MCP 출력 상한을 넘으면 각 계열의 오래된 점부터 절반씩 줄이고 `truncated=true`로 표시한다.

**노출 최소화**: 백엔드 응답의 PromQL 원문(`query`)은 DTO에 선언하지 않아 버린다. 비실측 사유·수집 경로 `warning`·`UNOBSERVED` 판정 사유에는 Prometheus 접속 오류 원문이 섞일 수 있어 상태별 고정 문구로 바꾼다. 실측 판정 사유(`processingReason`)와 재조정 상관 사유는 백엔드가 만든 사용자용 문장이라 보존한다.

**Annotation**: 현재값 경로는 `/consumer-lag`와 같은 Lag 관측(`OverviewReport`)을 거쳐 최초 자산 스냅샷 저장·포털 샘플러 표본 축적이 일어날 수 있고, `/consumer-lag` 호출도 같다. 따라서 GET만 쓰지만 `readOnlyHint=false`, `idempotentHint=false`, `destructiveHint=false`다.

**부하**: 도구 호출 1회에 REST 3회(현재값·시계열·Lag 관측)를 순서대로 보낸다. 시계열은 백엔드가 Prometheus 레인지 조회 3회를 수행한다. 현재값과 Lag 관측은 각각 Kafka Lag 관측 1회를 유발한다. 공통 timeout·호출 예산·취소가 세 호출 모두에 적용된다.

## `get_consumer_group_lag`의 `topic_name` (v0.6.2)

선택 인자 `topic_name`을 지정하면 그룹 전체 응답에서 그 Topic의 `partitions[]`·`topicLag[]`만 남기고 `data.topicFilter`에 Topic을 표시한다. 필터는 **`limit`보다 먼저** 적용한다 — 파티션이 많은 그룹에서 다른 Topic이 앞 `limit`건을 차지해 요청 Topic이 잘리던 문제를 없앤다. 부분 실패 판정도 그 Topic의 `errorPartitions` 기준이다. `totalLag`·`maxPartitionLag`·`status`·`reasons` 등 그룹 집계와 판정은 그룹 전체 값으로 유지하고 제약에 적는다. 그룹은 있으나 Topic 파티션이 없으면 `topic_not_found`(그룹 미존재 `not_found`와 구분)이며 Lag 0으로 보고하지 않는다. `topic_name`을 생략하면 입력·출력·백엔드 경로·권한(`topic.view`)이 이전과 같다.

## 부작용·노출·부하 경계

Lag overview는 최초 자산 조회 시 백엔드 `SyncAssets`가 스냅샷을 저장할 수 있다. 저장량 조회는 백엔드에 `STORAGE_VIEW` 감사 기록을 생성한다. 두 도구는 `readOnlyHint=false`, `idempotentHint=false`, `destructiveHint=false`이며 annotation은 권한 검사나 승인으로 사용하지 않는다. 다른 네 도구는 조회용 annotation을 유지한다. MCP는 GET만 전송한다.

DTO는 인증 설정·토큰·메시지·로그 디렉터리 경로·진단 `actual/message`·정책 수정자 정보를 제외한다. Kafka/DB 원문이 포함될 수 있는 오류 사유는 일반 안내로 대체하고 상태·규칙 코드·예외·임계치는 보존한다. 저장량·재배치 API는 응답에 클러스터 ID가 없으므로 모호한 인코딩이 발생할 수 있는 `/`, `;`, `,` 포함 클러스터 ID는 호출 전에 거부한다.

목록 `limit`은 기본 50, 최대 100이며 각 목록의 MCP 출력만 제한한다. 백엔드 REST는 전체 응답을 반환하므로 출력 제한이 네트워크 전송량이나 백엔드 내부 작업을 줄이지 않는다. Config audit의 백엔드 내부 토픽별 조회, Lag 경보 페이지 조회, Prometheus 폴백은 백엔드 자체 동작이며 MCP가 추가 순회하거나 재시도하지 않는다. 기존 전체 실행 timeout·호출 예산(8)·전송 동시성(4)·REST 2 MiB·구조화 출력 64 KiB·MCP 결과 128 KiB 제한을 공유한다. `get_consumer_target_throughput`을 제외하면 이 기능군은 도구당 REST 1회다.

## 로컬 계약 근거와 검증

참조 저장소는 `/Users/seokbong/dev/go-workspace/kafka-control-portal`, HEAD는 `6aead42e70be7dcdd1b701f225a00b5af6eafa4a` (`v1.7.2`)다. 확인 시 관련 미커밋 변경은 없었다. 읽기 전용으로 확인했으며 백엔드 서비스를 시작하거나 테스트를 실행하지 않았다. 전체 참조 기록은 [API 매핑](api-mapping.md)에 통합한다.

| 확인 범위 | 로컬 근거 |
| --- | --- |
| 업무 의미 | `docs/business-design/08-monitoring.md` |
| 경로·권한 | `internal/server/routes_cluster.go`, `routes_dashboard.go`, `clusters.go:clusterAdapterOr`, `metrics_cluster.go:clusterChartGate` |
| Lag 관측·정책·권한 테스트 | `internal/server/consumer_lag.go`, `consumer_lag_test.go`, `internal/domain/consumer_lag.go`, `internal/consumerlag/service.go`, `service_test.go`, `internal/kafkaadmin/grouptarget_test.go` |
| 최초 스냅샷 저장·경보 페이지 상한 | `cmd/api/main.go:consumerlag.Options`, `internal/clusters/sync.go:snapshot`, `internal/consumerlag/service.go:listActiveIncidents` |
| 시계열 범위·실측/데모·스코프 | `internal/server/metrics_cluster.go`, `internal/domain/metrics.go`, `internal/charts/service.go`, `service_test.go`, `internal/metricsource/prometheus.go:pointsToSeries` |
| 저장량·감사·부분 실패 | `internal/server/storage_health.go`, `internal/domain/storageview.go`, `internal/kafkaadmin/storagehealth.go`, `storagehealth_test.go` |
| 설정 진단 | `internal/server/policy_diagnosis.go`, `internal/domain/diagnosis.go`, `internal/policy/configaudit.go` (설정 진단 전용 백엔드 테스트 없음) |
| 재배치·추정치 | `internal/server/partition_health.go`, `internal/domain/partitionview.go`, `internal/kafkaadmin/reassignhealth.go`, `reassignhealth_test.go` |

`internal/tools/monitoring_test.go`는 `httptest`와 공식 SDK 클라이언트로 6개 도구 발견·Schema·필터·정책 상속·Lag 관측/판정/경보 구분·시계열 기간/해상도/단위/데모/범위·출력 제한·부분 실패·HTTP 200 본문 실패·민감 필드 제거를 검증한다. `internal/ableops/monitoring_test.go`는 전체 timeout·취소·재시도 없음·정책 소속·모호한 경로 거부를 검증한다. `go test ./internal/ableops ./internal/tools -run TestMonitoring -count=1`을 통과했다. 실제 Backend·Kafka·DB 실연동은 수행하지 않았다.

MCP 클라이언트는 여섯 도구를 허용목록에 추가하고 입력 Schema를 검증해야 한다. `status=partial`, `truncated`, `errors`, `limitations`, 시각·단위·`demo/data_mode/quality_verified`를 사용자에게 유지하고, 저장 부작용이 있는 두 도구의 annotation을 읽기 전용으로 덮어쓰지 않는다.
