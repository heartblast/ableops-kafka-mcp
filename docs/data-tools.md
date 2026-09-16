# 백업·샘플·미리보기 도구와 연계 API 보류

참조 루트는 `/Users/seokbong/dev/go-workspace/kafka-control-portal`, 브랜치 `v1.7.2`, HEAD `6aead42e70be7dcdd1b701f225a00b5af6eafa4a`다. 2026-09-16 확인 시 관련 미커밋 변경은 없었다. Backend 코드·테스트는 읽기만 했으며 서비스·Kafka·DB에 접속하지 않았다. 전체 인증·오류·크기 규약은 [API 매핑](api-mapping.md)을 따른다.

## 제공 도구

모든 도구는 `cluster_id` 필수이며 현재 사용자의 Backend 세션으로 REST를 **1회** 호출한다. 임의 URL·API 경로·헤더·SQL을 받지 않는다. 아래 이름과 주소는 합성 데이터다.

| 도구 | 질문·인자 예시 | 확인된 API·권한 |
| --- | --- | --- |
| `list_resource_backups` | “synthetic-dev의 부분 실패 백업 첫 20개를 보여줘.” `{"cluster_id":"synthetic-dev","status":"PARTIAL","limit":20}` | `GET /api/clusters/{id}/resource-backups`, `kafka.backup.view` |
| `sample_topic_messages` | “허용된 orders 토픽에서 메시지 위치 20개만 보여줘.” `{"cluster_id":"synthetic-dev","topic_name":"orders"}` | `GET /api/clusters/{id}/topics/{name}/messages?limit=20`, `topic.view`, 추가 MCP 기능 플래그·허용 정책 |
| `preview_acl_plan` | “User:reader의 orders 소비 권한을 미리 검사해줘.” `{"cluster_id":"synthetic-dev","template_key":"consumer","principal":"User:reader","topic":"orders","group":"readers","hosts":["127.0.0.1"]}` | `POST /api/clusters/{id}/acls/plan`, `acl.view` |
| `preview_flink_acl_plan` | “User:reader의 Source orders·Sink results 필요 ACL을 계산해줘.” `{"cluster_id":"synthetic-dev","principal":"User:reader","source_topic":"orders","source_group":"readers","sink_topic":"results"}` | `POST /api/clusters/{id}/flink-acl-plan`, `acl.view` |
| `preview_flink_ddl` | “orders에 id STRING 컬럼을 사용하는 Source 선언을 미리 보여줘.” `{"cluster_id":"synthetic-dev","topic_name":"orders","table_name":"src_orders","columns":[{"name":"id","type":"STRING"}]}` | `POST /api/flink/ddl-preview`, 전역 `flink.write`와 지정 클러스터 `flink.write` |

경로의 클러스터·토픽 식별자에 `/`, `;`, `,` 또는 제어문자가 있으면 거부한다. 레거시 기본 클러스터 경로로 재시도하지 않는다. 전체 REST 응답 2 MiB, MCP 구조화 출력 64 KiB·전체 결과 128 KiB, 동시 REST 4개 및 도구 전체 timeout·호출 예산을 재사용한다.

## 백업 상태

`status`는 `PENDING/RUNNING/COMPLETE/PARTIAL/FAILED/IMPORTED`를 그대로 보존한다. **논리 구성 백업이며 메시지 데이터 백업이 아니다.** 생성/수정 시각과 수집 시작/완료 시각을 분리한다. 이름·상태·시각·크기·경고/실패 건수만 공개하고 구성 본문, 인증 설정, 경고와 실패 원문은 제외한다.

실제 지원되는 `status`, `search→q`, `from`, `to`, `limit`, `offset`만 전달한다. 날짜는 RFC3339 또는 YYYY-MM-DD다. 기본 limit=50, 최대 100. Backend가 offset만큼 앞부분을 읽고 버리므로 MCP는 offset을 10,000으로 제한한다(Backend 허용 1,000,000보다 작음). 자동 페이지 순회는 없다. `total`, `returned`, `has_more`, `page_truncated`, `truncated`로 전체와 현재 페이지를 구분한다.

SQL 저장소가 count/목록/행 처리 실패를 숫자·빈 목록·누락으로 숨길 수 있어 항상 `partial`·`observation_completeness_unknown`을 반환한다. 실패를 실제 0건으로 확정하지 않는다. 반환 객체의 클러스터가 다르거나 필수 상태·페이지 계약이 잘못되면 `invalid_response`다.

## 메시지 샘플 정책

샘플 기능은 기본 비활성화다. **활성화해도 key/value/header는 DTO·결과·로그에 포함하지 않는다.** LLM 전달 범위는 `metadata_only`로 고정하며 임의 원문 전달 옵션은 거부한다. 정규식만으로 비식별화를 보장하는 기능은 없다. Backend REST 본문에 포함된 메시지는 제한된 메모리에서 읽어 공개 위치 필드만 해석하며 저장하지 않는다.

```yaml
message_sample:
  enabled: false
  allowed_topics: ['synthetic-dev/orders']
```

환경변수는 `ABLEOPS_MESSAGE_SAMPLE_ENABLED=true|false`, `ABLEOPS_MESSAGE_SAMPLE_TOPICS=synthetic-dev/orders,synthetic-dev/results`다. 환경변수가 YAML보다 우선하고 빈 환경변수는 파일 값을 지우지 않는다. 비활성화하려면 명시적 `false`를 사용한다. 활성화에는 비어 있지 않은 허용 목록이 필요하다. 정확한 `cluster_id/topic_name` 최대 100개만 지원하며 와일드카드·빈 조각·공백·모호한 구분자는 거부한다. 설정은 프로세스 시작 시 고정한다. HTTP 사용자는 같은 토픽 정책을 따르며 Backend의 각 사용자 권한 검사는 별도로 유지된다.

기본 20건, 최대 100건, **전체 샘플 호출 최대 5초**, 압축 해제 REST 본문 **256 KiB**다. 한계를 넘는 JSON 일부를 성공으로 반환하지 않는다. 위치 메타데이터는 partition/offset/timestamp 및 Backend 값 잘림 표지를 포함한다. 개수·바이트에 의한 MCP 잘림도 `truncated`에 반영한다. `null`은 이 샘플 경로에서만 빈 표본으로 허용하며 일반 상세 GET의 null 거부는 유지한다.

실제 어댑터는 파티션별 끝 offset 부근에서 읽으며 전체 최신순·무작위·모든 파티션 대표 표본이 아니다. Backend의 일부 파티션 실패·5초 폴링 만료가 결과에 표시되지 않고 mock도 같은 DTO를 쓰므로 `data_mode=unknown`, `status=partial`로 반환한다. 빈 표본은 토픽이 비었거나 조회가 정상이라는 증거가 아니며 실측 분석에 사용하면 안 된다. timestamp는 레코드 시각이고 조회 완료 시각은 별도 `queried_at`이다.

Backend는 `TOPIC_PEEK` 감사를 기록한다. 따라서 `readOnlyHint=false`, `idempotentHint=false`, `destructiveHint=false`다. 비활성 또는 허용 목록 외 토픽은 네트워크 호출 전에 `feature_disabled` 또는 `topic_not_allowed`로 거부한다.

## 미리보기 경계

ACL 미리보기는 Backend 템플릿 8종만 허용하고 `User:`가 명시된 Principal과 최대 20개의 Host를 받는다. Flink ACL은 Source 또는 Sink 토픽을 명시하며 선택적으로 Source 그룹·exactly_once·txn_id_prefix를 받는다. 응답 ACL의 Principal도 입력과 대조한다. 정책 `passed/riskLevel/violations(code,severity)`를 보존하며 자유 설명·오류 원문은 제외한다. `passed=false`를 통신 성공으로 덮어쓰지 않는다.

두 ACL API는 신청·ACL 반영을 수행하지 않는다. 다만 스냅샷이 없으면 내부 `clusters.ACLs`가 자산을 동기화·저장할 수 있어 readOnly/idempotent hint는 false다. 일반 ACL 계획은 스냅샷 오류와 시각이 없고 Flink 계획은 `aclKnown`만 제공하므로 항상 partial로 최신성 한계를 명시한다. `aclKnown=false`의 빈 held와 missing을 실제 부족 권한으로 확정하지 않는다. ACL 보유는 실제 접속 가능의 증거가 아니다. `limit`은 각 출력 목록에만 적용한다.

DDL 미리보기는 사용자 지정 컬럼 최대 100개, 단순 식별자, 허용 타입 9종만 받는다. 샘플·임의 SQL·계산식·접속 설정을 받지 않는다. 명시적 빈 metadata와 JSON 형식으로 기존 생성기에 전달한다. Backend는 저장·실행 없이 생성하며 해당 계약은 `TestDDLRegistryPreviewDoesNotPersist`에서도 확인했다. Backend가 생성한 컬럼 선언이 요청한 이름·타입과 일치할 때만 공개한다. **인증·접속 옵션이 포함되는 WITH 전체를 제외해 `column_ddl`은 실행 불가능한 조각**이고 `truncated=true`다. 인증값을 포함할 수 있는 검증 설명도 제외한다. `static_warn/static_error`는 partial과 원래 검증 상태로 보존하며 `static_ok`는 런타임 성공을 보장하지 않는다. 샘플 정책을 우회하는 추론·재분석 API를 호출하지 않는다.

## Backend 제약으로 보류한 연계 도구

| 도구 | 확인된 API·보류 근거 | 필요한 계약 |
| --- | --- | --- |
| `search_topic_catalog` | `GET /catalog`와 `/topics/{name}/catalog`는 전역 `topic.view`만 검사. TopicCatalog는 클러스터별 객체·필터가 없고 전체 목록 반환 | 클러스터 소속·RBAC·실제 검색 필터/페이지 |
| `list_governance_findings` | 미등록·미사용·드리프트 리포트는 전역 권한과 기본 어댑터 기반. 미사용도 ACL 미참조 추정이며 실제 사용량 측정 아님 | 클러스터별 보고서·RBAC·출처/부분 실패 계약 |
| `list_flink_jobs` | `/flink/jobs`는 전역 flink.view와 scope만, Kafka 클러스터 필터/접근 검사 없음. 모든 Job의 런타임 상태를 순회하고 조회 실패를 저장 상태로 숨김 | 클러스터/프로젝트 권한·페이지·저장/런타임/mock 및 실패 분리 |
| `get_flink_job` | `/flink/jobs/{jid}`는 flink.view 외 객체/클러스터 접근 검사 없음. SQL·로그·실행설정 동봉, 런타임 조회 실패 은닉 | 객체·클러스터 권한·안전 DTO·상태 출처 |
| `list_pipeline_projects`, `get_pipeline_project` | `/pipeline-projects`의 scope/env/q만 지원. 클러스터 필터 없음. 상세 `canViewPipeline`은 private도 전역 workspace 조회권한으로 허용 | Source/Sink 소속·객체/클러스터 접근 검사와 페이지 |
| `inspect_es_target` | 등록 ES 연결을 사용하나 `esConnClientOr`는 전역 flink.view만 검사하고 요청 Kafka 클러스터/프로젝트와 연결 소속을 검증하지 않음 | 등록 연결의 대상 소속·객체별 접근 검사 |
| `get_restore_plan` | `/resource-backups/{backupId}/restore/plan`은 POST 계획 생성뿐. 기존 계획 GET은 없음. `/resource-restore-jobs/{jobId}`는 실행 작업 조회 | 기존 계획 ID로 조회하는 읽기 계약. 새 계획 생성·복원 실행으로 대체 금지 |

이 8개 도구는 등록하지 않는다. 전역 결과를 MCP에서 사후 필터하거나 클러스터 인자를 무시하는 우회도 없다. 보안·이벤트 보류 4개는 [보안](security-tools.md)·[이벤트](event-tools.md) 문서를 참고한다.

## 로컬 근거와 검증

업무설계는 `docs/business-design/05-kafka-topic.md`, `06-kafka-security.md`, `10-workspace-flink.md`, `11-data-pipeline.md`, `12-governance-audit.md`의 관련 절을 확인했다. 이하 파일은 모두 위 Backend 참조 루트 기준이다.

| 계약 | 근거 파일 |
| --- | --- |
| 라우트·변경 경로와 분리 | `internal/server/routes_access.go`, `routes_topic.go`, `routes_backup.go`, `routes_flink.go`, `routes_workspace.go` |
| 백업 헤더·오류 은닉 | `internal/server/resource_backup.go`, `internal/domain/resourcebackup.go`, `internal/store/sqlstore_resourcebackup.go`, `internal/server/resource_backup_routes_test.go` |
| 메시지 조회·샘플 선택·mock | `internal/server/cluster_scope.go`: `clusterTopicMessages`, `internal/domain/topic.go`, `internal/kafkaadmin/franz.go`: `PeekMessages`, `internal/kafkaadmin/mock.go`: `PeekMessages` |
| ACL 미리보기·스냅샷 | `internal/server/acl_plan.go`, `flink_acl.go`, `internal/acl/templates.go`, `internal/clusters/sync.go`, `internal/server/acl_plan_test.go`, `flink_acl_test.go` |
| DDL 생성·인증 설정·저장 없음 | `internal/server/flink_ddl_registry.go`, `internal/flink/generate.go`, `internal/domain/flink_schema.go`, `internal/server/flink_ddl_registry_test.go`, `flink_ddl_registry_hardening_test.go` |
| 카탈로그·거버넌스 보류 | `internal/server/topic_catalog.go`, `reports.go` |
| Flink·파이프라인·ES 보류 | `internal/server/flink_phase3.go`, `pipeline.go`, `es_pipeline.go` |

`internal/ableops/data_operations_test.go`, `internal/tools/data_operations_test.go`, `internal/config/sample_test.go`는 합성 httptest와 공식 SDK로 경로·필터·필수 입력·출력 Schema, 위임 자격증명, 소속/Principal 불일치, 샘플 비활성·정책·null·정밀도·민감 필드·크기·취소, 업무 실패·잘림, 고정 미리보기 외 경로 미호출과 보류 도구 미등록을 검증한다. 실제 계정·Backend로는 새 기능 실연동을 수행하지 않았다.

클라이언트는 위 5개 허용목록, 새 입력 Schema, feature_disabled/topic_not_allowed, partial·원본 상태·잘림 표시를 반영해야 한다. 메시지 본문이나 실행 가능한 DDL을 기대하는 화면은 이 반환 계약에 맞춰야 한다. MCP 클라이언트 자체는 이번 작업에서 수정하지 않았다.
