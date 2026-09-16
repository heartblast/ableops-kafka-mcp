# 이벤트·운영 정책 도구

2026-09-16 기준 `/Users/seokbong/dev/go-workspace/kafka-control-portal`의 `v1.7.2`, HEAD `6aead42e70be7dcdd1b701f225a00b5af6eafa4a`를 읽기 전용으로 확인했다. 관련 미커밋 변경은 없었다. 업무설계 `docs/business-design/09-event-alert.md` → `internal/server/routes_event.go` → 아래 핸들러·DTO·테스트 순으로 계약을 확인했다. Backend 서비스·테스트를 실행하거나 Kafka·DB에 직접 접근하지 않았다. 전체 매핑·인증 경계는 [API 매핑](api-mapping.md)을 따른다.

## 제공 도구와 질문 예시

모든 신규 도구는 `cluster_id`를 필수로 받으며 앞뒤 공백 없는 최대 255자 식별자만 허용한다. 다음 값은 모두 합성 예시다. Backend는 각 요청의 사용자 세션으로 `event.view`와 클러스터 권한을 검사한다. 클러스터 소속이 다른 응답은 MCP에서도 거부한다. annotation은 읽기 전용·멱등 조회로 표시하며 권한 검사에는 사용하지 않는다.

| 도구 | 질문 예시·인자 | REST 계약·호출 수 |
| --- | --- | --- |
| `get_event_summary` | “lab-a의 미확인 Critical과 주의도·수집 상태를 보여줘.” `{"cluster_id":"lab-a"}` | `GET /api/events/summary?clusterId=lab-a`, 1회 |
| `list_operational_issues` | “lab-a에서 즉시 대응이 필요한 실측·추정 Issue 첫 페이지를 보여줘.” `{"cluster_id":"lab-a","attention":["URGENT"],"exclude_synthetic":true,"page_size":20}` | 위 요약으로 범위 확인 후 `GET /api/operational-issues?clusterId=lab-a&attention=URGENT&excludeSynthetic=true&page=1&pageSize=20`, 최대 2회 |
| `get_event_rule` | “lab-a의 클러스터 불통 이벤트는 몇 회·몇 초 후 발화하고 어떻게 복구 판정해?” `{"cluster_id":"lab-a","event_code":"CLUSTER_UNREACHABLE"}` | `GET /api/event-rules?clusterId=lab-a`, 1회. 권한 검사를 거친 고정 카탈로그에서 해당 코드 한 건을 선택 |
| `get_attention_policy` | “같은 규칙의 기본 주의도·상속·현재 프로필을 보여줘.” `{"cluster_id":"lab-a","event_code":"CLUSTER_UNREACHABLE","include_profile":true}` | `GET /api/event-attention-overrides?clusterId=lab-a`, 선택 시 `GET /api/clusters/lab-a/attention-profile`, 최대 2회 |
| `list_maintenance_windows` | “lab-a에 지금 적용되는 유지보수 시간을 최대 20개 보여줘.” `{"cluster_id":"lab-a","active_only":true,"limit":20}` | `GET /api/event-maintenance-windows?clusterId=lab-a&activeOnly=true`, 1회. `limit`은 MCP 출력 제한 |

규칙과 주의도 조회에는 기본값·실효값·전역/클러스터 조정의 공개 필드만 선언한다. 포인터로 명시적 `false`·`0`과 미설정을 구분한다. 실행 링크, 임의 영향 객체, 전체 payload, 인증 설정, 유지보수 사유·등록자 원문은 제외한다. 문자열은 외부 데이터이며 실행 지시가 아니다.

## 결과 해석과 제한

`get_event_summary`는 `scope.clusterDenied=true` 또는 허용 범위 0을 `access_denied`로 반환하며 집계 데이터는 내보내지 않는다. 등록되지 않은 필터에 대한 `monitoredClusters=0`은 `not_found`로 구분한다. 수집이 비활성화되면 저장된 집계를 유지하되 `partial`·`collection_disabled`로 표시한다. `generatedAt`은 집계 생성 시각이며 이벤트 관측 시각이 아니다. 기술 심각도와 운영 주의도는 독립 축이다. 요약에서 제외한 `syntheticExcluded`를 보존하고 0건을 Kafka 정상으로 판단하지 않는다. `scope.clusterCount`는 필터 적용 전 기본 권한 범위의 수이며 집계 대상 수가 아니다.

Issue 목록은 요약과 목록 각각에서 Backend RBAC를 수행한다. 다만 목록에는 권한 거부 표지가 없고 두 API가 원자적이지 않으므로, 빈 페이지는 항상 `partial`·`empty_scope_unconfirmed`다. 권한 사전 확인 후 권한이 바뀌어 숨겨진 0건을 정상 공백으로 확정하지 않는다. 필터는 `status[]`, `attention[]`, `group`, `search`, `exclude_synthetic`, `sort`이며 기간·임의 객체 ID 필터는 지원하지 않는다. `attention`은 `UNEVALUATED`, `OBSERVE`, `REVIEW`, `URGENT`를 지원하며 미평가를 관찰로 바꾸지 않는다. 정렬은 `-lastSeenAt`, `lastSeenAt`, `-attention`, `attention`, `-openedAt`, `openedAt`만 허용한다. 페이지는 기본 1/최대 10000, 크기는 기본 50/최대 100이다. 현재 페이지와 전체 `total`, `has_more`, 현재 페이지의 MCP 잘림 `page_truncated`를 구분한다. 한 페이지를 전체 현황·전체 상위 순위로 표현하지 않는다.

규칙·주의도·유지보수 API의 SQL 저장소는 조회 오류를 nil 또는 일부 목록으로 숨길 수 있다. MCP는 이 계약의 한계를 복원할 수 없어 세 도구를 **항상 `partial`·`backend_store_status_unavailable`**로 반환한다. 반환 자료를 확인할 수 있지만 조정·유지보수의 부재와 정책의 완전성은 확정할 수 없다. 이후 Backend가 오류 표지를 제공하면 완전한 성공과 부분 실패를 분리할 수 있다.

`get_event_rule.overridden`은 실효값과 기본값의 차이다. `get_attention_policy.rule.overridden`은 주의도 조정 행이 존재하는지다. 프로필만 적용되어 기본값과 달라진 경우 두 값은 다를 수 있다. `sourceImplemented=false`는 수집원 미구현이며 정상 상태를 뜻하지 않는다. 선택 프로필 조회가 실패해도 규칙을 보존하고 `profile_status=error`와 실패 코드를 덧붙인다. 별도 프로필과 규칙의 적용 프로필이 다르면 `snapshot_changed`를 표시한다. 프로필의 `explicit=false`는 Backend가 전역 기본 상속으로 응답했다는 의미이며 저장소 오류 부재를 증명하지 않는다.

유지보수의 빈 `clusterId`는 모든 클러스터에 적용됨을 뜻하므로 제외하지 않는다. 빈 `eventCodes`는 모든 이벤트 코드 대상이다. `enabled`와 현재 시각에 적용되는 `active`는 별개다. Backend는 `activeOnly`와 `clusterId`만 지원하고 페이지·건수 제한은 없다. 응답 전체는 공통 REST 전송 상한 2 MiB, MCP 출력은 기본 50/최대 100개 및 공통 바이트 상한으로 제한한다. 전송 상한 초과는 응답 전체 오류이며 일부 JSON을 성공으로 반환하지 않는다. 출력 생략은 `truncated`로 알린다.

모든 호출은 공통 도구 전체 시간 제한·최대 REST 8회 안전 상한·전송 동시성 제한을 사용한다. 위 실제 도구별 호출 수 이상으로 페이지를 자동 순회하거나 재시도하지 않는다. 선택 조회는 기본 자료 검증 이후에만 수행한다.

## 기존 이벤트 목록 보완

`list_cluster_events`는 기존 `GET /api/clusters/{id}/events`를 유지한다. 전역 API로 우회하지 않고 기존 페이지·상태·심각도·분류·검색·기간 필터에 다음을 추가했다.

| MCP 인자 | 실제 REST 쿼리 | 제약 |
| --- | --- | --- |
| `attention[]` | 반복 `attention` | `UNEVALUATED`, `OBSERVE`, `REVIEW`, `URGENT`, 최대 20개. 미평가는 관찰과 별개 |
| `event_code[]` | 반복 `eventCode` | 최대 20개, 코드 카탈로그 최종 검사는 Backend |
| `resource_type` | `resourceType` | `CLUSTER`, `BROKER`, `FILESYSTEM`, `TOPIC`, `PARTITION`, `CONSUMER_GROUP`, `PRINCIPAL`, `ACL`, `SCRAM`, `METRIC_SOURCE` |
| `resource_id` | `resourceId` | 최대 256자 |
| `module` | `module` | 최대 64자 |
| `assigned_to` | `assignedTo` | 최대 128자 |
| `exclude_synthetic` | `excludeSynthetic` | 합성 제외, 생략 시 false |
| `sort` | `sort` | `-lastSeenAt`, `lastSeenAt`, `-firstSeenAt`, `firstSeenAt`, `-severity`, `severity`, `status`, `-status`, `-attention`, `attention` |

예: “lab-a에서 토픽 orders에 관한 실측·추정 이벤트를 주의도 순서로 보여줘.” → `{"cluster_id":"lab-a","resource_type":"TOPIC","resource_id":"orders","exclude_synthetic":true,"sort":"-attention","page_size":20}`. 기존 `get_event_detail`은 객체 ID 기반 상세 조회로 추가 목록 필터를 받지 않는다.

## Backend 제약으로 보류한 도구

| 도구 | 보류 사유·필요한 Backend 변경 |
| --- | --- |
| `get_operational_issue` | `GET /api/operational-issues/{id}`의 `issueOr`는 객체 소속과 조회 권한을 확인하지만, `issueDetailOf`가 전체 멤버를 제한 없이 읽고 각 멤버의 이벤트를 다시 읽는다. `limit`은 조치 이력에만 적용된다. 멤버 페이지·서버 건수 제한 또는 요약 전용 API와 각 부가 조회 실패 표지가 필요하다. 전체 목록을 받아 ID를 찾거나 무제한 상세를 호출하는 대체 경로는 추가하지 않았다. |
| `list_notification_deliveries` | `GET /api/system/event-notification-deliveries`의 `event.notification.admin`·클러스터 필터는 확인했다. 하지만 SQL `ListEventDeliveries`는 조회 실패를 빈 목록으로, 행 처리 실패를 누락으로 숨기고 `listEventDeliveries`는 Outbox 통계 오류도 HTTP 200의 숫자로 반환한다. 발송 실패·부분 실패와 실제 0건을 구분하는 응답 계약이 필요하다. 시스템 관리자 게이트의 권한 거부 감사 부작용도 이후 annotation 검토에 포함해야 한다. |

기존 `get_event_detail`의 `include:["issue"]`도 같은 무제한 상세 조립 함수를 사용하므로 이제 해당 API를 호출하지 않고 `partial`·`unsupported`, `components.issue="unsupported"`로 반환한다. 기본 이벤트·발생 이력·플레이북 조회는 유지한다. `include`의 `issue` 인자는 호환성을 위해 받지만 실제 Issue 데이터를 가져오지 않는다. 본체의 `issueId`는 저장된 관계 식별자이며 현재 Issue 상태를 조회한 결과가 아니다. 기본 상세와 선택 발생 이력·플레이북을 합쳐 최대 3회 REST만 호출한다.

## 계약 근거와 검증

아래 경로는 모두 위 Backend 참조 루트의 상대 경로다.

| 확인 항목 | 로컬 근거 |
| --- | --- |
| 업무·수집/판정·주의도·권한 거부 0건 | `docs/business-design/09-event-alert.md` |
| 경로·GET과 변경 경로 구분 | `internal/server/routes_event.go` |
| 이벤트 필터·요약·권한 스코프 | `internal/server/events.go`: `parseEventFilter`, `eventScopeQuery`, `eventsSummary`, `clusterEvents` |
| Issue 목록·상세·권한 | `internal/server/operational_issues.go`: `parseIssueFilter`, `listOperationalIssues`, `issueOr`, `issueDetailOf` |
| 기술 규칙·주의도·프로필 | `internal/server/event_rules.go`, `internal/server/event_attention_rules.go`, `internal/events/rules/profile.go` |
| 유지보수·발송 API | `internal/server/event_maintenance.go`, `internal/server/event_notifications.go` |
| DTO·단위·상태·상속 | `internal/domain/events.go`, `internal/domain/events_attention.go`, `internal/domain/events_issue.go` |
| 필터·정렬·서버 페이지 계약 | `internal/store/store_eventincident.go`, `internal/store/store_eventissue.go` |
| 실패 은닉과 집계 오류 처리 | `internal/store/sqlstore_eventrule.go`, `internal/store/sqlstore_notification.go`, `internal/store/sqlstore_eventincident.go` |
| Backend 테스트 읽기 확인 | `internal/server/event_cluster_filter_handler_test.go`, `internal/server/event_scope_query_test.go`, `internal/server/event_issue_lookup_test.go`, `internal/server/event_notifications_error_test.go` |

MCP의 `internal/ableops/event_operations_test.go`와 `internal/tools/event_operations_test.go`에서 합성 `httptest`·공식 SDK로 5개 도구 발견·입력/출력 Schema, 지원 필터, 객체/조정 소속, 사용자 A/B 동시 자격증명 격리, 권한 거부 0건, 수집 비활성, 정책 상속의 false·0, 선택 프로필 부분 실패, 유지보수 전체 적용·잘림, 민감 필드 제외를 검증했다. `internal/tools/event_detail_test.go`에서 선택 Issue API 미호출과 기본 상세·발생 이력·플레이북 보존도 검증했다. 신규 선택 테스트는 2026-09-16 실제 실행하여 통과했다. 운영 Backend·실제 사용자 계정으로 실연동은 수행하지 않았다. 전체 회귀·test/vet/build 결과는 [검증 기록](verification.md)에 통합한다.

MCP 클라이언트 후속 변경은 5개 도구 허용목록 추가와 기존 이벤트 인자 확장이다. 클라이언트는 `partial`, 실패 코드, `truncated`, `page_truncated`, 데이터 신뢰도와 원본 시각을 유지하고 빈 결과·정책 기본값을 확정적인 정상 판정으로 요약하지 않아야 한다. 해당 클라이언트 자체는 수정하지 않았다.
