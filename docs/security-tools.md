# 계정·보안·통제 조회 도구

모든 입력은 `cluster_id`가 필수다. 각 호출은 현재 사용자의 백엔드 자격증명으로 다시 인가된다. MCP는 Kafka·DB에 직접 접속하지 않는다. 아래 `c1`, `User:reader`, `orders`는 합성 식별자다.

## 도구와 질문 예시

| 도구 | 질문·입력 예시 | 백엔드 계약과 공개 범위 |
| --- | --- | --- |
| `list_identities` | “c1에서 재인증 단계인 서비스 계정을 보여줘.” `{"cluster_id":"c1","kind":"SERVICE","stage":"REVIEW_DUE","limit":20}` | `GET /api/clusters/{id}/identities`. `query→q`, `kind`, `stage`, `department`, `environment`, `severity`, `finding`만 전달. 생명주기·명시/추론 분류·소유 메타·SCRAM 메타·ACL 요약. |
| `get_identity_detail` | “c1의 User:reader 선언 상태와 계산된 단계를 비교해줘.” `{"cluster_id":"c1","principal":"User:reader"}` | `GET /api/clusters/{id}/identities/{principal}`. 식별·생명주기·ACL·SCRAM 메타만 반환. Principal을 임의로 `User:` 정규화하지 않는다. |
| `list_acls` | “c1의 User:reader가 가진 ACL을 보여줘.” `{"cluster_id":"c1","principal":"User:reader","limit":20}` | 기본 `GET /api/clusters/{id}/acls`, Principal 지정 시 `GET /api/clusters/{id}/principals/{principal}/acls`. Principal 이외 조건·페이지는 미지원. |
| `get_acl_risk` | “c1의 ACL 정책 발견사항과 면제 여부를 보여줘.” `{"cluster_id":"c1","limit":20}` | `GET /api/clusters/{id}/acl-risk`. 원본 상태·대상·코드·실제값·면제 여부·백엔드 집계·진단 생성 시각. |
| `get_scram_audit` | “c1의 SCRAM 정책 진단이 지원되는지, 판정 보류가 있는지 보여줘.” `{"cluster_id":"c1","limit":20}` | `GET /api/clusters/{id}/scram-audit`. 지원 여부·원본 상태·발견사항·unresolved·자산 기준 시각·생성 시각. |
| `list_requests` | “내가 열람할 수 있는 c1 신청의 승인·반영 상태를 보여줘.” `{"cluster_id":"c1","limit":20}` | `GET /api/requests?cluster={id}`. 신청 ID·유형·요청자·상태·위험도·대상 식별자·생성/수정 시각. 임의 payload, 의견, 감사 원문 제외. |

모든 도구는 REST 호출 1회만 수행하며 후속 조회·자동 재시도·페이지 순회를 하지 않는다. `limit`은 기본 50·최대 100인 **MCP 출력 제한**이다. 백엔드의 전량 수집이나 REST 전송량은 줄이지 않는다. 공통 전송 2 MiB, 구조화 출력 64 KiB, 전체 MCP 결과 128 KiB와 도구 시간·호출 예산을 그대로 적용한다. 잘림은 `truncated`로 표시하며 목록에는 `received/returned`, 계정 목록에는 필터 적용 전 전체 `summary`가 별도로 남는다. 진단 집계는 제한으로 잘린 발견사항 목록의 개수와 다를 수 있다.

## 의미·불완전한 관측

- 계정 `status`는 담당자 선언, `lifecycle.stage`는 계산 결과, `identityKindSource=inferred`는 이름 규칙에 의한 추론이다. 생명주기 `ACTIVE`를 실제 접속 성공으로 해석하지 않는다.
- ACL 보유는 실제 접속 가능이나 사용 실적을 증명하지 않는다. `credential.locked`와 `CREDENTIAL_LOCKED` 면제는 포털의 삭제·재발급 통제이며 Kafka 인증 차단이 아니다. `UNUSED_PRINCIPAL` 등은 미사용 **의심**이다.
- SCRAM 도구는 정책 진단이다. 변경 감사 이력이 아니다. 미지원은 `supported=false`, MCP `partial`과 `unsupported`로 구분한다.
- 계정 API는 SCRAM·ACL 조회 오류를 숨긴다. MCP는 목록·상세를 `partial`과 `observation_completeness_unknown`으로 반환한다. 빈 목록·false·ACL 0건을 성공 관측으로 확정하지 않는다. 백엔드는 미존재 Principal에도 빈 상세를 생성할 수 있다.
- ACL 위험 진단과 SCRAM 진단은 부가 SCRAM/ACL/자산 조회 오류를 숨길 수 있다. 성공 판정에서도 완전성을 확정하지 않고 `partial`과 `observation_completeness_unknown`을 제공한다. HTTP 200의 `FORBIDDEN/UNAVAILABLE`은 원본 상태를 유지하면서 MCP `error`로 반환한다. 경고·위험 판정은 바꾸지 않는다.
- ACL 전체 목록은 `syncedAt`을 보존한다. Principal별 ACL은 기준 시각과 오류를 제공하지 않아 `partial`이다. 계정 API도 원본 관측 시각을 제공하지 않는다. `queried_at`은 MCP 조회 완료 시각이며 관측 시각 대용이 아니다. `assetSyncedAt`과 `generatedAt`도 서로 다른 의미다.
- 이 API들은 실측·mock 어댑터 구분을 응답에 제공하지 않는다. 결과만으로 실측 여부를 확정하지 않는다.
- 신청 목록은 현재 사용자에게 허용된 객체만 포함한다. SQL 저장소 조회 실패는 빈 목록으로, 행 처리 오류는 누락으로 숨겨질 수 있어 항상 `partial`·`observation_completeness_unknown`으로 반환한다. 빈 목록을 신청 부재·클러스터 전체 0건·권한 확인 성공으로 일반화하지 않는다. `APPROVED`, `APPLIED`, `VERIFIED`를 각각 보존한다. 레거시 빈 `clusterId`는 기본 클러스터로 대체하지 않고 응답 전체를 거부한다.

## 권한과 부작용

계정·ACL·진단 경로는 `clusterAdapterOr(..., acl.view)`에서 대상 클러스터 권한을 검사한다. 신청 목록은 `listRequests`의 `canSeeRequest`를 사용한다. **`/clusters/{id}/requests`는 `topic.view`만 검사하고 신청별 권한 검사를 하지 않으므로 사용하지 않는다.** 전역 `/requests`는 실제 지원되는 `cluster` 필터와 객체별 검사를 함께 수행하므로 사용한다.

계정 상세의 `grants`는 클러스터 키가 없어 다른 클러스터의 동일 Principal 기록을 구분할 수 없다. `recentAudit`는 별도 `audit.view` 검사가 없다. 두 필드와 연결 Filebeat·도달성 확장 정보는 DTO에 선언하지 않아 공개하지 않는다. 이를 다른 전역 API 조회나 MCP 사후 권한 필터로 보완하지 않는다.

GET 조회라도 백엔드의 자산 지연 동기화로 캐시가 저장될 수 있다. 전체 ACL은 만료 시 자동 동기화와 `CLUSTER_SYNC` 감사를 기록하고, SCRAM 진단은 자동 동기화 외에 조회 감사 `SCRAM_AUDIT`를 기록한다. 따라서 계정·ACL·진단 5개 도구는 보수적으로 `readOnlyHint=false`, `idempotentHint=false`다. `list_requests`는 두 hint가 true다. annotation은 권한 판정이 아니며 MCP는 Kafka 설정 변경·신청 생성·승인·반영 API를 호출하지 않는다.

## 백엔드 보완 전 보류

| 도구 | 확인한 경로 | 보류 이유와 필요한 백엔드 변경 |
| --- | --- | --- |
| `list_access_reviews` | `/grants`, `/reports/expiring-grants`, `/reports/review-due` | 전역 `acl.view`만 검사하고 클러스터 필터·소속 검증이 없다. `domain.Grant` 자체에 클러스터 키가 없다. 저장 모델과 조회에 클러스터 소속·권한 검사가 추가되어야 한다. `list_identities(stage=REVIEW_DUE)`는 계정 생명주기이며 Grant 재인증 목록 대체품이 아니다. |
| `search_audit_logs` | `/audit?cluster=...` | 전역 `audit.view`와 cluster 문자열 필터만 적용하며 `clusters.Can`이 없다. 최신 200건을 먼저 읽고 필터해 기간/전체 검색도 불가능하다. 백엔드 클러스터 접근 검사와 필요한 검색·페이지 계약을 먼저 추가해야 한다. |

두 도구는 등록하거나 API를 호출하지 않는다. `get_identity_detail`의 감사·Grant 원문을 활용한 우회도 하지 않는다.

## 로컬 계약 근거와 검증

참조 저장소는 `/Users/seokbong/dev/go-workspace/kafka-control-portal`, HEAD는 `6aead42e70be7dcdd1b701f225a00b5af6eafa4a` (`v1.7.2`)다. 참조 시 관련 미커밋 변경은 없었다. 저장소와 서비스는 변경하지 않았으며 테스트 코드도 읽기만 했다. 전체 매핑 기록은 [API 매핑](api-mapping.md)에서 관리한다.

| 범위 | 참조 파일 |
| --- | --- |
| 업무 설계 | `docs/business-design/06-kafka-security.md`, `12-governance-audit.md` |
| 라우트·클러스터 검사 | `internal/server/routes_access.go`, `routes_cluster.go`, `routes_governance.go`, `routes_dashboard.go`, `clusters.go` |
| 계정 계약·오류 누락 | `internal/server/identities.go`, `principals.go`, `principal_meta.go`, `internal/identity/lifecycle.go`, `findings.go`, `internal/domain/identity_lifecycle.go` |
| 진단 계약·미지원 | `internal/server/policy_diagnosis.go`, `internal/domain/diagnosis.go`, `internal/aclrisk/rules.go`, `internal/policy/scramaudit.go` |
| 신청 객체 검사·레거시 한계·저장소 실패 누락 | `internal/server/requests.go`, `cluster_scope.go`, `internal/domain/workflow.go`, `internal/store/sqlstore_workflow.go:ListRequests` |
| 보류 근거 | `internal/server/grants.go`, `reports.go`, `audit.go`, `internal/domain/acl.go` |
| 읽기 확인한 백엔드 테스트 | `internal/identity/lifecycle_test.go`, `internal/aclrisk/rules_test.go`, `internal/policy/scramaudit_test.go`, `internal/server/identity_meta_preserve_test.go`, `request_status_code_test.go`, `routes_contract_test.go` |

MCP의 `internal/ableops/security_test.go`와 `internal/tools/security_test.go`는 `httptest`와 공식 SDK 클라이언트로 실제 경로/필터·DTO 제외·클러스터/Principal 불일치·본문 실패/미지원·상태 의미·목록/바이트 잘림·취소·호출 상한·도구 발견·필수 인자·annotation·보류 도구 미등록을 검증한다. 인증 격리·HTTP/stdio·전송 제한은 공통 회귀 테스트도 함께 실행한다. 실제 백엔드에 접속하는 검증은 하지 않았다.

MCP 클라이언트는 추가된 6개 도구만 허용목록에 추가하고, `cluster_id`·Principal·enum·limit을 스키마대로 검증해야 한다. `partial`, `unsupported`, `observation_completeness_unknown`, 원본 진단 상태와 `truncated`를 사용자에게 구분해 보여줘야 한다. 클라이언트 자체는 이 작업에서 수정하지 않는다.
