너는 Go 백엔드 엔지니어다. `ableops-kafka-mcp`에 운영·보안·데이터 연계용 도구를 추가하고 코드·테스트·문서까지 완성하라.

## 1. 환경·진행 원칙

* Backend 참조: /Users/seokbong/dev/go-workspace/kafka-control-portal
* 업무설계: `docs/business-design`
* Backend: `http://localhost:8080`

적용 지침·Git 상태·기존 도구를 확인하고 사용자 작업을 보존한다. 기존 인증·REST Client·stdio/HTTP·테스트를 재사용하며 동일 기능은 보완한다.

필요한 업무설계 → 라우트 → 핸들러·DTO·권한만 분석한다. 실제 로컬 코드를 계약 기준으로 삼고 참조 HEAD를 기록한다. 독립적인 검색·읽기는 묶고, 기능군별 구현·검증 후 마지막에 전체 검증한다.

Backend·MCP 클라이언트 수정, Kafka·DB 직접 접속, 서비스 재시작, push·배포는 하지 않는다. API나 권한 통제가 부족한 기능은 사유를 기록하고 나머지를 계속 구현한다.

## 2. 추가 도구

이름은 기본안이며 기존 계약에 맞춰 중복 없이 구현한다.

| 기능군      | 도구와 기능                                                                                                                                                                        |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Lag      | `get_consumer_lag_overview`: 그룹×토픽 통합 관측·판정·경보 / `get_consumer_lag_policy`: 적용 임계치·상속·지속/회복 조건                                                                                |
| 모니터링     | `get_metric_series`: 허용 지표 추이 / `get_cluster_storage`: 저장량·파티션 분포 / `get_cluster_config_audit`: 설정 정책 진단 / `get_partition_reassignments`: 재배치 현황                              |
| 계정·보안    | `list_identities`, `get_identity_detail`: 계정·생명주기·ACL·SCRAM 메타 / `list_acls`: 조건별 ACL / `get_acl_risk`, `get_scram_audit`: 정책 진단                                              |
| 감사·통제    | `list_access_reviews`: 만료·재인증 대상 / `search_audit_logs`: 변경 감사 / `list_requests`: 신청 목록                                                                                        |
| 데이터 자산   | `search_topic_catalog`: 업무·담당자·연계 검색 / `list_governance_findings`: 미등록·ACL 미참조·드리프트 보고서 / `list_resource_backups`: 백업 상태                                                      |
| Flink·연계 | `list_flink_jobs`, `get_flink_job`: 작업 조회 / `list_pipeline_projects`, `get_pipeline_project`: 프로젝트·Source/Sink / `inspect_es_target`: 등록된 ES 대상 메타                            |
| 이벤트      | `get_event_summary`, `list_operational_issues`, `get_operational_issue`, `get_event_rule`, `get_attention_policy`, `list_maintenance_windows`, `list_notification_deliveries` |
| 샘플·계획    | `sample_topic_messages`, `preview_acl_plan`, `preview_flink_acl_plan`, `preview_flink_ddl`, `get_restore_plan`                                                                |

기존 이벤트 목록·상세 도구의 필터도 필요한 범위에서 보완한다.

## 3. 정확성과 권한

* 대상별 클러스터·객체 ID를 명시하고 기본 클러스터로 대체하지 않는다.
* 전역 API의 클러스터 필터·객체 소속·접근 통제를 확인한다. 레거시 API나 MCP 사후 필터로 부족한 Backend 통제를 우회하지 않는다.
* 사용자별 인증 격리와 Backend 최종 권한 검사를 유지한다.
* 실제 입력·출력 Schema를 정의하고 임의 URL·API 경로·SQL·헤더를 받지 않는다.
* 시크릿·인증 설정·전체 감사 payload를 노출하지 않고 필요한 필드만 반환한다.
* annotation은 실제 부작용에 맞추며 권한 검사로 취급하지 않는다.

다음 의미를 보존한다.

* 다른 그룹의 Lag 합계 ≠ 토픽 미처리량
* Kafka 상태·현재 판정·발생 경보는 별개
* 정책 상속·직접 설정·비활성 구분
* 실측·스냅샷·데모, 조회 시각·관측 시각 구분
* bytes/s·offset 차이·시간 단위 구분
* 계정 선언 상태·계산 단계·추론 분류 구분
* ACL 보유 ≠ 실제 접속 가능, ACL 미참조 ≠ 실제 미사용
* SCRAM 정책 진단 ≠ 변경 감사 이력
* 승인·반영·검증 상태 구분
* Flink 저장 상태·런타임·mock 구분
* 논리 구성 백업 ≠ 메시지 데이터 백업

합성값을 실측 분석에 쓰거나 메타데이터만으로 장애·정상 여부를 확정하지 않는다.

## 4. 샘플·미리보기 제한

* 메시지 샘플은 별도 기능 플래그와 허용 토픽 정책으로 기본 비활성화한다.
* 기본 20건·최대 100건 이내, 바이트·시간 제한과 선택 방식·partition/offset·잘림 표시를 적용한다.
* key/value/header의 민감정보 처리와 LLM 전달 허용 범위를 설정한다. 정규식만으로 완전한 비식별화를 보장하지 않는다.
* 미리보기 API의 실제 저장·외부 실행 여부를 확인한다. 신청·반영·SQL 실행·배포·복원 실행은 호출하지 않는다.
* DDL 생성의 샘플 사용에도 같은 정책을 적용하고 시크릿은 제외한다.
* 복원 계획은 기존 계획 조회만 제공한다. 조회 API가 없다고 새 계획 생성으로 대체하지 않는다.

## 5. 결과·부하 처리

기존 결과·오류 규약을 재사용하고 HTTP 200 본문의 업무 실패도 보존한다. 빈 결과·미지원·권한 거부·부분 실패·장애를 구분하며 조회 실패를 0이나 정상으로 바꾸지 않는다.

실제 지원되는 필터·페이지·기간만 사용한다. 결과 잘림·집계 범위를 표시하고 일부 페이지를 전체 검색·상위 순위·전체 현황으로 표현하지 않는다. 이벤트 권한 거부로 나온 0건도 정상 집계로 처리하지 않는다.

시계열 기간·해상도·포인트 수와 도구 전체 시간·호출 수·동시성·응답 크기를 제한한다. 부가 조회는 선택적으로 제공하고 무제한 페이지 순회·재귀·N+1·자동 재시도를 피한다.

## 6. 검증·완료

기존 mock을 재사용해 기능군별로 검증한다.

* DTO·필터·페이지·Schema
* 권한·객체 소속·사용자 격리
* 부분 실패·데모·공백·상태·단위·시각
* 샘플 비활성·토픽 제한·민감정보·크기 제한
* 미리보기의 변경 경로 미호출
* timeout·취소·호출 상한
* 기존 도구·stdio/HTTP 회귀

SDK Client로 도구 발견·대표 호출을 확인한다. 실제 Backend와 안전하게 주입된 토큰이 있으면 허용된 조회만 검증한다. 미제공 데이터·계정을 생성하지 말고 실연동 미검증 항목을 구분한다.

최종 실행:
`go test ./...`
`go vet ./...`
`go build ./cmd/ableops-kafka-mcp`

README·API 매핑·설정 예제·검증 문서·CI를 갱신한다. 도구별 질문 예시와 권한·제약을 기록하고 문서 중복을 줄인다.

완료 시 **추가/보완 도구, 테스트·실연동 결과, Backend 제약으로 보류한 항목, MCP 클라이언트의 허용목록·인자 검증·결과 처리 후속 변경사항**을 간결히 보고하라.
