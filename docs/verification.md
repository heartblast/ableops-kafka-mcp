# 구현 검증 기록

검증일: 2026-09-16. 환경: Windows amd64 / PowerShell, Git 2.53.0.windows.1.

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
