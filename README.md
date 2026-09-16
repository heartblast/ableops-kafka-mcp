# ableops-kafka-mcp

기존 AbleOps Kafka REST API의 **조회·미리보기 도구 33개를 stdio와 Streamable HTTP로 제공**합니다. stdio는 개인 사용자용이며 기본 전송입니다. HTTP는 사용자별 별도 MCP 토큰을 Backend 세션에 매핑하는 **loopback 전용 개발 검증 모드**입니다. Kafka·DB 직접 연결, 로그인 대행, 신청·반영·배포 실행, LLM·채팅, 공용 OAuth 서비스는 구현하지 않습니다.

## 요구 환경

- 소스 빌드에는 Go **1.25.0 이상**이 필요합니다. 공식 [MCP Go SDK v1.8.0](https://github.com/modelcontextprotocol/go-sdk/releases/tag/v1.8.0)을 고정했습니다. [해당 버전 go.mod](https://github.com/modelcontextprotocol/go-sdk/blob/v1.8.0/go.mod)의 최소 Go 버전을 따릅니다. 배포본 실행에는 Go 설치가 필요하지 않습니다.
- 빌드 스크립트는 Windows PowerShell 5.1 이상 또는 PowerShell 7, Linux Bash를 지원합니다. CI는 Windows와 Linux에서 검증하도록 구성했습니다.
- 실제 조회에는 접근 가능한 AbleOps Backend와 본인 계정의 유효한 API 세션 토큰이 필요합니다. 빌드와 테스트는 실제 Backend·Kafka·DB 없이 가능합니다.

## 빌드와 검증

```powershell
Set-Location 'D:\golang\go-workspace\ableops-kafka-mcp'
go mod download
.\scripts\test.ps1
.\scripts\build.ps1
```

`test.ps1`은 `go test ./...`, `go vet ./...`, `go build ./cmd/ableops-kafka-mcp`를 실행합니다. 테스트는 합성 DTO를 반환하는 `httptest` 서버와 **실제 stdio 자식 프로세스에 연결하는 공식 SDK 클라이언트**를 사용합니다. 빌드 스크립트는 기본적으로 Linux와 Windows의 `amd64` 배포본을 함께 생성합니다.

Linux에서는 저장소 루트에서 다음 명령을 실행합니다.

```bash
go mod download
go test ./...
go vet ./...
go build ./cmd/ableops-kafka-mcp
bash ./scripts/build.sh
```

OS와 CPU 아키텍처를 선택할 수도 있습니다. 두 스크립트 모두 현재 OS와 다른 대상의 실행파일을 교차 빌드하며, `CGO_ENABLED=0`을 사용합니다. `amd64`는 x64, `arm64`는 ARM64 환경용입니다.

```powershell
.\scripts\build.ps1 -TargetOS windows -Arch amd64
.\scripts\build.ps1 -TargetOS linux -Arch arm64
```

```bash
bash ./scripts/build.sh --os linux --arch amd64
bash ./scripts/build.sh --os all --arch arm64
```

지원 OS는 `all`, `linux`, `windows`, 아키텍처는 `amd64`, `arm64`입니다. 기본 빌드 결과는 다음과 같습니다. `arm64`를 선택하면 폴더 이름의 `amd64`가 `arm64`로 바뀝니다.

```text
dist/
  linux-amd64/
    ableops-kafka-mcp
    ableops-mcp-auth
    .env.example
    mcp-server-config.example.yaml
    README.md
    SHA256SUMS
  windows-amd64/
    ableops-kafka-mcp.exe
    ableops-mcp-auth.exe
    .env.example
    mcp-server-config.example.yaml
    README.md
    SHA256SUMS
```

각 폴더가 배포 단위이며, 대상 OS와 아키텍처에 맞는 폴더를 복사하여 사용합니다. `README.md`는 [배포 및 실행 안내](docs/deployment.md)의 사본이고, `SHA256SUMS`는 실행파일의 SHA-256 체크섬입니다. 압축 파일 생성이나 원격 서버 전송은 수행하지 않습니다. 빌드 스크립트와 테스트는 별도 명령이므로 배포본을 전달하기 전에 위 검증 명령도 실행하세요.

## 설정과 실행

서버는 `--config`로 지정한 YAML과 기존 환경변수를 지원합니다. **YAML과 `.env`를 자동으로 읽지 않습니다.** `--config`를 생략하면 기존 환경변수·플래그 방식으로 실행합니다. `.env.example`은 비밀값 없는 참고 파일입니다.

### YAML로 실행

로컬 환경의 `mcp-server-config.yaml`은 Backend `http://localhost:8080`, MCP HTTP `127.0.0.1:8081`, 기존 사용자별 인증 저장소를 사용합니다. 사용자 설정 파일은 저장소에 포함하지 않으므로 처음 내려받았다면 아래 공개 예제를 복사한 뒤 환경에 맞게 수정하세요.

```powershell
Copy-Item .\mcp-server-config.example.yaml .\mcp-server-config.yaml
```

프로젝트 디렉터리에서 실행합니다.

```powershell
.\ableops-kafka-mcp.exe --config .\mcp-server-config.yaml
```

다른 환경에서는 [공개 예제](mcp-server-config.example.yaml)를 복사하고 주소와 저장소 경로를 수정합니다. 실제 사용자 설정은 Git 제외 대상이고 배포 스크립트는 공개 예제만 복사합니다. 설정을 수정한 뒤에는 서버를 다시 시작해야 합니다.

```yaml
version: 1
server:
  transport: http
  http_address: '127.0.0.1:8081'
  allowed_origins: []
backend:
  base_url: 'http://localhost:8080'
  allow_http: true
  request_timeout: '15s'
auth:
  store_file: '${LOCALAPPDATA}/AbleOpsKafkaMCP/local.auth-store.json'
  token_env: 'ABLEOPS_API_TOKEN'
logging:
  level: info
```

우선순위는 **명시한 CLI 플래그 > 비어 있지 않은 기존 환경변수 > YAML > 기본값**입니다. 예를 들어 `--http-address=127.0.0.1:8082`가 YAML 포트를 변경하고, 기존 `ABLEOPS_BASE_URL`이 셸에 남아 있으면 YAML의 Backend 주소보다 우선합니다. `--allowed-origins=`는 YAML의 Origin 목록을 비웁니다. 환경변수 목록과 기본값은 아래 표를 참고하세요.

토큰 원문은 YAML에 넣지 않습니다. HTTP 서버는 `auth.store_file`의 기존 인증 매핑을 사용하고, stdio와 인증 등록은 `auth.token_env`에 지정한 환경변수에서 본인 Backend 토큰을 읽습니다(생략 시 `ABLEOPS_API_TOKEN`). 인증 저장소는 YAML로 자동 생성되지 않습니다. [인증 등록](docs/deployment.md#yaml과-인증-등록-cli)을 먼저 완료하세요.

`backend.ca_file`과 `auth.store_file`의 YAML 상대 경로는 설정 파일 위치 기준입니다. 이 두 경로만 `${환경변수명}` 확장을 지원하며 미설정·빈 변수는 오류입니다. 설정 파일은 최대 64 KiB이고 알 수 없는 키·중복 키·잘못된 타입·null·여러 문서·YAML anchor/alias/merge를 거부합니다. [실행 프롬프트](docs/reference_docs/ableops-kafka-mcp-YAML설정지원.md)와 [전체 설정 계약](docs/deployment.md#yaml-설정-계약)에 요구사항과 동작을 정리했습니다.

| 환경변수 | 규칙 |
| --- | --- |
| `ABLEOPS_BASE_URL` | YAML `backend.base_url`로도 지정 가능. `https://ableops.example.com`처럼 **`/api` 없는 origin**이 필수. 사용자정보·query·fragment·하위 경로 금지 |
| `ABLEOPS_API_TOKEN` | stdio 및 로컬 등록 CLI의 기본 토큰 변수. `auth.token_env`로 다른 이름 선택 가능. HTTP 서버는 읽지 않음 |
| `MCP_AUTH_STORE` | HTTP/인증 관리 CLI의 소유자 전용 매핑 파일 경로. YAML `auth.store_file`로도 지정 가능 |
| `ABLEOPS_REQUEST_TIMEOUT` | 기본 `15s`, 허용 `1s`~`120s` |
| `ABLEOPS_CA_FILE` | 선택. 사설 CA의 PEM 파일 경로. 이 REST 클라이언트의 시스템 CA 목록에 추가하며 OS 신뢰 저장소는 수정하지 않음 |
| `MCP_LOG_LEVEL` | `debug`, `info`(기본), `warn`, `error` |
| `ABLEOPS_MESSAGE_SAMPLE_ENABLED` | 기본 `false`. 활성화에는 허용 토픽 필요. 메시지 위치 메타데이터만 반환 |
| `ABLEOPS_MESSAGE_SAMPLE_TOPICS` | 정확한 `cluster_id/topic_name` 조합의 쉼표 구분 목록. YAML `message_sample.allowed_topics`로도 지정 가능, 와일드카드 금지 |
| `ABLEOPS_ALLOW_HTTP` | 기본 `false`. `true`이면 `localhost`, `127.0.0.1`, `::1`의 개발용 HTTP만 허용 |

예를 들어 PowerShell에서 아래처럼 토큰을 화면·명령 이력에 출력하지 않고 입력할 수 있습니다. 입력한 토큰은 서버 실행을 위해 해당 PowerShell 프로세스 환경에 저장됩니다.

```powershell
$env:ABLEOPS_BASE_URL = 'https://ableops.example.com'
$env:ABLEOPS_REQUEST_TIMEOUT = '15s'
$env:MCP_LOG_LEVEL = 'info'
# 사설 CA가 필요한 경우:
# $env:ABLEOPS_CA_FILE = 'C:\certificates\ableops-ca.pem'
$sessionSecret = Read-Host '본인 AbleOps 세션 토큰' -AsSecureString
$env:ABLEOPS_API_TOKEN = [System.Net.NetworkCredential]::new('', $sessionSecret).Password
& 'D:\golang\go-workspace\ableops-kafka-mcp\dist\windows-amd64\ableops-kafka-mcp.exe'
```

서버는 stdin으로 MCP 요청을 기다립니다. 대화형 CLI가 아니므로 직접 실행 시 조회 메뉴가 나타나지 않습니다. stdout은 MCP JSON-RPC 전용이며 로그는 stderr로만 출력합니다. 클라이언트가 stdin을 닫거나 Ctrl+C를 보내면 종료합니다. 종료 후 환경변수를 지우려면 `Remove-Item Env:ABLEOPS_API_TOKEN`을 실행합니다.

토큰은 기존 Backend에서 본인 인증 후 발급받습니다. 401은 `authentication_required`로 반환하며 자동 로그인·재시도하지 않습니다. 재인증하여 새 토큰을 주입하고 MCP 프로세스를 재시작하세요. Backend 세션 토큰은 MCP OAuth 토큰이 아닙니다.

## stdio 클라이언트 연결

아래는 `mcpServers` 형식을 사용하는 클라이언트의 설정 예시입니다. 절대 `.exe` 경로와 **JSON의 백슬래시 이스케이프**를 사용합니다. 먼저 해당 클라이언트를 위 환경변수가 설정된 프로세스에서 시작하거나, 클라이언트가 제공하는 비밀값 관리 기능으로 자식 프로세스에 `ABLEOPS_API_TOKEN`을 전달하세요. 환경 상속 여부와 설정 파일 위치는 사용하는 클라이언트에서 확인해야 합니다.

```json
{
  "mcpServers": {
    "ableops-kafka": {
      "command": "D:\\golang\\go-workspace\\ableops-kafka-mcp\\dist\\windows-amd64\\ableops-kafka-mcp.exe",
      "args": [],
      "env": {
        "ABLEOPS_BASE_URL": "https://ableops.example.com",
        "ABLEOPS_REQUEST_TIMEOUT": "15s",
        "MCP_LOG_LEVEL": "info"
      }
    }
  }
}
```

실제 토큰이나 미확인 `${VARIABLE}` 치환 문법은 예시에 넣지 않았습니다. 토큰이 자식 프로세스에 주입되지 않으면 시작 시 설정 오류로 종료합니다.

## 도구와 한계

| 도구 | 필수 입력 | 조회 |
| --- | --- | --- |
| `list_clusters` | 없음 | 사용자에게 허용된 클러스터의 공개 메타데이터 |
| `get_cluster_health` | `cluster_id` | 연결 상태와 파티션 건강 상태를 각각 조회하여 조합 |
| `list_topics` | `cluster_id` | DB 토픽 스냅샷 |
| `list_consumer_groups` | `cluster_id` | DB Consumer Group 스냅샷 |
| `get_consumer_group_lag` | `cluster_id`, `group_name` | 그룹의 실시간 파티션 Lag·Offset과 백엔드 상태 |
| `list_cluster_events` | `cluster_id` | 저장된 이벤트의 페이지 조회 |
| `get_topic_detail` | `cluster_id`, `topic_name` | 토픽 기본·소유 메타. `include:["configs","partitions"]`로 공개 설정·라이브 복제 상태 선택 |
| `get_consumer_group_members` | `cluster_id`, `group_name` | 멤버·클라이언트·호스트·토픽/파티션 할당 |
| `get_event_detail` | `cluster_id`, `event_id` | 심각도·주의도·상태·허용된 발생 근거. `include:["occurrences","playbook"]`로 부가 조회 선택 |
| `get_asset_impact` | `cluster_id`, `asset_type`, `asset_key` | 방향과 관계 유형을 보존한 그래프. `include:["impact"]`로 별도 영향 요약 선택 |
| `get_request_status` | `cluster_id`, `request_id` | 원본 신청 상태·정책 코드·대상. `include:["history"]`로 최근 상태 이력 공개 |

v0.2.0에서 추가한 22개 도구의 필수 인자·권한·질문 예시·제약은 기능군별 문서에 정리했습니다.

| 기능군 | 추가 도구 | 상세 계약 |
| --- | --- | --- |
| Lag·모니터링 | `get_consumer_lag_overview`, `get_consumer_lag_policy`, `get_metric_series`, `get_cluster_storage`, `get_cluster_config_audit`, `get_partition_reassignments` | [모니터링](docs/monitoring-tools.md) |
| 계정·보안·신청 | `list_identities`, `get_identity_detail`, `list_acls`, `get_acl_risk`, `get_scram_audit`, `list_requests` | [보안·통제](docs/security-tools.md) |
| 이벤트·정책 | `get_event_summary`, `list_operational_issues`, `get_event_rule`, `get_attention_policy`, `list_maintenance_windows` | [이벤트](docs/event-tools.md) |
| 백업·샘플·미리보기 | `list_resource_backups`, `sample_topic_messages`, `preview_acl_plan`, `preview_flink_acl_plan`, `preview_flink_ddl` | [데이터·미리보기](docs/data-tools.md) |

샘플은 기본 비활성화하고 허용 토픽의 partition/offset/timestamp만 반환합니다. DDL은 접속·인증 옵션을 생략한 컬럼 선언 조각입니다. 세 미리보기의 POST 경로는 고정하며 신청·반영·SQL 실행·복원을 수행하지 않습니다. 감사·스냅샷을 저장하는 신규 도구는 `readOnlyHint=false`로 표시합니다. `partial`은 Backend의 저장소 실패 표지 부재를 뜻할 수도 있으므로 `errors/limitations`를 함께 읽어야 합니다.

기존 이벤트 목록에 주의도·코드·자원·모듈·담당자·합성 제외·정렬 필터를 추가했습니다. `get_event_detail`의 `include=actions`와 `include=issue`는 Backend의 무제한 이력·멤버 조회 때문에 호출하지 않고 `partial/unsupported`로 반환합니다. 요청된 도구 중 Backend 권한·조회 계약이 부족한 12개는 등록하지 않았으며 위 문서에 보류 사유와 필요한 Backend 변경을 기록했습니다.

목록/상태/Lag 도구의 `limit`은 출력 목록별 기본 50, 최대 100입니다. 백엔드가 전체 목록만 제공하는 API에 가짜 페이지 인자를 보내지 않습니다. 이벤트는 실제 지원되는 `page`(기본 1), `page_size`(기본 50, 최대 100), `status`, `severity`, `category`, `search`, `from`, `to`를 사용합니다. 정확한 입력·출력 JSON Schema는 `tools/list`로 제공합니다.

예: `list_topics`에 `{"cluster_id":"dev-1","limit":20}`, 이벤트에는 `{"cluster_id":"dev-1","page":1,"page_size":20}`을 전달합니다. 누락한 클러스터를 기본 클러스터로 대체하지 않습니다.

- REST 응답 본문은 압축 해제 후 최대 **2 MiB**, 동시 REST 호출은 **4개**입니다. 초과 응답은 오류이며, 일부 JSON만 파싱하여 성공으로 만들지 않습니다.
- `ABLEOPS_REQUEST_TIMEOUT`은 개별 REST와 **도구 전체**에 적용합니다(기본 15초, 1~120초). 도구당 REST 호출의 공통 안전 상한은 8회이며 각 도구는 실제로 1~3회만 호출합니다. 샘플은 더 작은 5초·256 KiB 제한을 적용합니다. 재귀 조회와 자동 페이지 순회는 하지 않습니다.
- 구조화 결과는 최대 **64 KiB**, 동일 JSON의 호환용 텍스트를 포함한 `CallToolResult`는 최대 **128 KiB**입니다. JSON-RPC 포장 크기는 별도입니다. 목록을 줄일 때 `truncated=true`를 반환하며 단일 데이터도 담을 수 없으면 `output_too_large`입니다. stdio 입력 프레임은 64 KiB로 제한합니다.
- 결과의 `status`는 조회의 `ok`/`partial`/`error`이며 Kafka의 정상 여부와 다릅니다. 측정값·백엔드 판정·`errors`·`limitations`·`truncated`를 함께 확인하세요. 실행 실패는 MCP `isError`로 전달합니다.
- `queried_at`은 MCP 조회 완료 시각입니다. 스냅샷의 `synced_at`(백엔드 `syncedAt`), 실시간 결과의 `checkedAt`, 이벤트의 `lastSeenAt` 등 백엔드가 제공한 시각만 원본 데이터 시각으로 사용합니다.
- 스냅샷의 `syncedAt`이 없으면 조회 성공·빈 목록을 확정할 수 없습니다. 기존 Backend가 초기 동기화 오류를 목록 응답에서 생략하는 한계를 명시합니다.
- GET 목록 조회라도 기존 Backend가 필요시 lazy sync로 DB 스냅샷을 저장할 수 있고, 인증은 세션 idle 시각을 갱신합니다. 이 서버는 Kafka 변경 요청이나 명시적 동기화 요청을 보내지 않습니다.
- 토큰, 인증 설정, SCRAM 비밀번호, Kafka 메시지 본문을 노출하는 도구는 없습니다. 외부 문자열은 데이터로 취급합니다. 리다이렉트는 모두 차단하고 TLS 검증은 항상 켭니다. 자동 재시도는 하지 않습니다.
- 경로의 특수문자는 안전하게 인코딩합니다. 현재 Backend는 `/`가 포함된 그룹명의 percent-encoded 경로를 디코딩하지 않을 수 있습니다. Lag 도구는 반환된 그룹이 요청 대상과 다르면 `invalid_response`로 거부합니다. 멤버 API에는 반환 그룹 식별자가 없어 대조할 수 없으므로 경로 인자에 `/`, `;`, `,`가 포함되면 호출 전에 `unsupported`로 거부합니다. 지원하려면 Backend의 경로 디코딩 계약 개선이 필요합니다.

설계는 [architecture.md](docs/architecture.md), 로컬 소스 근거와 API 계약은 [api-mapping.md](docs/api-mapping.md), 후속 범위는 [roadmap.md](docs/roadmap.md), 실행한 검증과 미검증 범위는 [verification.md](docs/verification.md)에 정리했습니다. mock 테스트 성공은 실제 Backend 연동 검증을 뜻하지 않습니다.

### 운영 분석 호출 예시

모든 예시는 합성 식별자입니다. `tools/call`의 이름과 인자는 다음과 같습니다.

| 질문 | 도구와 인자 |
| --- | --- |
| 이 토픽의 설정과 복제 상태는? | `get_topic_detail` — `{"cluster_id":"synthetic-dev","topic_name":"synthetic-orders","include":["configs","partitions"],"limit":20}` |
| Lag가 큰 파티션을 어떤 Consumer가 맡고 있나? | `get_consumer_group_lag`와 `get_consumer_group_members`에 각각 `{"cluster_id":"synthetic-dev","group_name":"synthetic-readers","limit":20}` |
| 이 경보의 발생 근거와 대응 안내는? | `get_event_detail` — `{"cluster_id":"synthetic-dev","event_id":"synthetic-event-1","include":["occurrences","playbook"],"limit":10}` |
| 이 자산에 연결된 대상과 확인 가능한 영향은? | `get_asset_impact` — `{"cluster_id":"synthetic-dev","asset_type":"TOPIC","asset_key":"synthetic-orders","depth":2,"include":["impact"],"limit":30}` |
| 이 신청은 승인만 됐나, 실제 반영까지 됐나? | `get_request_status` — `{"cluster_id":"synthetic-dev","request_id":"synthetic-request-1","include":["history"],"limit":20}` |

`asset_type`은 백엔드가 지원하는 `TOPIC`, `PRINCIPAL`, `CONSUMER_GROUP`이며 `asset_key`는 실제 이름/Principal 식별자입니다. 예를 들어 Principal은 `User:synthetic-app`입니다. 깊이는 기본 1, 최대 2이고 `limit`은 기본 50, 최대 100입니다. 방향과 연결 관계가 장애 전파를 확정하지는 않습니다. 잘림과 수집 실패가 있으면 전체 영향 분석으로 해석하지 마세요.

토픽 설정의 직접 지정/상속 출처는 현재 API가 제공하지 않습니다. 빈 멤버 목록으로 그룹 존재나 장애를 단정할 수 없습니다. 이벤트 `actions`는 서버 건수 제한이 없어 호출하지 않으며 `partial`/`unsupported`를 반환합니다. 플레이북은 권장 안내이고 수행 이력이 아닙니다. 신청의 `APPROVED`, `APPLIED`, `VERIFIED`는 각각 승인·반영·검증 상태로 보존하고, 자유 형식 실패 원문과 재시도 가능 여부는 제공하지 않습니다. 상세 계약과 미지원 사유는 [API 매핑](docs/api-mapping.md#운영-분석-조회-도구)을 참고하세요.

## 로컬 Streamable HTTP 실행

HTTP 서버는 `ABLEOPS_API_TOKEN`을 공유하지 않습니다. 먼저 각 사용자가 본인 Backend 세션으로 로컬 관리자 등록 CLI를 실행하여 별도의 MCP 접근 토큰을 발급받습니다. 사용자 ID는 기존 `GET /api/me` 응답에서 확인하며 직접 지정할 수 없습니다. 등록·만료·폐기와 파일 권한 설정은 [배포 안내](docs/deployment.md#로컬-http-인증-등록과-실행)를 따릅니다.

```powershell
$env:ABLEOPS_BASE_URL = 'http://localhost:8080'
$env:ABLEOPS_ALLOW_HTTP = 'true'
$env:MCP_AUTH_STORE = 'C:\AbleOpsPrivate\local.auth-store.json'
go run ./cmd/ableops-kafka-mcp --transport=http
```

기본 주소는 `http://127.0.0.1:8081/mcp`이며 `/healthz`, `/readyz`도 제공합니다. `--http-address=127.0.0.1:8081`, `--allowed-origins=https://client.example`로 수신 주소와 명시적 브라우저 허용 Origin을 설정합니다. Origin 없는 요청에도 인증이 필요합니다. 클라이언트는 `/mcp`의 **매 HTTP 요청에** 발급받은 MCP 토큰의 Bearer 헤더를 붙여야 합니다. Backend 세션 토큰을 MCP에 직접 보내면 거부합니다.

이 인증은 OAuth 표준 연동이 아니며 임의 Bearer 설정을 지원하는 클라이언트에 한정됩니다. 비 loopback 바인딩은 금지합니다. 공용 HTTPS 배포에는 별도 OAuth·사용자 위임 작업이 남아 있습니다.

## 실제 Backend 검증

```powershell
# 본인 토큰은 위 SecureString 입력 절차로 환경에 주입합니다.
$env:ABLEOPS_VERIFY_CLUSTER_ID = 'synthetic-dev-cluster'
$env:ABLEOPS_VERIFY_GROUP_NAME = 'synthetic-consumer-group'
.\scripts\verify-backend.ps1
```

대상 예시는 합성 값이므로 승인된 실제 테스트 대상의 환경변수로 바꿉니다. 스크립트는 응답 본문이나 실제 ID를 출력하지 않으며, 필요한 설정이 없는 시나리오는 SKIP으로 남깁니다. 기본 테스트·CI는 실환경에 접속하지 않습니다. 추가 권한·만료·없는 대상 검증 설정은 [검증 기록](docs/verification.md)에 정리합니다.

새 도구의 실연동 대상은 `ABLEOPS_VERIFY_TOPIC_NAME`, `ABLEOPS_VERIFY_GROUP_NAME`, `ABLEOPS_VERIFY_EVENT_ID`, `ABLEOPS_VERIFY_ASSET_TYPE`/`ABLEOPS_VERIFY_ASSET_KEY`, `ABLEOPS_VERIFY_REQUEST_ID`로 각각 주입합니다. 기존 사용자 토큰과 `ABLEOPS_VERIFY_CLUSTER_ID`가 함께 필요합니다. 미제공 대상을 검색하거나 생성하지 않습니다.

기존 `list_topics`, `list_consumer_groups`, `get_topic_detail`도 최초 자산 스냅샷 동기화·저장 가능성이 있어 v0.2.0에서 readOnly/idempotent hint를 false로 보완했습니다. 실제 업무 권한과 조회 동작은 유지합니다. 근거는 Backend `internal/server/clusters.go`, `cluster_scope.go`, `internal/clusters/sync.go`입니다.
