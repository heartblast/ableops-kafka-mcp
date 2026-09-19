# ableops-kafka-mcp 배포 및 실행

이 프로젝트는 기존 AbleOps REST API를 호출하여 조회·미리보기 도구 33개를 제공하는 stdio/로컬 HTTP MCP 서버와 로컬 인증 관리 CLI입니다. 최신 소스로 빌드한 대상 OS와 CPU 아키텍처의 배포 폴더를 복사하면 사용할 수 있으며 **Go 설치는 필요하지 않습니다**. `amd64`는 x64, `arm64`는 ARM64용입니다. 이전에 생성한 `dist` 실행파일에는 이후 소스 변경이 자동 반영되지 않으므로 배포 전 다시 빌드해야 합니다.

이 문서는 **Standalone MCP**(`ableops-kafka-mcp`) 배포만 다룹니다. AbleOps Kafka Core 가 직접 기동하는 **Managed Extension**(`ableops-kafka-mcp-extension`)은 배포 단위가 `dist/` 폴더가 아니라 `.ableops-ext` 설치 패키지이고, `ABLEOPS_BASE_URL`·MCP 토큰·Web Delegation 공유 비밀을 따로 설정하지 않습니다. 패키지 빌드와 서명 정책은 [README 의 Managed Extension 패키지 빌드](../README.md#managed-extension-패키지-빌드)를 보세요.

## 로컬 HTTP 인증 등록과 실행

이 모드는 OAuth가 아닌 개발용 사용자 매핑이다. 수신은 loopback IP만 허용하며 공용 서비스 인증 완료를 의미하지 않는다. 임의 Authorization Bearer 설정이 가능한 MCP 클라이언트를 사용한다. stdio 실행은 아래 기존 안내를 그대로 따른다.

비밀 디렉터리를 먼저 준비한다. 아래 경로는 예시이며 저장소 외부에 두는 것이 좋다. 저장소 안에서 검증한다면 `.secrets/`는 Git 제외 대상이다. Windows에서는 현재 실행 계정만 접근할 수 있도록 상속을 끊고 기존 타 사용자 허용 ACL을 제거한다. CLI는 생성 시부터 파일 ACL을 현재 소유자 전용으로 설정하고 읽을 때마다 소유자·ACL을 검증한다. Linux는 디렉터리 0700, 파일 0600으로 제한한다. 서버와 등록 CLI는 같은 OS 계정으로 실행한다. 관리자가 타 사용자 세션을 등록할 때에도 승인된 본인 세션만 안전하게 전달받으며 공용 관리자 세션으로 대체하지 않는다.

```powershell
# 새 전용 디렉터리를 준비한다. 기존 공유 디렉터리에 그대로 적용하지 않는다.
$privateDir = 'C:\AbleOpsPrivate'
New-Item -ItemType Directory -Path $privateDir -ErrorAction Stop | Out-Null
$privateSID = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$privateACL = [System.Security.AccessControl.DirectorySecurity]::new()
$privateACL.SetOwner($privateSID)
$privateACL.SetAccessRuleProtection($true, $false)
$privateRule = [System.Security.AccessControl.FileSystemAccessRule]::new($privateSID, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
$privateACL.AddAccessRule($privateRule)
Set-Acl -LiteralPath $privateDir -AclObject $privateACL

$env:ABLEOPS_BASE_URL = 'http://localhost:8080'
$env:ABLEOPS_ALLOW_HTTP = 'true'
$env:MCP_AUTH_STORE = Join-Path $privateDir 'local.auth-store.json'
$sessionSecret = Read-Host '등록할 사용자 본인의 Backend 세션 토큰' -AsSecureString
$env:ABLEOPS_API_TOKEN = [System.Net.NetworkCredential]::new('', $sessionSecret).Password
try {
    .\ableops-mcp-auth.exe enroll --client-id local-client-a --ttl 1h --token-output "$privateDir\client-a.mcp-token"
    if ($LASTEXITCODE -ne 0) { throw '인증 등록 실패' }
} finally {
    Remove-Item Env:ABLEOPS_API_TOKEN -ErrorAction SilentlyContinue
}
.\ableops-kafka-mcp.exe --transport=http --http-address=127.0.0.1:8081
```

소스에서 실행할 때 각 `.exe` 대신 `go run ./cmd/ableops-mcp-auth`와 `go run ./cmd/ableops-kafka-mcp`를 사용한다. CLI는 `/api/me`의 사용자 ID를 확인하고 32바이트 암호학적 난수 MCP 토큰을 만든다. 매핑 파일에는 SHA-256 해시, 고정 audience, 사용자/클라이언트 ID, 발급·만료·폐기 상태, 해당 사용자의 Backend 세션을 저장한다. MCP 원문 토큰은 `--token-output`의 새 보호 파일에만 쓰며 기존 파일을 덮어쓰지 않는다. 이 파일의 토큰을 클라이언트 비밀 관리 기능에 등록하고 화면·로그·도구 인자에는 출력하지 않는다. 공개 등록 API는 없다.

MCP 토큰 기본 수명은 1시간, 최대 8시간이다. Backend 세션이 먼저 만료되거나 로그아웃되면 다음 요청에서 거부한다. `GET /api/me`로 매 요청 사용자 매핑을 재검증하고 실제 도구 조회에서도 Backend RBAC를 적용한다. 프로세스 공용 `ABLEOPS_API_TOKEN`, 임의 `user_id`/`X-User`, MCP 토큰의 Backend 직접 전달을 사용하지 않는다. Bearer를 소유한 클라이언트의 등록 별칭은 검증하지만 클라이언트 소프트웨어의 신뢰성을 증명하지는 않는다.

폐기는 아래처럼 수행한다. 파일을 다시 읽으므로 서버 재시작 없이 다음 요청부터 적용되며 저장된 Backend 세션도 매핑에서 지운다. 이미 진행 중인 요청은 소급 취소하지 않는다.

```powershell
.\ableops-mcp-auth.exe revoke --client-id local-client-a
# 새 Backend 세션을 안전하게 환경에 주입한 뒤 다른 출력 파일로 재등록한다.
.\ableops-mcp-auth.exe enroll --client-id local-client-a --ttl 1h --token-output 'C:\AbleOpsPrivate\client-a-renewed.mcp-token'
```

만료된 항목도 먼저 revoke하고 재등록한다. 사용하지 않는 원문 토큰 파일은 제거한다. 관리자 작업은 `.lock` 파일로 직렬화하며 비정상 종료로 잠금이 남으면 등록 CLI가 실행 중이지 않은지 확인한 후 해당 잠금만 제거한다. 매핑 파일 손상·권한 오류는 인증을 차단하며 내용을 로그로 출력하지 않는다. Backend 세션은 별도의 암호화 저장소가 없는 평문 비밀이므로 파일과 백업 모두 동일한 보호가 필요하다.

### Windows 등록 오류 진단

인증 폴더 생성만으로 등록이 끝나지 않는다. `enroll`은 본인 Backend 세션 토큰으로 `GET /api/me`를 검증한 뒤 인증 저장소와 별도 MCP 토큰 파일을 생성한다. 토큰 입력창에서 값 없이 Enter를 누르면 `ABLEOPS_API_TOKEN is required`로 중단된다. 암호 입력창은 입력한 문자를 표시하지 않으며 실제 토큰은 채팅·명령 문자열·오류 출력에 붙이지 않는다.

현재 로컬 Backend의 웹 클라이언트는 로그인 응답의 `token`을 브라우저 Local Storage의 `kadmin_token`에 저장한다(`web/src/api/client.ts`, `web/src/pages/LoginPage.tsx`, `OidcCallbackPage.tsx`). 본인 계정으로 로그인한 뒤 개발자 도구의 Application → Local Storage → 실제 로그인한 AbleOps 웹 출처에서 해당 항목의 Value만 복사하여 PowerShell 숨김 입력창에 붙여넣는다. 웹 출처가 Backend의 `localhost:8080`과 다르면 웹 출처를 선택한다. 따옴표나 `Bearer ` 접두어는 포함하지 않는다.

현재 CLI는 설정 검증/클라이언트 초기화 실패 시 해당 설정을 식별할 수 있는 고정 오류 문구를 함께 출력한다. 입력한 토큰·URL·파일 경로 등 실제 설정값을 오류에 포함하지 않는다. 설정 오류는 Backend HTTP 요청 이전의 실패이며 Backend가 반환한 401과 구분한다. URL과 HTTP 허용이 올바르더라도 빈 토큰, 토큰의 공백/비ASCII 문자, 잘못된 제한시간·로그 수준 등이 원인일 수 있다.

Windows PowerShell 5.1에서 한글 `.ps1` 파일은 **UTF-8 BOM 포함**으로 저장한다. BOM 없는 UTF-8을 시스템 기본 인코딩으로 읽으면 한글 안내가 깨질 수 있다. 이미 만든 폴더에 `New-Item -ErrorAction Stop`을 다시 실행하면 기존 폴더 오류가 나므로 최초 폴더 준비와 사용자 등록을 구분한다. 로컬 재등록 스크립트 `auth-init2.ps1`은 프로젝트 루트의 `ableops-mcp-auth.exe`를 사용한다. 이 파일은 `go build ./cmd/ableops-mcp-auth`로 생성하며, `dist`의 이전 실행파일과 구분한다.

HTTP는 `/mcp` POST를 사용하며 SDK의 stateless 전송이라 세션 ID를 인증이나 재접속 수단으로 사용하지 않는다. 독립 GET SSE 연결은 405다. 기본적으로 브라우저 Origin은 모두 거부한다. 필요할 때만 `--allowed-origins=https://client.example`처럼 정확한 출처 목록을 쉼표로 지정하며 wildcard는 허용하지 않는다. 허용된 Origin의 OPTIONS는 CORS 확인만 수행하고 실제 MCP 요청은 매번 인증한다. Origin 부재는 인증 성공을 뜻하지 않는다.

| 제한 | 기본값·동작 |
| --- | --- |
| HTTP 입력 본문 | 64 KiB, 초과 시 413 |
| HTTP 동시 요청 | 전체 32, 사용자 ID별 4, 초과 시 429·Retry-After |
| 처리 시간 | HTTP 요청 context 30초, 도구 전체와 Backend 요청별 기본 15초(`ABLEOPS_REQUEST_TIMEOUT` 설정 가능). 먼저 도달하는 마감 시각 적용 |
| 헤더/본문 읽기·유휴 연결 | 헤더 5초·16 KiB, 읽기 10초, 유휴 60초 |
| 응답 쓰기 | 매 Write/Flush마다 10초 유휴 제한 갱신, SSE 전체 응답 시간 제한 아님 |
| REST 제한 | 전체 동시 4, 응답 2 MiB, 도구 실행당 최대 8회 안전 상한; MCP 출력 제한과 별도 |
| 종료 | Ctrl+C/SIGTERM 때 진행 요청 취소, 최대 5초 정상 종료 후 연결 정리 |

`/healthz`와 `/readyz`는 프로세스의 HTTP 처리 준비 상태만 200/`ok`로 나타낸다. Backend·Kafka·매핑 파일의 지속적 정상 상태를 보장하지 않는다. 인증 실패는 401, 인증 중 Backend 권한 거부는 403, Backend/매핑 장애는 503, 인증 조회 시간 초과는 504다. 도구 수행 중 Backend 오류는 MCP 결과의 안전한 오류 코드로 구분한다. SDK의 HTTP 오류 본문은 정적 상태 문구로 제한하고 상세 구분은 stderr의 안전한 결과 코드로 남긴다.

## 사내 HTTPS 전환과 프록시

현재 로컬 매핑 모드를 공용 프록시 뒤에 연결하는 것은 지원되는 배포가 아니다. `0.0.0.0` 등 공용 바인딩이나 임의 Host 허용 옵션을 제공하지 않는다. 사내 공개 전환 시 먼저 인가 서버·보호 리소스 metadata/discovery, issuer·MCP audience·scope·만료 검증, 사용자 동의·Backend 위임 토큰 교환, 철회·다중 인스턴스 제한을 구현해야 한다.

그 후 사내 HTTPS 프록시는 신뢰된 인증서와 TLS 1.2 이상, 정확한 공용 Host/Origin, Authorization 전달과 비밀 로그 차단을 설정한다. `/mcp`는 응답 버퍼링·캐시를 끄고 POST/OPTIONS 및 선택한 SDK 전송을 전달하며, 읽기/쓰기 유휴 제한을 처리 시간보다 충분히 길게 둔다. 예를 들어 Nginx에서 `proxy_buffering off; proxy_cache off; proxy_http_version 1.1; proxy_read_timeout 60s;`를 전용 MCP location에 적용한다. 일괄 응답 timeout으로 SSE 전체 수명을 자르지 않는다. 실제 프록시·HTTPS·OAuth 배포와 외부 클라이언트 호환성은 별도 검증 사항이다. Backend 자체 HTTPS 연결은 현재도 `ABLEOPS_BASE_URL`/`ABLEOPS_CA_FILE`로 지원한다.

배포 폴더에는 서버와 `ableops-mcp-auth` 실행파일, `.env.example`, `mcp-server-config.example.yaml`, 이 안내서(`README.md`), 두 실행파일의 SHA-256 체크섬(`SHA256SUMS`)이 있습니다. 실제 조회에는 접근 가능한 AbleOps Backend와 본인 계정의 유효한 API 세션 토큰이 필요합니다. Kafka나 DB에 직접 연결하지 않습니다.

## Linux에 배치

Linux용 폴더의 내용을 `/opt/ableops-kafka-mcp`에 복사한 경우 다음과 같이 체크섬과 실행 권한을 확인합니다. 설치 경로는 원하는 위치로 변경할 수 있습니다.

```bash
cd /opt/ableops-kafka-mcp
sha256sum -c SHA256SUMS
chmod +x ./ableops-kafka-mcp ./ableops-mcp-auth
```

Windows에서 만든 배포본을 옮겼다면 실행 권한이 보존되지 않을 수 있으므로 `chmod`를 실행하세요. 체크섬 확인이 실패하면 배포본을 다시 복사하거나 빌드해야 합니다.

## Windows에 배치

Windows용 폴더의 내용을 `C:\AbleOps`에 복사한 경우 PowerShell에서 실행파일의 체크섬을 확인합니다.

```powershell
Set-Location 'C:\AbleOps'
foreach ($checksumLine in Get-Content -LiteralPath '.\SHA256SUMS') {
    $checksumParts = $checksumLine -split '\s+', 2
    $actualHash = (Get-FileHash -LiteralPath $checksumParts[1] -Algorithm SHA256).Hash
    if ($actualHash -ne $checksumParts[0]) {
        throw '실행파일 체크섬이 일치하지 않습니다. 배포본을 다시 복사하거나 빌드하세요.'
    }
}
Write-Host '실행파일 체크섬 확인 완료'
```

## 실행 설정

`--config <path>`를 명시하면 YAML을 읽고 기존 환경변수로 덮어쓸 수 있습니다. 미지정 시 기존 환경변수 방식으로 실행합니다. **YAML, `.env`, `.env.example`을 자동으로 읽지 않습니다.** `.env.example`은 설정 이름과 기본값을 보여 주는 참고 파일입니다.

### YAML 설정 계약

공개 배포본의 `mcp-server-config.example.yaml`을 사용자 설정으로 복사하고, 환경에 맞게 수정한 파일을 명시합니다. 로컬 프로젝트의 `mcp-server-config.yaml`은 Windows의 기존 인증 등록 경로와 Backend 8080/MCP 8081 환경용입니다.

```powershell
.\ableops-kafka-mcp.exe --config .\mcp-server-config.yaml
# 기존 CLI 플래그를 명시하면 해당 YAML 설정을 덮어씁니다.
.\ableops-kafka-mcp.exe --config .\mcp-server-config.yaml --http-address=127.0.0.1:8082
```

```bash
./ableops-kafka-mcp --config ./mcp-server-config.yaml
```

| YAML 항목 | 대응 환경변수 / CLI | 기본값·의미 |
| --- | --- | --- |
| `version` | 없음 | 파일에서 필수, 정수 1 |
| `server.transport` | `--transport` | `stdio`, 또는 `http` |
| `server.http_address` | `--http-address` | `127.0.0.1:8081`, 기존 loopback 제한 유지 |
| `server.allowed_origins` | `--allowed-origins` | 빈 목록, 브라우저 Origin 기본 거부 |
| `backend.base_url` | `ABLEOPS_BASE_URL` | 필수 origin, `/api` 제외 |
| `backend.allow_http` | `ABLEOPS_ALLOW_HTTP` | boolean `false`, loopback Backend HTTP만 명시 허용 |
| `backend.request_timeout` | `ABLEOPS_REQUEST_TIMEOUT` | 문자열 `15s`, 기존 1~120초 범위 |
| `backend.ca_file` | `ABLEOPS_CA_FILE` | 선택 PEM CA 파일 |
| `auth.store_file` | `MCP_AUTH_STORE` | HTTP/enroll/revoke의 기존 사용자별 저장소 |
| `auth.token_env` | 지정한 이름의 환경변수 | 기본 이름 `ABLEOPS_API_TOKEN`, stdio/enroll에서만 읽음 |
| `logging.level` | `MCP_LOG_LEVEL` | `info`, 기존 debug/info/warn/error |
| `message_sample.enabled` | `ABLEOPS_MESSAGE_SAMPLE_ENABLED` | boolean `false`, 샘플 위치 메타데이터 조회 활성화 |
| `message_sample.allowed_topics` | `ABLEOPS_MESSAGE_SAMPLE_TOPICS` | 빈 목록. 정확한 cluster_id/topic_name 조합 최대 100개, 환경변수에서는 쉼표로 구분 |
| `dynamic_tools.enabled` | `ABLEOPS_DYNAMIC_TOOLS` | boolean `false`. `true`일 때만 `/openapi.json`을 읽어 Dynamic 도구를 추가하고 주기적으로 갱신 |
| `dynamic_tools.operations` | `ABLEOPS_DYNAMIC_OPERATIONS`(쉼표 구분) | 생략 시 1차 파일럿 5개, `[]`는 선택 없음. lowerCamelCase operationId 최대 64개. 노출 안전 게이트가 SAFE로 분류한 것만 추가 |
| `dynamic_tools.refresh_interval` | `ABLEOPS_DYNAMIC_REFRESH_INTERVAL` | 문자열 `5m`, 허용 `1m`~`24h`. 실행 중 계약 변경 확인 주기 |

설정은 **명시 CLI > 비어 있지 않은 기존 환경변수 > YAML > 기존 기본값** 순서다. 비어 있는 환경변수는 YAML 값을 지우지 않는다. 명시한 `--allowed-origins=`는 빈 목록으로 덮어쓴다. `auth.token_env`는 토큰 값의 우선순위가 아닌 읽을 환경변수 이름의 선택이다. 별도 이름을 지정하면 `ABLEOPS_API_TOKEN`으로 대체하지 않는다. HTTP 서버는 선택한 토큰 변수와 공용 Backend 토큰을 읽지 않는다.

`auth.token_env`는 환경변수 이름만 받으며 기존 설정 이름인 `ABLEOPS_BASE_URL`, `ABLEOPS_ALLOW_HTTP`, `ABLEOPS_REQUEST_TIMEOUT`, `ABLEOPS_CA_FILE`, `MCP_AUTH_STORE`, `MCP_LOG_LEVEL`, `ABLEOPS_MESSAGE_SAMPLE_ENABLED`, `ABLEOPS_MESSAGE_SAMPLE_TOPICS`, `ABLEOPS_DYNAMIC_TOOLS`, `ABLEOPS_DYNAMIC_OPERATIONS`, `ABLEOPS_DYNAMIC_REFRESH_INTERVAL`과 대소문자 구분 없이 충돌하면 거부한다. stdio는 HTTP 인증 저장소를 읽거나 그 경로의 환경변수를 확장하지 않는다.

YAML의 `ca_file`/`store_file`은 `${ENV_NAME}` 확장을 지원하고 상대 경로를 YAML 디렉터리 기준으로 해석한다. 토큰 변수는 경로 확장에 사용할 수 없다. 미설정/빈 참조 변수는 오류이며 명령 실행, shell 구문, `%VAR%`, `~` 확장은 제공하지 않는다. 환경변수로 직접 지정한 경로는 기존 실행 디렉터리 기준 동작을 유지한다. 프로세스 환경변수를 바꿔 YAML을 적용하지 않는다.

파일은 최대 64 KiB, 하나의 YAML 문서, `version: 1`과 선언된 키만 허용한다. UTF-8 BOM은 허용한다. 알 수 없는 키·중복 키·다중 문서·잘못된 타입·null·anchor/alias/merge를 거부하며 토큰·비밀번호·Authorization 원문 필드는 제공하지 않는다. 파서 오류·파일 경로·설정값 원문을 로그에 인용하지 않는다. 파일을 지정했는데 없거나 잘못됐으면 환경변수로 조용히 대체하지 않고 시작을 중단한다.

설정은 시작할 때 한 번 읽으며 설정 파일의 자동 재시작·hot reload는 없다.

Dynamic 도구를 켜면 동작은 다음과 같다.

- OpenAPI 계약을 시작할 때 읽고, 이후 `refresh_interval`마다 ETag로 변경을 확인한다.
- 계약 조회는 `request_timeout`만큼 기동을 늦출 수 있다.
- 조회에 실패하면 경고를 남기고 Static 도구로 기동한 뒤 다음 주기에 다시 시도한다.
- 실행 중 갱신이 실패하면 마지막 정상 도구 목록을 유지한다.
- Dynamic이 꺼져 있으면 계약 조회와 주기 갱신이 모두 없다. HTTP 인증 저장소의 매 요청 재조회와 폐기 적용은 기존 동작을 유지한다. YAML 파일이 읽혔다고 인증 등록·Backend 연결·권한이 검증된 것은 아니다. `/healthz`도 MCP 프로세스 준비 상태만 나타낸다.

### YAML과 인증 등록 CLI

서버와 같은 파일을 등록·폐기 명령에도 사용한다. `--config`는 하위 명령 뒤에 지정한다. 아래 토큰 출력 경로는 새 파일이어야 한다.

```powershell
$sessionSecret = Read-Host '본인 AbleOps 로그인 세션 토큰' -AsSecureString
try {
    # YAML의 auth.token_env가 기본 이름일 때의 예입니다.
    $env:ABLEOPS_API_TOKEN = [Net.NetworkCredential]::new('', $sessionSecret).Password
    .\ableops-mcp-auth.exe enroll --config .\mcp-server-config.yaml `
        --client-id local-client --ttl 1h `
        --token-output "$env:LOCALAPPDATA\AbleOpsKafkaMCP\local-client.mcp-token"
    if ($LASTEXITCODE -ne 0) { throw '인증 등록 실패' }
} finally {
    Remove-Item Env:ABLEOPS_API_TOKEN -ErrorAction SilentlyContinue
    $sessionSecret.Dispose()
}

.\ableops-kafka-mcp.exe --config .\mcp-server-config.yaml
```

이미 등록된 client ID는 기존 폐기·재등록 절차를 따른다. `revoke --config .\mcp-server-config.yaml --client-id local-client`는 Backend URL·토큰·연결 없이 저장소를 변경한다. 등록은 YAML의 `server.transport`가 HTTP여도 본인 Backend 세션을 검증한다. 인증 저장소 파일 권한, 별도 MCP 접근 토큰, 최대 TTL, 사용자 매핑과 매 요청 Backend RBAC 검사는 그대로다.

### 기존 환경변수

| 환경변수 | 설정 |
| --- | --- |
| `ABLEOPS_BASE_URL` | YAML `backend.base_url`로도 지정 가능. `https://ableops.example.com`처럼 `/api` 없는 origin이 필수. 사용자정보, query, fragment, 하위 경로는 허용하지 않음 |
| `ABLEOPS_API_TOKEN` | stdio/인증 등록의 기본 토큰 변수. `auth.token_env`로 다른 이름 선택 가능. HTTP 서버는 읽지 않음 |
| `MCP_AUTH_STORE` | HTTP/인증 관리 CLI의 소유자 전용 매핑 파일 경로. YAML `auth.store_file`로도 지정 가능 |
| `ABLEOPS_REQUEST_TIMEOUT` | 도구 전체 및 개별 REST 요청 상한. 기본 `15s`, 허용 범위 `1s`~`120s` |
| `ABLEOPS_CA_FILE` | 선택. 사설 CA 인증서의 PEM 파일 경로 |
| `MCP_LOG_LEVEL` | `debug`, `info`(기본), `warn`, `error` |
| `ABLEOPS_ALLOW_HTTP` | 기본 `false`. `true`일 때 `localhost`, `127.0.0.1`, `::1`의 개발용 HTTP만 허용 |
| `ABLEOPS_DYNAMIC_TOOLS` | 기본 `false`. `true`/`false`만 허용. [Dynamic 도구](dynamic-mcp.md) |
| `ABLEOPS_DYNAMIC_OPERATIONS` | 선택. 선택할 operationId 쉼표 목록(SAFE만 노출) |
| `ABLEOPS_DYNAMIC_REFRESH_INTERVAL` | 선택. 계약 변경 확인 주기. 기본 `5m`, 허용 `1m`~`24h` |

사설 CA는 `ABLEOPS_CA_FILE`로 지정하면 이 프로세스의 REST 클라이언트가 사용하는 시스템 CA 목록에 추가됩니다. OS 신뢰 저장소를 수정하지 않으며 TLS 검증은 계속 수행합니다.

Linux Bash에서는 토큰을 화면이나 명령 이력에 출력하지 않고 입력할 수 있습니다.

```bash
export ABLEOPS_BASE_URL='https://ableops.example.com'
export ABLEOPS_REQUEST_TIMEOUT='15s'
export MCP_LOG_LEVEL='info'
# 사설 CA가 필요한 경우:
# export ABLEOPS_CA_FILE='/opt/ableops-kafka-mcp/ableops-ca.pem'
read -r -s -p '본인 AbleOps 세션 토큰: ' ABLEOPS_API_TOKEN
printf '\n'
export ABLEOPS_API_TOKEN
/opt/ableops-kafka-mcp/ableops-kafka-mcp
```

Windows PowerShell에서는 다음과 같이 설정합니다.

```powershell
$env:ABLEOPS_BASE_URL = 'https://ableops.example.com'
$env:ABLEOPS_REQUEST_TIMEOUT = '15s'
$env:MCP_LOG_LEVEL = 'info'
# 사설 CA가 필요한 경우:
# $env:ABLEOPS_CA_FILE = 'C:\AbleOps\ableops-ca.pem'
$sessionSecret = Read-Host '본인 AbleOps 세션 토큰' -AsSecureString
$env:ABLEOPS_API_TOKEN = [System.Net.NetworkCredential]::new('', $sessionSecret).Password
& 'C:\AbleOps\ableops-kafka-mcp.exe'
```

직접 실행하면 stdin으로 MCP 요청을 기다리며 조회 메뉴는 나타나지 않습니다. stdout은 MCP JSON-RPC 전용이고 로그는 stderr로만 출력합니다. stdin이 닫히거나 Ctrl+C를 받으면 종료합니다. 사용 후 현재 셸의 토큰을 지우려면 Bash는 `unset ABLEOPS_API_TOKEN`, PowerShell은 `Remove-Item Env:ABLEOPS_API_TOKEN`을 실행하세요.

## MCP 클라이언트 연결

기본 stdio 모드는 MCP 클라이언트가 실행파일을 **자식 프로세스로 시작하고 stdin/stdout으로 통신**합니다. stdio에서는 HTTP 포트를 열지 않습니다. 로컬 HTTP는 위 `--transport=http` 절차로 실행하며 systemd/Windows 서비스 설치는 자동화하지 않습니다.

아래는 `mcpServers` 형식을 지원하는 클라이언트의 예시입니다. 해당 클라이언트를 위 환경변수가 설정된 셸에서 시작하거나 클라이언트의 비밀값 관리 기능을 통해 자식 프로세스에 `ABLEOPS_API_TOKEN`을 주입하세요. 설정 파일 위치와 환경변수 상속 여부는 클라이언트마다 다릅니다. 이미 실행 중인 클라이언트는 새 셸의 환경변수를 자동으로 받지 않습니다.

Linux:

```json
{
  "mcpServers": {
    "ableops-kafka": {
      "command": "/opt/ableops-kafka-mcp/ableops-kafka-mcp",
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

Windows에서는 JSON의 백슬래시를 이스케이프합니다.

```json
{
  "mcpServers": {
    "ableops-kafka": {
      "command": "C:\\AbleOps\\ableops-kafka-mcp.exe",
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

예시에는 토큰을 포함하지 않았습니다. 토큰이 자식 프로세스에 전달되지 않으면 설정 오류로 종료합니다. 토큰 만료 시 `authentication_required`를 반환하며 자동 로그인은 수행하지 않습니다. 기존 Backend에서 재인증한 뒤 새 토큰을 주입하고 MCP 프로세스를 재시작하세요. Backend 세션 토큰은 MCP OAuth 토큰이 아닙니다.

연결 후 `tools/list`에서 조회·미리보기 도구 33개와 입력 스키마를 확인할 수 있습니다. `list_clusters`를 제외한 조회 도구는 `cluster_id`가 필수이며, 매 요청의 권한은 Backend가 판단합니다. 응답의 `status`, `errors`, `limitations`, `truncated`를 함께 확인하세요.

## v0.2.0 도구 정책

샘플은 활성화와 정확한 토픽 허용 목록이 모두 필요합니다. key/value/header는 활성화해도 공개하지 않으며 원문 전달 옵션은 없습니다. 빈 환경변수는 YAML 목록을 지우지 않으므로 비활성화에는 명시적 false를 사용하세요. 변경 뒤 프로세스 재시작 시 적용됩니다.

추가 도구 허용목록·인자·partial/잘림 처리와 Backend 제약은 [모니터링](monitoring-tools.md), [보안](security-tools.md), [이벤트](event-tools.md), [샘플·미리보기](data-tools.md)를 참고하세요. DDL 결과는 인증 옵션을 제외한 실행 불가능한 컬럼 선언이고 신청·SQL 실행·배포 API는 호출하지 않습니다.
