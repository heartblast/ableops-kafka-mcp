# 실행 프롬프트: YAML 기반 MCP 서버 설정 지원

너는 이 저장소의 Go 백엔드 엔지니어다. 기존 AbleOps Kafka MCP 서버를 `mcp-server-config.yaml`로 설정하여 실행할 수 있도록 개선하라. 계획만 제시하지 말고 구현·테스트·예제·문서를 완성하라.

## 적용 범위와 지침

- 대상은 `D:\golang\go-workspace\ableops-kafka-mcp`이며 `AGENTS.md`를 준수한다.
- 기존 11개 조회 도구, 공식 MCP SDK, stdio/HTTP 전송, 사용자별 인증과 Backend 권한 검사를 재사용한다.
- 기존 `kadmin`은 읽기 전용이다. Backend 서비스와 Kafka·DB를 직접 조작하지 않는다.
- 실행 중인 MCP 프로세스를 임의로 종료하거나 재시작하지 않는다. 통합 검증은 합성 Backend, 임시 파일·포트·자식 프로세스로 수행한다.
- 문서와 주석은 한국어로 작성하고 stdout은 MCP 전용으로 유지한다.

## 설정 파일과 실행 계약

1. 서버에 `--config <path>`를 추가한다. 사용 예는 `ableops-kafka-mcp.exe --config .\mcp-server-config.yaml`이다.
2. 파일은 명시적으로 지정했을 때만 읽는다. 현재 디렉터리나 실행파일 옆의 YAML을 자동 탐색하지 않는다. `--config` 미지정 시 기존 환경변수·플래그 동작을 유지한다. 빈 경로를 명시하면 오류다.
3. YAML 형식은 아래와 같다. `version`은 1만 지원하며 필수다. 나머지 항목은 기존 기본값 또는 환경변수로 보완할 수 있다.

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
  ca_file: ''
auth:
  store_file: '${LOCALAPPDATA}/AbleOpsKafkaMCP/local.auth-store.json'
  token_env: 'ABLEOPS_API_TOKEN'
logging:
  level: info
```

4. 우선순위는 **명시한 CLI 플래그 > 기존의 비어 있지 않은 환경변수 > YAML > 기존 기본값**이다. 기존 플래그의 기본값이 YAML을 덮어쓰지 않도록 실제 지정 여부를 구분한다. 명시한 빈 `--allowed-origins`는 목록을 초기화한다.
5. 환경변수는 기존 `ABLEOPS_BASE_URL`, `ABLEOPS_ALLOW_HTTP`, `ABLEOPS_REQUEST_TIMEOUT`, `ABLEOPS_CA_FILE`, `MCP_LOG_LEVEL`, `MCP_AUTH_STORE`를 지원한다. 새로운 transport 환경변수를 만들지 않는다.
6. `auth.token_env`는 stdio·인증 등록 시 읽을 환경변수 이름이다. 생략하면 `ABLEOPS_API_TOKEN`을 사용하고 지정하면 지정한 변수만 읽는다. HTTP 서버에서는 이 변수나 공용 Backend 토큰을 읽지 않는다.
7. YAML의 `ca_file`, `store_file` 상대 경로는 설정 파일 디렉터리를 기준으로 해석한다. 이 두 경로에서만 `${ENV_NAME}` 확장을 지원하며 미설정/빈 변수는 오류다. 환경변수로 직접 지정한 경로는 기존 실행 디렉터리 기준 동작을 유지한다. 인증 폐기는 Backend CA 경로나 토큰에 의존하지 않는다.

## 안전성과 호환성

- 성숙한 YAML 파서를 명시적 버전으로 고정한다. 기존 설정 검증을 재사용하며 YAML 입력으로 loopback·TLS·사용자별 인증 제한을 완화하지 않는다.
- 파일 최대 크기는 64 KiB다. 알 수 없는 키, 중복 키, 잘못된 타입, null, 여러 문서, alias/anchor·merge를 거부하고 UTF-8 BOM을 허용한다.
- 토큰 원문, 비밀번호, Authorization 헤더를 YAML 필드로 받지 않는다. YAML은 공개 실행 설정이며 인증 저장소를 자동 생성하거나 권한 검증을 우회하지 않는다.
- 파일/파서 오류는 고정 안전 설명으로 반환한다. 원문 설정값·비밀값·알 수 없는 키·파일 경로가 오류에 인용되지 않도록 한다.
- 프로세스 환경변수를 변경해서 YAML을 적용하지 않는다. 설정 우선순위를 메모리에서 계산한다.

## 인증 등록 CLI와 산출물

- `ableops-mcp-auth enroll --config <path>`는 동일 YAML의 Backend·인증 저장소·토큰 환경변수 참조를 사용한다. 서버 transport가 HTTP여도 등록에는 본인 Backend 세션이 필요하다.
- `ableops-mcp-auth revoke --config <path>`도 저장소 경로를 재사용하며 Backend 로그인이나 네트워크 연결을 요구하지 않는다.
- 현재 사용자 환경에 맞춘 루트 `mcp-server-config.yaml`과 비밀값 없는 범용 `mcp-server-config.example.yaml`을 제공한다. 사용자 설정 파일을 배포본에 복사하지 않는다.
- PowerShell/Bash 빌드 스크립트는 공개 예제 YAML만 배포본에 포함한다.
- README, 배포 안내, 아키텍처, 검증 기록과 필요한 API 매핑 설명을 갱신한다. HTTP 서버 설정과 인증 등록·클라이언트 토큰의 차이를 설명한다.

## 검증과 완료 보고

- 환경변수 전용 회귀, CLI/env/YAML 우선순위, false/빈 목록, 상대 경로·변수 확장, 잘못된 YAML·크기 상한·비밀값 비노출을 검증한다.
- 합성 Backend와 공식 SDK로 YAML을 사용한 실제 stdio/HTTP 자식 프로세스의 초기화·11개 도구 발견·호출을 검증한다. 인증 거부와 사용자별 Backend 세션 전달도 유지한다.
- YAML을 사용하는 인증 CLI 등록·폐기를 `httptest`로 검증한다. 실제 운영 서비스를 기본 테스트에 사용하지 않는다.
- 완료 전 `go test ./...`, `go vet ./...`, `go build ./cmd/ableops-kafka-mcp`, `go build ./cmd/ableops-mcp-auth`를 실행한다. 실행 중 파일 때문에 필요한 빌드 출력 경로 조정은 기록하고 운영 프로세스를 무단 종료하지 않는다.
- 원격 생성·push·배포를 하지 않는다. 최종 보고에는 작성한 프롬프트, 설정 파일, 실행 명령, 검증 결과, 실제 실행 중인 프로세스 반영 여부를 간결히 제시한다.
