# AbleOps Kafka MCP — Phase M3: Managed Extension 패키징

## 대상 저장소

'/home/netbee/sharedisk/dev/workspace/ableops-kafka-mcp'

## 선행 상태 — 반드시 보존

Phase M1과 M2가 완료된 현재 working tree를 기준으로 이어서 작업한다.

이미 구현된 주요 계약:

```text
Extension ID     : ableops-kafka-mcp
Version          : 0.6.0
Manifest         : internal/extension/manifest.yaml
Extension binary : cmd/ableops-kafka-mcp-extension
Backend kind     : process
Frontend         : disabled
Core compatibility:
                   >=1.8.0 <2.0.0
Capabilities     : 없음
Permissions      : 없음
Routes           : Manifest 공개 route 없음
```

Managed Extension 내부 MCP 경로:

```text
POST /mcp
POST /internal/delegations
GET  /health
```

`/mcp`, `/internal/delegations`은 Core 공개 Extension Proxy용 route가 아니다.

현재 구현을 되돌리거나 재설계하지 않는다.

특히 다음을 하지 않는다.

```text
git reset
git restore .
M1/M2 구조 원복
Manifest 위치 변경
Extension ID 변경
MCP 인증 재설계
```

---

# 목적

현재 M2에서 구현된:

```text
cmd/ableops-kafka-mcp-extension
```

바이너리를 AbleOps Kafka의 기존 Extension 설치 시스템에서 설치 가능한:

```text
ableops-kafka-mcp_0.6.0.ableops-ext
```

패키지로 생성할 수 있게 한다.

이번 Phase는 **패키징만** 수행한다.

MCP Tool, 인증, Runtime, Dynamic Tool 동작은 변경하지 않는다.

---

# 참고 대상

최신 `heartblast/ableops-kafka`의 다음 파일을 필요한 범위만 참고한다.

```text
scripts/build-extension.ps1
scripts/build-extension.sh
internal/extensionhost 패키지 검증 규칙
docs/reference_docs/extension-sdk/*
```

단, Core의 build script를 그대로 복사하지 않는다.

현재 MCP 저장소 구조는 Core의 예제:

```text
extensions/<id>/
  manifest.yaml
  cmd/<binary>/
```

구조가 아니기 때문이다.

현재 MCP의 실제 구조:

```text
internal/extension/manifest.yaml

cmd/
 ├─ ableops-kafka-mcp/
 └─ ableops-kafka-mcp-extension/
```

를 그대로 유지한 전용 패키징 스크립트를 만든다.

---

# 1. 패키지 구조

최종 `.ableops-ext` 구조는 최소한 다음과 같이 한다.

```text
manifest.yaml

bin/
 ├─ windows-amd64/
 │    └─ ableops-kafka-mcp-extension.exe
 │
 └─ linux-amd64/
      └─ ableops-kafka-mcp-extension
```

이번 Phase의 기본 빌드 대상은:

```text
windows/amd64
linux/amd64
```

으로 한다.

darwin, arm64 등은 현재 기본 패키지에 임의로 추가하지 않는다.

향후 `--targets` 또는 동등 옵션으로 확장할 수 있는 구조는 허용하지만, 실제 빌드 검증하지 않은 플랫폼을 기본 지원이라고 문서화하지 않는다.

---

# 2. Manifest 단일 출처

패키지 최상위:

```text
manifest.yaml
```

은 반드시 다음 파일을 **그대로 복사**한다.

```text
internal/extension/manifest.yaml
```

별도의 manifest template을 만들지 않는다.

빌드 스크립트 안에서 Manifest를 재작성하지 않는다.

다음 값의 단일 출처는 Manifest다.

```text
id
version
requires.core
backend.kind
frontend.enabled
capabilities
permissions
```

현재 값:

```text
id: ableops-kafka-mcp
version: 0.6.0
```

패키지명도 Manifest에서 읽는다.

결과:

```text
ableops-kafka-mcp_0.6.0.ableops-ext
```

`-v`, `--version` 등으로 Manifest와 다른 패키지 버전을 만들지 않는 것을 우선한다.

버전 override 기능이 꼭 필요하지 않으면 만들지 않는다.

---

# 3. Extension Binary

패키지에는 반드시 다음 binary만 Extension backend로 포함한다.

```text
./cmd/ableops-kafka-mcp-extension
```

Standalone binary:

```text
./cmd/ableops-kafka-mcp
```

는 `.ableops-ext` 안에 포함하지 않는다.

두 바이너리의 용도를 섞지 않는다.

---

# 4. 패키징 스크립트

현재 저장소에 다음을 추가한다.

```text
scripts/build-extension.ps1
scripts/build-extension.sh
```

기존 Core build script의 **패키지 규칙만 참고**하되 MCP 저장소 경로에 맞게 간결하게 구현한다.

스크립트가 임의의 Extension directory를 찾게 만들 필요 없다.

이 저장소에서 대상은 하나뿐이다.

고정 입력:

```text
Manifest:
internal/extension/manifest.yaml

Command:
./cmd/ableops-kafka-mcp-extension
```

---

# 5. 빌드 순서

패키징 스크립트는 다음 순서를 따른다.

## 5.1 사전 컴파일

기존 package/dist를 지우기 전에 먼저 현재 플랫폼 build를 확인한다.

```bash
go build ./cmd/ableops-kafka-mcp-extension
```

실패하면 기존 package 산출물을 삭제하지 않고 즉시 종료한다.

---

## 5.2 임시 Stage 생성

예:

```text
build/extensions/ableops-kafka-mcp/
```

아래에 다음을 만든다.

```text
manifest.yaml
bin/windows-amd64/ableops-kafka-mcp-extension.exe
bin/linux-amd64/ableops-kafka-mcp-extension
```

Manifest는 원본 파일을 byte-for-byte 그대로 복사한다.

---

## 5.3 Cross Compile

기본:

```text
GOOS=windows GOARCH=amd64 CGO_ENABLED=0
GOOS=linux   GOARCH=amd64 CGO_ENABLED=0
```

을 사용한다.

Go 1.27 이상을 전제로 한다.

현재 dependency가 CGO 없이 cross compile 가능한지 실제 빌드로 확인한다.

불가능하다면 임의로 workaround를 만들지 말고 원인을 보고한다.

---

# 6. ZIP 규칙

`.ableops-ext`는 실제 ZIP이어야 한다.

Core 설치 검증 규칙에 맞춰 다음을 지킨다.

최상위 허용 항목:

```text
manifest.yaml
bin/
web/
migrations/
LICENSE
README
README.md
README.txt
```

현재 MCP package에는 필요한 최소 항목만 넣는다.

권장:

```text
manifest.yaml
bin/
```

불필요한 파일을 넣지 않는다.

금지:

```text
./manifest.yaml
절대경로
역슬래시 ZIP entry
symbolic link
bin/ 밖의 executable
소스 코드
go.mod
go.sum
.env
auth-store
설정파일
Token/Secret
```

ZIP entry path는 `/`를 사용한다.

Windows PowerShell에서 `Compress-Archive`가 Core 규칙과 어긋나는 entry를 만들 가능성이 있다면 Core와 같은 방식으로 안전한 ZIP 생성 방식을 사용한다.

---

# 7. 패키지 자체 검증

패키지를 만든 직후 script가 스스로 검증한다.

최소 확인:

### ZIP format

파일 시작 signature가 실제 ZIP인지 확인한다.

### Top-Level

허용하지 않는 최상위 항목이 없어야 한다.

### Manifest

ZIP 안의:

```text
manifest.yaml
```

과:

```text
internal/extension/manifest.yaml
```

이 동일해야 한다.

### Binary

반드시 다음 두 파일이 존재한다.

```text
bin/windows-amd64/ableops-kafka-mcp-extension.exe
bin/linux-amd64/ableops-kafka-mcp-extension
```

Standalone binary가 포함되어 있으면 실패한다.

### 보안

다음 이름의 파일이나 경로가 package 안에 들어가면 실패한다.

예:

```text
.env
auth-store
secret
token
credentials
go.work
```

단순 문자열 오탐을 과도하게 만들지 말고 실제 민감 파일 경로를 검사한다.

---

# 8. Core build script를 dependency로 만들지 않는다

MCP package 생성 때문에:

```text
ableops-kafka repository checkout
Core internal package import
Core cmd/extsign compile
```

를 필수 dependency로 만들지 않는다.

MCP 저장소 하나만 checkout하여 unsigned package까지 만들 수 있어야 한다.

---

# 9. Extension Package 서명

이번 Phase에서는 Core의 Ed25519 서명 구현을 MCP 저장소에 복사하지 않는다.

현재 AbleOps Kafka의 기본 정책은:

```text
extensions.signatureMode = warn
```

이므로 **미서명 package도 경고와 함께 설치 가능**하다.

따라서 이번 Phase의 기본 산출물은:

```text
unsigned .ableops-ext
```

로 한다.

다만 문서에는 다음을 명확히 남긴다.

```text
signatureMode=off
  → unsigned 설치 가능

signatureMode=warn (기본)
  → unsigned 설치 가능 + 경고

signatureMode=require
  → 서명된 package 필요
```

`require` 환경용 정식 서명은 향후 release/distribution 단계에서 AbleOps 공식 signing tool을 사용하도록 한다.

서명 포맷을 MCP 저장소에서 독자 구현하지 않는다.

원한다면 후속 Phase에서 official signer를 사용하는 release workflow를 별도로 추가할 수 있도록만 문서화한다.

---

# 10.  ableops-sdk 의존성

M2에서 이미:

```text
github.com/heartblast/ableops-sdk v1.0.0
```

을 정식 dependency로 사용하고 있다.


이미 CI에:

```text
GOPRIVATE=github.com/heartblast/*
ABLEOPS_SDK_TOKEN
```

기반 인증 단계가 추가되어 있으므로 M3에서 또 다른 SDK 인증 방식을 만들지 않는다.

중요:

```text
secrets.ABLEOPS_SDK_TOKEN
```

이 GitHub 저장소에 아직 등록되지 않은 경우 이것은 **코드 오류가 아니라 CI 환경 prerequisite**로 보고한다.

secret이 없다는 이유로:

```text
SDK를 vendor
local replace
go.work
인증 우회
SDK source 복사
```

하지 않는다.

---

# 11. CI

현재 CI를 불필요하게 크게 늘리지 않는다.

기존 Go test/build는 그대로 유지한다.

Extension packaging 검증은 대표 환경 하나에서만 수행한다.

권장:

```text
ubuntu-latest
Go 1.27
```

여기서:

```bash
bash scripts/build-extension.sh
```

을 실행한다.

Linux에서:

```text
linux/amd64
windows/amd64
```

두 binary를 CGO_ENABLED=0으로 cross compile하면 충분하다.

Windows CI에서 동일 package를 다시 만드는 중복은 피한다.

---

# 12. scripts/test.ps1

M2에서 이미 다음 binary build 검증이 들어갔다면 중복 수정하지 않는다.

```text
go build ./cmd/ableops-kafka-mcp-extension
```

`go test`를 실행할 때마다 package ZIP까지 반복 생성하지 않는다.

Unit test와 release/package 검증을 분리한다.

---

# 13. 문서

README 또는 적절한 배포 문서에 두 실행 방식을 구분한다.

```text
Standalone MCP

- binary: ableops-kafka-mcp
- Claude Desktop / Codex 등 외부 MCP Client용
- stdio / standalone HTTP
- 별도 MCP 설정 사용


AbleOps Managed Extension

- binary: ableops-kafka-mcp-extension
- package: ableops-kafka-mcp_0.6.0.ableops-ext
- AbleOps Kafka Core가 process lifecycle 관리
- random loopback listener
- 별도 ABLEOPS_BASE_URL 불필요
- 별도 MCP fixed port 불필요
- 별도 Web Delegation Shared Secret 불필요
```

---

# 14. Dynamic Tool 설정은 이번 Phase에서 변경하지 않는다

M2 후속사항에 있는:

```text
Managed mode Dynamic Tool enable/disable을
ABLEOPS_EXT_CONFIG로 관리
```

는 패키징과 무관하다.

이번 Phase에서는 Manifest에 `config.schema`를 추가하지 않는다.

현재 동작:

```text
Dynamic 활성
기본 refresh 5분
실패 시 Static 33개 유지
```

을 그대로 둔다.

이 기능이 실제 운영에서 필요해질 때 별도 Phase로 처리한다.

---

# 테스트

먼저 targeted 검증:

```bash
go test ./internal/extension ./internal/app
go build ./cmd/ableops-kafka-mcp-extension
```

패키지 build:

```bash
bash scripts/build-extension.sh
```

PowerShell 사용 가능 환경에서는:

```powershell
./scripts/build-extension.ps1
```

도 동일 구조의 package를 만드는지 확인한다.

마지막에 전체 검증을 한 번만 수행한다.

```bash
go test ./...
go vet ./...
go build ./cmd/ableops-kafka-mcp
go build ./cmd/ableops-kafka-mcp-extension
go mod verify
```

실제 Kafka/DB에는 접속하지 않는다.

---

# 이번 Phase에서 하지 않을 것

* MCP Tool 변경
* Dynamic Tool 설정 추가
* Managed 인증 수정
* Core 저장소 수정
* Core internal package import
* Extension Manifest ID/version 임의 변경
* 별도 installer 구현
* Package signing algorithm 복제
* Extension Catalog 구현
* 실제 AbleOps Core 연결 구현
* 실제 Kafka/DB 접속
* External MCP 구조 변경
* 원격 push

---

# 완료 조건

다음이 모두 성립해야 한다.

1. `ableops-kafka-mcp_0.6.0.ableops-ext`가 생성된다.
2. package root의 Manifest가 `internal/extension/manifest.yaml`과 동일하다.
3. Windows amd64 binary가 올바른 경로에 존재한다.
4. Linux amd64 binary가 올바른 경로에 존재한다.
5. standalone binary는 package에 없다.
6. package가 실제 ZIP이다.
7. ZIP 경로가 Core Extension 규칙을 만족한다.
8. 민감 설정/Token 파일이 들어가지 않는다.
9. package build 때문에 Core source dependency가 생기지 않는다.
10. 기존 standalone와 Managed Extension tests가 모두 통과한다.

---

# 완료 보고

다음 형식으로 간단히 보고한다.

## 1. 변경 파일

## 2. 생성 package

예:

```text
dist/extensions/ableops-kafka-mcp_0.6.0.ableops-ext
```

## 3. Package 내용

실제 ZIP entry 목록을 요약한다.

## 4. 지원 플랫폼

실제 빌드 검증한 플랫폼만 기록한다.

## 5. Manifest 일치 검증

원본과 package root Manifest가 동일한지 보고한다.

## 6. 서명 상태

```text
unsigned
signatureMode warn/off 설치 가능
signatureMode require에서는 별도 signing 필요
```

## 7. Private SDK CI prerequisite

`ABLEOPS_SDK_TOKEN` 등록 필요 여부와 CI 검증 결과를 기록한다.

## 8. 테스트 결과

## 9. K1에 전달할 계약

다음 값을 다시 명시한다.

```text
Extension ID
Version
Package filename
Binary name
Core compatibility
/mcp
/internal/delegations
Call Token requirement
```
