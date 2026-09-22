# Managed Extension 패키지 서명

`scripts/build-extension.ps1` / `scripts/build-extension.sh` 가 만드는 `.ableops-ext` 는 **미서명** 패키지입니다.
이 문서는 그 패키지에 서명을 넣어 `signatureMode=require` 환경에 설치하는 절차를 정리합니다.

> 이 저장소에는 서명 기능이 없습니다(`-Sign` 류 옵션 없음). 서명 생성은 AbleOps Kafka Core 저장소의
> 빌드 도구 `cmd/extsign` 이 담당합니다. 서명 대상 바이트열이 Core 검증기와 한 글자라도 다르면 통과하지
> 못하므로, 서명 포맷을 이 저장소에서 다시 구현하지 않습니다.

---

## 1. 서명이 필요한지 먼저 확인한다

Core 설정 `extensions.signatureMode`(환경변수 `EXTENSIONS_SIGNATURE_MODE`, 기본 `warn`) 가 판단 기준입니다.

| 값 | 미서명 패키지 | 서명이 깨진 패키지 | 필요한 작업 |
| --- | --- | --- | --- |
| `off` | 설치됨 | 설치됨(검증하지 않음) | 없음 |
| `warn`(기본) | 설치됨 + 경고 표시 | **거부** | 없음(권장은 서명) |
| `require` | **거부** | **거부** | 이 문서의 절차 필요 |

`warn` 에서도 깨진 서명을 거부하는 이유는 **미서명(주장 없음)과 위조(거짓 주장)가 다른 사건**이기 때문입니다.
또한 업데이트에는 **서명 다운그레이드 금지**가 추가로 걸립니다 — 직전 설치가 서명 패키지였는데 새 패키지가
미서명이면, `warn` 에서도 거부됩니다. 한 번 서명해서 배포했으면 그 다음 버전부터는 계속 서명해야 합니다.

---

## 2. 서명 포맷(참고)

```text
META-INF/ableops-signature.json     ← 서명 파일(패키지 안에 들어간다. 별도 .sig 파일이 아니다)
```

- 알고리즘은 **Ed25519 하나**입니다. 알 수 없는 `alg` 는 "검증 생략"이 아니라 **거부**입니다.
- 서명 대상은 zip 바이트열이 아니라 **엔트리(파일) 다이제스트 목록**입니다. 서명 파일이 자기 자신의 해시를
  담을 수 없기 때문입니다. 그래서 Core 는 「목록 → zip」과 「zip → 목록」을 **양방향으로 대조**합니다
  (한 방향만 보면 서명된 패키지에 파일을 *추가*하는 공격이 통과합니다).
- 서명 파일의 `publisher` 는 **표시 전용**입니다. 신뢰 판정의 근거는 `keyId` ↔ 등록된 공개키 쌍뿐입니다.

신뢰의 뿌리는 X.509 인증서 체인이 아니라 **운영자가 관리 화면에 등록한 공개키 목록**입니다(폐쇄망 환경을
전제하므로 CRL/OCSP 를 쓰지 않습니다).

---

## 3. 준비물

| 항목 | 설명 |
| --- | --- |
| AbleOps Kafka Core **소스 저장소** | `cmd/extsign` 이 들어 있는 저장소. `extsign` 은 **운영 배포본(Release)에 포함되지 않습니다** — 서명 개인키를 운영 서버에 두지 않기 위해서입니다. |
| Go 툴체인 | `extsign` 을 `go run` 으로 실행합니다. |
| Ed25519 개인키 | 배포자(빌드) 환경에만 둡니다. 저장소·포털·운영 서버 어디에도 올리지 않습니다. |
| 포털 권한 `extension.manage` | 공개키 등록(신뢰 퍼블리셔 관리)에 필요합니다. SystemAdmin 권한입니다. |

---

## 4. 절차

### 4-1. 키쌍을 만든다 (최초 1회)

```bash
# openssl 이 있으면
openssl genpkey -algorithm ed25519 -out ableops-2026a.key

# 없으면 extsign 으로 (Core 저장소에서 실행)
go run ./cmd/extsign -keygen -out ableops-2026a.key
```

두 방법 모두 PKCS#8 PEM(`-----BEGIN PRIVATE KEY-----`) 개인키를 만듭니다.
등록용 공개키는 언제든 다시 출력할 수 있습니다.

```bash
go run ./cmd/extsign -key ableops-2026a.key -pubkey     # base64 공개키
```

개인키 취급 규칙:

- 파일 권한은 소유자 전용(`0600`, Windows 는 상속 제거 후 현재 계정만)으로 둡니다.
- **저장소에 커밋하지 않습니다.** `.gitignore` 에 키 파일 확장자를 넣어 두세요.
- `extsign -keygen` 은 **기존 파일을 덮어쓰지 않습니다.** 덮어쓰면 그 키로 서명한 기존 패키지를 다시는
  검증할 수 없기 때문입니다.
- 키를 잃어버리면 새 `keyId` 로 다시 등록하는 것 외에 복구 방법이 없습니다.

`keyId` 는 나중에 포털에 등록할 식별자와 **문자 그대로(대소문자까지)** 같아야 합니다.
형식은 영문·숫자로 시작하고 끝나며 `.` `_` `-` 만 허용하는 2~64자입니다(예: `ableops-2026a`).

### 4-2. 미서명 패키지를 만든다 (이 저장소)

```powershell
.\scripts\build-extension.ps1
```

```bash
bash ./scripts/build-extension.sh
```

산출물은 `dist/extensions/ableops-kafka-mcp_<version>.ableops-ext` 입니다.

### 4-3. 서명한다 (Core 저장소에서)

```bash
go run ./cmd/extsign \
  -package /path/to/ableops-kafka-mcp_0.6.0.ableops-ext \
  -key     ableops-2026a.key \
  -key-id  ableops-2026a \
  -publisher "AbleOps"
```

- `-out` 을 생략하면 **입력 패키지를 덮어씁니다.** 미서명본을 남기려면 `-out signed/....ableops-ext` 를 쓰세요.
- 이미 서명 파일이 들어 있는 패키지는 **거부**합니다. "누가 서명했는가"가 조용히 바뀌지 않게 하기 위해서이며,
  다시 서명하려면 4-2 부터 새로 만듭니다.
- 성공하면 등록할 공개키(base64)를 함께 출력합니다.

> ⚠ **서명은 마지막 단계입니다.** 서명한 뒤 패키지를 다시 빌드하거나 zip 안의 파일을 하나라도 고치면
> 서명은 무효가 됩니다. 반대로 `build-extension` 스크립트를 다시 실행하면 서명이 없는 새 패키지로 덮어씁니다.

### 4-4. 공개키를 포털에 등록한다

관리 화면 **[확장 기능 관리] → [신뢰 퍼블리셔]** 에서 다음을 등록합니다.

| 필드 | 값 |
| --- | --- |
| `keyId` | 서명할 때 쓴 `-key-id` 와 **문자 그대로** 동일 |
| `publicKey` | `extsign -pubkey` 출력(base64) 또는 PEM `PUBLIC KEY` |
| `algorithm` | `ed25519`(생략 시 기본값) |
| `publisher` | 표시용 이름 |
| 사용 여부 | 켬(`enabled`) |

REST 로도 같은 일을 할 수 있습니다(권한 `extension.manage`).

```text
GET    /api/extensions/trusted-publishers
POST   /api/extensions/trusted-publishers
DELETE /api/extensions/trusted-publishers/{keyId}
```

- 개인키를 보내는 필드는 **없습니다.** 포털은 공개키로 검증만 합니다.
- 같은 `keyId` 로 다시 등록하면 공개키가 **교체**됩니다. 교체는 그 키로 서명한 기존 패키지의 검증 결과를
  바꾸므로 응답이 `replaced`·`keyChanged` 로 알려 줍니다.
- 사용 중지(`enabled=false`)해 둔 키를 재등록할 때 사용 여부를 명시하지 않으면 다시 켜질 수 있습니다.
  화면의 유지/켬/끔 상태를 확인하고 저장하세요.

### 4-5. 설치하고 확인한다

[확장 기능 관리] → [설치] 에서 서명된 파일을 업로드합니다. 설치 전 미리보기
(`POST /api/extensions/install?dryRun=true`)로 서명 판정만 먼저 볼 수도 있습니다.
목록·상세의 서명 상태에 `signed`·`keyId`·`publisher` 가 표시되면 정상입니다.

패키지에 서명 파일이 들어갔는지는 로컬에서도 확인할 수 있습니다.

```powershell
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::OpenRead('dist\extensions\ableops-kafka-mcp_0.6.0.ableops-ext').Entries.FullName
```

```bash
unzip -l dist/extensions/ableops-kafka-mcp_0.6.0.ableops-ext
```

`META-INF/ableops-signature.json` 이 목록에 있어야 합니다.

---

## 5. 문제 해결

| 증상 / 메시지 | 원인 | 조치 |
| --- | --- | --- |
| `신뢰하지 않는 서명 키입니다(keyId=…)` | 공개키 미등록, 또는 `keyId` 표기 불일치 | 4-4 로 등록. 대소문자까지 같아야 합니다 |
| `사용 중지된 서명 키입니다` | 등록은 되어 있으나 `enabled=false` | 신뢰 퍼블리셔에서 사용을 켭니다 |
| `등록된 공개키의 알고리즘이 서명과 다릅니다` | 등록 시 `algorithm` 을 다른 값으로 저장 | `ed25519` 로 다시 등록 |
| `서명이 유효하지 않습니다` / 해시 불일치 | 서명 후 패키지가 바뀜, 또는 잘못된 개인키 | 4-2 → 4-3 을 다시 수행 |
| `이미 서명된 패키지입니다` | 서명본에 다시 서명 시도 | 미서명본을 새로 빌드해서 서명 |
| `신뢰 퍼블리셔 저장소를 사용할 수 없어…` | 포털 저장소 구성 문제 | Core 운영자에게 확인 요청 |
| 업데이트만 거부됨 | 서명 다운그레이드 금지 | 새 버전도 같은 키로 서명해서 올립니다 |
| `허용하지 않는 최상위 항목입니다` | 패키지에 규칙 밖 항목이 섞임 | 빌드 스크립트 산출물을 그대로 쓰고, 수동으로 zip 을 고치지 않습니다 |

---

## 6. 하지 않는 일

- 서명 개인키를 운영 서버·포털 DB·이 저장소에 두지 않습니다. 포털에는 **공개키만** 들어갑니다.
- 이 저장소에서 서명 포맷을 독자 구현하지 않습니다(검증기와 갈라지는 순간 원인을 찾기 어려운 실패가 됩니다).
- 패키지 서명은 실행파일 코드서명(Authenticode 등)과 별개입니다. 이 문서는 `.ableops-ext` 패키지 서명만 다룹니다.

관련 문서: [패키지 빌드](../README.md#managed-extension-패키지-빌드) · [배포 및 실행](deployment.md)
