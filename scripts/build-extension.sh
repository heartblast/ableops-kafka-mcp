#!/usr/bin/env bash
# =============================================================================
# AbleOps Kafka MCP — Managed Extension 패키지(*.ableops-ext) 빌드 (bash · Linux/macOS).
#
# 이 저장소의 패키징 대상은 **하나뿐**이다. 그래서 Core 의 scripts/build-extension.sh 처럼
# extensions/<id>/ 를 탐색하지 않는다(이 저장소는 그 구조가 아니다 — 아래 고정 입력 참조).
#
#   Manifest : internal/extension/manifest.yaml   ← id·version 의 단일 출처
#   Command  : ./cmd/ableops-kafka-mcp-extension  ← Extension backend 진입점
#
# 산출 패키지 구조(Core internal/extensionhost 검증 규칙과 1:1):
#
#   manifest.yaml                                        (최상위 · 원본 byte-for-byte 복사)
#   bin/<GOOS>-<GOARCH>/ableops-kafka-mcp-extension[.exe]  (Core 가 이 경로에서 찾는다)
#
# ⚠ Standalone binary(./cmd/ableops-kafka-mcp)는 패키지에 넣지 않는다. 두 바이너리는 용도가
#   다르다 — standalone 은 외부 MCP Client 용이고, 이 패키지는 Core 가 기동하는 Managed 쪽이다.
# ⚠ 패키지 버전은 Manifest 에서만 읽는다(--version 류 override 없음). Manifest 가 embed 되어
#   바이너리에 들어가므로, 파일명만 다른 버전으로 찍으면 설치본과 파일명이 어긋난다.
#
# 사용법:
#   bash scripts/build-extension.sh
#   bash scripts/build-extension.sh --targets "linux/amd64,windows/amd64" --out-dir dist/extensions
#
# 옵션:
#   -t, --targets <목록>  쉼표구분 "os/arch"(기본 linux/amd64,windows/amd64)
#   -o, --out-dir <경로>  패키지 출력 디렉터리(기본 dist/extensions)
#   -h, --help            도움말
#
# 서명: 이번 단계의 산출물은 **미서명**이다. Core 기본 정책 extensions.signatureMode=warn 에서는
#   경고와 함께 설치된다(off 도 설치 가능). require 환경에는 AbleOps 공식 signing tool 로 서명한
#   패키지가 필요하다 — 서명 포맷을 이 저장소에서 독자 구현하지 않는다.
# =============================================================================
# `sh scripts/build-extension.sh` 처럼 셔뱅을 무시하고 실행하면 dash 가 이 스크립트를 읽어
# `set -o pipefail`·배열·[[ ]] 에서 깨진다. 호출 방식을 탓하는 대신 bash 로 다시 실행한다
# (여기까지는 POSIX 문법만 쓰므로 dash 도 문제없이 읽는다).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

TARGETS="linux/amd64,windows/amd64"
OUT_DIR=""

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

# --- 고정 입력(이 저장소의 대상은 하나뿐이다) -------------------------------
MANIFEST="$ROOT/internal/extension/manifest.yaml"
CMD_PKG="./cmd/ableops-kafka-mcp-extension"
BIN_NAME="ableops-kafka-mcp-extension"
STANDALONE_BIN="ableops-kafka-mcp" # 패키지에 들어가면 안 되는 이름

# Core: internal/extensionhost/security.go 의 allowedTopLevel 과 동일해야 한다.
ALLOWED_TOP=("manifest.yaml" "web" "migrations" "bin" "META-INF" "LICENSE" "README" "README.md" "README.txt")
# Core: 같은 파일의 executableExts — bin/ 밖에 있으면 Core 가 거부한다.
EXEC_EXTS=(".exe" ".dll" ".so" ".dylib" ".bat" ".cmd" ".com" ".ps1" ".sh" ".bash" ".msi" ".scr")
# 패키지에 절대 들어가면 안 되는 파일(이름 그대로 검사한다 — "token" 같은 부분 문자열 오탐 방지).
FORBIDDEN_BASENAMES=(".env" "go.mod" "go.sum" "go.work" "go.work.sum" "secret.key" "mcp-server-config.yaml")
# 경로 일부가 이 단어로 시작/구성되면 민감 산출물로 본다(예: local.auth-store.json).
FORBIDDEN_PATTERNS=("*.auth-store.json" "*auth-store*" "*.mcp-token" "*credentials*" "*.pem" "*.key" ".env.*")

if [[ -t 1 ]]; then C_CYAN=$'\033[36m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_RESET=$'\033[0m'
else C_CYAN=""; C_GREEN=""; C_YELLOW=""; C_RESET=""; fi
step() { printf '%s==> %s%s\n' "$C_CYAN" "$1" "$C_RESET"; }
ok()   { printf '    %sOK  %s%s\n' "$C_GREEN" "$1" "$C_RESET"; }
note() { printf '    %s%s%s\n' "$C_YELLOW" "$1" "$C_RESET"; }
die()  { printf 'ERROR: %s\n' "$1" >&2; exit 1; }
usage() { sed -n '2,32p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }

while (($#)); do
    case "$1" in
        -t|--targets) TARGETS="${2:?--targets 값 필요}"; shift 2 ;;
        -o|--out-dir) OUT_DIR="${2:?--out-dir 값 필요}"; shift 2 ;;
        -h|--help)    usage ;;
        *)            die "알 수 없는 옵션: $1 (--help 참조)" ;;
    esac
done
[[ -n "$OUT_DIR" ]] || OUT_DIR="$ROOT/dist/extensions"
[[ "$OUT_DIR" = /* ]] || OUT_DIR="$ROOT/$OUT_DIR"

cd -- "$ROOT"
command -v go >/dev/null 2>&1 || die "go 가 PATH 에 없습니다(Go 1.27+ 필요)."
[[ -f "$MANIFEST" ]] || die "Manifest 가 없습니다: $MANIFEST"

# --- Manifest 가 id·version·패키지 파일명의 단일 출처다 ----------------------
# apiVersion: 같은 다른 키와 섞이지 않도록 줄 시작에 붙은 키만 본다.
EXT_ID="$(sed -n 's/^id:[[:space:]]*"\{0,1\}\([^"[:space:]]*\).*/\1/p' "$MANIFEST" | head -n 1)"
VERSION="$(sed -n 's/^version:[[:space:]]*"\{0,1\}\([^"[:space:]]*\).*/\1/p' "$MANIFEST" | head -n 1)"
[[ -n "$EXT_ID" ]]  || die "Manifest 에서 id 를 읽지 못했습니다: $MANIFEST"
[[ -n "$VERSION" ]] || die "Manifest 에서 version 을 읽지 못했습니다: $MANIFEST"

step "Managed Extension 패키지 빌드: $EXT_ID $VERSION"
ok "Go: $(go version)"
ok "Manifest: ${MANIFEST#"$ROOT"/}"
ok "Command: $CMD_PKG"

# --- 1) 사전 컴파일 점검 -----------------------------------------------------
# ⚠ 기존 산출물을 지우기 **전에** 현재 플랫폼 빌드를 확인한다. 컴파일도 되지 않는 상태에서
#   dist/ 를 비우면 직전에 쓰던 정상 패키지까지 잃는다.
step "컴파일 사전 점검 (go build $CMD_PKG)"
PRECHECK="$(mktemp -t ableops-ext-precheck.XXXXXX)"
if ! go build -o "$PRECHECK" "$CMD_PKG"; then
    rm -f -- "$PRECHECK"
    die "코드가 컴파일되지 않습니다. 기존 패키지는 삭제하지 않았습니다."
fi
rm -f -- "$PRECHECK"
ok "컴파일 점검 통과"

# --- 2) 스테이지 ------------------------------------------------------------
STAGE="$ROOT/build/extensions/$EXT_ID"
rm -rf -- "$STAGE"
mkdir -p -- "$STAGE" "$OUT_DIR"

# Manifest 는 원본을 그대로 복사한다(재작성·template 생성 금지 — 단일 출처).
cp -- "$MANIFEST" "$STAGE/manifest.yaml"
ok "manifest.yaml (원본 복사)"

# --- 3) 크로스 컴파일 --------------------------------------------------------
BUILT_TARGETS=()
IFS=',' read -ra TARGET_LIST <<< "$TARGETS"
for target in "${TARGET_LIST[@]}"; do
    target="$(printf '%s' "$target" | tr -d '[:space:]')"
    [[ -n "$target" ]] || continue
    goos="${target%%/*}"
    goarch="${target##*/}"
    [[ "$goos" != "$target" && -n "$goarch" ]] || die "잘못된 타겟 형식: '$target' (os/arch 여야 합니다)"
    ext=""; [[ "$goos" == windows ]] && ext=".exe"

    step "크로스 컴파일: $goos/$goarch"
    # Core 는 <installDir>/bin/<GOOS>-<GOARCH>/ 에서 실행 파일을 찾는다(하이픈 구분).
    bin_dir="$STAGE/bin/${goos}-${goarch}"
    mkdir -p -- "$bin_dir"
    # CGO_ENABLED=0: 순수 Go 정적 바이너리여야 Core 가 임의 호스트에서 기동할 수 있다.
    # GOWORK=off: 개발자 로컬의 go.work 가 모듈 해석에 끼어들지 못하게 한다.
    CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" GOWORK=off \
        go build -mod=readonly -trimpath -buildvcs=false -ldflags '-s -w' \
        -o "$bin_dir/${BIN_NAME}${ext}" "$CMD_PKG" \
        || die "go build 실패: $CMD_PKG ($goos/$goarch)"
    # Core 는 비-Windows 에서 실행 권한이 없는 파일을 건너뛴다(process.go resolveExtensionBinary).
    chmod 0755 -- "$bin_dir/${BIN_NAME}${ext}"
    ok "bin/${goos}-${goarch}/${BIN_NAME}${ext}"
    BUILT_TARGETS+=("${goos}-${goarch}")
done
[[ ${#BUILT_TARGETS[@]} -gt 0 ]] || die "빌드한 타겟이 없습니다(--targets 를 확인하세요)."

# --- 4) 스테이지 규칙 검증(Core 의 거부 규칙을 여기서 먼저 잡는다) -----------
step "스테이지 규칙 검증"
TOP_ENTRIES=()
for entry in "$STAGE"/*; do
    [[ -e "$entry" ]] || continue
    base="$(basename -- "$entry")"
    allowed=0
    for a in "${ALLOWED_TOP[@]}"; do [[ "$base" == "$a" ]] && allowed=1; done
    ((allowed)) || die "허용하지 않는 최상위 항목입니다: '$base' (허용: ${ALLOWED_TOP[*]})"
    TOP_ENTRIES+=("$base")
done
while IFS= read -r f; do
    rel="${f#"$STAGE"/}"
    if [[ "$rel" != bin/* ]]; then
        for e in "${EXEC_EXTS[@]}"; do
            [[ "$rel" == *"$e" ]] && die "실행 파일은 bin/ 아래에만 둘 수 있습니다: $rel"
        done
    fi
done < <(find "$STAGE" -type f)
# 심볼릭 링크가 하나라도 있으면 Core 가 패키지 전체를 거부한다(설치 루트 밖을 가리킬 수 있다).
[[ -z "$(find "$STAGE" -type l -print -quit)" ]] || die "패키지에 심볼릭 링크가 있습니다(Core 가 거부합니다)."
ok "최상위 항목·실행 파일 위치·링크 규칙 통과"

# --- 5) 패키징(zip) ---------------------------------------------------------
PKG_NAME="${EXT_ID}_${VERSION}.ableops-ext"
PKG_PATH="$OUT_DIR/$PKG_NAME"
rm -f -- "$PKG_PATH"

step "패키징: $PKG_NAME"
# ⚠ "." 로 압축하지 않는다 — "./" 항목이 들어가면 Core 가 항목 이름 검사에서 거부한다.
# ⚠ '-a'(확장자로 형식 추론)는 쓰지 않는다 — .ableops-ext 는 zip 으로 추론되지 않아 조용히
#   tar 가 만들어지고, 설치 단계에서야 "zip 형식이 아닙니다"로 드러난다.
if command -v zip >/dev/null 2>&1; then
    ( cd -- "$STAGE" && zip -qrX "$PKG_PATH" "${TOP_ENTRIES[@]}" ) || die "zip 패키징 실패"
elif command -v bsdtar >/dev/null 2>&1; then
    bsdtar -cf "$PKG_PATH" --format=zip -C "$STAGE" "${TOP_ENTRIES[@]}" || die "zip 패키징 실패(bsdtar)"
else
    # macOS 기본 tar 는 libarchive 라 --format=zip 을 지원한다. GNU tar 는 지원하지 않는다.
    tar -cf "$PKG_PATH" --format=zip -C "$STAGE" "${TOP_ENTRIES[@]}" 2>/dev/null \
        || die "zip 패키징 실패: zip 또는 libarchive(bsdtar)가 필요합니다. Windows 에서는 scripts/build-extension.ps1 을 쓰세요."
fi
ok "$PKG_PATH"

# --- 6) 패키지 자체 검증 -----------------------------------------------------
# 스테이지가 아니라 **만들어진 zip 을 다시 열어** 확인한다. 스테이지가 맞다고 zip 이 맞다는
# 보장은 없다(압축 도구가 항목 이름을 바꾸거나 형식을 바꿔 쓸 수 있다).
zip_list() {
    if command -v unzip >/dev/null 2>&1; then
        unzip -Z1 -- "$PKG_PATH"
    else
        python3 - "$PKG_PATH" <<'PY'
import sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as z:
    for n in z.namelist():
        print(n)
PY
    fi
}
zip_cat() {
    if command -v unzip >/dev/null 2>&1; then
        unzip -p -- "$PKG_PATH" "$1"
    else
        python3 - "$PKG_PATH" "$1" <<'PY'
import sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as z:
    sys.stdout.buffer.write(z.read(sys.argv[2]))
PY
    fi
}
command -v unzip >/dev/null 2>&1 || command -v python3 >/dev/null 2>&1 \
    || die "패키지 검증에 unzip 또는 python3 가 필요합니다."

step "패키지 검증"

# 6-1) 실제 ZIP 인가(형식이 어긋나면 설치 단계에서야 드러난다).
[[ "$(head -c 2 -- "$PKG_PATH")" == "PK" ]] || { rm -f -- "$PKG_PATH"; die "패키징 결과가 zip 형식이 아닙니다."; }
ok "ZIP signature"

ENTRIES="$(zip_list)"
[[ -n "$ENTRIES" ]] || die "패키지가 비어 있습니다."

# 6-2) 항목 이름 규칙 — 역슬래시·절대경로·상위참조·최상위 허용 목록.
while IFS= read -r name; do
    [[ -n "$name" ]] || continue
    [[ "$name" != *'\'* ]]  || die "항목 이름에 역슬래시가 있습니다: $name"
    [[ "$name" != /* ]]     || die "항목이 절대경로입니다: $name"
    [[ "$name" != ./* ]]    || die "항목에 './' 접두사가 있습니다: $name"
    [[ "$name" != *'..'/* && "$name" != */'..' ]] || die "항목 경로에 상위 참조(..)가 있습니다: $name"
    top="${name%%/*}"
    allowed=0
    for a in "${ALLOWED_TOP[@]}"; do [[ "$top" == "$a" ]] && allowed=1; done
    ((allowed)) || die "허용하지 않는 최상위 항목입니다: $top ($name)"
done <<< "$ENTRIES"
ok "항목 이름·최상위 규칙"

# 6-3) Manifest 가 원본과 동일한가(byte-for-byte).
if ! zip_cat manifest.yaml | cmp -s -- - "$MANIFEST"; then
    die "패키지의 manifest.yaml 이 원본과 다릅니다: ${MANIFEST#"$ROOT"/}"
fi
ok "manifest.yaml 이 ${MANIFEST#"$ROOT"/} 과 동일"

# 6-4) 빌드한 타겟의 바이너리가 실제로 들어 있는가.
for t in "${BUILT_TARGETS[@]}"; do
    want="bin/$t/$BIN_NAME"; [[ "$t" == windows-* ]] && want+=".exe"
    grep -qxF -- "$want" <<< "$ENTRIES" || die "바이너리가 패키지에 없습니다: $want"
    ok "$want"
done

# 6-5) Standalone binary 가 섞이지 않았는가(용도가 다른 바이너리다).
while IFS= read -r name; do
    base="$(basename -- "$name")"
    base="${base%.exe}"
    [[ "$base" != "$STANDALONE_BIN" ]] || die "Standalone binary 가 패키지에 들어 있습니다: $name"
done <<< "$ENTRIES"
ok "Standalone binary 없음"

# 6-6) 민감 파일·설정·소스가 섞이지 않았는가.
while IFS= read -r name; do
    base="$(basename -- "$name")"
    for b in "${FORBIDDEN_BASENAMES[@]}"; do
        [[ "$base" != "$b" ]] || die "패키지에 넣을 수 없는 파일입니다: $name"
    done
    for p in "${FORBIDDEN_PATTERNS[@]}"; do
        # shellcheck disable=SC2053 # 오른쪽은 glob 패턴이어야 한다
        [[ "$base" != $p ]] || die "패키지에 민감 파일로 보이는 항목이 있습니다: $name"
    done
    [[ "$base" != *.go ]] || die "패키지에 소스 코드가 있습니다: $name"
done <<< "$ENTRIES"
ok "민감 파일·소스 미포함"

# --- 7) 요약 ----------------------------------------------------------------
step "패키지 내용"
while IFS= read -r name; do printf '    %s\n' "$name"; done <<< "$ENTRIES"
step "완료: $PKG_NAME ($(wc -c < "$PKG_PATH" | tr -d ' ') bytes)"
note "미서명 패키지입니다 — signatureMode=off/warn 에서 설치 가능(warn 은 경고 표시), require 에서는 별도 서명이 필요합니다."
note "설치: AbleOps Kafka 관리 화면 [확장 기능 관리] → [설치] 에서 이 파일을 업로드하세요."
printf '%s\n' "$PKG_PATH"
