#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: bash scripts/build.sh [--os all|linux|windows] [--arch amd64|arm64]'
}

target_os=all
target_arch=amd64
while (($#)); do
    case "$1" in
        --os|--arch)
            if (($# < 2)); then usage >&2; exit 2; fi
            if [[ "$1" == --os ]]; then target_os=$2; else target_arch=$2; fi
            shift 2
            ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done
case "$target_os" in all|linux|windows) ;; *) usage >&2; exit 2 ;; esac
case "$target_arch" in amd64|arm64) ;; *) usage >&2; exit 2 ;; esac

project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$project_root"
command -v go >/dev/null
command -v sha256sum >/dev/null

targets=(linux windows)
if [[ "$target_os" != all ]]; then targets=("$target_os"); fi
for platform in "${targets[@]}"; do
    destination="$project_root/dist/$platform-$target_arch"
    binary_name=ableops-kafka-mcp
    auth_name=ableops-mcp-auth
    if [[ "$platform" == windows ]]; then binary_name+=.exe; fi
    if [[ "$platform" == windows ]]; then auth_name+=.exe; fi
    mkdir -p -- "$destination"
    printf 'Building %s/%s...\n' "$platform" "$target_arch"
    CGO_ENABLED=0 GOOS="$platform" GOARCH="$target_arch" GOAMD64=v1 GOARM64=v8.0 GOWORK=off \
        go build -mod=readonly -trimpath -buildvcs=false -ldflags '-s -w' \
        -o "$destination/$binary_name" ./cmd/ableops-kafka-mcp
    CGO_ENABLED=0 GOOS="$platform" GOARCH="$target_arch" GOAMD64=v1 GOARM64=v8.0 GOWORK=off \
        go build -mod=readonly -trimpath -buildvcs=false -ldflags '-s -w' \
        -o "$destination/$auth_name" ./cmd/ableops-mcp-auth

    # 배포에 필요한 공개 파일만 명시적으로 복사한다.
    cp -- .env.example "$destination/.env.example"
    cp -- mcp-server-config.example.yaml "$destination/mcp-server-config.example.yaml"
    cp -- docs/deployment.md "$destination/README.md"
    if [[ "$platform" == linux ]]; then chmod +x -- "$destination/$binary_name" "$destination/$auth_name"; fi
    (cd -- "$destination" && sha256sum -- "$binary_name" "$auth_name" > SHA256SUMS)
    printf '%s\n' "$destination"
done
