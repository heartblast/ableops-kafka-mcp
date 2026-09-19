# =============================================================================
# AbleOps Kafka MCP — Managed Extension 패키지(*.ableops-ext) 빌드 (PowerShell · Windows).
#
# scripts/build-extension.sh 의 PowerShell 대응본이다. **같은 구조의 패키지**를 만든다.
#
#   Manifest : internal/extension/manifest.yaml   ← id·version 의 단일 출처
#   Command  : ./cmd/ableops-kafka-mcp-extension  ← Extension backend 진입점
#
#   manifest.yaml                                          (최상위 · 원본 byte-for-byte 복사)
#   bin/<GOOS>-<GOARCH>/ableops-kafka-mcp-extension[.exe]  (Core 가 이 경로에서 찾는다)
#
# ⚠ Standalone binary(./cmd/ableops-kafka-mcp)는 패키지에 넣지 않는다.
# ⚠ 패키지 버전은 Manifest 에서만 읽는다(-Version 류 override 없음).
#
# 사용법:
#   ./scripts/build-extension.ps1
#   ./scripts/build-extension.ps1 -Targets "linux/amd64,windows/amd64" -OutDir dist/extensions
#
# 서명: 산출물은 **미서명**이다. signatureMode=off/warn 에서는 설치되고(warn 은 경고),
#   require 환경에는 AbleOps 공식 signing tool 로 서명한 패키지가 필요하다.
# =============================================================================
[CmdletBinding()]
param(
    [string]$Targets = 'linux/amd64,windows/amd64',
    [string]$OutDir = ''
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot

# --- 고정 입력(이 저장소의 대상은 하나뿐이다) -------------------------------
$manifestPath = Join-Path $projectRoot 'internal/extension/manifest.yaml'
$cmdPkg = './cmd/ableops-kafka-mcp-extension'
$binName = 'ableops-kafka-mcp-extension'
$standaloneBin = 'ableops-kafka-mcp'

# Core: internal/extensionhost/security.go 의 allowedTopLevel·executableExts 와 동일해야 한다.
$allowedTop = @('manifest.yaml', 'web', 'migrations', 'bin', 'META-INF', 'LICENSE', 'README', 'README.md', 'README.txt')
$execExts = @('.exe', '.dll', '.so', '.dylib', '.bat', '.cmd', '.com', '.ps1', '.sh', '.bash', '.msi', '.scr')
# 패키지에 절대 들어가면 안 되는 파일(이름 그대로 검사한다 — 부분 문자열 오탐 방지).
$forbiddenNames = @('.env', 'go.mod', 'go.sum', 'go.work', 'go.work.sum', 'secret.key', 'mcp-server-config.yaml')
$forbiddenPatterns = @('*.auth-store.json', '*auth-store*', '*.mcp-token', '*credentials*', '*.pem', '*.key', '.env.*', '*.go')

function Write-Step { param([string]$Message) Write-Host "==> $Message" -ForegroundColor Cyan }
function Write-Ok { param([string]$Message) Write-Host "    OK  $Message" -ForegroundColor Green }
function Write-Note { param([string]$Message) Write-Host "    $Message" -ForegroundColor Yellow }

Push-Location $projectRoot
try {
    if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'go 가 PATH 에 없습니다(Go 1.27+ 필요).' }
    if (-not (Test-Path -LiteralPath $manifestPath)) { throw "Manifest 가 없습니다: $manifestPath" }
    if (-not $OutDir) { $OutDir = Join-Path $projectRoot 'dist/extensions' }
    elseif (-not [System.IO.Path]::IsPathRooted($OutDir)) { $OutDir = Join-Path $projectRoot $OutDir }

    # --- Manifest 가 id·version·패키지 파일명의 단일 출처다 -----------------
    # apiVersion: 같은 다른 키와 섞이지 않도록 줄 시작에 붙은 키만 본다.
    $manifestLines = Get-Content -LiteralPath $manifestPath
    $extId = ($manifestLines | Select-String -Pattern '^id:\s*"?([^"\s]+)' | Select-Object -First 1).Matches[0].Groups[1].Value
    $version = ($manifestLines | Select-String -Pattern '^version:\s*"?([^"\s]+)' | Select-Object -First 1).Matches[0].Groups[1].Value
    if (-not $extId) { throw "Manifest 에서 id 를 읽지 못했습니다: $manifestPath" }
    if (-not $version) { throw "Manifest 에서 version 을 읽지 못했습니다: $manifestPath" }

    Write-Step "Managed Extension 패키지 빌드: $extId $version"
    Write-Ok "Go: $(go version)"
    Write-Ok "Manifest: internal/extension/manifest.yaml"
    Write-Ok "Command: $cmdPkg"

    # --- 1) 사전 컴파일 점검 -----------------------------------------------
    # ⚠ 기존 산출물을 지우기 **전에** 확인한다. 컴파일도 안 되는 상태에서 dist/ 를 비우면
    #   직전에 쓰던 정상 패키지까지 잃는다.
    Write-Step "컴파일 사전 점검 (go build $cmdPkg)"
    $precheck = Join-Path ([System.IO.Path]::GetTempPath()) ("ableops-ext-precheck-" + [guid]::NewGuid().ToString('N'))
    go build -o $precheck $cmdPkg
    if ($LASTEXITCODE -ne 0) { throw '코드가 컴파일되지 않습니다. 기존 패키지는 삭제하지 않았습니다.' }
    Remove-Item -LiteralPath $precheck -Force -ErrorAction SilentlyContinue
    Write-Ok '컴파일 점검 통과'

    # --- 2) 스테이지 --------------------------------------------------------
    $stage = Join-Path $projectRoot "build/extensions/$extId"
    if (Test-Path -LiteralPath $stage) { Remove-Item -LiteralPath $stage -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $stage | Out-Null
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

    # Manifest 는 원본을 그대로 복사한다(재작성·template 생성 금지 — 단일 출처).
    Copy-Item -LiteralPath $manifestPath -Destination (Join-Path $stage 'manifest.yaml') -Force
    Write-Ok 'manifest.yaml (원본 복사)'

    # --- 3) 크로스 컴파일 ---------------------------------------------------
    $buildEnvKeys = @('CGO_ENABLED', 'GOOS', 'GOARCH', 'GOWORK')
    $savedEnv = @{}
    foreach ($key in $buildEnvKeys) { $savedEnv[$key] = [Environment]::GetEnvironmentVariable($key, 'Process') }
    $builtTargets = @()
    try {
        # CGO_ENABLED=0: 순수 Go 정적 바이너리여야 Core 가 임의 호스트에서 기동할 수 있다.
        # GOWORK=off: 개발자 로컬의 go.work 가 모듈 해석에 끼어들지 못하게 한다.
        $env:CGO_ENABLED = '0'
        $env:GOWORK = 'off'
        foreach ($target in ($Targets -split ',')) {
            $target = $target.Trim()
            if (-not $target) { continue }
            $parts = $target -split '/'
            if ($parts.Count -ne 2 -or -not $parts[0] -or -not $parts[1]) { throw "잘못된 타겟 형식: '$target' (os/arch 여야 합니다)" }
            $goos = $parts[0]
            $goarch = $parts[1]
            $exeSuffix = if ($goos -eq 'windows') { '.exe' } else { '' }

            Write-Step "크로스 컴파일: $goos/$goarch"
            # Core 는 <installDir>/bin/<GOOS>-<GOARCH>/ 에서 실행 파일을 찾는다(하이픈 구분).
            $binDir = Join-Path $stage "bin/$goos-$goarch"
            New-Item -ItemType Directory -Force -Path $binDir | Out-Null
            $env:GOOS = $goos
            $env:GOARCH = $goarch
            go build -mod=readonly -trimpath -buildvcs=false -ldflags '-s -w' -o (Join-Path $binDir "$binName$exeSuffix") $cmdPkg
            if ($LASTEXITCODE -ne 0) { throw "go build 실패: $cmdPkg ($goos/$goarch)" }
            Write-Ok "bin/$goos-$goarch/$binName$exeSuffix"
            $builtTargets += "$goos-$goarch"
        }
    } finally {
        foreach ($key in $buildEnvKeys) { [Environment]::SetEnvironmentVariable($key, $savedEnv[$key], 'Process') }
    }
    if ($builtTargets.Count -eq 0) { throw '빌드한 타겟이 없습니다(-Targets 를 확인하세요).' }

    # --- 4) 스테이지 규칙 검증(Core 의 거부 규칙을 여기서 먼저 잡는다) ------
    Write-Step '스테이지 규칙 검증'
    foreach ($entry in Get-ChildItem -LiteralPath $stage) {
        if ($allowedTop -notcontains $entry.Name) {
            throw "허용하지 않는 최상위 항목입니다: '$($entry.Name)' (허용: $($allowedTop -join ', '))"
        }
    }
    foreach ($file in Get-ChildItem -LiteralPath $stage -Recurse -File) {
        $rel = $file.FullName.Substring($stage.Length + 1) -replace '\\', '/'
        if (($execExts -contains $file.Extension.ToLowerInvariant()) -and (($rel -split '/')[0] -ne 'bin')) {
            throw "실행 파일은 bin/ 아래에만 둘 수 있습니다: $rel"
        }
    }
    Write-Ok '최상위 항목·실행 파일 위치 규칙 통과'

    # --- 5) 패키징(zip) -----------------------------------------------------
    # ⚠ Compress-Archive 를 쓰지 않는다: PowerShell 5.1 은 항목 이름에 역슬래시를 남길 수 있고,
    #   Core 는 역슬래시가 든 항목을 거부한다(경로 탈출 방어). '/' 로 직접 만든다.
    Add-Type -AssemblyName System.IO.Compression | Out-Null
    Add-Type -AssemblyName System.IO.Compression.FileSystem | Out-Null

    $pkgName = "${extId}_${version}.ableops-ext"
    $pkgPath = Join-Path $OutDir $pkgName
    if (Test-Path -LiteralPath $pkgPath) { Remove-Item -LiteralPath $pkgPath -Force }

    Write-Step "패키징: $pkgName"
    $zip = [System.IO.Compression.ZipFile]::Open($pkgPath, [System.IO.Compression.ZipArchiveMode]::Create)
    try {
        foreach ($file in (Get-ChildItem -LiteralPath $stage -Recurse -File | Sort-Object FullName)) {
            $rel = $file.FullName.Substring($stage.Length + 1) -replace '\\', '/'
            $entry = $zip.CreateEntry($rel, [System.IO.Compression.CompressionLevel]::Optimal)
            # bin/ 항목에 POSIX 실행 비트를 남긴다. Core 는 해제 시 권한을 직접 정하므로 설치에는
            # 영향이 없지만, 패키지를 손으로 풀어 쓰는 Linux 쪽에서 chmod 를 다시 하지 않아도 된다.
            # (ExternalAttributes 는 PowerShell 5.1 의 .NET Framework 에 없을 수 있어 방어한다.)
            if ($rel.StartsWith('bin/') -and ($entry.PSObject.Properties.Name -contains 'ExternalAttributes')) {
                $entry.ExternalAttributes = 0x81ED0000  # 0100755 << 16
            }
            $entryStream = $entry.Open()
            try {
                $fileStream = [System.IO.File]::OpenRead($file.FullName)
                try { $fileStream.CopyTo($entryStream) } finally { $fileStream.Dispose() }
            } finally { $entryStream.Dispose() }
        }
    } finally {
        $zip.Dispose()
    }
    Write-Ok $pkgPath

    # --- 6) 패키지 자체 검증 ------------------------------------------------
    # 스테이지가 아니라 **만들어진 zip 을 다시 열어** 확인한다.
    Write-Step '패키지 검증'

    # 6-1) 실제 ZIP 인가.
    $signature = New-Object byte[] 2
    $signatureStream = [System.IO.File]::OpenRead($pkgPath)
    try { $null = $signatureStream.Read($signature, 0, 2) } finally { $signatureStream.Dispose() }
    if ($signature[0] -ne 0x50 -or $signature[1] -ne 0x4B) {
        Remove-Item -LiteralPath $pkgPath -Force
        throw '패키징 결과가 zip 형식이 아닙니다.'
    }
    Write-Ok 'ZIP signature'

    $names = @()
    $manifestInZip = $null
    $reader = [System.IO.Compression.ZipFile]::OpenRead($pkgPath)
    try {
        foreach ($entry in $reader.Entries) {
            $names += $entry.FullName
            if ($entry.FullName -eq 'manifest.yaml') {
                $stream = $entry.Open()
                try {
                    $buffer = New-Object System.IO.MemoryStream
                    $stream.CopyTo($buffer)
                    $manifestInZip = $buffer.ToArray()
                } finally { $stream.Dispose() }
            }
        }
    } finally { $reader.Dispose() }
    if ($names.Count -eq 0) { throw '패키지가 비어 있습니다.' }

    # 6-2) 항목 이름 규칙 — 역슬래시·절대경로·상위참조·최상위 허용 목록.
    foreach ($name in $names) {
        if ($name -like '*\*') { throw "항목 이름에 역슬래시가 있습니다: $name" }
        if ($name.StartsWith('/')) { throw "항목이 절대경로입니다: $name" }
        if ($name.StartsWith('./')) { throw "항목에 './' 접두사가 있습니다: $name" }
        if (($name -split '/') -contains '..') { throw "항목 경로에 상위 참조(..)가 있습니다: $name" }
        $top = ($name -split '/')[0]
        if ($allowedTop -notcontains $top) { throw "허용하지 않는 최상위 항목입니다: $top ($name)" }
    }
    Write-Ok '항목 이름·최상위 규칙'

    # 6-3) Manifest 가 원본과 동일한가(byte-for-byte).
    if ($null -eq $manifestInZip) { throw '패키지 최상위에 manifest.yaml 이 없습니다.' }
    $manifestOriginal = [System.IO.File]::ReadAllBytes($manifestPath)
    if ($manifestInZip.Length -ne $manifestOriginal.Length) {
        throw '패키지의 manifest.yaml 이 원본과 다릅니다: internal/extension/manifest.yaml'
    }
    for ($i = 0; $i -lt $manifestOriginal.Length; $i++) {
        if ($manifestInZip[$i] -ne $manifestOriginal[$i]) {
            throw '패키지의 manifest.yaml 이 원본과 다릅니다: internal/extension/manifest.yaml'
        }
    }
    Write-Ok 'manifest.yaml 이 internal/extension/manifest.yaml 과 동일'

    # 6-4) 빌드한 타겟의 바이너리가 실제로 들어 있는가.
    foreach ($t in $builtTargets) {
        $want = "bin/$t/$binName"
        if ($t.StartsWith('windows-')) { $want += '.exe' }
        if ($names -notcontains $want) { throw "바이너리가 패키지에 없습니다: $want" }
        Write-Ok $want
    }

    # 6-5) Standalone binary 가 섞이지 않았는가(용도가 다른 바이너리다).
    foreach ($name in $names) {
        $base = [System.IO.Path]::GetFileName($name)
        if ($base.EndsWith('.exe')) { $base = $base.Substring(0, $base.Length - 4) }
        if ($base -eq $standaloneBin) { throw "Standalone binary 가 패키지에 들어 있습니다: $name" }
    }
    Write-Ok 'Standalone binary 없음'

    # 6-6) 민감 파일·설정·소스가 섞이지 않았는가.
    foreach ($name in $names) {
        $base = [System.IO.Path]::GetFileName($name)
        if (-not $base) { continue }
        if ($forbiddenNames -contains $base) { throw "패키지에 넣을 수 없는 파일입니다: $name" }
        foreach ($pattern in $forbiddenPatterns) {
            if ($base -like $pattern) { throw "패키지에 민감 파일로 보이는 항목이 있습니다: $name" }
        }
    }
    Write-Ok '민감 파일·소스 미포함'

    # --- 7) 요약 ------------------------------------------------------------
    Write-Step '패키지 내용'
    foreach ($name in $names) { Write-Host "    $name" }
    Write-Step "완료: $pkgName ($((Get-Item -LiteralPath $pkgPath).Length) bytes)"
    Write-Note '미서명 패키지입니다 — signatureMode=off/warn 에서 설치 가능(warn 은 경고 표시), require 에서는 별도 서명이 필요합니다.'
    Write-Note '설치: AbleOps Kafka 관리 화면 [확장 기능 관리] → [설치] 에서 이 파일을 업로드하세요.'
    Write-Output $pkgPath
} finally {
    Pop-Location
}
