[CmdletBinding()]
param(
    [ValidateSet('all', 'linux', 'windows')]
    [string]$TargetOS = 'all',
    [ValidateSet('amd64', 'arm64')]
    [string]$Arch = 'amd64'
)

$ErrorActionPreference = 'Stop'
$TargetOS = $TargetOS.ToLowerInvariant()
$Arch = $Arch.ToLowerInvariant()
$projectRoot = Split-Path -Parent $PSScriptRoot
$targets = if ($TargetOS -eq 'all') { @('linux', 'windows') } else { @($TargetOS) }
$buildEnvironment = @{
    CGO_ENABLED = '0'
    GOOS = ''
    GOARCH = $Arch
    GOAMD64 = 'v1'
    GOARM64 = 'v8.0'
    GOWORK = 'off'
}
$savedEnvironment = @{}
foreach ($key in $buildEnvironment.Keys) {
    $savedEnvironment[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
}

Push-Location $projectRoot
try {
    foreach ($key in $buildEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($key, $buildEnvironment[$key], 'Process')
    }
    foreach ($platform in $targets) {
        $env:GOOS = $platform
        $destination = Join-Path $projectRoot "dist/$platform-$Arch"
        $binaryName = if ($platform -eq 'windows') { 'ableops-kafka-mcp.exe' } else { 'ableops-kafka-mcp' }
        $binaryPath = Join-Path $destination $binaryName
        New-Item -ItemType Directory -Force -Path $destination | Out-Null

        Write-Host "Building $platform/$Arch..."
        go build -mod=readonly -trimpath -buildvcs=false -ldflags '-s -w' -o $binaryPath ./cmd/ableops-kafka-mcp
        if ($LASTEXITCODE -ne 0) { throw "go build failed for $platform/$Arch" }

        $authName = if ($platform -eq 'windows') { 'ableops-mcp-auth.exe' } else { 'ableops-mcp-auth' }
        $authPath = Join-Path $destination $authName
        go build -mod=readonly -trimpath -buildvcs=false -ldflags '-s -w' -o $authPath ./cmd/ableops-mcp-auth
        if ($LASTEXITCODE -ne 0) { throw "auth CLI build failed for $platform/$Arch" }

        # 배포에 필요한 공개 파일만 명시적으로 복사한다.
        Copy-Item -LiteralPath '.env.example' -Destination (Join-Path $destination '.env.example') -Force
        Copy-Item -LiteralPath 'mcp-server-config.example.yaml' -Destination (Join-Path $destination 'mcp-server-config.example.yaml') -Force
        Copy-Item -LiteralPath 'docs/deployment.md' -Destination (Join-Path $destination 'README.md') -Force
        $digest = (Get-FileHash -LiteralPath $binaryPath -Algorithm SHA256).Hash.ToLowerInvariant()
        $authDigest = (Get-FileHash -LiteralPath $authPath -Algorithm SHA256).Hash.ToLowerInvariant()
        # Linux sha256sum에서도 바로 읽을 수 있도록 BOM 없이 LF로 기록한다.
        [IO.File]::WriteAllText((Join-Path $destination 'SHA256SUMS'), "$digest  $binaryName`n$authDigest  $authName`n", [Text.Encoding]::ASCII)
        Write-Output $destination
    }
} finally {
    foreach ($key in $savedEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($key, $savedEnvironment[$key], 'Process')
    }
    Pop-Location
}
