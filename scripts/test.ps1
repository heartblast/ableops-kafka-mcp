param()
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    go test ./...
    if ($LASTEXITCODE -ne 0) { throw 'go test failed' }
    go vet ./...
    if ($LASTEXITCODE -ne 0) { throw 'go vet failed' }
    go build ./cmd/ableops-kafka-mcp
    if ($LASTEXITCODE -ne 0) { throw 'go build failed' }
} finally {
    Pop-Location
}
