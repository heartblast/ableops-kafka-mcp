param()
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot

# 토큰과 실제 식별자는 프로세스 환경변수로만 주입하며 출력하거나 파일로 저장하지 않는다.
# 기본 테스트/CI에서는 실행하지 않는 실연동 옵션을 이 스크립트 범위에서만 활성화한다.
$previousEnvironment = @{}
foreach ($key in @('ABLEOPS_VERIFY_BACKEND', 'ABLEOPS_BASE_URL', 'ABLEOPS_ALLOW_HTTP')) {
    $previousEnvironment[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
}

Push-Location $projectRoot
try {
    if ([string]::IsNullOrWhiteSpace($env:ABLEOPS_BASE_URL)) {
        $env:ABLEOPS_BASE_URL = 'http://localhost:8080'
    }
    if ([string]::IsNullOrWhiteSpace($env:ABLEOPS_ALLOW_HTTP) -and $env:ABLEOPS_BASE_URL -match '^http://(localhost|127\.0\.0\.1|\[::1\])(?::\d+)?/?$') {
        $env:ABLEOPS_ALLOW_HTTP = 'true'
    }
    $env:ABLEOPS_VERIFY_BACKEND = '1'
    # -count=1은 실연동에서 이전 테스트 캐시를 재사용하지 않도록 한다.
    go test ./internal/integration -run '^(TestBackendLive|TestDynamicBackendLive)$' -count=1 -v
    if ($LASTEXITCODE -ne 0) { throw '실제 Backend 검증 실패: 출력된 안전한 시나리오 결과를 확인하세요.' }
} finally {
    foreach ($key in $previousEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($key, $previousEnvironment[$key], 'Process')
    }
    Pop-Location
}
