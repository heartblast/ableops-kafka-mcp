package extension

import (
	"strings"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/mcpserver"
	extv1 "github.com/heartblast/ableops-sdk/extension/v1"
	"github.com/heartblast/ableops-sdk/extension/v1/testkit"
)

// Manifest 는 SDK·Core 계약을 만족해야 한다. 여기서 막히면 Core 는 설치를 거부한다.
func TestManifestSatisfiesContract(t *testing.T) {
	manifest, err := LoadManifest()
	if err != nil {
		t.Fatal(err)
	}
	if issues := testkit.CheckManifest(manifest); len(issues) > 0 {
		t.Fatalf("Manifest 계약 위반: %s", strings.Join(issues, " / "))
	}
	if manifest.ID != ID {
		t.Fatalf("Manifest id = %q, 코드 상수 = %q", manifest.ID, ID)
	}
	if manifest.APIVersion != extv1.APIVersion {
		t.Fatalf("apiVersion = %q", manifest.APIVersion)
	}
	if !manifest.Backend.Enabled || manifest.Backend.Kind != extv1.BackendKindProcess {
		t.Fatalf("backend = %+v — 외부 프로세스 Extension 이어야 합니다", manifest.Backend)
	}
	if manifest.Frontend.Enabled {
		t.Fatal("MCP 는 화면을 기여하지 않습니다")
	}
	// 최소권한: 쓰지 않는 capability·권한·공개 라우트를 선언하지 않는다.
	if len(manifest.Capabilities) != 0 {
		t.Fatalf("capabilities = %v — MCP 도구는 Host API 를 쓰지 않습니다", manifest.Capabilities)
	}
	if len(manifest.Permissions) != 0 || len(manifest.Routes) != 0 || len(manifest.Menus) != 0 {
		t.Fatal("공개 라우트·권한·메뉴를 선언하지 않아야 합니다")
	}
	// Manifest 는 호출마다 같은 값이다(런타임에 선언을 바꿔 검증을 우회할 수 없다).
	again := New().Manifest()
	if again.ID != manifest.ID || again.Version != manifest.Version {
		t.Fatal("Manifest 가 호출마다 달라집니다")
	}
}

// Manifest 의 version 은 MCP 서버가 클라이언트에 알리는 구현 버전과 같아야 한다.
//
// 둘은 같은 프로그램의 버전이다. 한쪽만 올리면 관리 화면의 설치 버전과 MCP 클라이언트가 보는
// 서버 버전이 갈리고, 어느 쪽도 틀렸다고 알려주지 않는다(실제로 0.2.0 이 그대로 남아 있었다).
func TestManifestVersionMatchesServerVersion(t *testing.T) {
	manifest, err := LoadManifest()
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != mcpserver.Version {
		t.Fatalf("manifest.yaml version = %q, mcpserver.Version = %q — 같이 올려야 합니다",
			manifest.Version, mcpserver.Version)
	}
}

// Routes 는 Manifest 선언과 SDK 마운트 규칙을 만족해야 한다.
func TestRoutesSatisfyContract(t *testing.T) {
	ext := New()
	if issues := testkit.CheckRoutes(ext); len(issues) > 0 {
		t.Fatalf("라우트 계약 위반: %s", strings.Join(issues, " / "))
	}
	got := map[string]bool{}
	for _, r := range ext.Routes() {
		got[r.Method+" "+r.Pattern] = true
		if r.Permission != "" {
			t.Fatalf("라우트 %s 에 권한이 선언되어 있습니다 — 공개 프록시 경로가 아닙니다", r.Pattern)
		}
	}
	for _, want := range []string{"POST /mcp", "POST /internal/delegations"} {
		if !got[want] {
			t.Fatalf("라우트 %q 가 없습니다: %v", want, got)
		}
	}
	if len(got) != 2 {
		t.Fatalf("MCP 전용 경로 외의 라우트가 있습니다: %v", got)
	}
}

// Manifest 는 시크릿을 담지 않는다 — 패키지 파일로 배포되고 관리 화면·감사에 노출된다.
func TestManifestCarriesNoSecret(t *testing.T) {
	// 주석은 계약이 아니라 설명이므로 검사에서 제외하고, 실제 선언 줄만 본다.
	for _, line := range strings.Split(string(manifestYAML), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		lowered := strings.ToLower(trimmed)
		for _, forbidden := range []string{"secret", "token", "password", "credential"} {
			if strings.Contains(lowered, forbidden) {
				t.Fatalf("Manifest 선언 줄에 %q 가 들어 있습니다: %q", forbidden, trimmed)
			}
		}
	}
	manifest, err := LoadManifest()
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Config.Schema) != 0 {
		t.Fatal("설정 스키마를 선언하지 않았는데 값이 들어 있습니다")
	}
}
