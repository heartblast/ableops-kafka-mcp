package extension

import (
	"strings"
	"testing"

	"github.com/heartblast/ableops-kafka-mcp/internal/config"
)

// Core 가 준 Host API 주소에서 Backend REST origin 만 남겨야 한다(경로·query·자격증명 제거).
func TestBackendOrigin(t *testing.T) {
	for _, tc := range []struct {
		name      string
		hostURL   string
		want      string
		allowHTTP bool
	}{
		{name: "loopback 호스트 API 경로", hostURL: "http://127.0.0.1:8080/api/extensions/_host", want: "http://127.0.0.1:8080", allowHTTP: true},
		{name: "localhost", hostURL: "http://localhost:8080/api/extensions/_host", want: "http://localhost:8080", allowHTTP: true},
		{name: "IPv6 loopback", hostURL: "http://[::1]:8080/api/extensions/_host", want: "http://[::1]:8080", allowHTTP: true},
		{name: "HTTPS 원격", hostURL: "https://ableops.example.com/api/extensions/_host", want: "https://ableops.example.com"},
		{name: "경로 없음", hostURL: "https://ableops.example.com", want: "https://ableops.example.com"},
		{name: "끝의 슬래시", hostURL: "https://ableops.example.com/", want: "https://ableops.example.com"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, allowHTTP, err := backendOrigin(tc.hostURL)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("origin = %q, want %q", got, tc.want)
			}
			if allowHTTP != tc.allowHTTP {
				t.Fatalf("allowHTTP = %v, want %v", allowHTTP, tc.allowHTTP)
			}
		})
	}
}

// 잘못된 주소는 조용히 고쳐 쓰지 않고 거부한다.
func TestBackendOriginRejects(t *testing.T) {
	for _, tc := range []struct{ name, hostURL string }{
		{name: "빈 값", hostURL: ""},
		{name: "평문 HTTP 원격", hostURL: "http://ableops.example.com/api/extensions/_host"},
		{name: "scheme 없음", hostURL: "/api/extensions/_host"},
		{name: "알 수 없는 scheme", hostURL: "ftp://127.0.0.1:8080"},
		{name: "자격증명 포함", hostURL: "https://user:pass@ableops.example.com/api"},
		{name: "query 포함", hostURL: "https://ableops.example.com/api?token=x"},
		{name: "fragment 포함", hostURL: "https://ableops.example.com/api#x"},
		{name: "호스트 없음", hostURL: "http:///api"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, _, err := backendOrigin(tc.hostURL); err == nil {
				t.Fatalf("거부해야 하는 주소를 받아들였습니다: %q → %q", tc.hostURL, got)
			}
		})
	}
}

// Managed 설정은 공유 Backend 토큰·로컬 인증 저장소·공유 위임 비밀 없이 만들어져야 한다.
func TestManagedServerConfig(t *testing.T) {
	cfg, err := managedServerConfig("http://127.0.0.1:8080/api/extensions/_host")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Transport != "http" {
		t.Fatalf("transport = %q", cfg.Transport)
	}
	if cfg.Backend.BaseURL.String() != "http://127.0.0.1:8080" {
		t.Fatalf("backend base = %q", cfg.Backend.BaseURL.String())
	}
	if !cfg.Backend.AllowHTTP || !cfg.Backend.RequireRequestCredentials || cfg.Backend.Token != "" {
		t.Fatalf("backend = %+v — 사용자별 자격증명만 써야 합니다", cfg.Backend)
	}
	if cfg.AuthStore != "" {
		t.Fatal("Managed 모드는 로컬 인증 저장소를 쓰지 않습니다")
	}
	if len(cfg.AllowedOrigins) != 0 {
		t.Fatal("Managed 모드는 브라우저 Origin 을 허용하지 않습니다")
	}
	if !cfg.WebDelegation.Enabled || cfg.WebDelegation.Secret != "" {
		t.Fatalf("webDelegation = %+v — 공유 비밀은 설정으로 받지 않습니다", cfg.WebDelegation)
	}
	if !cfg.Dynamic.Enabled || cfg.Dynamic.RefreshInterval != config.DefaultDynamicRefreshInterval {
		t.Fatalf("dynamic = %+v", cfg.Dynamic)
	}
	// placeholder 주소는 loopback 이어야 mcpserver 의 검증을 통과한다. 실제 bind 는 extserver 몫이다.
	if cfg.HTTPAddress != managedAddress || !strings.HasPrefix(cfg.HTTPAddress, "127.0.0.1:") {
		t.Fatalf("httpAddress = %q", cfg.HTTPAddress)
	}
}

// 내부 공유 비밀은 매번 새로 만들어지고 mcpserver 의 하한(32바이트)을 넘어야 한다.
func TestNewInternalSecret(t *testing.T) {
	first, err := newInternalSecret()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newInternalSecret()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("내부 공유 비밀이 프로세스마다 같습니다")
	}
	if len(first) < 32 {
		t.Fatalf("내부 공유 비밀 길이 = %d", len(first))
	}
}
