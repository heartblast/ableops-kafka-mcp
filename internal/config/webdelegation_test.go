package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const syntheticDelegationSecret = "synthetic-delegation-secret-0123456789"

func writeServerConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mcp-server-config.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// 기본값은 비활성이고, 켜지 않으면 공유 비밀을 읽어 들고 다니지도 않는다.
func TestWebDelegationDisabledByDefault(t *testing.T) {
	path := writeServerConfig(t, "version: 1\nserver:\n  transport: http\nbackend:\n  base_url: https://ableops.example.com\nauth:\n  store_file: 'store.json'\n")
	cfg, err := LoadServer(path, func(k string) string {
		if k == envWebDelegationSecret {
			return syntheticDelegationSecret
		}
		return ""
	}, ServerOverrides{})
	if err != nil {
		t.Fatalf("기본 설정 적재 실패: %v", err)
	}
	if cfg.WebDelegation.Enabled {
		t.Fatal("기본값이 활성이다")
	}
	if cfg.WebDelegation.Secret != "" {
		t.Fatal("비활성인데 공유 비밀을 보관했다")
	}
	if cfg.WebDelegation.TTL != DefaultWebDelegationTTL {
		t.Fatalf("기본 TTL: %s", cfg.WebDelegation.TTL)
	}
}

// YAML 로 켜고 환경변수로 비밀을 주입하는 것이 표준 경로다. 환경변수가 YAML 보다 우선한다.
func TestWebDelegationYAMLAndEnvironmentPrecedence(t *testing.T) {
	path := writeServerConfig(t, "version: 1\nserver:\n  transport: http\nbackend:\n  base_url: https://ableops.example.com\nauth:\n  store_file: 'store.json'\nweb_delegation:\n  enabled: true\n  ttl: '5m'\n  secret_env: 'SYNTHETIC_DELEGATION_SECRET'\n")
	getenv := func(values map[string]string) func(string) string {
		return func(k string) string { return values[k] }
	}
	cfg, err := LoadServer(path, getenv(map[string]string{"SYNTHETIC_DELEGATION_SECRET": syntheticDelegationSecret}), ServerOverrides{})
	if err != nil {
		t.Fatalf("YAML 적재 실패: %v", err)
	}
	if !cfg.WebDelegation.Enabled || cfg.WebDelegation.TTL != 5*time.Minute || cfg.WebDelegation.Secret != syntheticDelegationSecret {
		t.Fatalf("YAML 설정 반영 실패: enabled=%v ttl=%s secret일치=%v",
			cfg.WebDelegation.Enabled, cfg.WebDelegation.TTL, cfg.WebDelegation.Secret == syntheticDelegationSecret)
	}
	// 환경변수 우선
	cfg, err = LoadServer(path, getenv(map[string]string{
		"SYNTHETIC_DELEGATION_SECRET": syntheticDelegationSecret,
		envWebDelegationTTL:           "20m",
	}), ServerOverrides{})
	if err != nil || cfg.WebDelegation.TTL != 20*time.Minute {
		t.Fatalf("환경변수가 YAML 보다 우선하지 않았다: %v %s", err, cfg.WebDelegation.TTL)
	}
	// 환경변수로 끌 수 있어야 한다(기능을 켠 파일을 그대로 두고 배포에서 차단하는 경로).
	cfg, err = LoadServer(path, getenv(map[string]string{
		"SYNTHETIC_DELEGATION_SECRET": syntheticDelegationSecret,
		envWebDelegation:              "false",
	}), ServerOverrides{})
	if err != nil || cfg.WebDelegation.Enabled {
		t.Fatalf("환경변수로 비활성화되지 않았다: %v", err)
	}
}

// 설정 오류는 기동 시점에 드러나야 한다 — 조용히 기본값으로 떨어지지 않는다.
func TestWebDelegationConfigurationFailures(t *testing.T) {
	base := "version: 1\nserver:\n  transport: %s\nbackend:\n  base_url: https://ableops.example.com\nauth:\n  store_file: 'store.json'\n%s"
	for _, tc := range []struct {
		name, transport, section, secret string
	}{
		{"비밀 없이 활성", "http", "web_delegation:\n  enabled: true\n", ""},
		{"짧은 비밀", "http", "web_delegation:\n  enabled: true\n", "short"},
		{"TTL 하한 미만", "http", "web_delegation:\n  enabled: true\n  ttl: '30s'\n", syntheticDelegationSecret},
		{"TTL 상한 초과", "http", "web_delegation:\n  enabled: true\n  ttl: '1h'\n", syntheticDelegationSecret},
		{"stdio 전송에서 활성", "stdio", "web_delegation:\n  enabled: true\n", syntheticDelegationSecret},
		{"enabled 값 오류", "http", "web_delegation:\n  enabled: 'yes'\n", syntheticDelegationSecret},
		{"예약된 secret_env", "http", "web_delegation:\n  enabled: true\n  secret_env: 'ABLEOPS_BASE_URL'\n", syntheticDelegationSecret},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeServerConfig(t, replaceTwice(base, tc.transport, tc.section))
			_, err := LoadServer(path, func(k string) string {
				if k == envWebDelegationSecret {
					return tc.secret
				}
				if k == "ABLEOPS_API_TOKEN" {
					return "synthetic-session"
				}
				return ""
			}, ServerOverrides{})
			if err == nil {
				t.Fatal("설정 오류가 기동 시점에 드러나지 않았다")
			}
			// 오류 메시지에 비밀이 섞이면 안 된다.
			if tc.secret != "" && strings.Contains(err.Error(), tc.secret) {
				t.Fatal("오류 메시지에 공유 비밀이 노출됐다")
			}
		})
	}
}

// replaceTwice는 서식 문자열의 두 자리를 차례로 채운다(fmt.Sprintf 가 YAML 의 %% 를 요구하지 않게).
func replaceTwice(template, first, second string) string {
	return strings.Replace(strings.Replace(template, "%s", first, 1), "%s", second, 1)
}

// 공개 예제 설정은 그대로 적재되어야 한다(문서와 파서가 갈라지지 않게).
func TestExampleConfigIncludesWebDelegation(t *testing.T) {
	cfg, err := LoadServer("../../mcp-server-config.example.yaml", func(string) string { return "" }, ServerOverrides{})
	if err != nil {
		t.Fatalf("예제 설정 적재 실패: %v", err)
	}
	if cfg.WebDelegation.Enabled {
		t.Fatal("예제 설정이 위임을 켜 두었다")
	}
}
