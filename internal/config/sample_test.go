package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMessageSamplePolicyConfiguration(t *testing.T) {
	base := map[string]string{"ABLEOPS_BASE_URL": "https://ableops.example.com", "ABLEOPS_API_TOKEN": "synthetic-session"}
	for _, tc := range []struct {
		name, enabled, topics string
		wantErr               bool
	}{
		{"default", "", "", false}, {"enabled_without_policy", "true", "", true}, {"explicit", "true", "c1/orders,c2/events", false}, {"wildcard", "true", "c1/*", true}, {"missing_cluster", "true", "orders", true}, {"empty_cluster", "true", "/orders", true}, {"bad_bool", "yes", "c1/orders", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadFrom(func(k string) string {
				switch k {
				case "ABLEOPS_MESSAGE_SAMPLE_ENABLED":
					return tc.enabled
				case "ABLEOPS_MESSAGE_SAMPLE_TOPICS":
					return tc.topics
				}
				return base[k]
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("정책 검증: %v", err)
			}
			if err == nil && cfg.SampleEnabled != (tc.enabled == "true") {
				t.Fatal("기능 플래그 불일치")
			}
		})
	}
}

func TestMessageSampleYAMLAndEnvironmentPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sample.yaml")
	body := "version: 1\nbackend:\n  base_url: https://ableops.example.com\nmessage_sample:\n  enabled: true\n  allowed_topics: ['c1/orders']\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	getenv := func(k string) string {
		if k == "ABLEOPS_API_TOKEN" {
			return "synthetic-session"
		}
		return ""
	}
	cfg, err := LoadServer(path, getenv, ServerOverrides{})
	if err != nil || !cfg.Backend.SampleEnabled || len(cfg.Backend.SampleTopics) != 1 {
		t.Fatalf("YAML 정책: %v", err)
	}
	cfg, err = LoadServer(path, func(k string) string {
		if k == "ABLEOPS_MESSAGE_SAMPLE_ENABLED" {
			return "false"
		}
		if k == "ABLEOPS_MESSAGE_SAMPLE_TOPICS" {
			return "c2/events"
		}
		return getenv(k)
	}, ServerOverrides{})
	if err != nil || cfg.Backend.SampleEnabled || cfg.Backend.SampleTopics[0] != "c2/events" {
		t.Fatalf("환경 우선순위: %v", err)
	}
	if validTokenEnvName("ABLEOPS_MESSAGE_SAMPLE_ENABLED") || validTokenEnvName("ABLEOPS_MESSAGE_SAMPLE_TOPICS") {
		t.Fatal("인증 토큰 환경변수 예약 누락")
	}
	if err := os.WriteFile(path, []byte(body+"  value_mode: raw\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadServer(path, getenv, ServerOverrides{}); err == nil {
		t.Fatal("원문 전달 설정 허용")
	}
}
