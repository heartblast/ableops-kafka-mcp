package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

const dynamicBase = "version: 1\nbackend: {base_url: 'https://backend.example.test'}\n"

func stdioEnv(values map[string]string) func(string) string {
	values["ABLEOPS_API_TOKEN"] = "synthetic-session"
	return configEnv(values)
}

func TestLoadDynamicDefaultsDisabled(t *testing.T) {
	for _, path := range []string{"", writeYAMLConfig(t, dynamicBase)} {
		cfg, err := LoadServer(path, stdioEnv(map[string]string{"ABLEOPS_BASE_URL": "https://backend.example.test"}), ServerOverrides{})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Dynamic.Enabled || cfg.Dynamic.Operations != nil || cfg.Dynamic.RefreshInterval != DefaultDynamicRefreshInterval {
			t.Fatalf("dynamic=%+v", cfg.Dynamic)
		}
	}
}

func TestLoadDynamicRefreshInterval(t *testing.T) {
	path := writeYAMLConfig(t, dynamicBase+"dynamic_tools:\n  enabled: true\n  refresh_interval: 2m\n")
	cfg, err := LoadServer(path, stdioEnv(map[string]string{}), ServerOverrides{})
	if err != nil || cfg.Dynamic.RefreshInterval != 2*time.Minute {
		t.Fatalf("yaml=%+v err=%v", cfg.Dynamic, err)
	}
	// 비어 있지 않은 환경변수가 YAML보다 우선하고, 공백 환경변수는 설정하지 않은 것으로 본다.
	for env, want := range map[string]time.Duration{"90s": 90 * time.Second, " 1m ": time.Minute, "24h": 24 * time.Hour, "  ": 2 * time.Minute} {
		cfg, err := LoadServer(path, stdioEnv(map[string]string{"ABLEOPS_DYNAMIC_REFRESH_INTERVAL": env}), ServerOverrides{})
		if err != nil || cfg.Dynamic.RefreshInterval != want {
			t.Fatalf("env %q: %+v err=%v", env, cfg.Dynamic, err)
		}
	}
	// Dynamic이 꺼져 있어도 값은 검증하고 보관한다(켜는 시점까지 설정 오류를 숨기지 않는다).
	cfg, err = LoadServer("", stdioEnv(map[string]string{"ABLEOPS_BASE_URL": "https://backend.example.test", "ABLEOPS_DYNAMIC_REFRESH_INTERVAL": "10m"}), ServerOverrides{})
	if err != nil || cfg.Dynamic.Enabled || cfg.Dynamic.RefreshInterval != 10*time.Minute {
		t.Fatalf("disabled=%+v err=%v", cfg.Dynamic, err)
	}
}

func TestLoadDynamicPrecedence(t *testing.T) {
	path := writeYAMLConfig(t, dynamicBase+"dynamic_tools:\n  enabled: true\n  operations: [getTopic, listEvents]\n")
	cfg, err := LoadServer(path, stdioEnv(map[string]string{}), ServerOverrides{})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Dynamic.Enabled || !reflect.DeepEqual(cfg.Dynamic.Operations, []string{"getTopic", "listEvents"}) {
		t.Fatalf("yaml=%+v", cfg.Dynamic)
	}
	// 비어 있지 않은 환경변수가 YAML보다 우선한다.
	cfg, err = LoadServer(path, stdioEnv(map[string]string{
		"ABLEOPS_DYNAMIC_TOOLS":      "false",
		"ABLEOPS_DYNAMIC_OPERATIONS": " getEvent , listClusters ",
	}), ServerOverrides{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Dynamic.Enabled || !reflect.DeepEqual(cfg.Dynamic.Operations, []string{"getEvent", "listClusters"}) {
		t.Fatalf("env=%+v", cfg.Dynamic)
	}
	// 공백 환경변수는 설정하지 않은 것으로 본다.
	cfg, err = LoadServer(path, stdioEnv(map[string]string{"ABLEOPS_DYNAMIC_TOOLS": "  ", "ABLEOPS_DYNAMIC_OPERATIONS": " "}), ServerOverrides{})
	if err != nil || !cfg.Dynamic.Enabled || len(cfg.Dynamic.Operations) != 2 {
		t.Fatalf("blank env=%+v err=%v", cfg.Dynamic, err)
	}
	// YAML의 빈 목록은 "선택 없음"이며 기본 목록으로 바꾸지 않는다.
	empty := writeYAMLConfig(t, dynamicBase+"dynamic_tools: {enabled: true, operations: []}\n")
	cfg, err = LoadServer(empty, stdioEnv(map[string]string{}), ServerOverrides{})
	if err != nil || cfg.Dynamic.Operations == nil || len(cfg.Dynamic.Operations) != 0 {
		t.Fatalf("empty=%+v err=%v", cfg.Dynamic, err)
	}
	// 환경변수만으로도 켤 수 있다.
	cfg, err = LoadServer("", stdioEnv(map[string]string{"ABLEOPS_BASE_URL": "https://backend.example.test", "ABLEOPS_DYNAMIC_TOOLS": "true"}), ServerOverrides{})
	if err != nil || !cfg.Dynamic.Enabled || cfg.Dynamic.Operations != nil {
		t.Fatalf("env only=%+v err=%v", cfg.Dynamic, err)
	}
}

func TestLoadDynamicRejectsInvalidValues(t *testing.T) {
	secret := "synthetic-secret-value"
	envCases := map[string]map[string]string{
		"불리언 아님":     {"ABLEOPS_DYNAMIC_TOOLS": "yes"},
		"대문자 불리언":    {"ABLEOPS_DYNAMIC_TOOLS": "TRUE"},
		"잘못된 ID":     {"ABLEOPS_DYNAMIC_OPERATIONS": "getTopic," + secret},
		"빈 항목":       {"ABLEOPS_DYNAMIC_OPERATIONS": "getTopic,,listEvents"},
		"중복":         {"ABLEOPS_DYNAMIC_OPERATIONS": "getTopic,getTopic"},
		"snake_case": {"ABLEOPS_DYNAMIC_OPERATIONS": "get_topic"},
		"개수 초과":      {"ABLEOPS_DYNAMIC_OPERATIONS": strings.TrimSuffix(strings.Repeat("getA,", 65), ",")},
		"주기 하한 미만":   {"ABLEOPS_DYNAMIC_REFRESH_INTERVAL": "59s"},
		"주기 상한 초과":   {"ABLEOPS_DYNAMIC_REFRESH_INTERVAL": "25h"},
		"주기 0":       {"ABLEOPS_DYNAMIC_REFRESH_INTERVAL": "0"},
		"음수 주기":      {"ABLEOPS_DYNAMIC_REFRESH_INTERVAL": "-1m"},
		"단위 없는 주기":   {"ABLEOPS_DYNAMIC_REFRESH_INTERVAL": "300"},
		"주기에 비밀값":    {"ABLEOPS_DYNAMIC_REFRESH_INTERVAL": secret},
	}
	for name, env := range envCases {
		t.Run(name, func(t *testing.T) {
			env["ABLEOPS_BASE_URL"] = "https://backend.example.test"
			_, err := LoadServer("", stdioEnv(env), ServerOverrides{})
			if err == nil || strings.Contains(err.Error(), secret) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	yamlCases := map[string]string{
		"문자열 enabled":  "dynamic_tools: {enabled: 'true'}\n",
		"알 수 없는 키":     "dynamic_tools: {enabled: true, refresh: 10}\n",
		"목록 아님":        "dynamic_tools: {operations: getTopic}\n",
		"잘못된 ID":       "dynamic_tools: {operations: ['" + secret + "!']}\n",
		"null":         "dynamic_tools: {operations: null}\n",
		"최상위 오타":       "dynamic_tool: {enabled: true}\n",
		"숫자 operation": "dynamic_tools: {operations: [1]}\n",
		"정수 주기":        "dynamic_tools: {refresh_interval: 300}\n",
		"주기 하한 미만":     "dynamic_tools: {refresh_interval: '30s'}\n",
		"주기 null":      "dynamic_tools: {refresh_interval: null}\n",
		"주기 비밀값":       "dynamic_tools: {refresh_interval: '" + secret + "'}\n",
		"주기 중복":        "dynamic_tools: {refresh_interval: 5m, refresh_interval: 6m}\n",
	}
	for name, body := range yamlCases {
		t.Run(name, func(t *testing.T) {
			path := writeYAMLConfig(t, dynamicBase+body)
			_, err := LoadServer(path, stdioEnv(map[string]string{}), ServerOverrides{})
			if err == nil || strings.Contains(err.Error(), secret) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestDynamicSettingsCannotBeTokenEnv(t *testing.T) {
	// 동적 도구 설정 이름도 기존 설정처럼 토큰 환경변수로 지정할 수 없다(대소문자 무시).
	for _, name := range []string{"ABLEOPS_DYNAMIC_TOOLS", "ableops_dynamic_operations", "ABLEOPS_DYNAMIC_OPERATIONS", "ABLEOPS_DYNAMIC_REFRESH_INTERVAL"} {
		path := writeYAMLConfig(t, dynamicBase+"auth: {token_env: "+name+"}\n")
		_, err := LoadServer(path, func(string) string {
			t.Fatal("거부 전에 환경변수를 읽음")
			return ""
		}, ServerOverrides{})
		if err == nil || !strings.Contains(err.Error(), "token_env") {
			t.Fatalf("%s: err=%v", name, err)
		}
	}
}

func TestEnrollmentAcceptsDynamicSection(t *testing.T) {
	path := writeYAMLConfig(t, dynamicBase+"auth: {store_file: auth.json}\ndynamic_tools: {enabled: true, operations: [getTopic], refresh_interval: 10m}\n")
	if _, _, err := LoadEnrollment(path, configEnv(map[string]string{"ABLEOPS_API_TOKEN": "synthetic-session"})); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAuthStore(path, configEnv(map[string]string{})); err != nil {
		t.Fatal(err)
	}
}

// 배포본에 복사되는 공개 예제가 현재 설정 계약으로 그대로 읽히는지 확인한다.
func TestExampleConfigLoads(t *testing.T) {
	cfg, err := LoadServer("../../mcp-server-config.example.yaml", configEnv(map[string]string{}), ServerOverrides{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Transport != "http" || cfg.Dynamic.Enabled || cfg.Dynamic.Operations != nil || cfg.Dynamic.RefreshInterval != DefaultDynamicRefreshInterval {
		t.Fatalf("example=%+v", cfg)
	}
}
