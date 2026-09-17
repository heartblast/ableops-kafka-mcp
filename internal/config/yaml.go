package config

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

const maxConfigBytes = 64 * 1024

// ServerOverrides는 명시적으로 전달한 서버 실행 인자만 담는다.
type ServerOverrides struct {
	Transport      *string
	HTTPAddress    *string
	AllowedOrigins *[]string
}

// ServerConfig는 환경변수와 파일, 실행 인자의 우선순위를 반영한 서버 설정이다.
type ServerConfig struct {
	Backend        Config
	Transport      string
	HTTPAddress    string
	AllowedOrigins []string
	AuthStore      string
	Dynamic        DynamicConfig
}

type fileConfig struct {
	dir            string
	transport      string
	httpAddress    string
	allowedOrigins []string
	baseURL        string
	allowHTTP      string
	timeout        string
	caFile         string
	storeFile      string
	tokenEnv       string
	logLevel       string
	sampleEnabled  string
	sampleTopics   []string
	dynamic        dynamicFile
}

// LoadServer는 지정한 파일만 읽으며 프로세스 환경변수를 변경하지 않는다.
// 명시한 실행 인자, 비어 있지 않은 기존 환경변수, 파일, 기본값 순으로 적용한다.
func LoadServer(path string, getenv func(string) string, overrides ServerOverrides) (ServerConfig, error) {
	file, err := readConfig(path, getenv)
	if err != nil {
		return ServerConfig{}, err
	}
	cfg := ServerConfig{Transport: "stdio", HTTPAddress: "127.0.0.1:8081"}
	if file.transport != "" {
		cfg.Transport = file.transport
	}
	if file.httpAddress != "" {
		cfg.HTTPAddress = file.httpAddress
	}
	cfg.AllowedOrigins = append([]string(nil), file.allowedOrigins...)
	if overrides.Transport != nil {
		cfg.Transport = *overrides.Transport
	}
	if overrides.HTTPAddress != nil {
		cfg.HTTPAddress = *overrides.HTTPAddress
	}
	if overrides.AllowedOrigins != nil {
		cfg.AllowedOrigins = append([]string(nil), (*overrides.AllowedOrigins)...)
	}
	if cfg.Transport != "stdio" && cfg.Transport != "http" {
		return ServerConfig{}, errors.New("서버 전송 방식은 stdio 또는 http여야 합니다")
	}
	cfg.Backend, err = file.loadBackend(getenv, cfg.Transport == "http")
	if err != nil {
		return ServerConfig{}, err
	}
	if cfg.Transport == "http" {
		cfg.AuthStore, err = file.authStore(getenv)
		if err != nil {
			return ServerConfig{}, err
		}
	}
	cfg.Dynamic, err = loadDynamic(file.dynamic, getenv)
	if err != nil {
		return ServerConfig{}, err
	}
	return cfg, nil
}

// LoadEnrollment는 등록에 필요한 백엔드 설정과 인증 저장소 경로를 읽는다.
func LoadEnrollment(path string, getenv func(string) string) (Config, string, error) {
	file, err := readConfig(path, getenv)
	if err != nil {
		return Config{}, "", err
	}
	backend, err := file.loadBackend(getenv, false)
	if err != nil {
		return Config{}, "", err
	}
	store, err := file.authStore(getenv)
	if err != nil {
		return Config{}, "", err
	}
	return backend, store, nil
}

// LoadAuthStore는 토큰 폐기에 필요한 저장소 경로만 읽는다.
// 백엔드 토큰, URL, CA 파일의 환경변수는 조회하지 않는다.
func LoadAuthStore(path string, getenv func(string) string) (string, error) {
	file, err := readConfig(path, getenv)
	if err != nil {
		return "", err
	}
	return file.authStore(getenv)
}

func (f fileConfig) loadBackend(getenv func(string) string, requestCredentials bool) (Config, error) {
	caFile := strings.TrimSpace(getenv("ABLEOPS_CA_FILE"))
	if caFile == "" && f.caFile != "" {
		var err error
		caFile, err = f.resolvePath(f.caFile, getenv)
		if err != nil {
			return Config{}, err
		}
	}
	tokenEnv := f.tokenEnv
	if tokenEnv == "" {
		tokenEnv = "ABLEOPS_API_TOKEN"
	}
	defaults := map[string]string{
		"ABLEOPS_BASE_URL":               f.baseURL,
		"ABLEOPS_ALLOW_HTTP":             f.allowHTTP,
		"ABLEOPS_REQUEST_TIMEOUT":        f.timeout,
		"MCP_LOG_LEVEL":                  f.logLevel,
		"ABLEOPS_MESSAGE_SAMPLE_ENABLED": f.sampleEnabled,
		"ABLEOPS_MESSAGE_SAMPLE_TOPICS":  strings.Join(f.sampleTopics, ","),
	}
	return loadFrom(func(key string) string {
		if key == "ABLEOPS_API_TOKEN" {
			// HTTP 모드에서는 기존 읽기 처리가 이 분기를 호출하지 않는다.
			return getenv(tokenEnv)
		}
		if key == "ABLEOPS_CA_FILE" {
			return caFile
		}
		if value := getenv(key); strings.TrimSpace(value) != "" {
			return value
		}
		return defaults[key]
	}, requestCredentials)
}

func (f fileConfig) authStore(getenv func(string) string) (string, error) {
	if value := strings.TrimSpace(getenv("MCP_AUTH_STORE")); value != "" {
		return value, nil
	}
	if f.storeFile != "" {
		return f.resolvePath(f.storeFile, getenv)
	}
	return "", errors.New("인증 저장소 경로가 필요합니다")
}

func (f fileConfig) resolvePath(raw string, getenv func(string) string) (string, error) {
	var result strings.Builder
	for raw != "" {
		start := strings.Index(raw, "${")
		if start < 0 {
			result.WriteString(raw)
			break
		}
		result.WriteString(raw[:start])
		raw = raw[start+2:]
		end := strings.IndexByte(raw, '}')
		if end < 0 || !validEnvName(raw[:end]) {
			return "", errors.New("설정 파일 경로의 환경변수 참조가 올바르지 않습니다")
		}
		if strings.EqualFold(raw[:end], "ABLEOPS_API_TOKEN") || (f.tokenEnv != "" && strings.EqualFold(raw[:end], f.tokenEnv)) {
			return "", errors.New("인증 토큰 환경변수는 파일 경로에 사용할 수 없습니다")
		}
		value := getenv(raw[:end])
		if strings.TrimSpace(value) == "" {
			return "", errors.New("설정 파일 경로에 필요한 환경변수가 비어 있습니다")
		}
		result.WriteString(value)
		raw = raw[end+1:]
	}
	value := strings.TrimSpace(result.String())
	if value == "" || strings.ContainsRune(value, 0) {
		return "", errors.New("설정 파일 경로가 올바르지 않습니다")
	}
	if !filepath.IsAbs(value) {
		value = filepath.Join(f.dir, value)
	}
	return filepath.Clean(value), nil
}

func validEnvName(value string) bool {
	if value == "" {
		return false
	}
	for i, ch := range value {
		if ch != '_' && !(ch >= 'A' && ch <= 'Z') && !(ch >= 'a' && ch <= 'z') && !(i > 0 && ch >= '0' && ch <= '9') {
			return false
		}
	}
	return true
}

func validTokenEnvName(value string) bool {
	if !validEnvName(value) {
		return false
	}
	// 설정 읽기가 토큰 환경변수를 우회 조회하지 않도록 기존 설정 이름을 예약한다.
	for _, reserved := range []string{
		"ABLEOPS_BASE_URL", "ABLEOPS_ALLOW_HTTP", "ABLEOPS_REQUEST_TIMEOUT", "ABLEOPS_CA_FILE", "MCP_AUTH_STORE", "MCP_LOG_LEVEL",
		"ABLEOPS_MESSAGE_SAMPLE_ENABLED", "ABLEOPS_MESSAGE_SAMPLE_TOPICS",
		envDynamicTools, envDynamicOperations, envDynamicRefreshInterval,
	} {
		if strings.EqualFold(value, reserved) {
			return false
		}
	}
	return true
}

func readConfig(path string, getenv func(string) string) (fileConfig, error) {
	if getenv == nil {
		return fileConfig{}, errors.New("환경변수 읽기 함수가 필요합니다")
	}
	if path == "" {
		return fileConfig{}, nil
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return fileConfig{}, errors.New("설정 파일은 읽을 수 있는 일반 파일이어야 합니다")
	}
	file, err := os.Open(path)
	if err != nil {
		return fileConfig{}, errors.New("설정 파일을 읽을 수 없습니다")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil {
		return fileConfig{}, errors.New("설정 파일을 읽을 수 없습니다")
	}
	if len(data) > maxConfigBytes {
		return fileConfig{}, errors.New("설정 파일은 64 KiB 이하여야 합니다")
	}
	data = bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var doc, extra yaml.Node
	if err := decoder.Decode(&doc); err != nil {
		return fileConfig{}, errors.New("설정 파일의 YAML 형식이 올바르지 않습니다")
	}
	if err := decoder.Decode(&extra); err != io.EOF {
		return fileConfig{}, errors.New("설정 파일은 YAML 문서 하나만 포함해야 합니다")
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || !safeNode(&doc) {
		return fileConfig{}, errors.New("설정 파일에는 별칭, 앵커, null을 사용할 수 없습니다")
	}
	var cfg fileConfig
	versionFound := false
	err = readMapping(doc.Content[0], map[string]func(*yaml.Node) error{
		"version": func(node *yaml.Node) error {
			if node.Kind != yaml.ScalarNode || node.Tag != "!!int" || node.Value != "1" {
				return errors.New("설정 파일 version은 정수 1이어야 합니다")
			}
			versionFound = true
			return nil
		},
		"server": func(node *yaml.Node) error {
			return readMapping(node, map[string]func(*yaml.Node) error{
				"transport": func(node *yaml.Node) error {
					if err := stringValue(&cfg.transport)(node); err != nil {
						return err
					}
					if cfg.transport != "stdio" && cfg.transport != "http" {
						return errors.New("설정 파일 server.transport는 stdio 또는 http여야 합니다")
					}
					return nil
				},
				"http_address":    stringValue(&cfg.httpAddress),
				"allowed_origins": stringList(&cfg.allowedOrigins),
			})
		},
		"backend": func(node *yaml.Node) error {
			return readMapping(node, map[string]func(*yaml.Node) error{
				"base_url":        stringValue(&cfg.baseURL),
				"allow_http":      boolValue(&cfg.allowHTTP),
				"request_timeout": stringValue(&cfg.timeout),
				"ca_file":         stringValue(&cfg.caFile),
			})
		},
		"auth": func(node *yaml.Node) error {
			return readMapping(node, map[string]func(*yaml.Node) error{
				"store_file": stringValue(&cfg.storeFile),
				"token_env": func(node *yaml.Node) error {
					if err := stringValue(&cfg.tokenEnv)(node); err != nil {
						return err
					}
					if !validTokenEnvName(cfg.tokenEnv) {
						return errors.New("설정 파일 auth.token_env에는 기존 설정과 충돌하지 않는 환경변수 이름이 필요합니다")
					}
					return nil
				},
			})
		},
		"logging": func(node *yaml.Node) error {
			return readMapping(node, map[string]func(*yaml.Node) error{
				"level": stringValue(&cfg.logLevel),
			})
		},
		"message_sample": func(node *yaml.Node) error {
			return readMapping(node, map[string]func(*yaml.Node) error{
				"enabled": boolValue(&cfg.sampleEnabled),
				"allowed_topics": func(node *yaml.Node) error {
					if err := stringList(&cfg.sampleTopics)(node); err != nil {
						return err
					}
					for _, topic := range cfg.sampleTopics {
						if strings.Contains(topic, ",") {
							return errors.New("샘플 허용 토픽의 각 항목에 쉼표를 사용할 수 없습니다")
						}
					}
					return nil
				},
			})
		},
		"dynamic_tools": func(node *yaml.Node) error {
			return readMapping(node, map[string]func(*yaml.Node) error{
				"enabled": boolValue(&cfg.dynamic.enabled),
				"operations": func(node *yaml.Node) error {
					cfg.dynamic.operationsSet = true
					cfg.dynamic.operations = []string{}
					return stringList(&cfg.dynamic.operations)(node)
				},
				"refresh_interval": stringValue(&cfg.dynamic.refreshInterval),
			})
		},
	})
	if err != nil {
		return fileConfig{}, err
	}
	if !versionFound {
		return fileConfig{}, errors.New("설정 파일 version이 필요합니다")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return fileConfig{}, errors.New("설정 파일 경로가 올바르지 않습니다")
	}
	cfg.dir = filepath.Dir(abs)
	return cfg, nil
}

func safeNode(node *yaml.Node) bool {
	if node.Kind == yaml.AliasNode || node.Anchor != "" || node.Tag == "!!null" {
		return false
	}
	for _, child := range node.Content {
		if !safeNode(child) {
			return false
		}
	}
	return true
}

func readMapping(node *yaml.Node, fields map[string]func(*yaml.Node) error) error {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content)%2 != 0 {
		return errors.New("설정 파일의 객체 형식이 올바르지 않습니다")
	}
	seen := make(map[string]bool)
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return errors.New("설정 파일의 키 형식이 올바르지 않습니다")
		}
		field, ok := fields[key.Value]
		if !ok || seen[key.Value] {
			return errors.New("설정 파일에 알 수 없거나 중복된 키가 있습니다")
		}
		seen[key.Value] = true
		if err := field(node.Content[i+1]); err != nil {
			return err
		}
	}
	return nil
}

func stringValue(dest *string) func(*yaml.Node) error {
	return func(node *yaml.Node) error {
		if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
			return errors.New("설정 파일의 문자열 형식이 올바르지 않습니다")
		}
		*dest = node.Value
		return nil
	}
}

func boolValue(dest *string) func(*yaml.Node) error {
	return func(node *yaml.Node) error {
		if node.Kind != yaml.ScalarNode || node.Tag != "!!bool" {
			return errors.New("설정 파일의 불리언 형식이 올바르지 않습니다")
		}
		value, err := strconv.ParseBool(node.Value)
		if err != nil {
			return errors.New("설정 파일의 불리언 형식이 올바르지 않습니다")
		}
		*dest = strconv.FormatBool(value)
		return nil
	}
}

func stringList(dest *[]string) func(*yaml.Node) error {
	return func(node *yaml.Node) error {
		if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" {
			return errors.New("설정 파일의 문자열 목록 형식이 올바르지 않습니다")
		}
		for _, item := range node.Content {
			var value string
			if err := stringValue(&value)(item); err != nil {
				return err
			}
			*dest = append(*dest, value)
		}
		return nil
	}
}
