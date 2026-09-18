package mcpserver

import (
	"context"
	"errors"
	"net"
	"testing"
)

// 기동 실패는 운영자가 조치할 수 있어야 한다. 고정 문구를 StartupError 로 돌려주지 않으면
// 호출자는 원인을 감춘 protocol_error 만 남기게 된다.
func requireStartupError(t *testing.T, err error, message string) {
	t.Helper()
	var startup *StartupError
	if !errors.As(err, &startup) {
		t.Fatalf("StartupError 를 기대했지만 %v", err)
	}
	if startup.Error() != message {
		t.Fatalf("문구가 다릅니다: %q", startup.Error())
	}
}

func TestRunHTTPRejectsNonLoopbackAddress(t *testing.T) {
	for _, address := range []string{"0.0.0.0:8081", "localhost:8081", "edu-cluster1-04:8081", "127.0.0.1"} {
		options := httpTestOptions()
		options.Address = address
		err := RunHTTP(context.Background(), emptyMCPServer(), options)
		requireStartupError(t, err, "로컬 HTTP 주소는 포트를 포함한 loopback IP여야 합니다")
	}
}

func TestRunHTTPReportsAddressAlreadyInUse(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	options := httpTestOptions()
	options.Address = held.Addr().String()
	requireStartupError(t, RunHTTP(context.Background(), emptyMCPServer(), options),
		"HTTP loopback 수신 주소가 이미 사용 중입니다")
}

func TestRunHTTPRejectsInvalidOrigin(t *testing.T) {
	options := httpTestOptions()
	options.Address = "127.0.0.1:0"
	options.AllowedOrigins = []string{"https://example.test/path"}
	requireStartupError(t, RunHTTP(context.Background(), emptyMCPServer(), options),
		"허용 Origin은 경로 없는 정확한 HTTP 또는 HTTPS 출처여야 합니다")
}
