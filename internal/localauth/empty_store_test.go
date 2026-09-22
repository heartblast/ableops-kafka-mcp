package localauth

// 빈 인증 저장소(`entries: []`)로 기동할 수 있는지 고정한다.
//
// standalone HTTP 는 Web Delegation 을 켜도 저장소 경로를 반드시 요구한다. 그래서 "위임 전용
// 운영"은 경로를 비우는 방식이 아니라 **빈 저장소 파일**을 두는 방식으로 만든다
// (docs/deployment.md 「Web Delegation 전용 운영」). 문서 검증이 좁아져 이 파일을 거부하면
// 그 운영 방식이 조용히 막히므로 시험으로 묶어 둔다.
//
// ⚠ 파일은 반드시 createPrivate 로 만든다. os.WriteFile 이나 파일 복사로 만든 파일은 Windows
// 에서 DACL 을 상속하므로(SE_DACL_PROTECTED 아님) 권한 검사에서 거부된다. 문서의 안내가 이
// 제약을 반영해야 하는 이유이며, 아래 시험이 두 경우를 함께 확인한다.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// emptyStoreDocument는 등록 항목이 없는 저장소다. BOM 을 붙이지 않는다 — readDocument 는
// YAML 설정 로더와 달리 BOM 을 허용하지 않는다.
const emptyStoreDocument = `{"version":1,"audience":"ableops-kafka-mcp","entries":[]}`

// bomPrefix는 UTF-8 BOM 이다. 이 파일 자체에 BOM 문자를 그대로 넣으면 Go 가 소스를 거부하므로
// 바이트로 적는다.
const bomPrefix = "\xef\xbb\xbf"

// writePrivateStore는 CLI 와 같은 방식(소유자 전용 생성)으로 저장소 파일을 만든다.
func writePrivateStore(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "local.auth-store.json")
	file, err := createPrivate(path)
	if err != nil {
		t.Fatalf("저장소 파일을 만들지 못했다: %v", err)
	}
	if _, err := file.WriteString(content); err != nil {
		file.Close()
		t.Fatalf("저장소 파일을 쓰지 못했다: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("저장소 파일을 닫지 못했다: %v", err)
	}
	return path
}

// 빈 저장소로 기동하고, 형식이 올바른 토큰도 등록 항목이 없으므로 거부해야 한다.
func TestEmptyStoreOpensAndRejectsEveryToken(t *testing.T) {
	path := writePrivateStore(t, t.TempDir(), emptyStoreDocument)
	// 빈 저장소에서는 Backend 를 부를 일이 없다 — 토큰 대조에서 끝난다.
	verify := func(context.Context, string) (string, error) {
		t.Error("빈 저장소인데 Backend 세션 검사가 호출됐다")
		return "", nil
	}
	store, err := NewStore(path, verify)
	if err != nil {
		t.Fatalf("빈 저장소로 기동하지 못했다: %v", err)
	}
	token := tokenPrefix + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if !validMCPToken(token) {
		t.Fatalf("시험용 토큰 형식이 잘못됐다: %s", token)
	}
	if _, err := store.Authenticate(context.Background(), token); err == nil {
		t.Fatal("빈 저장소가 토큰을 받아들였다")
	}
}

// 예시 파일의 **내용**이 빈 저장소로 받아들여지는지 본다.
//
// 파일을 그대로 복사하지 않고 createPrivate 로 다시 쓰는 이유가 곧 문서에 적어야 하는 제약이다
// — 복사한 파일은 Windows 에서 권한 검사를 통과하지 못한다.
func TestExampleStoreContentIsAcceptedWhenWrittenPrivately(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "local.auth-store.example.json"))
	if err != nil {
		t.Fatalf("예시 저장소 파일을 읽지 못했다: %v", err)
	}
	path := writePrivateStore(t, t.TempDir(), string(data))
	verify := func(context.Context, string) (string, error) { return "", nil }
	if _, err := NewStore(path, verify); err != nil {
		t.Fatalf("예시 저장소 내용으로 기동하지 못했다: %v", err)
	}
}

// UTF-8 BOM 이 붙은 저장소는 거부된다.
//
// YAML 설정 파일은 BOM 을 허용하는데(docs/deployment.md) 인증 저장소는 허용하지 않는다. 두
// 규칙이 다르므로 운영자가 Windows PowerShell 의 `Set-Content -Encoding utf8`(5.1 은 BOM 을
// 붙인다)로 파일을 만들면 원인을 알기 어려운 authentication_unavailable 을 만난다.
// 이 차이를 시험으로 드러내 두어야 문서의 경고가 근거를 갖는다.
func TestBOMPrefixedStoreIsRejected(t *testing.T) {
	path := writePrivateStore(t, t.TempDir(), bomPrefix+emptyStoreDocument)
	verify := func(context.Context, string) (string, error) { return "", nil }
	if _, err := NewStore(path, verify); err == nil {
		t.Fatal("BOM 이 붙은 저장소가 받아들여졌다")
	}
}
