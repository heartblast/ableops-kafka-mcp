package localauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

func enrollFixture(t *testing.T, path, clientID, backendToken, userID string) string {
	t.Helper()
	output := filepath.Join(filepath.Dir(path), clientID+".token")
	verify := func(context.Context, string) (string, error) { return userID, nil }
	if _, err := Enroll(context.Background(), path, clientID, backendToken, time.Hour, output, verify); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(data))
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var target *Error
	if !errors.As(err, &target) || target.AuthCode() != code {
		t.Fatalf("오류 분류 불일치: %v", err)
	}
}

func TestEnrollmentHashAndAuthenticationRefresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	token := enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), token) {
		t.Fatal("원문 MCP 토큰을 저장하면 안 됩니다")
	}
	if !validMCPToken(token) {
		t.Fatal("발급 토큰 형식이 올바르지 않습니다")
	}
	var calls atomic.Int32
	var denied atomic.Bool
	verify := func(ctx context.Context, backend string) (string, error) {
		calls.Add(1)
		p, ok := requestctx.FromContext(ctx)
		if !ok || backend != "synthetic-backend-a" || p.BackendToken != backend || p.MCPToken != token || p.RequestID != "trace-a" {
			t.Error("요청 자격증명 또는 추적 ID 불일치")
		}
		if denied.Load() {
			return "", &ableops.Error{Code: "authentication_required"}
		}
		return "user-a", nil
	}
	store, err := NewStore(path, verify)
	if err != nil {
		t.Fatal(err)
	}
	ctx := requestctx.WithPrincipal(context.Background(), requestctx.Principal{RequestID: "trace-a"})
	for range 2 {
		principal, err := store.Authenticate(ctx, token)
		if err != nil {
			t.Fatal(err)
		}
		if principal.UserID != "user-a" || principal.ClientID != "client-a" || principal.RequestID != "trace-a" {
			t.Fatal("검증한 신원 또는 추적 ID 불일치")
		}
	}
	denied.Store(true)
	_, err = store.Authenticate(ctx, token)
	requireCode(t, err, "backend_authentication_required")
	if calls.Load() != 3 {
		t.Fatal("각 요청에서 백엔드 세션을 다시 검증해야 합니다")
	}
}

func TestRejectedTokensDoNotReachBackend(t *testing.T) {
	for _, mode := range []string{"empty", "backend", "random", "expired", "future", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.json")
			token := enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
			doc, err := readDocument(path)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "empty":
				token = ""
			case "backend":
				token = "synthetic-backend-a"
			case "random":
				token = tokenPrefix + strings.Repeat("A", 43)
			case "expired":
				doc.Entries[0].IssuedAt = time.Now().Add(-2 * time.Hour)
				doc.Entries[0].ExpiresAt = time.Now().Add(-time.Hour)
			case "future":
				doc.Entries[0].IssuedAt = time.Now().Add(time.Hour)
				doc.Entries[0].ExpiresAt = time.Now().Add(2 * time.Hour)
			case "revoked":
				doc.Entries[0].Revoked = true
			}
			if err := writeDocument(path, doc); err != nil {
				t.Fatal(err)
			}
			store, err := NewStore(path, func(context.Context, string) (string, error) {
				t.Error("거부된 MCP 토큰은 백엔드에 전달하면 안 됩니다")
				return "", nil
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Authenticate(context.Background(), token)
			requireCode(t, err, "authentication_required")
		})
	}
}

func TestRevocationReloadAndReregistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	token := enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
	store, err := NewStore(path, func(context.Context, string) (string, error) { return "user-a", nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authenticate(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	if err := Revoke(path, "client-a"); err != nil {
		t.Fatal(err)
	}
	_, err = store.Authenticate(context.Background(), token)
	requireCode(t, err, "authentication_required")
	data, _ := os.ReadFile(path)
	if strings.Contains(string(data), "synthetic-backend-a") {
		t.Fatal("폐기한 백엔드 토큰을 보관하면 안 됩니다")
	}
	newOutput := filepath.Join(filepath.Dir(path), "rotated.token")
	verify := func(context.Context, string) (string, error) { return "user-a", nil }
	if _, err := Enroll(context.Background(), path, "client-a", "synthetic-backend-new", time.Hour, newOutput, verify); err != nil {
		t.Fatal(err)
	}
	_, err = store.Authenticate(context.Background(), token)
	requireCode(t, err, "authentication_required")
	newToken, _ := os.ReadFile(newOutput)
	if _, err := store.Authenticate(context.Background(), strings.TrimSpace(string(newToken))); err != nil {
		t.Fatal(err)
	}
}

func TestRevocationWhileStoreReaderIsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	token := enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
	store, err := NewStore(path, func(context.Context, string) (string, error) { return "user-a", nil })
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openPrivate(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := Revoke(path, "client-a"); err != nil {
		t.Fatal("저장소를 읽는 요청이 있어도 폐기를 적용해야 합니다:", err)
	}
	_, err = store.Authenticate(context.Background(), token)
	requireCode(t, err, "authentication_required")
}

func TestMappingMismatchAndErrorRedaction(t *testing.T) {
	for _, test := range []struct {
		name, userID, code string
		err                error
	}{
		{"changed_identity", "different-user", "authentication_required", nil},
		{"missing_identity", "", "authentication_required", nil},
		{"expired_backend", "", "backend_authentication_required", &ableops.Error{Code: "authentication_required"}},
		{"forbidden", "", "access_denied", &ableops.Error{Code: "access_denied"}},
		{"timeout", "", "timeout", context.DeadlineExceeded},
		{"canceled", "", "canceled", context.Canceled},
		{"unsafe_error", "", "backend_unavailable", errors.New("synthetic-secret-in-error")},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.json")
			token := enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
			store, err := NewStore(path, func(context.Context, string) (string, error) { return test.userID, test.err })
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Authenticate(context.Background(), token)
			requireCode(t, err, test.code)
			if strings.Contains(err.Error(), "synthetic-secret") {
				t.Fatal("내부 오류 원문이 노출되었습니다")
			}
		})
	}
}

func TestInvalidStoreMappings(t *testing.T) {
	for _, mode := range []string{"audience", "version", "hash", "duplicate_hash", "duplicate_client", "backend_user_conflict", "backend_is_mcp", "too_long_ttl"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.json")
			token := enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
			enrollFixture(t, path, "client-b", "synthetic-backend-b", "user-b")
			doc, err := readDocument(path)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "audience":
				doc.Audience = "backend"
			case "version":
				doc.Version = 2
			case "hash":
				doc.Entries[0].TokenHash = "bad"
			case "duplicate_hash":
				doc.Entries[1].TokenHash = doc.Entries[0].TokenHash
			case "duplicate_client":
				doc.Entries[1].ClientID = doc.Entries[0].ClientID
			case "backend_user_conflict":
				doc.Entries[1].BackendToken = doc.Entries[0].BackendToken
			case "backend_is_mcp":
				doc.Entries[0].BackendToken = token
			case "too_long_ttl":
				doc.Entries[0].ExpiresAt = doc.Entries[0].IssuedAt.Add(MaxTTL + time.Second)
			}
			if err := writeDocument(path, doc); err != nil {
				t.Fatal(err)
			}
			_, err = NewStore(path, func(context.Context, string) (string, error) { return "", nil })
			requireCode(t, err, "authentication_unavailable")
		})
	}
}

func TestConcurrentUsersKeepSeparateCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	tokens := map[string]string{"user-a": enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a"), "user-b": enrollFixture(t, path, "client-b", "synthetic-backend-b", "user-b")}
	store, err := NewStore(path, func(ctx context.Context, backend string) (string, error) {
		p, ok := requestctx.FromContext(ctx)
		if !ok || p.BackendToken != backend || p.MCPToken != tokens[p.UserID] {
			return "", errors.New("잘못된 요청 자격증명")
		}
		switch backend {
		case "synthetic-backend-a":
			return "user-a", nil
		case "synthetic-backend-b":
			return "user-b", nil
		}
		return "", errors.New("알 수 없는 백엔드 토큰")
	})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for userID, token := range tokens {
		for range 20 {
			wait.Add(1)
			go func() {
				defer wait.Done()
				p, err := store.Authenticate(context.Background(), token)
				if err != nil || p.UserID != userID {
					t.Error("동시 호출의 사용자 또는 자격증명이 섞였습니다")
				}
			}()
		}
	}
	wait.Wait()
}

func TestEnrollmentRejectsOverwriteAndInvalidInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	token := enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
	verify := func(context.Context, string) (string, error) { return "user-a", nil }
	_, err := Enroll(context.Background(), path, "client-a", "synthetic-backend-a", time.Hour, filepath.Join(filepath.Dir(path), "new.token"), verify)
	requireCode(t, err, "client_already_registered")
	output := filepath.Join(filepath.Dir(path), "client-a.token")
	_, err = Enroll(context.Background(), path, "client-b", "synthetic-backend-a", time.Hour, output, verify)
	requireCode(t, err, "token_output_unavailable")
	data, _ := os.ReadFile(output)
	if strings.TrimSpace(string(data)) != token {
		t.Fatal("기존 토큰 파일을 덮어썼습니다")
	}
	for _, test := range []struct {
		client, backend string
		ttl             time.Duration
	}{
		{"", "synthetic-backend-a", time.Hour}, {"client-b", token, time.Hour}, {"client-b", "synthetic-backend-a", 0}, {"client-b", "synthetic-backend-a", MaxTTL + time.Second},
	} {
		_, err = Enroll(context.Background(), path, test.client, test.backend, test.ttl, filepath.Join(filepath.Dir(path), "new.token"), verify)
		requireCode(t, err, "invalid_enrollment")
	}
	_, err = Enroll(context.Background(), path, "client-b", "synthetic-backend-a", time.Hour, path, verify)
	requireCode(t, err, "invalid_enrollment")
}

func TestPrivateStoreAndBoundedJSON(t *testing.T) {
	for _, contents := range []string{"{", `{"version":1,"audience":"ableops-kafka-mcp","entries":[],"unexpected":true}`, `{"version":1,"audience":"ableops-kafka-mcp","entries":[]} {}`, strings.Repeat(" ", maxStoreBytes+1)} {
		path := filepath.Join(t.TempDir(), "store.json")
		file, err := createPrivate(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = file.WriteString(contents); err != nil {
			t.Fatal(err)
		}
		file.Close()
		_, err = NewStore(path, func(context.Context, string) (string, error) { return "", nil })
		requireCode(t, err, "authentication_unavailable")
	}
	path := filepath.Join(t.TempDir(), "store.json")
	enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
	file, err := openPrivate(path)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	data, _ := os.ReadFile(path)
	var doc document
	if json.Unmarshal(data, &doc) != nil {
		t.Fatal("저장 형식이 올바르지 않습니다")
	}
	if fmt.Sprint(doc.Version) != "1" {
		t.Fatal("저장 버전 불일치")
	}
}
