package webdelegation

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

// sessions는 Backend 세션 → 사용자 ID 를 흉내 낸다. 로그아웃은 항목 제거로 표현한다.
type sessions struct {
	mu      sync.Mutex
	users   map[string]string
	calls   atomic.Int32
	failure error
}

func newSessions(pairs map[string]string) *sessions {
	return &sessions{users: pairs}
}

func (s *sessions) verify(_ context.Context, token string) (string, error) {
	s.calls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != nil {
		return "", s.failure
	}
	id, ok := s.users[token]
	if !ok {
		return "", &ableops.Error{Code: "authentication_required"}
	}
	return id, nil
}

func (s *sessions) logout(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.users, token)
}

func newTestStore(t *testing.T, verify VerifyBackend) *Store {
	t.Helper()
	store, err := NewStore(verify, DefaultTTL)
	if err != nil {
		t.Fatalf("저장소 생성 실패: %v", err)
	}
	return store
}

func authCode(t *testing.T, err error) string {
	t.Helper()
	var coded interface{ AuthCode() string }
	if !errors.As(err, &coded) {
		t.Fatalf("코드 없는 오류: %v", err)
	}
	return coded.AuthCode()
}

// 발급된 위임이 Backend 토큰과 **다른 값**이고, 인증 시 해당 사용자의 Backend 토큰으로만 이어지는지 본다.
func TestIssueAndAuthenticateBindsOneUser(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a", "session-b": "b"})
	store := newTestStore(t, backend.verify)

	grants := map[string]Delegation{}
	for _, id := range []string{"a", "b"} {
		grant, err := store.Issue(context.Background(), "session-"+id)
		if err != nil {
			t.Fatalf("발급 실패(%s): %v", id, err)
		}
		if grant.UserID != id {
			t.Fatalf("발급 사용자 불일치: got %q want %q", grant.UserID, id)
		}
		if !strings.HasPrefix(grant.Token, TokenPrefix) || !ValidToken(grant.Token) {
			t.Fatal("위임 토큰 형식이 계약과 다르다")
		}
		if grant.Token == "session-"+id {
			t.Fatal("위임 토큰과 Backend 토큰이 같은 값이다")
		}
		grants[id] = grant
	}
	if grants["a"].Token == grants["b"].Token {
		t.Fatal("서로 다른 사용자에게 같은 위임 토큰이 발급됐다")
	}

	for _, id := range []string{"a", "b"} {
		principal, err := store.Authenticate(context.Background(), grants[id].Token)
		if err != nil {
			t.Fatalf("인증 실패(%s): %v", id, err)
		}
		if principal.UserID != id || principal.BackendToken != "session-"+id {
			t.Fatalf("Principal 혼선: user=%q backend 일치=%v", principal.UserID, principal.BackendToken == "session-"+id)
		}
		if principal.MCPToken != grants[id].Token {
			t.Fatal("MCPToken 이 위임 토큰이 아니다")
		}
		if principal.ClientID != ClientID {
			t.Fatalf("ClientID 가 위임 경로를 나타내지 않는다: %q", principal.ClientID)
		}
	}
	// A 의 위임으로 B 의 Backend 토큰이 나오는 경로가 없어야 한다.
	principal, _ := store.Authenticate(context.Background(), grants["a"].Token)
	if principal.BackendToken == "session-b" {
		t.Fatal("A 위임이 B 세션으로 이어졌다")
	}
}

// 기존 Principal 문맥(RequestID)은 보존하고 자격증명만 덮어써야 한다.
func TestAuthenticatePreservesRequestContext(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a"})
	store := newTestStore(t, backend.verify)
	grant, err := store.Issue(context.Background(), "session-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx := requestctx.WithPrincipal(context.Background(), requestctx.Principal{RequestID: "req-1"})
	principal, err := store.Authenticate(ctx, grant.Token)
	if err != nil {
		t.Fatal(err)
	}
	if principal.RequestID != "req-1" {
		t.Fatalf("요청 추적 ID 가 사라졌다: %q", principal.RequestID)
	}
}

// TTL 만 믿지 않는다 — 만료된 위임은 Backend 를 부르지도 않고 거부하고 저장소에서 사라져야 한다.
func TestExpiredDelegationRejectedAndPurged(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a"})
	store := newTestStore(t, backend.verify)
	base := time.Now().UTC()
	store.now = func() time.Time { return base }
	grant, err := store.Issue(context.Background(), "session-a")
	if err != nil {
		t.Fatal(err)
	}
	before := backend.calls.Load()

	store.now = func() time.Time { return base.Add(DefaultTTL) } // 경계 시각은 이미 만료다
	if _, err := store.Authenticate(context.Background(), grant.Token); authCode(t, err) != "authentication_required" {
		t.Fatalf("만료 위임이 거부되지 않았다: %v", err)
	}
	if backend.calls.Load() != before {
		t.Fatal("만료 위임으로 Backend 를 호출했다")
	}
	if store.Len() != 0 {
		t.Fatal("만료 위임이 메모리에 남았다")
	}
}

// Backend 로그아웃은 다음 MCP 요청에서 즉시 드러나고 위임도 함께 사라져야 한다.
func TestBackendLogoutInvalidatesDelegation(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a", "session-b": "b"})
	store := newTestStore(t, backend.verify)
	a, err := store.Issue(context.Background(), "session-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Issue(context.Background(), "session-b")
	if err != nil {
		t.Fatal(err)
	}
	backend.logout("session-a")
	if _, err := store.Authenticate(context.Background(), a.Token); authCode(t, err) != "backend_authentication_required" {
		t.Fatalf("로그아웃한 세션의 위임이 통과했다: %v", err)
	}
	if store.Len() != 1 {
		t.Fatalf("무효해진 위임이 정리되지 않았다: %d", store.Len())
	}
	if _, err := store.Authenticate(context.Background(), b.Token); err != nil {
		t.Fatalf("다른 사용자의 위임이 함께 무효화됐다: %v", err)
	}
}

// 일시적 Backend 장애로 위임을 지우면 복구 후에도 재로그인을 요구하게 된다 — 지우지 않는다.
func TestTransientBackendFailureKeepsDelegation(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a"})
	store := newTestStore(t, backend.verify)
	grant, err := store.Issue(context.Background(), "session-a")
	if err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	backend.failure = &ableops.Error{Code: "backend_unavailable"}
	backend.mu.Unlock()
	if _, err := store.Authenticate(context.Background(), grant.Token); authCode(t, err) != "backend_unavailable" {
		t.Fatalf("장애 코드 매핑 오류: %v", err)
	}
	if store.Len() != 1 {
		t.Fatal("일시 장애로 위임이 삭제됐다")
	}
	backend.mu.Lock()
	backend.failure = nil
	backend.mu.Unlock()
	if _, err := store.Authenticate(context.Background(), grant.Token); err != nil {
		t.Fatalf("장애 복구 후에도 위임이 동작하지 않는다: %v", err)
	}
}

// 발급 요청의 Backend 자격증명은 그대로 신뢰하지 않는다.
func TestIssueRejectsUnverifiedOrMalformedCredentials(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a"})
	store := newTestStore(t, backend.verify)
	for _, tc := range []struct{ name, token, want string }{
		{"빈 값", "", "invalid_delegation_request"},
		{"공백 포함", "session a", "invalid_delegation_request"},
		{"위임 토큰 되먹임", TokenPrefix + "AAAA", "invalid_delegation_request"},
		{"local MCP 토큰 되먹임", "ableops_mcp_AAAA", "invalid_delegation_request"},
		{"길이 초과", strings.Repeat("x", maxBackendTokenBytes+1), "invalid_delegation_request"},
		{"미인증 세션", "session-unknown", "backend_authentication_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.Issue(context.Background(), tc.token); authCode(t, err) != tc.want {
				t.Fatalf("got %v want %s", err, tc.want)
			}
		})
	}
	if store.Len() != 0 {
		t.Fatal("거부된 요청이 위임을 남겼다")
	}
}

// 형태가 다른 Bearer 는 Backend 를 부르기 전에 거부한다(localauth 토큰이 여기로 새면 안 된다).
func TestAuthenticateRejectsForeignTokens(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a"})
	store := newTestStore(t, backend.verify)
	if _, err := store.Issue(context.Background(), "session-a"); err != nil {
		t.Fatal(err)
	}
	before := backend.calls.Load()
	for _, token := range []string{"", "session-a", "ableops_mcp_AAAA", TokenPrefix, TokenPrefix + "not-base64!", TokenPrefix + "AAAA"} {
		if _, err := store.Authenticate(context.Background(), token); authCode(t, err) != "authentication_required" {
			t.Fatalf("형식 위반 토큰이 통과했다: %v", err)
		}
	}
	if backend.calls.Load() != before {
		t.Fatal("형식 위반 토큰으로 Backend 를 호출했다")
	}
}

// TTL 범위 밖 설정은 기동 시점에 거부한다(운영 중 조용히 기본값으로 떨어지지 않는다).
func TestNewStoreValidatesTTL(t *testing.T) {
	backend := newSessions(map[string]string{})
	for _, ttl := range []time.Duration{MinTTL - time.Second, MaxTTL + time.Second, -time.Minute} {
		if _, err := NewStore(backend.verify, ttl); err == nil {
			t.Fatalf("범위 밖 TTL 이 허용됐다: %s", ttl)
		}
	}
	if _, err := NewStore(nil, DefaultTTL); err == nil {
		t.Fatal("검사기 없이 저장소가 만들어졌다")
	}
	store, err := NewStore(backend.verify, 0)
	if err != nil || store.TTL() != DefaultTTL {
		t.Fatalf("0 은 기본 TTL 이어야 한다: %v %s", err, store.TTL())
	}
}

// 동시 발급·인증에서 사용자 경계가 섞이지 않아야 한다(-race 로 함께 검증한다).
func TestConcurrentIssueKeepsUserBoundary(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a", "session-b": "b"})
	store := newTestStore(t, backend.verify)
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for range 20 {
				grant, err := store.Issue(context.Background(), "session-"+id)
				if err != nil {
					t.Error("동시 발급 실패")
					return
				}
				principal, err := store.Authenticate(context.Background(), grant.Token)
				if err != nil || principal.UserID != id || principal.BackendToken != "session-"+id {
					t.Error("동시 인증에서 사용자 경계가 섞였다")
					return
				}
				store.Revoke(grant.Token)
			}
		}(id)
	}
	wg.Wait()
	if store.Len() != 0 {
		t.Fatalf("폐기한 위임이 남았다: %d", store.Len())
	}
}
