// Package webdelegation은 AbleOps Web 세션을 MCP 인증으로 잇는 **단기 위임**을 메모리에만 보관한다.
//
// localauth(파일 기반 local.auth-store.json)와 일부러 분리한 저장소다. 성격이 다르기 때문이다.
//   - localauth : 사람이 CLI 로 등록하는 장기 클라이언트(Claude Desktop·Codex). 재기동 후에도 살아야 한다.
//   - 여기      : Web/Chat Backend 가 요청마다 발급하는 수 분짜리 위임. 디스크에 남으면 안 된다.
//
// ⚠ Backend 세션 토큰은 **절대 디스크에 쓰지 않는다**. 프로세스 재시작 시 모든 위임이 소멸하는 것은
// 결함이 아니라 설계다 — 재발급 경로(Backend 세션 → Issue)가 항상 존재한다.
// ⚠ 위임 토큰과 Backend 토큰은 반드시 다른 값이다(접두사로 타입 수준 분리 — validBackendToken 참조).
// ⚠ 토큰 원문은 로그·오류·응답 어디에도 넣지 않는다. Error 는 고정 코드만 갖는다.
package webdelegation

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

const (
	// TokenPrefix는 위임 토큰의 고정 접두사다. localauth의 "ableops_mcp_" 와 겹치지 않아야
	// HTTP 인증기가 두 저장소를 접두사만으로 갈라 보낼 수 있다.
	TokenPrefix = "ableops_web_"
	// localAuthTokenPrefix는 localauth 토큰 접두사다. Backend 토큰 검사에서 되먹임을 막는 데만 쓴다.
	localAuthTokenPrefix = "ableops_mcp_"
	// DefaultTTL은 발급 요청이 수명을 지정하지 않았을 때의 기본 수명이다.
	DefaultTTL = 10 * time.Minute
	// MaxTTL은 설정으로도 넘을 수 없는 상한이다. Web 위임은 재발급이 싸므로 길게 둘 이유가 없다.
	MaxTTL = 30 * time.Minute
	// MinTTL은 발급 직후 만료되어 쓸 수 없는 설정을 막는 하한이다.
	MinTTL = time.Minute
	// maxEntries는 발급 폭주가 프로세스 메모리를 잠식하지 못하게 하는 상한이다.
	//
	// ⚠ 이 값만으로는 **공정성**을 지킬 수 없다. 전역 상한 하나뿐이면 한 사용자가 저장소를 다
	// 채워 다른 모든 사용자의 신규 발급을 막는다. 사용자별 상한은 perUserIssueInterval 을 보라.
	maxEntries = 4096
	// perUserIssueInterval은 사용자별 상한을 정하는 기준 발급 간격이다.
	//
	// 보유 위임 수는 "발급률 × TTL"로 늘어나므로 사용자별 상한을 상수로 두면 TTL 설정에 따라
	// 의미가 달라진다. 짧은 TTL 에서는 정상 사용자를 막고, 긴 TTL 에서는 한 사용자가 전역 상한에
	// 닿기 쉬워진다. 그래서 상한을 개수가 아니라 **지속 발급률**로 정한다 — 이 간격보다 빠르게
	// 계속 발급하는 사용자만 한도에 닿으며, 그 판정은 TTL 설정과 무관하게 같다.
	perUserIssueInterval = 5 * time.Second
	// minEntriesPerUser는 사용자별 상한의 하한이다. TTL 을 최소값으로 둔 배포에서도 재시도·
	// 병렬 탭 같은 정상 사용이 한도에 닿지 않아야 한다.
	minEntriesPerUser = 32
	// maxBackendTokenBytes는 localauth와 같은 Backend 토큰 길이 상한이다.
	maxBackendTokenBytes = 4096
)

// VerifyBackend는 캐시 없이 Backend 세션을 검사하고 현재 사용자 ID를 반환한다(localauth와 동일 계약).
type VerifyBackend func(context.Context, string) (string, error)

// Error에는 외부 입력이나 내부 오류 원문을 보관하지 않는다.
type Error struct{ Code string }

func (e *Error) Error() string    { return e.Code }
func (e *Error) AuthCode() string { return e.Code }

func authError(code string) error { return &Error{Code: code} }

// Delegation은 발급 결과 중 **호출자에게 돌려줘도 되는 것만** 담는다.
// ⚠ Backend 토큰 필드를 여기에 추가하지 않는다 — 이 구조체는 내부 발급 응답으로 직렬화된다.
type Delegation struct {
	Token     string
	UserID    string
	ExpiresAt time.Time
}

type entry struct {
	userID       string
	backendToken string
	issuedAt     time.Time
	expiresAt    time.Time
}

// Store는 위임을 메모리에만 보관한다. 키는 토큰 원문이 아니라 SHA-256 다이제스트다.
type Store struct {
	mu      sync.Mutex
	entries map[string]entry
	verify  VerifyBackend
	ttl     time.Duration
	// perUser는 사용자 한 명이 동시에 보유할 수 있는 위임 수다. TTL 로부터 기동 시점에 정한다.
	perUser int
	now     func() time.Time
}

// NewStore는 TTL 상·하한을 기동 시점에 확정한다. ttl 이 0 이면 DefaultTTL 을 쓴다.
func NewStore(verify VerifyBackend, ttl time.Duration) (*Store, error) {
	if verify == nil {
		return nil, authError("delegation_unavailable")
	}
	if ttl == 0 {
		ttl = DefaultTTL
	}
	if ttl < MinTTL || ttl > MaxTTL {
		return nil, authError("delegation_unavailable")
	}
	return &Store{entries: make(map[string]entry), verify: verify, ttl: ttl, perUser: perUserLimit(ttl), now: time.Now}, nil
}

// perUserLimit은 TTL 에서 사용자별 상한을 정한다.
//
// 상한을 TTL 에 비례시키는 이유는 보유 위임 수가 "발급률 × TTL"로 늘기 때문이다. 고정 개수로
// 두면 같은 발급률이 TTL 설정에 따라 통과하기도, 막히기도 한다. 비례시키면 판정 기준이 언제나
// "perUserIssueInterval 보다 빠른 지속 발급" 하나로 유지된다.
func perUserLimit(ttl time.Duration) int {
	limit := int(ttl / perUserIssueInterval)
	if limit < minEntriesPerUser {
		return minEntriesPerUser
	}
	return limit
}

// TTL은 현재 적용 중인 위임 수명이다(설정 확인·응답 메타데이터용).
func (s *Store) TTL() time.Duration { return s.ttl }

// PerUserLimit은 현재 적용 중인 사용자별 위임 상한이다(설정 확인·시험용).
func (s *Store) PerUserLimit() int { return s.perUser }

// Issue는 전달받은 Backend 세션 자격증명을 **그대로 신뢰하지 않는다**.
// 기존 AbleOps Client 로 현재 사용자를 먼저 확인하고, 성공했을 때만 메모리에 등록한다.
// 반환값에 Backend 토큰은 포함하지 않는다.
func (s *Store) Issue(ctx context.Context, backendToken string) (Delegation, error) {
	if !validBackendToken(backendToken) {
		return Delegation{}, authError("invalid_delegation_request")
	}
	userID, err := s.verify(backendSessionContext(ctx, backendToken), backendToken)
	if err != nil {
		return Delegation{}, backendError(err)
	}
	if !validID(userID) {
		return Delegation{}, authError("invalid_delegation_request")
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return Delegation{}, authError("delegation_unavailable")
	}
	token := TokenPrefix + base64.RawURLEncoding.EncodeToString(random)
	now := s.now().UTC()
	value := entry{userID: userID, backendToken: backendToken, issuedAt: now, expiresAt: now.Add(s.ttl)}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(now)
	// 전역 상한은 프로세스 메모리를, 사용자별 상한은 다른 사용자의 발급 가능성을 지킨다.
	// 둘 다 같은 코드(delegation_limit)로 답한다 — 어느 쪽에 걸렸는지는 호출자에게 알려 줄
	// 정보가 아니고, 조치(잠시 후 재시도)도 같다.
	if len(s.entries) >= maxEntries || s.countUserLocked(userID) >= s.perUser {
		return Delegation{}, authError("delegation_limit")
	}
	s.entries[digest(token)] = value
	return Delegation{Token: token, UserID: value.userID, ExpiresAt: value.expiresAt}, nil
}

// Authenticate는 TTL 만 믿지 않는다. 매 요청 Backend 세션이 아직 유효한지 확인하고,
// 무효해진 위임은 저장소에서 즉시 제거한다(로그아웃이 다음 MCP 요청에서 바로 드러나야 한다).
func (s *Store) Authenticate(ctx context.Context, bearer string) (requestctx.Principal, error) {
	if !ValidToken(bearer) {
		return requestctx.Principal{}, authError("authentication_required")
	}
	key := digest(bearer)
	now := s.now().UTC()
	s.mu.Lock()
	s.purgeExpiredLocked(now)
	value, found := s.entries[key]
	s.mu.Unlock()
	if !found || !now.Before(value.expiresAt) || now.Before(value.issuedAt) {
		return requestctx.Principal{}, authError("authentication_required")
	}
	principal, _ := requestctx.FromContext(ctx)
	principal.UserID, principal.ClientID = value.userID, ClientID
	principal.BackendToken, principal.MCPToken = value.backendToken, bearer
	userID, err := s.verify(requestctx.WithPrincipal(ctx, principal), value.backendToken)
	if err != nil {
		// Backend 세션이 죽었다(로그아웃·만료·권한 회수). 위임을 남겨 두면 TTL 동안 재시도가 계속된다.
		if isSessionFailure(err) {
			s.Revoke(bearer)
		}
		return requestctx.Principal{}, backendError(err)
	}
	if userID != value.userID || userID == "" {
		// 같은 Backend 토큰이 다른 사용자로 해석됐다 — 신원 혼선이므로 위임을 폐기한다.
		s.Revoke(bearer)
		return requestctx.Principal{}, authError("authentication_required")
	}
	return principal, nil
}

// ClientID는 위임으로 인증된 요청의 고정 클라이언트 식별자다. localauth의 사람이 정한
// client_id 와 구분되어야 감사 로그에서 두 경로를 가를 수 있다.
const ClientID = "web-delegation"

// Revoke는 위임 하나를 즉시 제거한다. 없는 토큰은 조용히 무시한다(존재 여부를 알려주지 않는다).
func (s *Store) Revoke(token string) {
	key := digest(token)
	s.mu.Lock()
	delete(s.entries, key)
	s.mu.Unlock()
}

// Len은 시험·운영 점검용 보유 위임 수다(토큰 내용은 노출하지 않는다).
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(s.now().UTC())
	return len(s.entries)
}

// countUserLocked는 userID 가 보유한 위임 수다. 호출 전에 purgeExpiredLocked 로 만료분을
// 지워야 만료된 위임이 한도를 차지하지 않는다.
//
// 별도 카운터 맵을 두지 않는 이유는 Issue·Revoke·purge 세 곳과 동기화해야 해 불일치 위험이
// 오히려 커지기 때문이다. 엔트리는 maxEntries(4096) 이하이고 발급 경로는 이미 Backend 왕복을
// 한 번 하므로 선형 순회 비용은 무시할 수 있다.
func (s *Store) countUserLocked(userID string) int {
	count := 0
	for _, value := range s.entries {
		if value.userID == userID {
			count++
		}
	}
	return count
}

func (s *Store) purgeExpiredLocked(now time.Time) {
	for key, value := range s.entries {
		if !now.Before(value.expiresAt) {
			delete(s.entries, key)
		}
	}
}

// backendSessionContext는 발급 검증 호출에 Backend 토큰을 문맥으로 전달한다.
// 아직 위임 토큰이 없으므로 MCPToken 은 비워 둔다.
func backendSessionContext(ctx context.Context, backendToken string) context.Context {
	principal, _ := requestctx.FromContext(ctx)
	principal.BackendToken = backendToken
	principal.MCPToken = ""
	return requestctx.WithPrincipal(ctx, principal)
}

func digest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// ValidToken은 위임 토큰의 형태를 검사한다. HTTP 인증기가 저장소를 고르기 전에 쓴다.
func ValidToken(value string) bool {
	if !strings.HasPrefix(value, TokenPrefix) {
		return false
	}
	raw := strings.TrimPrefix(value, TokenPrefix)
	data, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	return err == nil && len(data) == 32 && base64.RawURLEncoding.EncodeToString(data) == raw
}

// validBackendToken은 localauth와 같은 규칙을 쓴다. MCP 토큰 접두사를 가진 값은 Backend
// 토큰으로 받지 않는다 — 발급 응답을 그대로 되먹여 위임을 무한 연장하는 경로를 막는다.
func validBackendToken(value string) bool {
	if value == "" || len(value) > maxBackendTokenBytes ||
		strings.HasPrefix(value, TokenPrefix) || strings.HasPrefix(value, localAuthTokenPrefix) {
		return false
	}
	for _, ch := range value {
		if ch < 0x21 || ch > 0x7e {
			return false
		}
	}
	return true
}

func validID(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, ch := range value {
		if ch < 0x20 || ch == 0x7f {
			return false
		}
	}
	return true
}

// isSessionFailure는 "세션이 죽었다"와 "백엔드에 못 닿았다"를 가른다.
// 일시적 장애로 위임을 지우면 백엔드가 돌아와도 사용자가 다시 로그인해야 한다.
func isSessionFailure(err error) bool {
	var backend *ableops.Error
	if errors.As(err, &backend) {
		return backend.Code == "authentication_required" || backend.Code == "access_denied"
	}
	return false
}

func backendError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return authError("timeout")
	}
	if errors.Is(err, context.Canceled) {
		return authError("canceled")
	}
	var backend *ableops.Error
	if errors.As(err, &backend) {
		switch backend.Code {
		case "authentication_required":
			return authError("backend_authentication_required")
		case "access_denied", "timeout", "canceled":
			return authError(backend.Code)
		}
	}
	return authError("backend_unavailable")
}
