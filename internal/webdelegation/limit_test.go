package webdelegation

// 발급 한도가 **사용자별로** 나뉘는지 고정한다.
//
// 전역 상한 하나뿐이던 시절에는 한 사용자가 저장소를 다 채우면 다른 모든 사용자의 신규 발급이
// delegation_limit 으로 막혔다. 보유 위임 수는 "발급률 × TTL"로 늘기 때문에 그 상황은 TTL 을
// 길게 둔 배포일수록 쉽게 만들어진다 — 여기 시험들이 두 성질을 함께 묶어 둔다.

import (
	"context"
	"testing"
	"time"
)

// 한 사용자가 자기 상한을 다 써도 다른 사용자는 그대로 발급받을 수 있어야 한다.
func TestPerUserLimitDoesNotBlockOtherUsers(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a", "session-b": "b"})
	store := newTestStore(t, backend.verify)
	ctx := context.Background()

	for i := 0; i < store.PerUserLimit(); i++ {
		if _, err := store.Issue(ctx, "session-a"); err != nil {
			t.Fatalf("상한 안 발급이 실패했다(%d번째): %v", i+1, err)
		}
	}
	// 상한에 닿은 사용자만 막힌다.
	if _, err := store.Issue(ctx, "session-a"); authCode(t, err) != "delegation_limit" {
		t.Fatalf("상한을 넘겨 발급됐다: %v", err)
	}
	// 다른 사용자는 영향을 받지 않는다 — 이것이 전역 상한만 있던 때와 갈리는 지점이다.
	grant, err := store.Issue(ctx, "session-b")
	if err != nil {
		t.Fatalf("다른 사용자의 발급이 막혔다: %v", err)
	}
	if grant.UserID != "b" {
		t.Fatalf("사용자 경계가 어긋났다: %s", grant.UserID)
	}
	// 전역 상한에는 한참 못 미친 상태여야 한다(사용자별 상한이 먼저 걸렸다는 뜻).
	if store.Len() >= maxEntries {
		t.Fatalf("전역 상한이 먼저 걸렸다: %d", store.Len())
	}
}

// 만료된 위임은 사용자별 한도를 차지하지 않아야 한다. 그렇지 않으면 한 번 상한에 닿은 사용자가
// TTL 과 무관하게 영구히 막힌다.
func TestPerUserLimitFreesOnExpiry(t *testing.T) {
	backend := newSessions(map[string]string{"session-a": "a"})
	store := newTestStore(t, backend.verify)
	base := time.Now().UTC()
	store.now = func() time.Time { return base }
	ctx := context.Background()

	for i := 0; i < store.PerUserLimit(); i++ {
		if _, err := store.Issue(ctx, "session-a"); err != nil {
			t.Fatalf("상한 안 발급이 실패했다(%d번째): %v", i+1, err)
		}
	}
	if _, err := store.Issue(ctx, "session-a"); authCode(t, err) != "delegation_limit" {
		t.Fatalf("상한을 넘겨 발급됐다: %v", err)
	}
	// TTL 경과 후에는 만료분이 정리되어 다시 발급된다.
	store.now = func() time.Time { return base.Add(DefaultTTL) }
	if _, err := store.Issue(ctx, "session-a"); err != nil {
		t.Fatalf("만료 후에도 상한이 풀리지 않았다: %v", err)
	}
	if store.Len() != 1 {
		t.Fatalf("만료분이 정리되지 않았다: %d", store.Len())
	}
}

// 사용자별 상한은 TTL 에 비례해야 한다.
//
// 고정 개수로 두면 같은 발급률이 TTL 설정에 따라 통과하기도 막히기도 한다. 비례시켜야 판정
// 기준이 "perUserIssueInterval 보다 빠른 지속 발급" 하나로 유지된다.
func TestPerUserLimitScalesWithTTL(t *testing.T) {
	backend := newSessions(map[string]string{})
	for _, tc := range []struct {
		ttl  time.Duration
		want int
	}{
		{MinTTL, minEntriesPerUser}, // 60s/5s=12 → 하한 32 로 올린다
		{5 * time.Minute, 60},       // 300s/5s
		{DefaultTTL, 120},           // 600s/5s
		{MaxTTL, 360},               // 1800s/5s
	} {
		store, err := NewStore(backend.verify, tc.ttl)
		if err != nil {
			t.Fatalf("저장소 생성 실패(%s): %v", tc.ttl, err)
		}
		if got := store.PerUserLimit(); got != tc.want {
			t.Fatalf("TTL %s 의 사용자별 상한이 %d 다(기대 %d)", tc.ttl, got, tc.want)
		}
	}
}

// 저장소를 가득 채우는 데 필요한 **서로 다른 사용자 수**가 1 보다 크다는 것을 고정한다.
//
// 이 시험이 지키는 성질은 "한 사용자가 전역 상한에 닿을 수 없다"이다. TTL 을 최대값으로 둔
// 배포에서도 성립해야 한다 — 긴 TTL 은 같은 발급률로 더 많은 위임이 살아 있게 만들기 때문이다.
func TestSingleUserCannotExhaustGlobalLimit(t *testing.T) {
	for _, ttl := range []time.Duration{MinTTL, DefaultTTL, MaxTTL} {
		backend := newSessions(map[string]string{"session-a": "a"})
		store, err := NewStore(backend.verify, ttl)
		if err != nil {
			t.Fatalf("저장소 생성 실패(%s): %v", ttl, err)
		}
		if store.PerUserLimit() >= maxEntries {
			t.Fatalf("TTL %s 에서 한 사용자가 전역 상한에 닿는다: %d >= %d", ttl, store.PerUserLimit(), maxEntries)
		}
		ctx := context.Background()
		for {
			if _, err := store.Issue(ctx, "session-a"); err != nil {
				if authCode(t, err) != "delegation_limit" {
					t.Fatalf("예상 밖 발급 실패(%s): %v", ttl, err)
				}
				break
			}
		}
		// 한 사용자가 멈춘 지점이 전역 상한보다 한참 아래여야 다른 사용자의 여유가 남는다.
		if store.Len() >= maxEntries {
			t.Fatalf("TTL %s 에서 한 사용자가 저장소를 채웠다: %d", ttl, store.Len())
		}
		if remaining := maxEntries - store.Len(); remaining < maxEntries/2 {
			t.Fatalf("TTL %s 에서 한 사용자가 저장소 절반을 넘게 썼다: 잔여 %d", ttl, remaining)
		}
	}
}
