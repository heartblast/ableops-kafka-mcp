package mcpserver

// HTTP 요청 빈도 제한이다. 기존 동시성 제한(global/user)과는 **다른 축**을 막는다.
//
//   - 동시성 제한은 "같은 순간에 몇 개가 떠 있나"를 본다. 짧은 요청을 순차로 수만 번 던지면
//     동시성은 1 을 넘지 않으므로 전혀 걸리지 않는다.
//   - 여기서 막는 것은 "일정 시간 안에 몇 번 왔나"다. 인증 실패 반복(토큰 대입·공유 비밀 대입)과
//     인증된 사용자의 과도한 반복 호출이 대상이다.
//
// 외부 의존성은 쓰지 않는다. 토큰 버킷 하나면 충분하고, 창(window)이 끝나기를 기다리지 않고
// 시간에 비례해 회복하므로 정상 사용자는 제한에 걸려도 곧바로 풀린다.

import (
	"net"
	"sync"
	"time"
)

// rateLimiter는 키별 토큰 버킷이다. 키 수에는 상한이 있으며 상한에 닿으면 정리한다.
// ⚠ 키는 IP 또는 사용자 ID처럼 공격자가 무한히 만들어 낼 수 있는 값이므로 상한이 곧 메모리 상한이다.
type rateLimiter struct {
	mu         sync.Mutex
	burst      float64 // 버킷 용량 = 창당 허용 횟수
	refill     float64 // 초당 보충 토큰 수
	maxEntries int
	entries    map[string]*rateEntry
	// now는 테스트에서 시계를 고정하기 위한 주입점이다. nil 이면 time.Now 를 쓴다.
	now func() time.Time
}

type rateEntry struct {
	tokens float64
	seen   time.Time
}

// newRateLimiter는 window 동안 limit 회를 허용하는 제한기를 만든다.
// limit 이 0 이하면 제한을 끄고(nil 이 아닌 통과 전용 제한기) 호출자의 분기를 없앤다.
func newRateLimiter(limit int, window time.Duration, maxEntries int) *rateLimiter {
	if limit <= 0 || window <= 0 {
		return nil
	}
	if maxEntries <= 0 {
		maxEntries = defaultRateLimitEntries
	}
	return &rateLimiter{
		burst:      float64(limit),
		refill:     float64(limit) / window.Seconds(),
		maxEntries: maxEntries,
		entries:    make(map[string]*rateEntry),
	}
}

func (l *rateLimiter) clock() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

// entryLocked는 키의 버킷을 현재 시각까지 회복시킨 뒤 돌려준다. 없으면 만든다.
func (l *rateLimiter) entryLocked(key string, at time.Time) *rateEntry {
	entry, ok := l.entries[key]
	if ok {
		elapsed := at.Sub(entry.seen).Seconds()
		if elapsed > 0 {
			entry.tokens = min(l.burst, entry.tokens+elapsed*l.refill)
		}
		entry.seen = at
		return entry
	}
	l.evictLocked(at)
	entry = &rateEntry{tokens: l.burst, seen: at}
	l.entries[key] = entry
	return entry
}

// evictLocked는 새 키를 넣기 전에 자리를 만든다. 가득 찬(=완전히 회복된) 버킷은 지워도
// 판정이 달라지지 않으므로 먼저 버리고, 그래도 상한이면 가장 오래 안 쓰인 항목을 버린다.
// ⚠ "상한이면 새 키를 거부"로 만들지 않는다. 그러면 공격자가 키를 채워 정상 사용자를 잠글 수 있다.
func (l *rateLimiter) evictLocked(at time.Time) {
	if len(l.entries) < l.maxEntries {
		return
	}
	for key, entry := range l.entries {
		if entry.tokens+at.Sub(entry.seen).Seconds()*l.refill >= l.burst {
			delete(l.entries, key)
		}
	}
	for len(l.entries) >= l.maxEntries {
		oldestKey, oldest := "", time.Time{}
		for key, entry := range l.entries {
			if oldest.IsZero() || entry.seen.Before(oldest) {
				oldestKey, oldest = key, entry.seen
			}
		}
		delete(l.entries, oldestKey)
	}
}

// allow는 토큰 하나를 소비한다. 남은 토큰이 없으면 false 이며 아무것도 소비하지 않는다.
func (l *rateLimiter) allow(key string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := l.entryLocked(key, l.clock())
	if entry.tokens < 1 {
		return false
	}
	entry.tokens--
	return true
}

// permitted는 소비하지 않고 남은 토큰만 본다. 인증 실패처럼 "실패했을 때만 세는" 제한에서
// 요청 처리 전에 이미 소진된 키를 걸러 내는 데 쓴다.
func (l *rateLimiter) permitted(key string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entryLocked(key, l.clock()).tokens >= 1
}

// penalize는 실패 한 번을 기록한다. 반환값은 남은 토큰이 있는지 여부다.
func (l *rateLimiter) penalize(key string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := l.entryLocked(key, l.clock())
	if entry.tokens >= 1 {
		entry.tokens--
	}
}

// size는 보관 중인 키 수다(테스트와 상한 검증용).
func (l *rateLimiter) size() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// peerKey는 요청의 원 피어 주소에서 제한 키를 뽑는다.
// ⚠ 프록시 헤더는 절대 보지 않는다 — 공격자가 마음대로 바꿀 수 있는 값으로 키를 나누면
// 제한 자체가 무력해진다. 그런 헤더가 붙은 요청은 애초에 앞단에서 거부된다.
func peerKey(remoteAddr string) string {
	if remoteAddr == "" {
		return "unknown"
	}
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil && host != "" {
		return host
	}
	return remoteAddr
}
