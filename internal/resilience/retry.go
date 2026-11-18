package resilience

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RetryBudget limits retries to a fraction of overall traffic, so a failing
// provider cannot multiply load by MaxAttempts (a retry storm). Each request
// deposits Ratio tokens; each retry withdraws one. A time-based floor keeps
// retries possible at low traffic. Modeled on the retry budgets used by
// Finagle and Envoy.
type RetryBudget struct {
	mu        sync.Mutex
	now       func() time.Time
	ratio     float64
	minPerSec float64
	max       float64
	tokens    float64
	last      time.Time
}

// NewRetryBudget creates a budget that starts full so cold starts can retry.
func NewRetryBudget(ratio, minPerSec float64, now func() time.Time) *RetryBudget {
	if now == nil {
		now = time.Now
	}
	max := minPerSec*10 + 10
	return &RetryBudget{now: now, ratio: ratio, minPerSec: minPerSec, max: max, tokens: max, last: now()}
}

// Deposit records one original request.
func (b *RetryBudget) Deposit() {
	b.mu.Lock()
	b.refill()
	b.tokens = min(b.max, b.tokens+b.ratio)
	b.mu.Unlock()
}

// TryWithdraw reports whether one retry is allowed and, if so, spends it.
func (b *RetryBudget) TryWithdraw() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	// The epsilon absorbs float drift (10 deposits of 0.2 sum to 1.999...).
	if b.tokens+1e-9 < 1 {
		return false
	}
	b.tokens--
	return true
}

// Tokens returns the current balance (for metrics and tests).
func (b *RetryBudget) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return b.tokens
}

func (b *RetryBudget) refill() {
	now := b.now()
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(b.max, b.tokens+elapsed*b.minPerSec)
	}
	b.last = now
}

// Backoff returns a "full jitter" delay for the given retry number (1-based):
// a uniform random value in [0, min(max, base*2^(n-1))]. Full jitter spreads
// synchronized retries apart better than equal or decorrelated jitter for
// this access pattern.
func Backoff(n int, base, max time.Duration) time.Duration {
	if n < 1 {
		n = 1
	}
	ceiling := base
	for i := 1; i < n && ceiling < max; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, max)
	if ceiling <= 0 {
		return 0
	}
	return rand.N(ceiling + 1)
}

// ParseRetryAfter interprets a Retry-After header (delta-seconds or an
// HTTP-date). It returns ok=false for a missing or unusable value.
func ParseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs * float64(time.Second)), true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
