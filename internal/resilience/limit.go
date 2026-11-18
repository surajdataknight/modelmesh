package resilience

import (
	"context"
	"sync"
	"time"
)

// Semaphore bounds concurrency. It is a buffered channel: Go's channel
// implementation already provides a fair, allocation-free counting
// semaphore with context-aware waiting.
type Semaphore struct {
	slots chan struct{}
}

// NewSemaphore returns a semaphore with n slots.
func NewSemaphore(n int) *Semaphore {
	return &Semaphore{slots: make(chan struct{}, n)}
}

// TryAcquire takes a slot without waiting.
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// Acquire waits up to maxWait (or until ctx is done) for a slot. A zero
// maxWait means do not wait at all: under overload the gateway sheds load
// immediately instead of building an unbounded queue.
func (s *Semaphore) Acquire(ctx context.Context, maxWait time.Duration) bool {
	if s.TryAcquire() {
		return true
	}
	if maxWait <= 0 {
		return false
	}
	t := time.NewTimer(maxWait)
	defer t.Stop()
	select {
	case s.slots <- struct{}{}:
		return true
	case <-t.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// Release returns a slot.
func (s *Semaphore) Release() { <-s.slots }

// InUse returns the number of held slots.
func (s *Semaphore) InUse() int { return len(s.slots) }

// Cap returns the number of slots.
func (s *Semaphore) Cap() int { return cap(s.slots) }

// TokenBucket is a classic token-bucket rate limiter.
type TokenBucket struct {
	mu     sync.Mutex
	now    func() time.Time
	rate   float64 // tokens per second
	burst  float64
	tokens float64
	last   time.Time
}

// NewTokenBucket returns a full bucket.
func NewTokenBucket(rate float64, burst int, now func() time.Time) *TokenBucket {
	if now == nil {
		now = time.Now
	}
	return &TokenBucket{now: now, rate: rate, burst: float64(burst), tokens: float64(burst), last: now()}
}

// Allow takes one token if available. When it returns false, retryAfter is
// how long until a token will be available.
func (b *TokenBucket) Allow() (ok bool, retryAfter time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(b.burst, b.tokens+elapsed*b.rate)
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	need := (1 - b.tokens) / b.rate
	return false, time.Duration(need * float64(time.Second))
}
