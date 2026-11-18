package resilience

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestBreakerLifecycle(t *testing.T) {
	clk := newClock()
	var transitions []string
	b := NewBreaker(3, 10*time.Second, 1, clk.Now, func(from, to State) {
		transitions = append(transitions, from.String()+"->"+to.String())
	})

	// Two failures and a success keep it closed (consecutive count resets).
	for _, r := range []Result{Failure, Failure, Success, Failure, Failure} {
		if !b.Allow() {
			t.Fatal("closed breaker must allow")
		}
		b.Done(r)
	}
	if b.State() != Closed {
		t.Fatalf("state = %v, want closed", b.State())
	}
	// Ignore results never trip it.
	for i := 0; i < 10; i++ {
		b.Allow()
		b.Done(Ignore)
	}
	// Third consecutive failure trips it.
	b.Allow()
	b.Done(Failure)
	if b.State() != Open {
		t.Fatalf("state = %v, want open", b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker must reject")
	}

	// After the cool-off exactly one probe is admitted.
	clk.Advance(10 * time.Second)
	if !b.Allow() {
		t.Fatal("expected half-open probe")
	}
	if b.Allow() {
		t.Fatal("only one concurrent probe allowed")
	}
	b.Done(Failure) // probe fails -> open again
	if b.State() != Open || b.Allow() {
		t.Fatal("failed probe must reopen")
	}

	clk.Advance(10 * time.Second)
	if !b.Allow() {
		t.Fatal("expected second probe")
	}
	b.Done(Success)
	if b.State() != Closed || !b.Allow() {
		t.Fatal("successful probe must close")
	}
	b.Done(Success)

	want := []string{"closed->open", "open->half_open", "half_open->open", "open->half_open", "half_open->closed"}
	if len(transitions) != len(want) {
		t.Fatalf("transitions = %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", transitions, want)
		}
	}
}

func TestBreakerIgnoredProbeReleasesSlot(t *testing.T) {
	clk := newClock()
	b := NewBreaker(1, time.Second, 1, clk.Now, nil)
	b.Allow()
	b.Done(Failure)
	clk.Advance(time.Second)
	if !b.Allow() {
		t.Fatal("expected probe")
	}
	b.Done(Ignore) // e.g. provider saturated, attempt never sent
	if !b.Allow() {
		t.Fatal("ignored probe must free the probe slot")
	}
}

func TestBreakerLateFailure(t *testing.T) {
	clk := newClock()
	b := NewBreaker(2, time.Second, 1, clk.Now, nil)
	b.Allow()
	b.Done(Success) // stream started
	b.Fail()        // ...and broke later
	if b.State() != Closed {
		t.Fatal("one late failure must not trip a threshold-2 breaker")
	}
	b.Fail()
	if b.State() != Open {
		t.Fatal("two consecutive failures must trip it")
	}
	b.Fail() // late failures while open are ignored
	clk.Advance(time.Second)
	if !b.Allow() {
		t.Fatal("expected half-open probe")
	}
}

func TestRetryBudget(t *testing.T) {
	clk := newClock()
	b := NewRetryBudget(0.2, 0, clk.Now) // no time floor
	start := b.Tokens()
	for i := 0; i < int(start); i++ {
		if !b.TryWithdraw() {
			t.Fatalf("withdraw %d failed with initial balance %v", i, start)
		}
	}
	if b.TryWithdraw() {
		t.Fatal("budget should be exhausted")
	}
	// 10 requests at 20% earn 2 retries.
	for i := 0; i < 10; i++ {
		b.Deposit()
	}
	if !b.TryWithdraw() || !b.TryWithdraw() || b.TryWithdraw() {
		t.Fatal("expected exactly two retries after ten deposits")
	}

	// The time floor refills slowly even without traffic.
	b2 := NewRetryBudget(0, 2, clk.Now)
	for b2.TryWithdraw() {
	}
	clk.Advance(time.Second)
	if !b2.TryWithdraw() || !b2.TryWithdraw() || b2.TryWithdraw() {
		t.Fatal("expected two retries after one second at 2/s")
	}
}

func TestBackoffBounds(t *testing.T) {
	base, max := 100*time.Millisecond, time.Second
	for n := 1; n <= 8; n++ {
		ceiling := base << (n - 1)
		if ceiling > max {
			ceiling = max
		}
		for i := 0; i < 200; i++ {
			d := Backoff(n, base, max)
			if d < 0 || d > ceiling {
				t.Fatalf("Backoff(%d) = %v outside [0, %v]", n, d, ceiling)
			}
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"", 0, false},
		{"3", 3 * time.Second, true},
		{"0.5", 500 * time.Millisecond, true},
		{"-1", 0, false},
		{"Thu, 01 Jan 2026 00:00:10 GMT", 10 * time.Second, true},
		{"soon", 0, false},
	}
	for _, c := range cases {
		got, ok := ParseRetryAfter(c.in, now)
		if got != c.want || ok != c.ok {
			t.Errorf("ParseRetryAfter(%q) = %v, %v; want %v, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestSemaphore(t *testing.T) {
	s := NewSemaphore(1)
	if !s.TryAcquire() || s.TryAcquire() {
		t.Fatal("capacity 1 violated")
	}
	if s.Acquire(context.Background(), 0) {
		t.Fatal("zero wait must not block or succeed when full")
	}
	start := time.Now()
	if s.Acquire(context.Background(), 20*time.Millisecond) {
		t.Fatal("should time out")
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("returned before maxWait")
	}
	go func() { time.Sleep(10 * time.Millisecond); s.Release() }()
	if !s.Acquire(context.Background(), time.Second) {
		t.Fatal("should acquire after release")
	}
	if s.InUse() != 1 || s.Cap() != 1 {
		t.Fatalf("InUse=%d Cap=%d", s.InUse(), s.Cap())
	}
}

func TestTokenBucket(t *testing.T) {
	clk := newClock()
	b := NewTokenBucket(2, 2, clk.Now)
	if ok, _ := b.Allow(); !ok {
		t.Fatal("first")
	}
	if ok, _ := b.Allow(); !ok {
		t.Fatal("second")
	}
	ok, wait := b.Allow()
	if ok || wait != 500*time.Millisecond {
		t.Fatalf("third: ok=%v wait=%v", ok, wait)
	}
	clk.Advance(500 * time.Millisecond)
	if ok, _ := b.Allow(); !ok {
		t.Fatal("refill")
	}
}
