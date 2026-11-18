// Package resilience contains the gateway's reliability primitives. Every
// time-dependent type takes an injectable clock so tests are deterministic
// without sleeping.
package resilience

import (
	"sync"
	"time"
)

// State is a circuit breaker state.
type State int

// Breaker states. Values double as the metric gauge value.
const (
	Closed   State = 0
	HalfOpen State = 1
	Open     State = 2
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half_open"
	default:
		return "open"
	}
}

// Result classifies an attempt for the breaker.
type Result int

// Attempt results. Ignore is for outcomes that say nothing about provider
// health (client cancelled, client error, 429 handled by cooldown).
const (
	Success Result = iota
	Failure
	Ignore
)

// Breaker is a consecutive-failure circuit breaker with a bounded number of
// concurrent half-open probes. Consecutive failures (rather than a rolling
// error rate) keep the behavior easy to reason about and test.
type Breaker struct {
	mu        sync.Mutex
	now       func() time.Time
	threshold int
	openFor   time.Duration
	maxProbes int

	state    State
	failures int
	openedAt time.Time
	probes   int

	onChange func(from, to State)
}

// NewBreaker returns a closed breaker. The optional onChange callback runs
// after each state transition, outside the breaker's lock.
func NewBreaker(threshold int, openFor time.Duration, maxProbes int, now func() time.Time, onChange func(from, to State)) *Breaker {
	if now == nil {
		now = time.Now
	}
	return &Breaker{threshold: threshold, openFor: openFor, maxProbes: maxProbes, now: now, onChange: onChange}
}

// Allow reports whether an attempt may proceed. Every true result must be
// followed by exactly one Done call.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	from := b.state
	allowed := false
	switch b.state {
	case Closed:
		allowed = true
	case Open:
		if b.now().Sub(b.openedAt) >= b.openFor {
			b.state = HalfOpen
			b.probes = 1
			allowed = true
		}
	case HalfOpen:
		if b.probes < b.maxProbes {
			b.probes++
			allowed = true
		}
	}
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
	return allowed
}

// Done records the result of an allowed attempt.
func (b *Breaker) Done(r Result) {
	b.mu.Lock()
	from := b.state
	switch b.state {
	case Closed:
		switch r {
		case Success:
			b.failures = 0
		case Failure:
			b.failures++
			if b.failures >= b.threshold {
				b.trip()
			}
		}
	case HalfOpen:
		b.probes--
		switch r {
		case Success:
			b.state = Closed
			b.failures = 0
			b.probes = 0
		case Failure:
			b.trip()
		}
	case Open:
		// A straggler from before the breaker opened; nothing to do.
	}
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
}

// Fail records a failure observed after the attempt was already resolved
// with Done — for example a stream that broke after it had started and been
// counted as a success. It counts toward tripping only while Closed.
func (b *Breaker) Fail() {
	b.mu.Lock()
	from := b.state
	if b.state == Closed {
		b.failures++
		if b.failures >= b.threshold {
			b.trip()
		}
	}
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
}

// State returns the current state without side effects. An open breaker
// whose cool-off has elapsed still reports Open until the next Allow.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) trip() {
	b.state = Open
	b.openedAt = b.now()
	b.failures = 0
	b.probes = 0
}

func (b *Breaker) notify(from, to State) {
	if from != to && b.onChange != nil {
		b.onChange(from, to)
	}
}
