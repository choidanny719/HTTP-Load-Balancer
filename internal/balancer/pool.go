package balancer

import (
	"net/url"
	"sync"
	"time"
)

type backend struct {
	url        *url.URL
	active     int
	failures   int
	openUntil  time.Time
	probing    bool
	generation uint64
}

type reservation struct {
	backend    *backend
	generation uint64
}

type outcome int

const (
	healthy outcome = iota
	failed
	canceled
)

type pool struct {
	mu               sync.Mutex
	backends         []*backend
	next             int
	algorithm        string
	failureThreshold int
	cooldown         time.Duration
}

func (p *pool) pick(now time.Time) *reservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	chosen := -1
	for offset := range len(p.backends) {
		i := (p.next + offset) % len(p.backends)
		b := p.backends[i]
		if b.probing || now.Before(b.openUntil) {
			continue
		}
		if chosen == -1 || b.active < p.backends[chosen].active {
			chosen = i
		}
		if p.algorithm == "round-robin" {
			break
		}
	}
	if chosen == -1 {
		return nil
	}
	b := p.backends[chosen]
	b.active++
	if !b.openUntil.IsZero() {
		b.probing = true
	}
	p.next = (chosen + 1) % len(p.backends)
	return &reservation{backend: b, generation: b.generation}
}

func (p *pool) finish(r *reservation, result outcome, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b := r.backend
	b.active--
	if r.generation != b.generation {
		return
	}
	if result == canceled {
		b.probing = false
		return
	}
	if result == healthy {
		b.failures = 0
		if b.probing {
			b.openUntil = time.Time{}
			b.probing = false
			b.generation++
		}
		return
	}
	b.failures++
	if b.probing || b.failures >= p.failureThreshold {
		b.openUntil = now.Add(p.cooldown)
		b.probing = false
		b.generation++
	}
}
