package balancer

import (
	"math"
	"sync"
	"time"
)

type bucket struct {
	tokens float64
	seen   time.Time
}

type limiter struct {
	mu         sync.Mutex
	clients    map[string]bucket
	rate       float64
	burst      float64
	maxClients int
	idleTTL    time.Duration
	nextSweep  time.Time
}

func newLimiter(rate float64, burst int) *limiter {
	ttl := 10 * time.Minute
	if rate > 0 {
		ttl = max(ttl, time.Duration(float64(burst)/rate*float64(time.Second)))
	}
	return &limiter{clients: make(map[string]bucket), rate: rate, burst: float64(burst), maxClients: 10000, idleTTL: ttl}
}

func (l *limiter) allow(client string, now time.Time) (bool, int) {
	if l.rate == 0 {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !now.Before(l.nextSweep) {
		for key, b := range l.clients {
			if now.Sub(b.seen) >= l.idleTTL {
				delete(l.clients, key)
			}
		}
		l.nextSweep = now.Add(time.Minute)
	}
	b, found := l.clients[client]
	if !found {
		if len(l.clients) >= l.maxClients {
			return false, 60
		}
		b = bucket{tokens: l.burst, seen: now}
	}
	b.tokens = min(l.burst, b.tokens+max(0, now.Sub(b.seen).Seconds())*l.rate)
	b.seen = now
	if b.tokens < 1 {
		l.clients[client] = b
		return false, max(1, int(math.Ceil((1-b.tokens)/l.rate)))
	}
	b.tokens--
	l.clients[client] = b
	return true, 0
}
