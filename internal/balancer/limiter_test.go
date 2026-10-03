package balancer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucketRefillAndClientIsolation(t *testing.T) {
	l := newLimiter(2, 2)
	now := time.Now()
	for range 2 {
		if ok, _ := l.allow("a", now); !ok {
			t.Fatal("initial burst rejected")
		}
	}
	if ok, retry := l.allow("a", now); ok || retry != 1 {
		t.Fatal("exhausted client was allowed or retry was missing")
	}
	if ok, _ := l.allow("b", now); !ok {
		t.Fatal("one client consumed another client's tokens")
	}
	if ok, _ := l.allow("a", now.Add(250*time.Millisecond)); ok {
		t.Fatal("partial token was accepted")
	}
	if ok, _ := l.allow("a", now.Add(500*time.Millisecond)); !ok {
		t.Fatal("refilled token rejected")
	}
	now = now.Add(time.Minute)
	for range 2 {
		l.allow("a", now)
	}
	if ok, _ := l.allow("a", now); ok {
		t.Fatal("idle time increased the burst capacity")
	}
}

func TestLimiterConcurrencyAndExpiry(t *testing.T) {
	l := newLimiter(1, 5)
	l.maxClients = 1
	now := time.Now()
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if ok, _ := l.allow("a", now); ok {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 5 {
		t.Fatalf("accepted %d requests from a burst of 5", accepted.Load())
	}
	if ok, _ := l.allow("b", now); ok {
		t.Fatal("client table exceeded capacity")
	}
	if ok, _ := l.allow("b", now.Add(10*time.Minute)); !ok || len(l.clients) != 1 {
		t.Fatal("expired client was not removed")
	}
}
