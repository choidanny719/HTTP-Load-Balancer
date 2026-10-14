package balancer

import (
	"testing"
	"time"
)

func TestRetryAfterRoundsUpUntilTokenIsAvailable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		elapsed time.Duration
		allowed bool
		retry   int
	}{
		{"empty bucket", 0, false, 3},
		{"fractional wait", time.Second, false, 2},
		{"above one second", 1499 * time.Millisecond, false, 2},
		{"below one second", 1501 * time.Millisecond, false, 1},
		{"just before refill", 2499 * time.Millisecond, false, 1},
		{"exact refill", 2500 * time.Millisecond, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newLimiter(0.4, 1)
			now := time.Unix(1000, 0)
			if ok, _ := l.allow("client", now); !ok {
				t.Fatal("initial request rejected")
			}
			allowed, retry := l.allow("client", now.Add(tc.elapsed))
			if allowed != tc.allowed || retry != tc.retry {
				t.Fatalf("allowed = %t, retry = %d; want %t, %d", allowed, retry, tc.allowed, tc.retry)
			}
		})
	}
}

func TestClockMovingBackwardDoesNotGrantTokens(t *testing.T) {
	l := newLimiter(0.5, 1)
	now := time.Unix(1000, 0)
	l.allow("client", now)
	if allowed, retry := l.allow("client", now.Add(-time.Second)); allowed || retry != 2 {
		t.Fatalf("backward clock: allowed = %t, retry = %d", allowed, retry)
	}
}

func TestIdleExpiryKeepsRecentlyUsedClients(t *testing.T) {
	l := newLimiter(1, 1)
	l.maxClients = 2
	now := time.Unix(1000, 0)
	l.allow("idle", now)
	l.allow("active", now)
	if allowed, retry := l.allow("new", now); allowed || retry != 60 {
		t.Fatalf("full table: allowed = %t, retry = %d", allowed, retry)
	}
	if allowed, _ := l.allow("active", now.Add(9*time.Minute)); !allowed {
		t.Fatal("active client could not use its refilled token")
	}
	if allowed, _ := l.allow("new", now.Add(10*time.Minute)); !allowed {
		t.Fatal("expired client did not free capacity")
	}
	if _, found := l.clients["idle"]; found {
		t.Fatal("idle client remained at the expiry boundary")
	}
	if _, found := l.clients["active"]; !found {
		t.Fatal("recently used client was evicted")
	}
	if len(l.clients) != 2 {
		t.Fatalf("tracked %d clients, want 2", len(l.clients))
	}
}
