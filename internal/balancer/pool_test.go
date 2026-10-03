package balancer

import (
	"net/url"
	"sync"
	"testing"
	"time"
)

func testPool(algorithm string, count int) *pool {
	p := &pool{algorithm: algorithm, failureThreshold: 3, cooldown: time.Second}
	for range count {
		p.backends = append(p.backends, &backend{url: &url.URL{Scheme: "http", Host: "example.com"}})
	}
	return p
}

func TestLeastConnections(t *testing.T) {
	p := testPool("least-connections", 3)
	now := time.Now()
	slow := p.pick(now)
	for range 20 {
		r := p.pick(now)
		if r.backend == slow.backend {
			t.Fatal("sent a request to the busy backend")
		}
		p.finish(r, healthy, now)
	}
	p.finish(slow, healthy, now)
	counts := make(map[*backend]int)
	for range 30 {
		r := p.pick(now)
		counts[r.backend]++
		p.finish(r, healthy, now)
	}
	for _, b := range p.backends {
		if counts[b] != 10 || b.active != 0 {
			t.Fatalf("uneven tie-breaking or leaked active count: %+v", counts)
		}
	}
}

func TestCircuitRecoveryAndSingleProbe(t *testing.T) {
	p := testPool("round-robin", 1)
	now := time.Now()
	for range 3 {
		p.finish(p.pick(now), failed, now)
	}
	if p.pick(now.Add(time.Millisecond)) != nil {
		t.Fatal("open circuit accepted a request")
	}
	probeTime := now.Add(time.Second)
	var wg sync.WaitGroup
	probes := make(chan *reservation, 20)
	for range 20 {
		wg.Go(func() {
			if r := p.pick(probeTime); r != nil {
				probes <- r
			}
		})
	}
	wg.Wait()
	close(probes)
	if len(probes) != 1 {
		t.Fatalf("got %d simultaneous recovery probes", len(probes))
	}
	p.finish(<-probes, failed, probeTime)
	if p.pick(probeTime) != nil {
		t.Fatal("failed probe did not reopen circuit")
	}
	r := p.pick(probeTime.Add(time.Second))
	p.finish(r, healthy, probeTime.Add(time.Second))
	if !p.backends[0].openUntil.IsZero() || p.backends[0].failures != 0 || p.backends[0].active != 0 {
		t.Fatal("successful probe did not restore backend")
	}
}

func TestOldResponsesCannotCloseNewCircuit(t *testing.T) {
	p := testPool("round-robin", 1)
	now := time.Now()
	old := p.pick(now)
	for range 3 {
		p.finish(p.pick(now), failed, now)
	}
	p.finish(old, healthy, now)
	if p.pick(now) != nil || p.backends[0].active != 0 {
		t.Fatal("stale success closed the circuit or leaked a request")
	}
}

func TestCanceledRequestsDoNotCountAsFailures(t *testing.T) {
	p := testPool("round-robin", 1)
	now := time.Now()
	for range 10 {
		p.finish(p.pick(now), canceled, now)
	}
	if p.backends[0].failures != 0 || p.backends[0].active != 0 {
		t.Fatal("cancellation changed backend health")
	}
	for range 3 {
		p.finish(p.pick(now), failed, now)
	}
	now = now.Add(time.Second)
	p.finish(p.pick(now), canceled, now)
	if p.pick(now) == nil {
		t.Fatal("canceled probe left backend stuck")
	}
}
