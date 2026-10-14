package balancer

import (
	"testing"
	"time"
)

func TestStaleCompletionsCannotReleaseRecoveryProbe(t *testing.T) {
	for _, algorithm := range []string{"round-robin", "least-connections"} {
		for _, tc := range []struct {
			name   string
			result outcome
		}{{"success", healthy}, {"failure", failed}, {"cancellation", canceled}} {
			t.Run(algorithm+"/"+tc.name, func(t *testing.T) {
				p := testPool(algorithm, 1)
				p.failureThreshold = 1
				now := time.Unix(1000, 0)
				old := p.pick(now)
				p.finish(p.pick(now), failed, now)
				now = now.Add(p.cooldown)
				probe := p.pick(now)
				if probe == nil {
					t.Fatal("recovery probe was rejected")
				}
				p.finish(old, tc.result, now)
				if !probe.backend.probing || probe.backend.active != 1 || probe.backend.failures != 1 {
					t.Fatalf("stale completion changed the recovery probe: %+v", probe.backend)
				}
				if p.pick(now) != nil {
					t.Fatal("stale completion allowed a second recovery probe")
				}
				p.finish(probe, healthy, now)
				request := p.pick(now)
				if request == nil {
					t.Fatal("successful probe did not restore routing")
				}
				p.finish(request, healthy, now)
				if request.backend.active != 0 {
					t.Fatal("recovery leaked an active request")
				}
			})
		}
	}
}

func TestCancellationPreservesFailureStreak(t *testing.T) {
	p := testPool("round-robin", 1)
	p.failureThreshold = 2
	now := time.Unix(1000, 0)
	for _, result := range []outcome{failed, canceled, failed} {
		request := p.pick(now)
		if request == nil {
			t.Fatal("circuit opened before the second backend failure")
		}
		p.finish(request, result, now)
	}
	if p.pick(now) != nil || p.backends[0].active != 0 {
		t.Fatal("cancellation reset the failure streak or leaked a request")
	}
}

func TestFailedProbeStartsFullCooldown(t *testing.T) {
	for _, algorithm := range []string{"round-robin", "least-connections"} {
		t.Run(algorithm, func(t *testing.T) {
			p := testPool(algorithm, 1)
			p.failureThreshold = 1
			now := time.Unix(1000, 0)
			p.finish(p.pick(now), failed, now)
			if p.pick(now.Add(p.cooldown-time.Nanosecond)) != nil {
				t.Fatal("probe started before cooldown ended")
			}
			probe := p.pick(now.Add(p.cooldown))
			if probe == nil {
				t.Fatal("probe rejected at the cooldown boundary")
			}
			finished := now.Add(2 * p.cooldown)
			p.finish(probe, failed, finished)
			if p.pick(finished.Add(p.cooldown-time.Nanosecond)) != nil {
				t.Fatal("failed probe did not restart cooldown from completion")
			}
			probe = p.pick(finished.Add(p.cooldown))
			if probe == nil {
				t.Fatal("backend remained unavailable after the new cooldown")
			}
			p.finish(probe, healthy, finished.Add(p.cooldown))
		})
	}
}

func TestBackendCooldownsAreIndependent(t *testing.T) {
	for _, algorithm := range []string{"round-robin", "least-connections"} {
		t.Run(algorithm, func(t *testing.T) {
			p := testPool(algorithm, 2)
			p.failureThreshold = 1
			now := time.Unix(1000, 0)
			first := p.pick(now)
			second := p.pick(now)
			p.finish(first, failed, now)
			p.finish(second, failed, now.Add(p.cooldown/2))
			probe := p.pick(now.Add(p.cooldown))
			if probe == nil || probe.backend != first.backend {
				t.Fatal("did not probe the first backend after its cooldown")
			}
			if p.pick(now.Add(p.cooldown)) != nil {
				t.Fatal("second backend recovered before its own cooldown")
			}
			other := p.pick(now.Add(2 * p.cooldown))
			if other == nil || other.backend != second.backend {
				t.Fatal("one backend's probe blocked another backend's recovery")
			}
			p.finish(probe, healthy, now.Add(2*p.cooldown))
			p.finish(other, healthy, now.Add(2*p.cooldown))
			for _, backend := range p.backends {
				if backend.active != 0 || backend.probing || !backend.openUntil.IsZero() {
					t.Fatalf("backend did not recover: %+v", backend)
				}
			}
		})
	}
}
