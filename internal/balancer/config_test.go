package balancer

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestConfigurationBoundaries(t *testing.T) {
	base := Config{
		Backends: []string{"http://example.com"}, Timeout: time.Second,
		Algorithm: "round-robin", FailureThreshold: 3, Cooldown: time.Second, Rate: 20, Burst: 40,
	}
	for _, tc := range []struct {
		name  string
		edit  func(*Config)
		valid bool
	}{
		{"no backends", func(c *Config) { c.Backends = nil }, false},
		{"too many backends", func(c *Config) { c.Backends = make([]string, 101) }, false},
		{"zero timeout", func(c *Config) { c.Timeout = 0 }, false},
		{"small timeout", func(c *Config) { c.Timeout = time.Millisecond - 1 }, false},
		{"minimum timeout", func(c *Config) { c.Timeout = time.Millisecond }, true},
		{"maximum timeout", func(c *Config) { c.Timeout = time.Minute }, true},
		{"large timeout", func(c *Config) { c.Timeout = time.Minute + 1 }, false},
		{"unknown algorithm", func(c *Config) { c.Algorithm = "random" }, false},
		{"least connections", func(c *Config) { c.Algorithm = "least-connections" }, true},
		{"negative threshold", func(c *Config) { c.FailureThreshold = -1 }, false},
		{"minimum threshold", func(c *Config) { c.FailureThreshold = 1 }, true},
		{"maximum threshold", func(c *Config) { c.FailureThreshold = 100 }, true},
		{"large threshold", func(c *Config) { c.FailureThreshold = 101 }, false},
		{"small cooldown", func(c *Config) { c.Cooldown = time.Millisecond - 1 }, false},
		{"minimum cooldown", func(c *Config) { c.Cooldown = time.Millisecond }, true},
		{"maximum cooldown", func(c *Config) { c.Cooldown = time.Hour }, true},
		{"large cooldown", func(c *Config) { c.Cooldown = time.Hour + 1 }, false},
		{"disabled limiter", func(c *Config) { c.Rate = 0 }, true},
		{"minimum rate", func(c *Config) { c.Rate = 0.1 }, true},
		{"maximum rate", func(c *Config) { c.Rate = 100000 }, true},
		{"small rate", func(c *Config) { c.Rate = 0.01 }, false},
		{"large rate", func(c *Config) { c.Rate = 100001 }, false},
		{"negative rate", func(c *Config) { c.Rate = -1 }, false},
		{"NaN rate", func(c *Config) { c.Rate = math.NaN() }, false},
		{"infinite rate", func(c *Config) { c.Rate = math.Inf(1) }, false},
		{"negative infinite rate", func(c *Config) { c.Rate = math.Inf(-1) }, false},
		{"negative burst", func(c *Config) { c.Burst = -1 }, false},
		{"minimum burst", func(c *Config) { c.Burst = 1 }, true},
		{"maximum burst", func(c *Config) { c.Burst = 10000 }, true},
		{"large burst", func(c *Config) { c.Burst = 10001 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := base
			tc.edit(&config)
			b, err := New(config)
			if b != nil {
				defer b.Close()
			}
			if (err == nil) != tc.valid {
				t.Fatalf("valid = %t, error = %v", tc.valid, err)
			}
		})
	}
}

func TestBackendOriginsAndMaximumPool(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{" https://EXAMPLE.com/ ", "https://example.com"},
		{"http://[::1]:9001/", "http://[::1]:9001"},
		{"http://localhost:65535", "http://localhost:65535"},
	} {
		b, err := New(Config{Backends: []string{tc.input}, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		b.Close()
		if got := b.pool.backends[0].url.String(); got != tc.want {
			t.Fatalf("normalized %q to %q, want %q", tc.input, got, tc.want)
		}
	}
	for _, address := range []string{"http://example.com?", "http://[::1", "http://example.com:abc", "http://example.com/%zz"} {
		if b, err := New(Config{Backends: []string{address}, Timeout: time.Second}); err == nil {
			b.Close()
			t.Errorf("accepted %q", address)
		}
	}
	addresses := make([]string, 100)
	for i := range addresses {
		addresses[i] = fmt.Sprintf("http://backend-%d:9000", i)
	}
	b, err := New(Config{Backends: addresses, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	now := time.Now()
	for _, address := range addresses {
		r := b.pool.pick(now)
		if r.backend.url.String() != address {
			t.Fatalf("selected %s, want %s", r.backend.url, address)
		}
		b.pool.finish(r, healthy, now)
	}
}
