package balancer

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Backends         []string
	Timeout          time.Duration
	Algorithm        string
	FailureThreshold int
	Cooldown         time.Duration
}

func (c Config) targets() ([]*url.URL, error) {
	if len(c.Backends) == 0 || len(c.Backends) > 100 {
		return nil, fmt.Errorf("provide between 1 and 100 backends")
	}
	if c.Timeout < time.Millisecond || c.Timeout > time.Minute {
		return nil, fmt.Errorf("timeout must be between 1ms and 1m")
	}
	if c.Algorithm != "round-robin" && c.Algorithm != "least-connections" {
		return nil, fmt.Errorf("algorithm must be round-robin or least-connections")
	}
	if c.FailureThreshold < 1 || c.FailureThreshold > 100 {
		return nil, fmt.Errorf("failure threshold must be between 1 and 100")
	}
	if c.Cooldown < time.Millisecond || c.Cooldown > time.Hour {
		return nil, fmt.Errorf("cooldown must be between 1ms and 1h")
	}
	seen := make(map[string]bool)
	targets := make([]*url.URL, 0, len(c.Backends))
	for _, address := range c.Backends {
		u, err := url.Parse(strings.TrimSpace(address))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" ||
			u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
			(u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("backend must be an HTTP or HTTPS origin: %q", address)
		}
		if port := u.Port(); port != "" {
			n, err := strconv.Atoi(port)
			if err != nil || n < 1 || n > 65535 {
				return nil, fmt.Errorf("invalid backend port: %q", address)
			}
		} else if strings.HasSuffix(u.Host, ":") {
			return nil, fmt.Errorf("invalid backend port: %q", address)
		}
		u.Path = ""
		u.Host = strings.ToLower(u.Host)
		if seen[u.String()] {
			return nil, fmt.Errorf("duplicate backend: %q", address)
		}
		seen[u.String()] = true
		targets = append(targets, u)
	}
	return targets, nil
}
