package balancer

import (
	"net/url"
	"sync"
)

type pool struct {
	mu      sync.Mutex
	targets []*url.URL
	next    int
}

func (p *pool) pick() *url.URL {
	p.mu.Lock()
	defer p.mu.Unlock()
	target := p.targets[p.next]
	p.next = (p.next + 1) % len(p.targets)
	return target
}
