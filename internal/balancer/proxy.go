package balancer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"time"
)

type Balancer struct {
	pool      *pool
	transport *http.Transport
	timeout   time.Duration
}

func New(config Config) (*Balancer, error) {
	if config.Algorithm == "" {
		config.Algorithm = "round-robin"
	}
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 3
	}
	if config.Cooldown == 0 {
		config.Cooldown = 10 * time.Second
	}
	targets, err := config.targets()
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 20
	transport.ResponseHeaderTimeout = config.Timeout
	p := &pool{algorithm: config.Algorithm, failureThreshold: config.FailureThreshold, cooldown: config.Cooldown}
	for _, target := range targets {
		p.backends = append(p.backends, &backend{url: target})
	}
	return &Balancer{pool: p, transport: transport, timeout: config.Timeout}, nil
}

func (b *Balancer) Close() {
	b.transport.CloseIdleConnections()
}

func (b *Balancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect || r.Header.Get("Upgrade") != "" {
		writeError(w, http.StatusBadRequest, "only ordinary HTTP requests are supported")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), b.timeout)
	defer cancel()
	reservation := b.pool.pick(time.Now())
	if reservation == nil {
		writeError(w, http.StatusServiceUnavailable, "all backend circuits are open")
		return
	}
	result := healthy
	defer func() {
		panicValue := recover()
		if r.Context().Err() != nil {
			result = canceled
		} else if panicValue != nil || ctx.Err() != nil {
			result = failed
		}
		b.pool.finish(reservation, result, time.Now())
		if panicValue != nil {
			panic(panicValue)
		}
	}()
	proxy := httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(reservation.backend.url)
			req.SetXForwarded()
		},
		Transport: b.transport,
		ErrorLog:  log.New(io.Discard, "", 0),
		ModifyResponse: func(response *http.Response) error {
			if response.StatusCode >= 500 {
				result = failed
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			result = failed
			if r.Context().Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
				writeError(w, http.StatusGatewayTimeout, "backend timed out")
				return
			}
			writeError(w, http.StatusBadGateway, "backend unavailable")
		},
	}
	proxy.ServeHTTP(w, r.WithContext(ctx))
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
