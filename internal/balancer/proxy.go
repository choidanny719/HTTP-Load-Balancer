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
	pool      pool
	transport *http.Transport
	timeout   time.Duration
}

func New(config Config) (*Balancer, error) {
	targets, err := config.targets()
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 20
	transport.ResponseHeaderTimeout = config.Timeout
	return &Balancer{pool: pool{targets: targets}, transport: transport, timeout: config.Timeout}, nil
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
	target := b.pool.pick()
	proxy := httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(target)
			req.SetXForwarded()
		},
		Transport: b.transport,
		ErrorLog:  log.New(io.Discard, "", 0),
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
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
