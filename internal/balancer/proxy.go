package balancer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"time"
)

const maxRequestBytes = 1 << 20

type Balancer struct {
	pool      *pool
	transport *http.Transport
	timeout   time.Duration
	limiter   *limiter
	slots     chan struct{}
	metrics   *metrics
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
	if config.Burst == 0 {
		config.Burst = 40
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
	b := &Balancer{
		pool: p, transport: transport, timeout: config.Timeout,
		limiter: newLimiter(config.Rate, config.Burst), slots: make(chan struct{}, 256),
	}
	b.metrics = newMetrics(p, b.slots)
	return b, nil
}

func (b *Balancer) Close() {
	b.transport.CloseIdleConnections()
}

func (b *Balancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	backendName := "none"
	response := &responseWriter{ResponseWriter: w}
	w = response
	defer func() {
		status := response.status
		if status == 0 {
			status = http.StatusOK
			if r.Context().Err() != nil {
				status = 499
			}
		}
		b.metrics.observe(backendName, status, time.Since(started))
	}()
	if r.Method == http.MethodConnect || r.Header.Get("Upgrade") != "" {
		writeError(w, http.StatusBadRequest, "only ordinary HTTP requests are supported")
		return
	}
	client, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		client = r.RemoteAddr
	}
	if allowed, retry := b.limiter.allow(client, time.Now()); !allowed {
		w.Header().Set("Retry-After", strconv.Itoa(retry))
		writeError(w, http.StatusTooManyRequests, "client request limit reached")
		return
	}
	select {
	case b.slots <- struct{}{}:
		defer func() { <-b.slots }()
	default:
		writeError(w, http.StatusServiceUnavailable, "proxy is at capacity")
		return
	}
	clientContext := r.Context()
	ctx, cancel := context.WithTimeout(clientContext, b.timeout)
	defer cancel()
	reader := http.NewResponseController(w)
	if r.Body != http.NoBody {
		reader.SetReadDeadline(time.Now().Add(b.timeout))
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	r.Body.Close()
	reader.SetReadDeadline(time.Time{})
	if err != nil {
		var tooLarge *http.MaxBytesError
		var networkError net.Error
		switch {
		case errors.As(err, &tooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds 1 MiB")
		case errors.As(err, &networkError) && networkError.Timeout():
			writeError(w, http.StatusRequestTimeout, "request body timed out")
		default:
			writeError(w, http.StatusBadRequest, "could not read request body")
		}
		return
	}
	r = r.WithContext(ctx)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.TransferEncoding = nil
	if len(body) == 0 {
		r.Body = http.NoBody
	}
	reservation := b.pool.pick(time.Now())
	if reservation == nil {
		writeError(w, http.StatusServiceUnavailable, "all backend circuits are open")
		return
	}
	backendName = reservation.backend.url.String()
	result := healthy
	defer func() {
		panicValue := recover()
		if clientContext.Err() != nil {
			result = canceled
		} else if panicValue != nil || ctx.Err() != nil {
			result = failed
		}
		b.pool.finish(reservation, result, time.Now())
		if result == failed {
			b.metrics.failures.WithLabelValues(backendName).Inc()
		}
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
			if clientContext.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
				writeError(w, http.StatusGatewayTimeout, "backend timed out")
				return
			}
			writeError(w, http.StatusBadGateway, "backend unavailable")
		},
	}
	proxy.ServeHTTP(w, r)
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
