package balancer

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	failures *prometheus.CounterVec
}

func newMetrics(p *pool, slots chan struct{}) *metrics {
	m := &metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lb_requests_total", Help: "Proxy requests by backend and response status.",
		}, []string{"backend", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "lb_request_duration_seconds", Help: "Time spent handling proxy requests.",
			Buckets: prometheus.DefBuckets,
		}, []string{"backend"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lb_backend_failures_total", Help: "Backend responses of 5xx and transport failures.",
		}, []string{"backend"}),
	}
	m.registry.MustRegister(m.requests, m.duration, m.failures,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "lb_active_requests", Help: "Admitted requests currently being handled.",
		}, func() float64 { return float64(len(slots)) }))
	for _, b := range p.backends {
		labels := prometheus.Labels{"backend": b.url.String()}
		m.failures.WithLabelValues(b.url.String())
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "lb_backend_active_requests", Help: "Requests assigned to a backend.", ConstLabels: labels,
		}, func() float64 {
			p.mu.Lock()
			defer p.mu.Unlock()
			return float64(b.active)
		}))
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "lb_backend_circuit_state", Help: "Circuit state: 0 closed, 1 open, 2 half-open.", ConstLabels: labels,
		}, func() float64 {
			p.mu.Lock()
			defer p.mu.Unlock()
			if b.probing {
				return 2
			}
			if !b.openUntil.IsZero() {
				return 1
			}
			return 0
		}))
	}
	return m
}

func (m *metrics) observe(backend string, status int, elapsed time.Duration) {
	m.requests.WithLabelValues(backend, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(backend).Observe(elapsed.Seconds())
}

func (b *Balancer) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"status\":\"ok\"}\n"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(b.metrics.registry, promhttp.HandlerOpts{}))
	return mux
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	if status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
