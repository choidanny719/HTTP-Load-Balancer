package balancer

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsAndAdminIsolation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer upstream.Close()
	b, _ := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second, FailureThreshold: 1, Rate: 0.1, Burst: 2})
	for _, want := range []int{500, 503, 429} {
		response := httptest.NewRecorder()
		b.ServeHTTP(response, httptest.NewRequest("GET", "http://proxy/", nil))
		if response.Code != want {
			t.Fatalf("status = %d, want %d", response.Code, want)
		}
	}
	admin := b.AdminHandler()
	response := httptest.NewRecorder()
	admin.ServeHTTP(response, httptest.NewRequest("GET", "http://admin/metrics", nil))
	for _, metric := range []string{
		`lb_requests_total{backend="` + upstream.URL + `",code="500"} 1`,
		`lb_requests_total{backend="none",code="503"} 1`,
		`lb_requests_total{backend="none",code="429"} 1`,
		`lb_request_duration_seconds_count{backend="none"} 2`,
		`lb_backend_failures_total{backend="` + upstream.URL + `"} 1`,
		`lb_backend_circuit_state{backend="` + upstream.URL + `"} 1`,
		`lb_backend_active_requests{backend="` + upstream.URL + `"} 0`,
		`lb_active_requests 0`,
	} {
		if !strings.Contains(response.Body.String(), metric) {
			t.Errorf("missing metric %s", metric)
		}
	}
	for range 5 {
		response := httptest.NewRecorder()
		admin.ServeHTTP(response, httptest.NewRequest("GET", "http://admin/healthz", nil))
		if response.Code != 200 {
			t.Fatal("health endpoint was rate limited or blocked by circuit")
		}
	}
	response = httptest.NewRecorder()
	admin.ServeHTTP(response, httptest.NewRequest("GET", "http://admin/orders", nil))
	if response.Code != 404 {
		t.Fatal("admin listener forwarded application traffic")
	}
}

func TestStreamedResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "first\n")
		w.(http.Flusher).Flush()
		io.WriteString(w, "second\n")
	}))
	defer upstream.Close()
	_, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second})
	response, err := proxy.Client().Get(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "first\nsecond\n" {
		t.Fatalf("stream = %q, error = %v", body, err)
	}
}
