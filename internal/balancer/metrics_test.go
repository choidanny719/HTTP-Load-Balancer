package balancer

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestResponseWriterRecordsFinalStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"implicit OK", 0},
		{"explicit status", http.StatusAccepted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writer := &responseWriter{ResponseWriter: recorder}
			want := http.StatusOK
			if tc.status != 0 {
				writer.WriteHeader(tc.status)
				want = tc.status
			}
			n, err := writer.Write([]byte("accepted"))
			writer.WriteHeader(http.StatusInternalServerError)
			if err != nil || n != len("accepted") || recorder.Body.String() != "accepted" {
				t.Fatalf("body = %q, bytes = %d, error = %v", recorder.Body.String(), n, err)
			}
			if writer.status != want || recorder.Code != want {
				t.Fatalf("recorded status = %d, sent status = %d, want %d", writer.status, recorder.Code, want)
			}
		})
	}
}

func TestEarlyHintsPreserveFinalResponseAndMetrics(t *testing.T) {
	const link = "</style.css>; rel=preload; as=style"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", link)
		w.WriteHeader(http.StatusEarlyHints)
		w.Header().Del("Link")
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, "created")
	}))
	defer upstream.Close()
	b, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second})
	hints := make(chan textproto.MIMEHeader, 1)
	req, err := http.NewRequest("GET", proxy.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
			if code != http.StatusEarlyHints {
				t.Errorf("informational status = %d, want 103", code)
			}
			select {
			case hints <- textproto.MIMEHeader(http.Header(header).Clone()):
			default:
				t.Error("received more than one informational response")
			}
			return nil
		},
	}))
	response, err := proxy.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusCreated || string(body) != "created" {
		t.Fatalf("final status = %d, body = %q, error = %v", response.StatusCode, body, err)
	}
	select {
	case header := <-hints:
		if header.Get("Link") != link {
			t.Fatalf("early hints Link = %q, want %q", header.Get("Link"), link)
		}
	default:
		t.Fatal("early hints were not forwarded")
	}
	if response.Header.Get("Link") != "" {
		t.Fatal("informational header leaked into final response")
	}
	metric := `lb_requests_total{backend="` + upstream.URL + `",code="201"} 1`
	deadline := time.Now().Add(time.Second)
	for {
		scrape := httptest.NewRecorder()
		b.AdminHandler().ServeHTTP(scrape, httptest.NewRequest("GET", "http://admin/metrics", nil))
		if strings.Contains(scrape.Body.String(), `code="103"`) {
			t.Fatal("informational response counted as a completed request")
		}
		if strings.Contains(scrape.Body.String(), metric) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing metric %s", metric)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCircuitMetricsFollowRecovery(t *testing.T) {
	const target = "http://backend.example"
	b, err := New(Config{Backends: []string{target}, Timeout: time.Second, FailureThreshold: 1, Cooldown: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	assertState := func(state string, active string) {
		t.Helper()
		response := httptest.NewRecorder()
		b.AdminHandler().ServeHTTP(response, httptest.NewRequest("GET", "http://admin/metrics", nil))
		for _, metric := range []string{
			`lb_backend_circuit_state{backend="` + target + `"} ` + state,
			`lb_backend_active_requests{backend="` + target + `"} ` + active,
		} {
			if !strings.Contains(response.Body.String(), metric) {
				t.Errorf("missing metric %s", metric)
			}
		}
	}
	now := time.Now()
	assertState("0", "0")
	request := b.pool.pick(now)
	if request == nil {
		t.Fatal("healthy backend was not available")
	}
	b.pool.finish(request, failed, now)
	assertState("1", "0")
	probe := b.pool.pick(now.Add(time.Second))
	if probe == nil {
		t.Fatal("backend was not available for a recovery probe")
	}
	assertState("2", "1")
	b.pool.finish(probe, healthy, now.Add(time.Second))
	assertState("0", "0")
}

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

func TestMetricsWhileRequestsAreActive(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-release:
			w.WriteHeader(http.StatusCreated)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(upstream.Close)
	b, _ := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: 5 * time.Second})
	var workers sync.WaitGroup
	t.Cleanup(workers.Wait)
	t.Cleanup(finish)
	for range 2 {
		workers.Go(func() {
			response := httptest.NewRecorder()
			b.ServeHTTP(response, httptest.NewRequest("GET", "http://proxy/", nil))
			if response.Code != 201 {
				t.Errorf("status = %d, want 201", response.Code)
			}
		})
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("requests did not reach backend")
		}
	}
	admin := b.AdminHandler()
	for range 10 {
		response := httptest.NewRecorder()
		admin.ServeHTTP(response, httptest.NewRequest("GET", "http://admin/metrics", nil))
		for _, metric := range []string{
			"lb_active_requests 2",
			`lb_backend_active_requests{backend="` + upstream.URL + `"} 2`,
			`lb_backend_circuit_state{backend="` + upstream.URL + `"} 0`,
		} {
			if !strings.Contains(response.Body.String(), metric) {
				t.Errorf("missing metric %s", metric)
			}
		}
	}
	finish()
	workers.Wait()
	response := httptest.NewRecorder()
	admin.ServeHTTP(response, httptest.NewRequest("GET", "http://admin/metrics", nil))
	for _, metric := range []string{
		"lb_active_requests 0",
		`lb_backend_active_requests{backend="` + upstream.URL + `"} 0`,
		`lb_requests_total{backend="` + upstream.URL + `",code="201"} 2`,
		`lb_request_duration_seconds_count{backend="` + upstream.URL + `"} 2`,
		`lb_backend_failures_total{backend="` + upstream.URL + `"} 0`,
	} {
		if !strings.Contains(response.Body.String(), metric) {
			t.Errorf("missing metric %s", metric)
		}
	}
}
