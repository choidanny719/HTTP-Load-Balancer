package balancer

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testProxy(t *testing.T, config Config) (*Balancer, *httptest.Server) {
	t.Helper()
	b, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	server := httptest.NewServer(b)
	t.Cleanup(server.Close)
	return b, server
}

func TestForwardRequest(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || r.URL.RequestURI() != "/items/a%2Fb?q=two+words" || string(body) != "payload" {
			t.Errorf("unexpected request: %s %s %q", r.Method, r.URL.RequestURI(), body)
		}
		if r.Header.Get("X-Forwarded-For") != "127.0.0.1" || r.Header.Get("Forwarded") != "" {
			t.Error("untrusted forwarding headers were not replaced")
		}
		if r.Header.Get("X-Remove") != "" {
			t.Error("hop-by-hop header reached backend")
		}
		w.Header().Set("X-Backend", "one")
		w.WriteHeader(http.StatusCreated)
		w.Write(body)
	}))
	defer upstream.Close()
	_, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second})
	req, _ := http.NewRequest("POST", proxy.URL+"/items/a%2Fb?q=two+words", strings.NewReader("payload"))
	req.Header.Set("X-Forwarded-For", "spoofed")
	req.Header.Set("Forwarded", "for=spoofed")
	req.Header.Set("Connection", "X-Remove")
	req.Header.Set("X-Remove", "secret")
	res, err := proxy.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 201 || res.Header.Get("X-Backend") != "one" || string(body) != "payload" {
		t.Fatalf("response: %d %q %q", res.StatusCode, res.Header.Get("X-Backend"), body)
	}
}

func TestRoundRobinConcurrent(t *testing.T) {
	var counts [3]atomic.Int64
	var addresses []string
	for i := range counts {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			counts[i].Add(1)
			fmt.Fprint(w, i)
		}))
		t.Cleanup(server.Close)
		addresses = append(addresses, server.URL)
	}
	_, proxy := testProxy(t, Config{Backends: addresses, Timeout: 5 * time.Second})
	var wg sync.WaitGroup
	for range 60 {
		wg.Go(func() {
			res, err := proxy.Client().Get(proxy.URL + "/")
			if err != nil {
				t.Error(err)
				return
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != 200 {
				t.Errorf("status = %d", res.StatusCode)
			}
		})
	}
	wg.Wait()
	for i := range counts {
		if counts[i].Load() != 20 {
			t.Errorf("backend %d received %d requests", i, counts[i].Load())
		}
	}
}

func TestTimeoutAndConnectionFailure(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer slow.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	for _, tc := range []struct {
		name, target string
		status       int
	}{{"timeout", slow.URL, 504}, {"connection", closed.URL, 502}} {
		t.Run(tc.name, func(t *testing.T) {
			_, proxy := testProxy(t, Config{Backends: []string{tc.target}, Timeout: 50 * time.Millisecond})
			res, err := proxy.Client().Get(proxy.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			if res.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.status)
			}
		})
	}
}

func TestInvalidBackends(t *testing.T) {
	for _, address := range []string{"", "ftp://example.com", "http://", "http://user:pass@example.com", "http://example.com/path", "http://example.com?q=1", "http://example.com#fragment", "http://example.com:0", "http://example.com:70000", "http://example.com:"} {
		if _, err := New(Config{Backends: []string{address}, Timeout: time.Second}); err == nil {
			t.Errorf("accepted %q", address)
		}
	}
	if _, err := New(Config{Backends: []string{"http://example.com", "http://example.com/"}, Timeout: time.Second}); err == nil {
		t.Error("accepted duplicate origins")
	}
}

func TestFailedWritesAreNotRetriedAndCircuitSkipsBackend(t *testing.T) {
	var calls atomic.Int64
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer good.Close()
	_, proxy := testProxy(t, Config{Backends: []string{bad.URL, good.URL}, Timeout: time.Second, FailureThreshold: 1})
	for i, want := range []int{500, 200, 200, 200} {
		res, err := proxy.Client().Post(proxy.URL+"/orders", "text/plain", strings.NewReader("order"))
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("request %d: got %d, want %d", i, res.StatusCode, want)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("failed backend received %d requests", calls.Load())
	}
}

func TestAllCircuitsOpen(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer backend.Close()
	_, proxy := testProxy(t, Config{Backends: []string{backend.URL}, Timeout: time.Second, FailureThreshold: 1})
	for _, want := range []int{503, 503} {
		res, err := proxy.Client().Get(proxy.URL)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("status = %d", res.StatusCode)
		}
		if strings.Contains(string(body), "all backend circuits are open") {
			return
		}
	}
	t.Fatal("proxy did not reject after the circuit opened")
}
