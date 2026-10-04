package balancer

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPMethodsAndResponseHeaders(t *testing.T) {
	for _, method := range []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"} {
		t.Run(method, func(t *testing.T) {
			payload := ""
			if method == "POST" || method == "PUT" || method == "PATCH" {
				payload = "request body"
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil || string(body) != payload || r.Method != method {
					t.Errorf("request: %s %q, error %v", r.Method, body, err)
				}
				if r.Header.Get("Authorization") != "Bearer example" || r.Header.Get("Cookie") != "session=example" {
					t.Error("application headers were dropped")
				}
				w.Header().Add("Set-Cookie", "a=1")
				w.Header().Add("Set-Cookie", "b=2")
				w.Header().Set("Connection", "X-Internal")
				w.Header().Set("X-Internal", "remove")
				w.Header().Set("X-Result", "kept")
				io.WriteString(w, "response body")
			}))
			defer upstream.Close()
			_, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second})
			req, _ := http.NewRequest(method, proxy.URL, strings.NewReader(payload))
			req.Header.Set("Authorization", "Bearer example")
			req.Header.Set("Cookie", "session=example")
			res, err := proxy.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			want := "response body"
			if method == "HEAD" {
				want = ""
			}
			if err != nil || string(body) != want || res.StatusCode != 200 {
				t.Fatalf("response: %d %q, error %v", res.StatusCode, body, err)
			}
			if len(res.Header.Values("Set-Cookie")) != 2 || res.Header.Get("X-Internal") != "" || res.Header.Get("X-Result") != "kept" {
				t.Fatalf("response headers: %v", res.Header)
			}
		})
	}
}

func TestRedirectsAndClientErrorsDoNotOpenCircuit(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/destination", http.StatusFound)
		case "/missing":
			http.NotFound(w, r)
		case "/empty":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Error("proxy followed redirect")
		}
	}))
	defer upstream.Close()
	_, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second, FailureThreshold: 1})
	client := proxy.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/redirect", 302}, {"/missing", 404}, {"/empty", 204}, {"/missing", 404},
	} {
		res, err := client.Get(proxy.URL + tc.path)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != tc.status || (tc.status == 302 && res.Header.Get("Location") != "/destination") {
			t.Fatalf("response: %d %v", res.StatusCode, res.Header)
		}
	}
	if calls.Load() != 4 {
		t.Fatalf("backend received %d requests, want 4", calls.Load())
	}
}

func TestHTTPSBackendCertificateVerification(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "secure backend")
	}))
	defer upstream.Close()
	for _, trusted := range []bool{false, true} {
		t.Run(fmt.Sprintf("trusted=%t", trusted), func(t *testing.T) {
			b, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second})
			if trusted {
				roots := x509.NewCertPool()
				roots.AddCert(upstream.Certificate())
				b.transport.TLSClientConfig = &tls.Config{RootCAs: roots}
			}
			res, err := proxy.Client().Get(proxy.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			want := 502
			if trusted {
				want = 200
				if string(body) != "secure backend" {
					t.Fatalf("body = %q", body)
				}
			}
			if res.StatusCode != want {
				t.Fatalf("status = %d, want %d", res.StatusCode, want)
			}
		})
	}
}

func TestUnsupportedRequestsAreRejectedBeforeForwarding(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer upstream.Close()
	_, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second})
	for _, method := range []string{"CONNECT", "GET"} {
		req, _ := http.NewRequest(method, proxy.URL, nil)
		if method == "GET" {
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
		}
		res, err := proxy.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 400 {
			t.Fatalf("%s status = %d", method, res.StatusCode)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsupported request reached backend")
	}
}

func TestIncompleteUploadDoesNotReachBackend(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer upstream.Close()
	_, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second})
	conn, err := net.Dial("tcp", strings.TrimPrefix(proxy.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	fmt.Fprint(conn, "POST / HTTP/1.1\r\nHost: proxy\r\nContent-Length: 20\r\n\r\nx")
	conn.(*net.TCPConn).CloseWrite()
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 400 || calls.Load() != 0 {
		t.Fatalf("status = %d, backend calls = %d", res.StatusCode, calls.Load())
	}
}

func TestBrokenResponseStreamsOpenCircuitAndReleaseCapacity(t *testing.T) {
	for _, mode := range []string{"timeout", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Content-Length", "20")
				io.WriteString(w, "start")
				w.(http.Flusher).Flush()
				if mode == "timeout" {
					<-r.Context().Done()
				}
			}))
			defer upstream.Close()
			b, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: 100 * time.Millisecond, FailureThreshold: 1})
			res, err := proxy.Client().Get(proxy.URL)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err == nil || res.StatusCode != 200 || string(body) != "start" {
				t.Fatalf("broken stream: %d %q, error %v", res.StatusCode, body, err)
			}
			b.pool.mu.Lock()
			active := b.pool.backends[0].active
			b.pool.mu.Unlock()
			if active != 0 || len(b.slots) != 0 {
				t.Fatal("aborted response leaked a request slot")
			}
			res, err = proxy.Client().Get(proxy.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, _ = io.ReadAll(res.Body)
			if res.StatusCode != 503 || !strings.Contains(string(body), "all backend circuits are open") {
				t.Fatalf("failed stream did not open circuit: %d %q", res.StatusCode, body)
			}
		})
	}
}

func TestTimeoutReleasesCapacityForNextRequest(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			return
		}
		io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	b, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: 100 * time.Millisecond})
	b.slots = make(chan struct{}, 1)
	for _, tc := range []struct {
		path   string
		status int
	}{{"/slow", 504}, {"/", 200}} {
		res, err := proxy.Client().Get(proxy.URL + tc.path)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != tc.status {
			t.Fatalf("%s: status = %d, want %d", tc.path, res.StatusCode, tc.status)
		}
	}
}
