package balancer

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"
)

func TestContinueUploadWaitsForBodyAndPreservesConnection(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading forwarded body: %v", err)
		}
		if r.Method == http.MethodPost {
			if string(body) != "payload" || r.ContentLength != 7 {
				t.Errorf("forwarded body = %q, length = %d", body, r.ContentLength)
			}
			w.Write(body)
			return
		}
		io.WriteString(w, "ready")
	}))
	defer upstream.Close()
	b, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: 2 * time.Second})
	conn, err := net.Dial("tcp", strings.TrimPrefix(proxy.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	if _, err := fmt.Fprint(conn, "POST / HTTP/1.1\r\nHost: proxy\r\nContent-Length: 7\r\nExpect: 100-continue\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusContinue {
		t.Fatalf("interim status = %d, want 100", response.StatusCode)
	}
	b.pool.mu.Lock()
	active := b.pool.backends[0].active
	b.pool.mu.Unlock()
	if calls.Load() != 0 || active != 0 || len(b.slots) != 1 {
		t.Fatalf("pending upload: backend calls = %d, active = %d, slots = %d", calls.Load(), active, len(b.slots))
	}
	for _, tc := range []struct {
		request string
		body    string
	}{
		{"payload", "payload"},
		{"GET / HTTP/1.1\r\nHost: proxy\r\nConnection: close\r\n\r\n", "ready"},
	} {
		if _, err := io.WriteString(conn, tc.request); err != nil {
			t.Fatal(err)
		}
		response, err = http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		for response.StatusCode == http.StatusContinue {
			response.Body.Close()
			response, err = http.ReadResponse(reader, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || string(body) != tc.body {
			t.Fatalf("response = %d %q, error = %v", response.StatusCode, body, err)
		}
	}
	if calls.Load() != 2 || len(b.slots) != 0 {
		t.Fatalf("backend calls = %d, occupied slots = %d", calls.Load(), len(b.slots))
	}
	scrape := httptest.NewRecorder()
	b.AdminHandler().ServeHTTP(scrape, httptest.NewRequest("GET", "http://admin/metrics", nil))
	want := `lb_requests_total{backend="` + upstream.URL + `",code="200"} 2`
	if !strings.Contains(scrape.Body.String(), want) || strings.Contains(scrape.Body.String(), `code="100"`) {
		t.Fatalf("unexpected response metrics:\n%s", scrape.Body.String())
	}
}

func TestMalformedChunkedUploadsDoNotAffectBackendHealth(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"invalid size", "Z\r\n"},
		{"missing chunk terminator", "1\r\nx!!"},
		{"truncated chunk", "5\r\nabc"},
		{"invalid trailer", "1\r\nx\r\n0\r\nnot-a-header\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer upstream.Close()
			b, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: 2 * time.Second, FailureThreshold: 1})
			conn, err := net.Dial("tcp", strings.TrimPrefix(proxy.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := fmt.Fprintf(conn, "POST / HTTP/1.1\r\nHost: proxy\r\nTransfer-Encoding: chunked\r\n\r\n%s", tc.body); err != nil {
				t.Fatal(err)
			}
			if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			response, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]string
			err = json.NewDecoder(response.Body).Decode(&payload)
			response.Body.Close()
			if err != nil || response.StatusCode != http.StatusBadRequest || payload["error"] != "could not read request body" {
				t.Fatalf("response = %d %v, error = %v", response.StatusCode, payload, err)
			}
			b.pool.mu.Lock()
			backend := b.pool.backends[0]
			active, failures, openUntil := backend.active, backend.failures, backend.openUntil
			b.pool.mu.Unlock()
			if calls.Load() != 0 || active != 0 || failures != 0 || !openUntil.IsZero() || len(b.slots) != 0 {
				t.Fatalf("bad upload: backend calls = %d, active = %d, failures = %d, open until = %v, slots = %d", calls.Load(), active, failures, openUntil, len(b.slots))
			}
			response, err = proxy.Client().Get(proxy.URL)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusNoContent || calls.Load() != 1 {
				t.Fatalf("next request = %d, backend calls = %d", response.StatusCode, calls.Load())
			}
		})
	}
}

type trackedRequestBody struct {
	io.Reader
	closed bool
}

func (b *trackedRequestBody) Close() error {
	b.closed = true
	return nil
}

func TestRejectedUploadsCloseBodyAndReleaseCapacity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reader  io.Reader
		status  int
		message string
	}{
		{"too large", strings.NewReader(strings.Repeat("x", maxRequestBytes+1)), 413, "request body exceeds 1 MiB"},
		{"read failure", io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(errors.New("broken upload"))), 400, "could not read request body"},
		{"read timeout", iotest.ErrReader(&net.DNSError{IsTimeout: true}), 408, "request body timed out"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer upstream.Close()
			b, _ := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second, FailureThreshold: 1})
			b.slots = make(chan struct{}, 1)
			body := &trackedRequestBody{Reader: tc.reader}
			response := httptest.NewRecorder()
			b.ServeHTTP(response, httptest.NewRequest("POST", "http://proxy/", body))
			var payload map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatalf("invalid error JSON: %v", err)
			}
			if response.Code != tc.status || response.Header().Get("Content-Type") != "application/json" || payload["error"] != tc.message {
				t.Fatalf("rejection = %d %v %v", response.Code, response.Header(), payload)
			}
			if !body.closed || len(b.slots) != 0 || calls.Load() != 0 {
				t.Fatalf("closed = %t, occupied slots = %d, backend calls = %d", body.closed, len(b.slots), calls.Load())
			}
			backend := b.pool.backends[0]
			if backend.active != 0 || backend.failures != 0 || !backend.openUntil.IsZero() {
				t.Fatalf("bad upload affected backend health: %+v", backend)
			}
			response = httptest.NewRecorder()
			b.ServeHTTP(response, httptest.NewRequest("GET", "http://proxy/", nil))
			if response.Code != http.StatusNoContent || calls.Load() != 1 || len(b.slots) != 0 {
				t.Fatalf("next request = %d, backend calls = %d, occupied slots = %d", response.Code, calls.Load(), len(b.slots))
			}
		})
	}
}
