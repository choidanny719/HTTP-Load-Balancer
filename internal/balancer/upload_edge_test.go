package balancer

import (
	"encoding/json"
	"errors"
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
