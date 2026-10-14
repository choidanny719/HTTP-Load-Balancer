package balancer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBufferedUploadsPreservePayloadAndLength(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    []byte
		chunked bool
	}{
		{"empty", nil, false},
		{"empty chunked", nil, true},
		{"binary", []byte{0, 1, 127, 128, 255, '\r', '\n'}, false},
		{"binary chunked", []byte{0, 1, 127, 128, 255, '\r', '\n'}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil || !bytes.Equal(body, tc.body) {
					t.Errorf("body = %v, error = %v; want %v", body, err, tc.body)
				}
				if r.ContentLength != int64(len(tc.body)) || len(r.TransferEncoding) != 0 {
					t.Errorf("length = %d, transfer encoding = %v", r.ContentLength, r.TransferEncoding)
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Write(body)
			}))
			defer upstream.Close()
			_, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second})
			request, err := http.NewRequest("POST", proxy.URL, io.NopCloser(bytes.NewReader(tc.body)))
			if err != nil {
				t.Fatal(err)
			}
			request.ContentLength = int64(len(tc.body))
			if tc.chunked {
				request.ContentLength = -1
				request.TransferEncoding = []string{"chunked"}
			}
			response, err := proxy.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != 200 || !bytes.Equal(body, tc.body) {
				t.Fatalf("response = %d %v, error = %v", response.StatusCode, body, err)
			}
		})
	}
}

func TestResponseTrailersReachClient(t *testing.T) {
	for _, declared := range []bool{true, false} {
		name := "late trailer"
		if declared {
			name = "declared trailer"
		}
		t.Run(name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if declared {
					w.Header().Set("Trailer", "X-Checksum")
				}
				io.WriteString(w, "payload")
				w.(http.Flusher).Flush()
				if declared {
					w.Header().Set("X-Checksum", "verified")
				} else {
					w.Header().Set(http.TrailerPrefix+"X-Checksum", "verified")
				}
			}))
			defer upstream.Close()
			_, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: time.Second})
			response, err := proxy.Client().Get(proxy.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != 200 || string(body) != "payload" {
				t.Fatalf("response = %d %q, error = %v", response.StatusCode, body, err)
			}
			if response.Trailer.Get("X-Checksum") != "verified" || response.Header.Get("X-Checksum") != "" {
				t.Fatalf("headers = %v, trailers = %v", response.Header, response.Trailer)
			}
		})
	}
}

func TestClientDisconnectMetricsAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stream bool
		code   string
	}{{"before headers", false, "499"}, {"during stream", true, "200"}} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{}, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/hold" {
					io.WriteString(w, "recovered")
					return
				}
				if tc.stream {
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "first\n")
					w.(http.Flusher).Flush()
				}
				started <- struct{}{}
				<-r.Context().Done()
			}))
			defer upstream.Close()
			b, proxy := testProxy(t, Config{Backends: []string{upstream.URL}, Timeout: 5 * time.Second, FailureThreshold: 1})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, "GET", proxy.URL+"/hold", nil)
			if err != nil {
				t.Fatal(err)
			}
			streamReady := make(chan struct{}, 1)
			done := make(chan error, 1)
			go func() {
				response, err := proxy.Client().Do(request)
				if err != nil {
					done <- err
					return
				}
				defer response.Body.Close()
				prefix := make([]byte, len("first\n"))
				if _, err := io.ReadFull(response.Body, prefix); err != nil {
					done <- err
					return
				}
				if string(prefix) != "first\n" || response.StatusCode != 200 {
					t.Errorf("stream = %d %q", response.StatusCode, prefix)
				}
				streamReady <- struct{}{}
				_, err = io.Copy(io.Discard, response.Body)
				done <- err
			}()
			ready := started
			if tc.stream {
				ready = streamReady
			}
			select {
			case <-ready:
			case err := <-done:
				t.Fatalf("request ended before cancellation: %v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("request did not reach the expected stage")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("request error = %v, want context cancellation", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("canceled request did not finish")
			}
			metric := `lb_requests_total{backend="` + upstream.URL + `",code="` + tc.code + `"} 1`
			deadline := time.Now().Add(2 * time.Second)
			for {
				scrape := httptest.NewRecorder()
				b.AdminHandler().ServeHTTP(scrape, httptest.NewRequest("GET", "http://admin/metrics", nil))
				if strings.Contains(scrape.Body.String(), metric) {
					for _, want := range []string{
						"lb_active_requests 0\n",
						`lb_backend_active_requests{backend="` + upstream.URL + `"} 0`,
						`lb_backend_failures_total{backend="` + upstream.URL + `"} 0`,
						`lb_backend_circuit_state{backend="` + upstream.URL + `"} 0`,
					} {
						if !strings.Contains(scrape.Body.String(), want) {
							t.Errorf("missing metric %s", want)
						}
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("missing cancellation metric %s", metric)
				}
				time.Sleep(time.Millisecond)
			}
			response, err := proxy.Client().Get(proxy.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != 200 || string(body) != "recovered" {
				t.Fatalf("request after cancellation = %d %q, error = %v", response.StatusCode, body, err)
			}
		})
	}
}
