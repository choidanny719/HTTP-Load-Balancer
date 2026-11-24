package balancer

import (
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

const maxRequestBytes = 1 << 20

func (b *Balancer) readRequestBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	reader := http.NewResponseController(w)
	if r.Body != http.NoBody {
		reader.SetReadDeadline(time.Now().Add(b.timeout))
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	r.Body.Close()
	reader.SetReadDeadline(time.Time{})
	if err == nil {
		return body, true
	}
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
	return nil, false
}
