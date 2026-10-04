# HTTP Load Balancer

A Go reverse proxy with round-robin and least-connections routing, per-client
token buckets, circuit breakers, and Prometheus metrics.

## Try it

```sh
docker compose up --build -d
curl http://localhost:8080/
curl http://localhost:8080/
curl http://localhost:8080/
```

Each response identifies one of three demo backends. Prometheus runs at
http://localhost:9091; query `lb_requests_total` to see the distribution.
The demo also has `/slow` (two-second response) and `/fail` (HTTP 500).
Stop it with `docker compose down`.

## Run locally

Requires Go 1.27+. Start a backend, then run the proxy in another terminal:

```sh
go run ./cmd/backend -listen=127.0.0.1:9001
go run ./cmd/lb -backends=http://127.0.0.1:9001
```

| Flag | Default |
| --- | --- |
| `-backends` | Required comma-separated HTTP/HTTPS origins |
| `-listen` / `-admin` | `127.0.0.1:8080` / `127.0.0.1:9090` |
| `-algorithm` | `round-robin`; also accepts `least-connections` |
| `-timeout` | `5s`, including uploads and response streaming |
| `-rate` / `-burst` | 20 requests/second per client IP; burst 40; rate 0 disables |
| `-failure-threshold` / `-cooldown` | 3 consecutive failures / `10s` |

Least-connections counts active HTTP requests, with rotating ties. Backend 5xx
responses, connection errors, and timeouts open a circuit after the failure
threshold. After cooldown, one incoming request probes recovery; success closes
the circuit and failure starts another cooldown. Checks are driven by traffic.
Client cancellations do not count as backend failures. Requests are never retried
by the balancer, including failed writes.

The proxy returns 429 for rate limits, 502 for connection failures, 504 for backend
timeouts before response headers, and 503 when no backend is eligible or all 256
request slots are occupied. A timeout after headers closes the response stream.
Bodies are limited to 1 MiB. Rate limits use the socket peer IP, ignore supplied
forwarding headers, and retain at most 10,000 clients with idle expiry.

The admin listener exposes `/healthz` (process health) and `/metrics`. Metrics
include request counts, duration histograms, active requests, backend failures,
and circuit states (0 closed, 1 open, 2 half-open). Rejected requests use backend
label `none`. Limiter and circuit state are local to one process and reset on
restart. This is an HTTP proxy; WebSocket upgrades and CONNECT are rejected.

## Tests

```sh
go test -race ./...
go vet ./...
go build -o bin/lb ./cmd/lb
python3 tests/process_test.py
```

CI runs these checks and tests the Docker Compose demo, including a backend
outage, recovery, and Prometheus scraping.
