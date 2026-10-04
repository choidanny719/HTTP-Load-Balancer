import json
import subprocess
import urllib.parse

from process_test import request, wait_for


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, timeout=60)


def backend():
    status, body = request("http://127.0.0.1:8080/")
    return status, json.loads(body).get("backend")


wait_for(lambda: request("http://127.0.0.1:9090/healthz")[0] == 200)
responses = [backend() for _ in range(9)]
assert all(status == 200 for status, _ in responses), responses
assert {name for _, name in responses} == {"backend-1", "backend-2", "backend-3"}, responses
compose("stop", "backend-1")
try:
    responses = [backend() for _ in range(12)]
    assert sum(status == 502 for status, _ in responses) == 3, responses
    assert all(name != "backend-1" for status, name in responses if status == 200), responses
    for _ in range(6):
        status, name = backend()
        assert status == 200 and name in {"backend-2", "backend-3"}, (status, name)
finally:
    compose("start", "backend-1")
wait_for(lambda: backend() == (200, "backend-1"), timeout=20)
query = urllib.parse.urlencode({"query": 'up{job="load-balancer"}'})


def scraped():
    status, body = request("http://127.0.0.1:9091/api/v1/query?" + query)
    results = json.loads(body).get("data", {}).get("result", [])
    return status == 200 and results and results[0]["value"][1] == "1"


wait_for(scraped, timeout=20)
print("Docker routing, backend outage, recovery, and Prometheus scraping passed.")
