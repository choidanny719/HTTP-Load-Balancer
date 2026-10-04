import http.server
import http.client
import json
import pathlib
import socket
import subprocess
import threading
import time
import unittest
import urllib.error
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parents[1]


def request(url, timeout=4):
    try:
        response = urllib.request.urlopen(url, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, response.read().decode()


def wait_for(check, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.1)
    raise AssertionError("condition did not become true before deadline")


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Backend(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/hold":
            self.server.started.set()
            self.server.release.wait(15)
        body = json.dumps({"backend": self.server.name}).encode()
        self.send_response(200 if self.server.healthy else 500)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):
        pass


class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.backends = []
        for i in range(3):
            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Backend)
            server.name = f"backend-{i + 1}"
            server.healthy = True
            server.started = threading.Event()
            server.release = threading.Event()
            threading.Thread(target=server.serve_forever, daemon=True).start()
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
            self.backends.append(server)
        self.process = None
        self.addCleanup(self.stop)
        self.start()

    def start(self, *options):
        proxy_port = free_port()
        admin_port = free_port()
        while admin_port == proxy_port:
            admin_port = free_port()
        self.proxy = f"http://127.0.0.1:{proxy_port}"
        self.admin = f"http://127.0.0.1:{admin_port}"
        origins = ",".join(f"http://127.0.0.1:{s.server_port}" for s in self.backends)
        self.process = subprocess.Popen([
            str(ROOT / "bin/lb"), f"-listen={self.proxy.removeprefix('http://')}",
            f"-admin={self.admin.removeprefix('http://')}", f"-backends={origins}",
            "-rate=0", "-timeout=4s", "-failure-threshold=1", "-cooldown=1s", *options,
        ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        wait_for(lambda: request(self.admin + "/healthz")[0] == 200)

    def stop(self):
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.process.stderr.close()

    def test_routing_and_admin_listener(self):
        names = []
        for _ in range(9):
            status, body = request(self.proxy + "/")
            self.assertEqual(status, 200)
            names.append(json.loads(body)["backend"])
        self.assertEqual(names, ["backend-1", "backend-2", "backend-3"] * 3)
        self.assertEqual(request(self.admin + "/unknown")[0], 404)
        status, body = request(self.proxy + "/metrics")
        self.assertEqual(status, 200)
        self.assertIn("backend", json.loads(body))
        metrics = request(self.admin + "/metrics")[1]
        self.assertIn("lb_requests_total", metrics)
        self.assertIn("lb_request_duration_seconds_bucket", metrics)

    def test_failure_isolation_and_recovery(self):
        self.backends[0].healthy = False
        statuses = [request(self.proxy + "/")[0] for _ in range(9)]
        self.assertEqual(statuses.count(500), 1)
        self.assertEqual(statuses.count(200), 8)
        self.backends[0].healthy = True
        wait_for(lambda: json.loads(request(self.proxy + "/")[1]).get("backend") == "backend-1")
        statuses = [request(self.proxy + "/")[0] for _ in range(6)]
        self.assertEqual(statuses, [200] * 6)

    def test_shutdown_finishes_active_request(self):
        result = []
        worker = threading.Thread(target=lambda: result.append(request(self.proxy + "/hold")))
        worker.start()
        self.addCleanup(worker.join, 5)
        self.addCleanup(self.backends[0].release.set)
        self.assertTrue(self.backends[0].started.wait(2))
        self.process.terminate()
        self.assertIsNone(self.process.poll())
        self.backends[0].release.set()
        worker.join(5)
        self.assertEqual(result[0][0], 200)
        self.assertEqual(self.process.wait(timeout=8), 0)

    def test_bad_configuration_exits(self):
        for flag in ["-algorithm=unknown", "-rate=NaN", "-timeout=0s", "-burst=0", "-failure-threshold=0"]:
            result = subprocess.run([
                str(ROOT / "bin/lb"), "-backends=http://127.0.0.1:9001", flag,
            ], capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"configuration", result.stderr)

    def test_least_connections_avoids_busy_backend(self):
        self.stop()
        self.start("-algorithm=least-connections")
        result = []
        worker = threading.Thread(target=lambda: result.append(request(self.proxy + "/hold")))
        worker.start()
        self.addCleanup(worker.join, 5)
        self.addCleanup(self.backends[0].release.set)
        self.assertTrue(self.backends[0].started.wait(2))
        names = [json.loads(request(self.proxy + "/")[1])["backend"] for _ in range(6)]
        self.assertEqual(names, ["backend-2", "backend-3"] * 3)
        self.backends[0].release.set()
        worker.join(5)
        self.assertEqual(result[0][0], 200)
        names = {json.loads(request(self.proxy + "/")[1])["backend"] for _ in range(6)}
        self.assertEqual(names, {"backend-1", "backend-2", "backend-3"})

    def test_shutdown_deadline_closes_stalled_request(self):
        self.stop()
        self.start("-timeout=30s")
        result = []

        def hold():
            try:
                result.append(request(self.proxy + "/hold", timeout=10))
            except (OSError, http.client.HTTPException) as error:
                result.append(error)

        worker = threading.Thread(target=hold)
        worker.start()
        self.addCleanup(worker.join, 10)
        self.addCleanup(self.backends[0].release.set)
        self.assertTrue(self.backends[0].started.wait(2))
        started = time.monotonic()
        self.process.terminate()
        self.assertEqual(self.process.wait(timeout=9), 0)
        self.assertGreaterEqual(time.monotonic() - started, 4.5)
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertIsInstance(result[0], (OSError, http.client.HTTPException))

    def test_large_headers_are_rejected(self):
        req = urllib.request.Request(self.proxy + "/", headers={"X-Large": "x" * (40 << 10)})
        with self.assertRaises(urllib.error.HTTPError) as response:
            urllib.request.urlopen(req, timeout=4)
        self.assertEqual(response.exception.code, 431)
        response.exception.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
