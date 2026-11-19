#!/usr/bin/env python3
"""Self-test for the DNS load-test service (dnslab.loadtest_app).

Starts the real load-test app (uvicorn) against a fake UDP slave whose
reply delay and RCODEs are controllable, then checks the report accounting
and the timing rules:

- every query is answered on time, grouped by RCODE;
- replies later than scheduled + 500 ms are timeouts and are never credited
  to another query;
- beats skipped because the in-flight cap is full are counted as not sent;
- answered + timeout + not_sent always equals the planned number of beats;
- one run at a time and the documented limits are enforced.

Run:  python tests/loadtest_selftest.py
Env:  PYBIN (python with deps, default sys.executable)
      LOADTEST_PORT (18090) FAKE_DNS_PORT (15390)
"""

from __future__ import annotations

import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

PYBIN = os.environ.get("PYBIN", sys.executable)
LOADTEST_PORT = int(os.environ.get("LOADTEST_PORT", "18090"))
FAKE_DNS_PORT = int(os.environ.get("FAKE_DNS_PORT", "15390"))
BASE = f"http://127.0.0.1:{LOADTEST_PORT}"

RESULTS: list[tuple[bool, str, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    RESULTS.append((bool(cond), name, detail))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  -- {detail}" if detail and not cond else ""))


def http(method: str, url: str, body: dict | None = None, timeout: float = 30.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except Exception:
            return e.code, {}
    except Exception as e:  # noqa: BLE001 - surfaced in the check detail
        return None, {"error": str(e)}


def http_text(url: str, timeout: float = 10.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status, resp.read(2048).decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, ""
    except Exception:  # noqa: BLE001
        return None, ""


# --------------------------------------------------------------------------
# fake slave: replies after a configurable delay with a configurable RCODE
# --------------------------------------------------------------------------

class FakeSlave:
    def __init__(self, port: int) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", port))
        self.sock.settimeout(0.1)
        self.port = self.sock.getsockname()[1]
        self.reply_fn = None      # (xid) -> rcode or None (drop)
        self.delay = 0.0
        self.received = 0
        self.received_xids: list[int] = []
        self.stop = False
        self.lock = threading.Lock()
        self.heap: list[tuple[float, bytes, tuple]] = []
        self.cv = threading.Condition()
        self.recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.send_thread = threading.Thread(target=self._send_loop, daemon=True)

    def start(self, reply_fn, delay: float) -> None:
        self.reply_fn, self.delay = reply_fn, delay
        self.received = 0
        self.received_xids = []
        with self.lock:
            self.heap.clear()
        if not self.recv_thread.is_alive():
            self.recv_thread.start()
        if not self.send_thread.is_alive():
            self.send_thread.start()

    def _recv_loop(self) -> None:
        while not self.stop:
            try:
                data, addr = self.sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                return
            xid = struct.unpack("!H", data[:2])[0]
            self.received += 1
            self.received_xids.append(xid)
            rcode = self.reply_fn(xid) if self.reply_fn else 0
            if rcode is None:
                continue
            flags = 0x8000 | 0x0400 | rcode
            reply = struct.pack("!HHHHHH", xid, flags, 1, 0, 0, 0) + data[12:]
            with self.cv:
                self.heap.append((time.monotonic() + self.delay, reply, addr))
                self.cv.notify()

    def _send_loop(self) -> None:
        while not self.stop:
            with self.cv:
                while not self.heap and not self.stop:
                    self.cv.wait(0.1)
                if self.stop:
                    return
                heap = self.heap
                while heap and heap[0][0] <= time.monotonic():
                    _due, reply, addr = heap.pop(0)
                    try:
                        self.sock.sendto(reply, addr)
                    except OSError:
                        pass
                wait = max(0.0, heap[0][0] - time.monotonic()) if heap else 0.1
            time.sleep(min(wait, 0.05))

    def reset_counters(self) -> int:
        with self.cv:
            self.received = 0
        return 0

    def close(self) -> None:
        self.stop = True
        with self.cv:
            self.cv.notify_all()
        time.sleep(0.15)
        self.sock.close()


# --------------------------------------------------------------------------
# test flow
# --------------------------------------------------------------------------

def wait_up(url: str, proc: subprocess.Popen, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        status, _ = http("GET", url, timeout=2)
        if status == 200:
            return True
        time.sleep(0.2)
    return False


def run(body: dict, timeout: float = 30.0):
    return http("POST", f"{BASE}/run", body, timeout=timeout)


def expect_rejected(label: str, body: dict) -> None:
    st, resp = run(body, timeout=10)
    check(f"{label} -> 422", st == 422, f"got {st} {resp}")


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="dnslab-loadtest-")
    print(f"# workdir: {tmp}")
    fake = FakeSlave(FAKE_DNS_PORT)
    env = {
        **os.environ,
        "SLAVE_DNS_HOST": "127.0.0.1",
        "SLAVE_DNS_PORT": str(fake.port),
        "PYTHONPATH": REPO + (os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else ""),
    }
    log = open(os.path.join(tmp, "loadtest.log"), "ab")
    proc = subprocess.Popen(
        [PYBIN, "-m", "uvicorn", "dnslab.loadtest_app:app",
         "--host", "127.0.0.1", "--port", str(LOADTEST_PORT)],
        cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    try:
        assert wait_up(f"{BASE}/status", proc), f"loadtest app did not start; see {tmp}"
        fake.start(lambda xid: None, 0.0)  # no traffic yet

        print("\n== 1. empty state and input limits ==")
        st, body = http("GET", f"{BASE}/report")
        check("report before any run -> 404", st == 404, f"got {st} {body}")
        st, body = http("GET", f"{BASE}/")
        check("root exposes fixed target and limits",
              st == 200 and body.get("target") == f"127.0.0.1:{fake.port}", json.dumps(body))

        valid = {"name": "host1.lab.test", "qps": 10, "duration_seconds": 1, "max_in_flight": 10}
        expect_rejected("qps above 200", {**valid, "qps": 201})
        expect_rejected("duration above 30 s", {**valid, "duration_seconds": 31})
        expect_rejected("in-flight cap 0", {**valid, "max_in_flight": 0})
        expect_rejected("in-flight cap above 6000", {**valid, "max_in_flight": 6001})
        expect_rejected("name with empty label", {**valid, "name": "bad..lab.test"})
        expect_rejected("missing field", {"qps": 10, "duration_seconds": 1, "max_in_flight": 10})
        st, _ = http("POST", f"{BASE}/run", "not-an-object")
        check("non-JSON body -> 422", st == 422, f"got {st}")

        print("\n== 2. happy path: all answered, grouped by RCODE ==")
        fake.start(lambda xid: 0, 0.05)
        st, rep = run({"name": "Host1.Lab.Test.", "qps": 20, "duration_seconds": 1, "max_in_flight": 50})
        check("run accepted", st == 200, f"got {st} {rep}")
        check("name normalized", rep.get("name") == "host1.lab.test", json.dumps(rep.get("name")))
        check("planned = 20 beats", rep.get("planned_queries") == 20, json.dumps(rep))
        check("all 20 answered on time", rep.get("on_time_answered") == 20 and rep.get("timeouts") == 0,
              json.dumps(rep))
        check("RCODE NOERROR = 20", rep.get("rcode_counts") == {"NOERROR": 20}, json.dumps(rep.get("rcode_counts")))
        check("nothing skipped", rep.get("not_sent") == 0, json.dumps(rep))
        check("accounting: answered + timeout + not_sent == planned",
              rep.get("accounting_ok") is True and
              rep["on_time_answered"] + rep["timeouts"] + rep["not_sent"] == rep["planned_queries"],
              json.dumps(rep))
        check("report names the fixed target and 500 ms window",
              rep.get("target") == f"127.0.0.1:{fake.port}" and rep.get("response_window_ms") == 500,
              json.dumps(rep))
        check("fake slave saw all 20 queries", fake.received == 20, f"saw {fake.received}")

        print("\n== 3. mixed RCODEs and drops: counts add up ==")
        # xid % 3 == 0 -> dropped, xid % 2 == 0 -> NXDOMAIN, else NOERROR.
        # The split is checked against what the fake slave actually received,
        # so a machine hiccup that legitimately costs a beat (recorded as
        # not sent) cannot make the classification check wrong.
        fake.start(lambda xid: None if xid % 3 == 0 else (3 if xid % 2 == 0 else 0), 0.02)
        st, rep = run({"name": "host1.lab.test", "qps": 30, "duration_seconds": 1, "max_in_flight": 100})
        check("run accepted", st == 200, f"got {st} {rep}")
        check("planned = 30 beats", rep.get("planned_queries") == 30, json.dumps(rep))
        seen = list(fake.received_xids)
        want_timeouts = sum(1 for x in seen if x % 3 == 0)
        want_noerror = sum(1 for x in seen if x % 3 != 0 and x % 2 != 0)
        want_nxdomain = sum(1 for x in seen if x % 3 != 0 and x % 2 == 0)
        check("NOERROR / NXDOMAIN / timeouts follow per-query replies",
              rep.get("rcode_counts") == {"NXDOMAIN": want_nxdomain, "NOERROR": want_noerror}
              and rep.get("timeouts") == want_timeouts,
              f"report {json.dumps(rep)}, fake saw {len(seen)}")
        check("not sent = planned - sent, and sent matches what the slave saw",
              rep.get("sent") == len(seen)
              and rep.get("not_sent") == rep.get("planned_queries") - rep.get("sent"),
              json.dumps(rep))
        check("accounting holds", rep.get("accounting_ok") is True, json.dumps(rep))

        print("\n== 4. a reply later than scheduled + 500 ms is a timeout ==")
        fake.start(lambda xid: 0, 0.53)
        st, rep = run({"name": "host1.lab.test", "qps": 1, "duration_seconds": 1, "max_in_flight": 10})
        check("run accepted", st == 200, f"got {st} {rep}")
        check("late reply not counted as answered",
              rep.get("on_time_answered") == 0 and rep.get("timeouts") == 1, json.dumps(rep))
        check("late reply counted as diagnostic late",
              rep.get("diagnostics", {}).get("late_responses") == 1, json.dumps(rep.get("diagnostics")))
        check("late reply not credited to another query (no duplicates/unknown)",
              rep.get("diagnostics", {}).get("duplicate_responses") == 0
              and rep.get("diagnostics", {}).get("unknown_responses") == 0,
              json.dumps(rep.get("diagnostics")))
        check("accounting holds", rep.get("accounting_ok") is True, json.dumps(rep))

        print("\n== 5. in-flight cap: skipped beats are not sent and not retried ==")
        fake.start(lambda xid: None, 0.0)  # nothing ever answers
        st, rep = run({"name": "host1.lab.test", "qps": 50, "duration_seconds": 0.5, "max_in_flight": 3})
        check("run accepted", st == 200, f"got {st} {rep}")
        check("planned = 25 beats", rep.get("planned_queries") == 25, json.dumps(rep))
        check("only the first 3 attempts were sent (cap reached immediately)",
              rep.get("sent") == 3 and rep.get("timeouts") == 3, json.dumps(rep))
        check("the rest counted as not sent due to in-flight cap",
              rep.get("not_sent") == 22
              and rep.get("not_sent_breakdown", {}).get("in_flight_full") == 22,
              json.dumps(rep))
        check("fake slave saw exactly the 3 sent queries", fake.received == 3, f"saw {fake.received}")
        check("accounting holds", rep.get("accounting_ok") is True, json.dumps(rep))

        print("\n== 6. only one run at a time ==")
        fake.start(lambda xid: 0, 0.01)
        outcome: dict = {}

        def background_run():
            outcome["resp"] = run({"name": "host1.lab.test", "qps": 2, "duration_seconds": 2,
                                   "max_in_flight": 10}, timeout=30)

        t = threading.Thread(target=background_run)
        t.start()
        deadline = time.time() + 5
        running = False
        while time.time() < deadline:
            st, body = http("GET", f"{BASE}/status")
            if st == 200 and body.get("running"):
                running = True
                break
            time.sleep(0.02)
        check("status reports the run as running", running, "never saw running=true")
        st, body = run({"name": "host1.lab.test", "qps": 2, "duration_seconds": 2, "max_in_flight": 10})
        check("concurrent run -> 409", st == 409, f"got {st} {body}")
        t.join(timeout=30)
        st, body = outcome.get("resp", (None, {}))
        check("background run still completes normally",
              st == 200 and body.get("accounting_ok") is True, f"got {st} {body}")
        st, body = http("GET", f"{BASE}/report")
        check("GET /report returns the latest report",
              st == 200 and body.get("started_at") == outcome["resp"][1].get("started_at"),
              f"got {st} {body}")

        print("\n== 7. /docs ==")
        st, html = http_text(f"{BASE}/docs")
        check("/docs reachable and serves Swagger UI",
              st == 200 and "swagger" in html.lower(), f"got {st} {html[:80]!r}")
        st, spec = http("GET", f"{BASE}/openapi.json")
        paths = spec.get("paths", {})
        check("openapi exposes /run for the form", st == 200 and "post" in paths.get("/run", {}), json.dumps(list(paths)))
        schema = spec.get("components", {}).get("schemas", {}).get("RunRequest", {})
        fields = set(schema.get("properties", {}))
        check("form fields: name, qps, duration_seconds, max_in_flight",
              fields == {"name", "qps", "duration_seconds", "max_in_flight"}, json.dumps(sorted(fields)))
        check("/report and /status exposed", "/report" in paths and "/status" in paths, json.dumps(list(paths)))

    finally:
        fake.close()
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

    failed = [r for r in RESULTS if not r[0]]
    print(f"\n{'=' * 60}\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed; logs in {tmp}")
    if failed:
        print("FAILED:")
        for _, name, detail in failed:
            print(f"  - {name}  {detail}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
