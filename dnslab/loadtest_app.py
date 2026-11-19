"""Independent DNS load-test service for the lab.test slave.

Sends a fixed-rate stream of UDP A queries at the slave in the same
deployment and reports how many were answered on time, grouped by RCODE,
plus timeouts and queries that were never sent.

Timing rules (deliberately strict, so the numbers mean one thing):

- query i is planned for ``start + i / qps``; sends are never shifted to
  "catch up" with a late schedule;
- a planned query is *not sent* when a whole send interval has been missed
  (``now - scheduled >= 1/qps``) or when the in-flight cap is already full;
  skipped queries are not retried;
- a query sent later than scheduled (but by less than one interval) is
  counted as a timeout immediately: its 500 ms window is measured from the
  scheduled time, so no reply to it could ever be on time;
- a sent query is only considered answered if a matching response is
  received no later than 500 ms after its *scheduled* time (not its actual
  send time).  Anything later is a timeout and is never credited to another
  query: every query in a run uses a distinct DNS message id.

Only one run may be active at a time.  The target is fixed by
``SLAVE_DNS_HOST`` / ``SLAVE_DNS_PORT``; callers cannot redirect the load.
"""

from __future__ import annotations

import asyncio
import math
import os
import re
import socket
import struct
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from dnslab import dnsproto

TARGET_HOST = os.environ.get("SLAVE_DNS_HOST", "slave")
TARGET_PORT = int(os.environ.get("SLAVE_DNS_PORT", "5353"))

MAX_QPS = 200.0
MAX_DURATION_SECONDS = 30.0
RESPONSE_WINDOW_SECONDS = 0.5  # each query is only waited for until scheduled + 500 ms
MAX_PLANNED = int(MAX_QPS * MAX_DURATION_SECONDS)  # 6000, also the useful in-flight maximum
FINAL_GRACE_SECONDS = 0.05  # let datagrams already queued before the last deadline be read

RCODE_NAMES = {
    0: "NOERROR",
    1: "FORMERR",
    2: "SERVFAIL",
    3: "NXDOMAIN",
    4: "NOTIMP",
    5: "REFUSED",
    6: "YXDOMAIN",
    7: "YXRRSET",
    8: "NXRRSET",
    9: "NOTAUTH",
    10: "NOTZONE",
}

_LABEL_RE = re.compile(r"^[a-z0-9_](?:[a-z0-9_-]{0,61}[a-z0-9_])?$")


# --------------------------------------------------------------------------
# request / report models (also the /docs form and response documentation)
# --------------------------------------------------------------------------

class RunRequest(BaseModel):
    name: str = Field(
        ...,
        description="要查询的域名（只发 A 查询），如 host1.lab.test",
        examples=["host1.lab.test"],
    )
    qps: float = Field(
        ...,
        gt=0,
        le=MAX_QPS,
        description="每秒查询数（固定节拍，1/qps 秒一条，最多 200）",
        examples=[50],
    )
    duration_seconds: float = Field(
        ...,
        gt=0,
        le=MAX_DURATION_SECONDS,
        description="发包秒数（最多 30 秒）",
        examples=[10],
    )
    max_in_flight: int = Field(
        ...,
        ge=1,
        le=MAX_PLANNED,
        description="在途上限：已发出但尚未按时应答/超时的查询数；到上限的节拍记为未发",
        examples=[100],
    )


class NotSentBreakdown(BaseModel):
    interval_missed: int = Field(description="因为落后超过一个发包间隔而跳过的节拍数")
    in_flight_full: int = Field(description="因为达到在途上限而跳过的节拍数")
    send_error: int = Field(description="本机发送失败（UDP send 报错）的节拍数")


class Diagnostics(BaseModel):
    late_responses: int = Field(description="计划时刻 500 毫秒之后才到、因此未计入的应答")
    duplicate_responses: int = Field(description="同一查询的重复应答")
    unknown_responses: int = Field(description="找不到对应在途查询的应答（如上一轮的迟到包）")
    malformed_responses: int = Field(description="无法解析或问题段不匹配的报文")


class RunReport(BaseModel):
    name: str
    qps: float
    duration_seconds: float
    max_in_flight: int
    response_window_ms: int = Field(description="每条查询的等待窗口：计划时刻后多少毫秒")
    target: str = Field(description="UDP 目标（同一部署内的从节点）")
    planned_queries: int = Field(description="本轮的节拍总数")
    sent: int = Field(description="实际发出的查询数 = 按时应答 + 超时")
    late_sent: int = Field(
        description="在窗口已过后才发出的查询数（此时不再等待应答，直接记为超时；未发数不含它）"
    )
    on_time_answered: int = Field(description="按时应答总数（所有 RCODE 合计）")
    rcode_counts: dict[str, int] = Field(description="按时应答按 RCODE 分类的数量（只列出现过的）")
    timeouts: int = Field(description="已发出但等到计划时刻 + 500 毫秒仍没有应答的查询数")
    not_sent: int = Field(description="未发出的计划查询数")
    not_sent_breakdown: NotSentBreakdown
    accounting_ok: bool = Field(
        description="on_time_answered + timeouts + not_sent == planned_queries"
    )
    diagnostics: Diagnostics
    started_at: str
    finished_at: str
    elapsed_seconds: float


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _utcnow() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def normalize_name(raw: str) -> str:
    name = raw.strip().rstrip(".").lower()
    if not name:
        raise HTTPException(422, "name is empty")
    if len(name) > 253:
        raise HTTPException(422, "name is longer than 253 characters")
    labels = name.split(".")
    for label in labels:
        if not _LABEL_RE.match(label):
            raise HTTPException(422, f"invalid DNS label {label!r} in name {name!r}")
    return name


def planned_queries(qps: float, duration: float) -> int:
    """Number of fixed-rate ticks strictly inside the send window.

    Ticks are at 0, 1/qps, 2/qps, ...; the last one is < duration seconds
    after the start.  Rounding first keeps e.g. 0.3 * 10 from becoming 4.
    """
    product = round(qps * duration, 9)
    return max(0, math.ceil(product))


# --------------------------------------------------------------------------
# one run
# --------------------------------------------------------------------------

class _ClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, runner: "_Runner") -> None:
        self.runner = runner

    def datagram_received(self, data: bytes, addr) -> None:
        self.runner.on_datagram(data)

    def error_received(self, exc: Exception) -> None:
        # A connected UDP socket can report ICMP errors here (e.g. the
        # slave process is gone).  Outstanding queries simply time out.
        pass


class _Runner:
    def __init__(self, name: str, planned: int, xids: list[int]) -> None:
        self.name = name
        self.planned = planned
        self.xids = xids
        self.active: dict[int, float] = {}     # xid -> deadline (still awaiting an on-time reply)
        self.deadlines: dict[int, float] = {}  # every sent xid -> deadline, for late diagnostics
        self.answered: dict[int, int] = {}     # rcode -> count
        self.answered_count = 0
        self.timeouts = 0
        self.late_sent = 0
        self.interval_missed = 0
        self.in_flight_full = 0
        self.send_error = 0
        self.late_responses = 0
        self.duplicate_responses = 0
        self.unknown_responses = 0
        self.malformed_responses = 0
        self.start = 0.0

    # -- responses ---------------------------------------------------------

    def on_datagram(self, data: bytes) -> None:
        now = asyncio.get_running_loop().time()
        if len(data) < 12:
            self.malformed_responses += 1
            return
        xid, flags, qdcount = struct.unpack("!HHH", data[:6])
        if not flags & dnsproto.FLAG_QR:
            self.malformed_responses += 1
            return
        deadline = self.deadlines.get(xid)
        if deadline is None:
            # Not from this run (or never sent): never let it count.
            self.unknown_responses += 1
            return

        # A response normally echoes the question.  qdcount == 0 is accepted
        # (e.g. a FORMERR) because the unique xid already ties it to a query.
        if qdcount == 1:
            try:
                qname, offset = dnsproto.parse_name(data, 12)
                if offset + 4 > len(data):
                    raise dnsproto.DnsError("short question")
                qtype, qclass = struct.unpack("!HH", data[offset:offset + 4])
            except dnsproto.DnsError:
                self.malformed_responses += 1
                return
            if (
                qname != self.name
                or qtype != dnsproto.QTYPE_A
                or qclass != dnsproto.QCLASS_IN
            ):
                self.malformed_responses += 1
                return
        elif qdcount != 0:
            self.malformed_responses += 1
            return

        if now > deadline:
            # Late: the query is already (or will be) a timeout, and this
            # packet must not be matched to any other query.
            self.late_responses += 1
            if self.active.pop(xid, None) is not None:
                self.timeouts += 1
            return

        if self.active.pop(xid, None) is None:
            self.duplicate_responses += 1
            return

        rcode = flags & 0xF
        self.answered[rcode] = self.answered.get(rcode, 0) + 1
        self.answered_count += 1

    # -- bookkeeping -------------------------------------------------------

    def _expire(self, now: float) -> None:
        """Retire queries whose window has closed without a response.

        Deadlines are handed out in send order, so insertion order in
        `active` is deadline order: stop at the first one still in its
        window.
        """
        while True:
            first = next(iter(self.active.items()), None)
            if first is None or first[1] > now:
                return
            del self.active[first[0]]
            self.timeouts += 1

    def report(
        self,
        req: RunRequest,
        started_at: str,
        finished_at: str,
        elapsed: float,
    ) -> RunReport:
        not_sent = self.interval_missed + self.in_flight_full + self.send_error
        total = self.answered_count + self.timeouts + not_sent
        rcodes = {
            RCODE_NAMES.get(code, f"RCODE_{code}"): count
            for code, count in sorted(self.answered.items())
        }
        return RunReport(
            name=self.name,
            qps=req.qps,
            duration_seconds=req.duration_seconds,
            max_in_flight=req.max_in_flight,
            response_window_ms=int(RESPONSE_WINDOW_SECONDS * 1000),
            target=f"{TARGET_HOST}:{TARGET_PORT}",
            planned_queries=self.planned,
            sent=self.answered_count + self.timeouts,
            late_sent=self.late_sent,
            on_time_answered=self.answered_count,
            rcode_counts=rcodes,
            timeouts=self.timeouts,
            not_sent=not_sent,
            not_sent_breakdown=NotSentBreakdown(
                interval_missed=self.interval_missed,
                in_flight_full=self.in_flight_full,
                send_error=self.send_error,
            ),
            accounting_ok=(total == self.planned),
            diagnostics=Diagnostics(
                late_responses=self.late_responses,
                duplicate_responses=self.duplicate_responses,
                unknown_responses=self.unknown_responses,
                malformed_responses=self.malformed_responses,
            ),
            started_at=started_at,
            finished_at=finished_at,
            elapsed_seconds=round(elapsed, 3),
        )

    # -- sender ------------------------------------------------------------

    async def run(self, req: RunRequest, packets: list[bytes], transport) -> RunReport:
        loop = asyncio.get_running_loop()
        interval = 1.0 / req.qps
        started_at = _utcnow()
        self.start = loop.time()

        for index in range(self.planned):
            target = self.start + index * interval
            delay = target - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)

            now = loop.time()
            self._expire(now)

            if now - target >= interval:
                # A whole send interval has been missed: the slot is gone and
                # is NOT shifted to "catch up" (that would burst).
                self.interval_missed += 1
                continue
            if len(self.active) >= req.max_in_flight:
                self.in_flight_full += 1
                continue

            xid = self.xids[index]  # unique across runs
            deadline = target + RESPONSE_WINDOW_SECONDS
            self.deadlines[xid] = deadline
            try:
                transport.sendto(packets[index])
            except OSError:
                del self.deadlines[xid]
                self.send_error += 1
                continue
            if now > deadline:
                # The beat was already late (still less than one interval):
                # send it, but its window is over, so a reply can never be
                # counted on time -- record the timeout now.
                self.late_sent += 1
                self.timeouts += 1
            else:
                self.active[xid] = deadline

        # Wait for the last query's 500 ms window (measured from its
        # *scheduled* time), then give the loop a moment to deliver anything
        # already sitting in the socket.
        last_deadline = max(self.deadlines.values(), default=self.start)
        remaining = last_deadline + FINAL_GRACE_SECONDS - loop.time()
        if remaining > 0:
            await asyncio.sleep(remaining)
        await asyncio.sleep(0)
        self._expire(loop.time())

        finished_at = _utcnow()
        return self.report(
            req, started_at, finished_at, loop.time() - self.start
        )


_next_xid = 1


def allocate_xids(count: int) -> list[int]:
    """Hand out message ids that are not reused by the next runs.

    A late reply from an earlier run can therefore never be mistaken for a
    reply to a query of a later run, even if both runs query the same name
    (each run also uses a fresh UDP socket, which helps the same way).
    """
    global _next_xid
    if _next_xid + count - 1 > 65535:
        _next_xid = 1
    xids = list(range(_next_xid, _next_xid + count))
    _next_xid += count
    return xids


def build_packets(name: str, xids: list[int]) -> list[bytes]:
    question = dnsproto.encode_name(name) + struct.pack("!HH", dnsproto.QTYPE_A, dnsproto.QCLASS_IN)
    return [
        struct.pack("!HHHHHH", xid, dnsproto.FLAG_RD, 1, 0, 0, 0) + question
        for xid in xids
    ]


# --------------------------------------------------------------------------
# application state / API
# --------------------------------------------------------------------------

class _State:
    running = False
    last_report: Optional[RunReport] = None
    last_error: Optional[str] = None


state = _State()

app = FastAPI(
    title="lab.test DNS load tester",
    description=(
        "独立的试跑服务：只向同一部署内的从节点发送 UDP A 查询。"
        "按固定节拍发包；错过一个发包间隔或在途已满的节拍记为未发且不补发；"
        "每条查询只等到计划时刻后 500 毫秒，迟到的应答不会算给任何查询。"
        "一次只允许跑一轮。"
    ),
    version="1.0.0",
)


@app.get("/")
def root() -> dict:
    return {
        "service": "dns-loadtest",
        "target": f"{TARGET_HOST}:{TARGET_PORT}",
        "running": state.running,
        "limits": {
            "max_qps": MAX_QPS,
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_in_flight": MAX_PLANNED,
            "response_window_ms": int(RESPONSE_WINDOW_SECONDS * 1000),
        },
    }


@app.get("/status")
def status() -> dict:
    return {
        "running": state.running,
        "target": f"{TARGET_HOST}:{TARGET_PORT}",
        "last_error": state.last_error,
        "last_report": state.last_report,
    }


@app.get("/report")
def report() -> RunReport:
    """Report of the most recent completed run."""
    if state.last_report is None:
        raise HTTPException(404, "no run has completed yet")
    return state.last_report


@app.post(
    "/run",
    response_model=RunReport,
    responses={
        409: {"description": "另一轮正在运行"},
        422: {"description": "参数无效"},
        503: {"description": "无法向从节点打开 UDP 套接字"},
    },
)
async def run(req: RunRequest) -> RunReport:
    """Run one fixed-rate load round and return its report (blocks until done).

    The request body is the form shown in /docs: name, qps, duration_seconds,
    max_in_flight.  A round takes at most 30 s plus the final 500 ms window.
    """
    name = normalize_name(req.name)
    if not math.isfinite(req.qps) or not math.isfinite(req.duration_seconds):
        raise HTTPException(422, "qps and duration_seconds must be finite")
    if req.qps > MAX_QPS:
        raise HTTPException(422, f"qps must be <= {MAX_QPS:g}")
    if req.duration_seconds > MAX_DURATION_SECONDS:
        raise HTTPException(422, f"duration_seconds must be <= {MAX_DURATION_SECONDS:g}")

    planned = planned_queries(req.qps, req.duration_seconds)
    if planned < 1:
        raise HTTPException(
            422, "the send window contains no query tick; increase qps or duration_seconds"
        )
    if planned > MAX_PLANNED:
        raise HTTPException(422, f"planned queries must be <= {MAX_PLANNED}")

    if state.running:
        raise HTTPException(409, "a load round is already running; wait for it to finish")
    state.running = True
    try:
        xids = allocate_xids(planned)
        packets = build_packets(name, xids)
        runner = _Runner(name, planned, xids)
        loop = asyncio.get_running_loop()
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _ClientProtocol(runner),
                remote_addr=(TARGET_HOST, TARGET_PORT),
                family=socket.AF_INET,
            )
        except OSError as exc:
            state.last_error = f"cannot open UDP socket to {TARGET_HOST}:{TARGET_PORT}: {exc}"
            raise HTTPException(503, state.last_error)
        try:
            result = await runner.run(req, packets, transport)
        finally:
            transport.close()
        state.last_report = result
        state.last_error = None
        return result
    except HTTPException as exc:
        state.last_error = str(exc.detail)
        raise
    except Exception as exc:  # pragma: no cover - defensive
        state.last_error = f"run failed: {exc}"
        raise HTTPException(500, state.last_error)
    finally:
        state.running = False
