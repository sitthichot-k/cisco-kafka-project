"""Backend API: accepts jobs from the frontend, publishes them to Kafka,
and tracks their status from the results topic written by the worker.

Router IPs are not configured: the worker discovers routers by hostname
(a "discover" job) and the backend keeps the latest name -> IP table."""

import json
import logging
import os
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
COMMAND_TOPIC = os.environ.get("COMMAND_TOPIC", "cisco-commands")
RESULT_TOPIC = os.environ.get("RESULT_TOPIC", "cisco-results")
ROUTERS_FILE = os.environ.get("ROUTERS_FILE", "/config/routers.json")

MAX_JOBS = 200
MAX_COMMANDS = 50
MODES = ("exec", "config")
TERMINAL_STATUSES = ("success", "failed")
DISCOVERY_ROUTER = "(scan)"
FIRST_SCAN_DELAY = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [backend] %(message)s")
log = logging.getLogger(__name__)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_config():
    # Read on every call so edits to routers.json apply without a restart.
    with open(ROUTERS_FILE, encoding="utf-8") as f:
        return json.load(f)


def connect_kafka(factory, what):
    while True:
        try:
            client = factory()
            log.info("connected %s to Kafka at %s", what, KAFKA_BOOTSTRAP)
            return client
        except NoBrokersAvailable:
            log.warning("Kafka not reachable for %s, retrying in 3s", what)
            time.sleep(3)


class JobStore:
    """In-memory job and router tables. The results topic is replayed from
    the start on boot, so finished jobs and the last scan survive a restart."""

    def __init__(self):
        self._jobs = OrderedDict()
        self._discovered = []
        self._last_scan = None
        self._lock = threading.Lock()

    def add(self, job):
        with self._lock:
            self._jobs[job["job_id"]] = job
            while len(self._jobs) > MAX_JOBS:
                self._jobs.popitem(last=False)

    def apply_event(self, event):
        job_id = event.get("job_id")
        if not job_id:
            return
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                job = {k: event.get(k) for k in ("job_id", "router", "host", "mode", "commands", "submitted_at")}
                self._jobs[job_id] = job
            if job.get("status") in TERMINAL_STATUSES and event.get("status") not in TERMINAL_STATUSES:
                return
            for key in ("status", "output", "error", "worker", "started_at", "finished_at"):
                if key in event:
                    job[key] = event[key]
            if job.get("mode") == "discover":
                self._last_scan = job
                if event.get("status") == "success" and "routers" in event:
                    self._discovered = event["routers"]

    def get(self, job_id):
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job else None

    def list(self):
        with self._lock:
            return [dict(j) for j in reversed(self._jobs.values())]

    def discovered(self):
        with self._lock:
            return list(self._discovered)

    def last_scan(self):
        with self._lock:
            return dict(self._last_scan) if self._last_scan else None


jobs = JobStore()
producer = connect_kafka(
    lambda: KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    ),
    "producer",
)


def known_routers():
    """Discovered routers, with entries from routers.json taking priority
    for the same name (a manual fallback when discovery cannot reach them)."""
    routers = {r["name"]: {**r, "source": "discovered"} for r in jobs.discovered()}
    for r in load_config().get("routers", []):
        if r.get("name") and r.get("host"):
            routers[r["name"]] = {"name": r["name"], "host": r["host"], "source": "config"}
    return sorted(routers.values(), key=lambda r: r["name"])


def publish_job(router, mode, commands=None, host=None):
    job = {
        "job_id": uuid.uuid4().hex[:12],
        "router": router,
        "host": host,
        "mode": mode,
        "commands": commands or [],
        "submitted_at": now_iso(),
    }
    # Store before sending: a fast worker's "running" event must not arrive
    # for a job we have not recorded yet and then be overwritten by "queued".
    jobs.add({**job, "status": "queued"})
    try:
        # Keyed by router so every job for one router lands on the same
        # partition and runs in submission order, even with several workers.
        producer.send(COMMAND_TOPIC, key=router, value=job).get(timeout=10)
    except Exception as exc:
        log.exception("failed to publish job %s", job["job_id"])
        jobs.apply_event({"job_id": job["job_id"], "status": "failed", "error": f"Kafka publish failed: {exc}"})
    else:
        log.info("queued job %s for %s (%s): %s", job["job_id"], router, mode, job["commands"])
    return jobs.get(job["job_id"])


def request_scan():
    """Queue a discovery job unless one is already waiting or running."""
    last = jobs.last_scan()
    if last and last.get("status") not in TERMINAL_STATUSES:
        return last
    return publish_job(DISCOVERY_ROUTER, "discover")


def consume_results():
    consumer = connect_kafka(
        lambda: KafkaConsumer(
            RESULT_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            group_id=None,
            auto_offset_reset="earliest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        ),
        "results consumer",
    )
    while True:
        try:
            for message in consumer:
                jobs.apply_event(message.value)
        except Exception:
            log.exception("results consumer failed, continuing")
            time.sleep(1)


def scan_periodically():
    """Scan once at startup, then every interval_seconds (0 = startup only;
    the setting is re-read each minute so it can be changed live)."""
    time.sleep(FIRST_SCAN_DELAY)
    first = True
    while True:
        interval = 0
        try:
            interval = int(load_config().get("discovery", {}).get("interval_seconds", 300))
            if first or interval > 0:
                request_scan()
        except Exception:
            log.exception("periodic scan failed")
        first = False
        time.sleep(interval if interval > 0 else 60)


threading.Thread(target=consume_results, daemon=True, name="results-consumer").start()
threading.Thread(target=scan_periodically, daemon=True, name="periodic-scan").start()

app = Flask(__name__)


@app.get("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/api/routers")
def list_routers():
    return jsonify({"routers": known_routers(), "last_scan": jobs.last_scan()})


@app.post("/api/discover")
def discover():
    job = request_scan()
    return jsonify(job), 503 if job.get("status") == "failed" else 202


@app.get("/api/jobs")
def list_jobs():
    return jsonify(jobs.list())


@app.get("/api/jobs/<job_id>")
def get_job(job_id):
    job = jobs.get(job_id)
    if job is None:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@app.post("/api/jobs")
def create_job():
    data = request.get_json(silent=True) or {}
    router = data.get("router")
    mode = data.get("mode", "exec")
    commands = data.get("commands", [])
    if isinstance(commands, str):
        commands = commands.splitlines()
    commands = [c.strip() for c in commands if isinstance(c, str) and c.strip()]

    host = next((r["host"] for r in known_routers() if r["name"] == router), None)
    if host is None:
        return jsonify({"error": f"unknown router {router!r}: scan the network first"}), 400
    if mode not in MODES:
        return jsonify({"error": f"mode must be one of {MODES}"}), 400
    if not commands:
        return jsonify({"error": "at least one command is required"}), 400
    if len(commands) > MAX_COMMANDS:
        return jsonify({"error": f"at most {MAX_COMMANDS} commands per job"}), 400

    job = publish_job(router, mode, commands, host)
    return jsonify(job), 503 if job.get("status") == "failed" else 202
