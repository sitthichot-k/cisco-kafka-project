"""Backend API: accepts jobs from the frontend, publishes them to Kafka,
and tracks their status from the results topic written by the worker."""

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [backend] %(message)s")
log = logging.getLogger(__name__)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_routers():
    # Read on every call so edits to routers.json apply without a restart.
    with open(ROUTERS_FILE, encoding="utf-8") as f:
        return json.load(f)["routers"]


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
    """In-memory job table. The results topic is replayed from the start on
    boot, so finished jobs survive a backend restart."""

    def __init__(self):
        self._jobs = OrderedDict()
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
                job = {k: event.get(k) for k in ("job_id", "router", "mode", "commands", "submitted_at")}
                self._jobs[job_id] = job
            if job.get("status") in TERMINAL_STATUSES and event.get("status") not in TERMINAL_STATUSES:
                return
            for key in ("status", "output", "error", "worker", "started_at", "finished_at"):
                if key in event:
                    job[key] = event[key]

    def get(self, job_id):
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job else None

    def list(self):
        with self._lock:
            return [dict(j) for j in reversed(self._jobs.values())]


jobs = JobStore()
producer = connect_kafka(
    lambda: KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    ),
    "producer",
)


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


threading.Thread(target=consume_results, daemon=True, name="results-consumer").start()

app = Flask(__name__)


@app.get("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/api/routers")
def list_routers():
    routers = [
        {"name": r["name"], "host": r["host"], "description": r.get("description", "")}
        for r in load_routers()
    ]
    return jsonify(routers)


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

    if router not in {r["name"] for r in load_routers()}:
        return jsonify({"error": f"unknown router: {router!r}"}), 400
    if mode not in MODES:
        return jsonify({"error": f"mode must be one of {MODES}"}), 400
    if not commands:
        return jsonify({"error": "at least one command is required"}), 400
    if len(commands) > MAX_COMMANDS:
        return jsonify({"error": f"at most {MAX_COMMANDS} commands per job"}), 400

    job = {
        "job_id": uuid.uuid4().hex[:12],
        "router": router,
        "mode": mode,
        "commands": commands,
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
        return jsonify(jobs.get(job["job_id"])), 503

    log.info("queued job %s for %s: %s", job["job_id"], router, commands)
    return jsonify(jobs.get(job["job_id"])), 202
