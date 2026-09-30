"""Worker: consumes jobs from Kafka, runs them on the GNS3 routers over SSH,
and publishes status updates and output back to the results topic."""

import json
import logging
import os
import socket
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from cisco_ssh import RouterError, find_router, run_commands

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
COMMAND_TOPIC = os.environ.get("COMMAND_TOPIC", "cisco-commands")
RESULT_TOPIC = os.environ.get("RESULT_TOPIC", "cisco-results")
GROUP_ID = os.environ.get("WORKER_GROUP_ID", "cisco-workers")
WORKER_NAME = socket.gethostname()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(message)s")
log = logging.getLogger(__name__)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def connect_kafka(factory, what):
    while True:
        try:
            client = factory()
            log.info("connected %s to Kafka at %s", what, KAFKA_BOOTSTRAP)
            return client
        except NoBrokersAvailable:
            log.warning("Kafka not reachable for %s, retrying in 3s", what)
            time.sleep(3)


def handle(job, producer):
    router_name = job.get("router", "")

    def publish(**fields):
        # Echo the job so the backend can rebuild its history from this topic alone.
        event = {
            "job_id": job.get("job_id"),
            "router": router_name,
            "mode": job.get("mode"),
            "commands": job.get("commands"),
            "submitted_at": job.get("submitted_at"),
            "worker": WORKER_NAME,
            **fields,
        }
        producer.send(RESULT_TOPIC, key=router_name, value=event)
        producer.flush()

    log.info("job %s -> %s (%s): %s", job.get("job_id"), router_name, job.get("mode"), job.get("commands"))
    publish(status="running", started_at=now_iso())
    try:
        router = find_router(router_name)
        output = run_commands(router, job.get("commands") or [], job.get("mode", "exec"))
        publish(status="success", output=output, finished_at=now_iso())
        log.info("job %s succeeded", job.get("job_id"))
    except RouterError as exc:
        publish(status="failed", error=str(exc), finished_at=now_iso())
        log.warning("job %s failed: %s", job.get("job_id"), exc)
    except Exception as exc:
        publish(status="failed", error=f"worker error: {type(exc).__name__}: {exc}", finished_at=now_iso())
        log.exception("job %s crashed", job.get("job_id"))


def main():
    producer = connect_kafka(
        lambda: KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        ),
        "producer",
    )
    consumer = connect_kafka(
        lambda: KafkaConsumer(
            COMMAND_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        ),
        "consumer",
    )
    log.info("%s waiting for jobs on %s", WORKER_NAME, COMMAND_TOPIC)
    for message in consumer:
        if isinstance(message.value, dict):
            handle(message.value, producer)
        else:
            log.warning("skipping malformed message at offset %s", message.offset)


if __name__ == "__main__":
    main()
