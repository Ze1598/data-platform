"""Synthetic sales-event producer for the streaming/ module (Roadmap Phase
11). Mirrors this platform's existing synthetic in-memory generators
(customers/sales in orchestration/dagster_data_platform/dagster_data_platform/assets/)
-- there's no real real-time source yet, so this stands in for one the same
way those two feeds stand in for real batch sources, producing realistic
fake events on a timer rather than a fixed batch.

branch/city/product_line vocabulary matches sales_assets.py's own
_BRANCHES/_PRODUCT_LINES exactly, so values genuinely line up with
sales_dim_branch's real rows for the Step 6 serve-layer join, not an
unrelated fictional set.

event_timestamp is emitted in the SQL-standard space-separated format
('%Y-%m-%d %H:%M:%S.%f'), not Python's default `.isoformat()` ('T'
separator) -- the Flink sink's CAST(... AS TIMESTAMP(6)) silently drops any
row using the 'T'-separated form, a real gotcha found live and documented
in Learnings.md ("Flink SQL's CAST(string AS TIMESTAMP(n)) fails silently
on an ISO-8601 'T'-separated string").
"""

import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka.streaming.svc.cluster.local:9092"
)
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sales-events")
EVENT_INTERVAL_SECONDS = float(os.environ.get("EVENT_INTERVAL_SECONDS", "5"))

# Same vocabulary as orchestration/dagster_data_platform/dagster_data_platform/assets/sales_assets.py
_BRANCHES = [("A", "Yangon"), ("B", "Mandalay"), ("C", "Naypyitaw")]
_PRODUCT_LINES = [
    "Health and beauty",
    "Electronic accessories",
    "Home and lifestyle",
    "Sports and travel",
    "Food and beverages",
    "Fashion accessories",
]
_EVENT_TYPES = ["sale", "return"]

_running = True


def _handle_sigterm(signum, frame) -> None:
    global _running
    _running = False


def _generate_event() -> dict:
    branch, city = random.choice(_BRANCHES)
    return {
        "event_id": str(uuid4()),
        "event_type": random.choice(_EVENT_TYPES),
        "branch": branch,
        "city": city,
        "product_line": random.choice(_PRODUCT_LINES),
        "amount": round(random.uniform(5, 500), 2),
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
    }


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    print(f"Producing to {KAFKA_TOPIC} on {KAFKA_BOOTSTRAP_SERVERS} every {EVENT_INTERVAL_SECONDS}s", flush=True)

    while _running:
        event = _generate_event()
        producer.produce(KAFKA_TOPIC, json.dumps(event).encode("utf-8"))
        producer.poll(0)
        print(f"Produced: {event}", flush=True)
        time.sleep(EVENT_INTERVAL_SECONDS)

    print("Received SIGTERM, flushing remaining messages...", flush=True)
    producer.flush()
    sys.exit(0)


if __name__ == "__main__":
    main()
