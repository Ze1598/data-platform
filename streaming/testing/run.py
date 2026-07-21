"""Isolated streaming tests -- proves each active `streaming_source` works
end-to-end (Kafka -> Flink -> Iceberg -> dbt serve view) without requiring
the batch pipeline (master_pipeline) to have run first. Runs in-cluster
(see ../module.just) since Kafka's Service is ClusterIP-only.

Three modes, run in sequence by `streaming-testing::test`:

  setup       -- ensure_fixtures() (dummy model-layer dependency tables,
                 non-destructive) + seed_messages() (hand-maintained sample
                 messages per known source) + discover_schemas() (schema
                 discovery for any active source not yet discovered) --
                 everything needed before scripts/generate_streaming_ingestion.py
                 will consider a source "ready" to deploy.
  verify-raw  -- after the real sinks are deployed (module.just runs
                 generate-streaming-ingestion + kafka::start + flink::start
                 between setup and this stage): produces a further batch of
                 messages and confirms each source's raw Iceberg sink table
                 (iceberg.streaming.<table>) actually grows.
  verify-serve -- after module.just runs `dbt build --select tag:streaming`
                 (dbt is a host-side, not in-cluster, step -- see
                 module.just): confirms each source's dbt serve view
                 (iceberg.serve.<table>) returns real rows, proving the
                 hand-authored join/business-logic actually works against
                 live data, not just that the raw sink does.

Both verify stages matter separately -- a raw sink can grow correctly while
its serve view is still broken (wrong join key, wrong column reference),
and that's exactly the class of bug worth catching per-source without
requiring a full batch pipeline run (see _MODEL_LAYER_FIXTURES below for
how a serve view's model-layer join dependency is satisfied without one).
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

import polars as pl
import psycopg
import trino.dbapi
from confluent_kafka import Consumer, Producer
from connectors.inference import infer_column_definitions

TRINO_HOST = "trino.query-engine.svc.cluster.local"
TRINO_PORT = 8080
KAFKA_BOOTSTRAP_SERVERS = "kafka.streaming.svc.cluster.local:9092"
POSTGRES_CONN = dict(
    host=os.environ.get("POSTGRES_HOST", "postgres.metadata.svc.cluster.local"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    user=os.environ.get("POSTGRES_USER", "platform"),
    password=os.environ.get("POSTGRES_PASSWORD", "platform"),
    dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
)

# ---------------------------------------------------------------------------
# Dummy model-layer fixtures -- lets a streaming serve view's join resolve
# without a real master_pipeline run. Hand-maintained, one entry per real
# join a streaming serve view currently makes -- same posture as the join
# itself (dbt/domains/*/models/serve/streaming/*.sql is hand-authored
# business logic; nothing in metadata describes which dimension to join to
# or on what key, see generate_streaming_serve_scaffolds.py's own
# docstring). A new streaming source with a new join dependency needs a new
# entry here, by hand, same as it needs its own hand-written join.
#
# Non-destructive: CREATE TABLE IF NOT EXISTS never touches a real,
# batch-built table's schema, and rows are only inserted if the table is
# currently empty, checked immediately before insert -- a real dbt run may
# have legitimately built it with zero rows (e.g. no sales yet), so "empty"
# is a live check, not assumed from context.
#
# Branch/city vocabulary matches sales_assets.py's real _BRANCHES exactly
# (also mirrored by streaming/producer/producer/main.py and the synthetic
# message fixtures below), so a real sales_events message's `branch` value
# resolves against this fixture the same way it would against the real
# batch-built table.
_MODEL_LAYER_FIXTURES = {
    "model.sales_dim_branch": {
        "ddl": "branch VARCHAR, city VARCHAR, is_deleted BOOLEAN",
        "rows": [
            ("A", "Yangon", False),
            ("B", "Mandalay", False),
            ("C", "Naypyitaw", False),
        ],
    },
}

_BRANCHES = [("A", "Yangon"), ("B", "Mandalay"), ("C", "Naypyitaw")]
_PRODUCT_LINES = ["Health and beauty", "Electronic accessories", "Home and lifestyle"]


def _sales_events_batch(n: int) -> list[dict]:
    events = []
    for i in range(n):
        branch, city = _BRANCHES[i % len(_BRANCHES)]
        events.append(
            {
                "event_id": str(uuid4()),
                "event_type": "sale",
                "branch": branch,
                "city": city,
                "product_line": _PRODUCT_LINES[i % len(_PRODUCT_LINES)],
                "amount": round(12.5 + i, 2),
                "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
            }
        )
    return events


def _inventory_events_batch(n: int) -> list[dict]:
    events = []
    for i in range(n):
        branch, _city = _BRANCHES[i % len(_BRANCHES)]
        events.append(
            {
                "event_id": str(uuid4()),
                "sku": f"SKU-{1000 + i}",
                "branch": branch,
                "quantity_change": (i % 5) - 2,
                "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
            }
        )
    return events


# Hand-maintained, one entry per real streaming source currently onboarded
# -- same "no metadata describes the message shape ahead of discovery"
# reasoning as the fixtures above. A source with no entry here is skipped
# with a clear message, not silently ignored (see seed_messages()).
_MESSAGE_FIXTURES = {
    "sales_events": _sales_events_batch,
    "inventory_events": _inventory_events_batch,
}


def _sql_literal(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def ensure_fixtures() -> None:
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="streaming_testing", catalog="iceberg")
    cur = conn.cursor()
    for qualified_name, spec in _MODEL_LAYER_FIXTURES.items():
        cur.execute(f"CREATE TABLE IF NOT EXISTS iceberg.{qualified_name} ({spec['ddl']})")
        cur.fetchall()
        cur.execute(f"SELECT count(*) FROM iceberg.{qualified_name}")
        (count,) = cur.fetchone()
        if count == 0:
            values = ", ".join("(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in spec["rows"])
            cur.execute(f"INSERT INTO iceberg.{qualified_name} VALUES {values}")
            cur.fetchall()
            print(f"Seeded {len(spec['rows'])} dummy row(s) into {qualified_name} (was empty).")
        else:
            print(f"{qualified_name} already has {count} row(s) -- left untouched (likely real batch data).")


def _active_sources(cur) -> list[dict]:
    cur.execute(
        "select id, friendly_name, topic_name, table_name, model_schema "
        "from streaming_source where is_active = true order by friendly_name"
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def seed_source_messages(friendly_name: str, topic_name: str, count: int = 5) -> bool:
    if friendly_name not in _MESSAGE_FIXTURES:
        print(f"SKIPPED '{friendly_name}': no test fixture defined -- add one to _MESSAGE_FIXTURES in run.py")
        return False
    events = _MESSAGE_FIXTURES[friendly_name](count)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    for event in events:
        producer.produce(topic_name, json.dumps(event).encode("utf-8"))
    producer.flush(timeout=10)
    print(f"Produced {len(events)} test message(s) to topic '{topic_name}' for '{friendly_name}'.")
    return True


def seed_messages() -> list[str]:
    with psycopg.connect(**POSTGRES_CONN) as conn, conn.cursor() as cur:
        sources = _active_sources(cur)
    return [s["friendly_name"] for s in sources if seed_source_messages(s["friendly_name"], s["topic_name"])]


def _write_schema_registry_version(cur, *, controlling_object_id, column_definitions, created_by) -> None:
    # Mirrors PostgresMetadataResource.update_schema_registry's polymorphic
    # write path (orchestration/.../postgres_metadata_resource.py) exactly
    # -- duplicated here rather than imported (that resource lives inside
    # the orchestration package, which streaming-testing has no real
    # dependency on), but the first attempt at this duplication omitted the
    # `version` column entirely (NOT NULL, computed as
    # max(version)+1 there) and `effective_to`/`effective_from`, and failed
    # live with NotNullViolation -- fixed by copying the real INSERT
    # verbatim instead of reconstructing it from memory.
    cur.execute(
        "update schema_registry set is_current = false, effective_to = now() "
        "where controlling_object_id = %s and controlling_object_type = 'streaming_source' and is_current",
        (controlling_object_id,),
    )
    cur.execute(
        """
        insert into schema_registry (controlling_object_id, controlling_object_type, version, column_definitions, primary_key_columns, is_current, effective_from, created_by)
        values (
            %(controlling_object_id)s,
            'streaming_source',
            coalesce((select max(version) from schema_registry where controlling_object_id = %(controlling_object_id)s and controlling_object_type = 'streaming_source'), 0) + 1,
            %(column_definitions)s,
            %(primary_key_columns)s,
            true,
            now(),
            %(created_by)s
        )
        """,
        {
            "controlling_object_id": controlling_object_id,
            "column_definitions": json.dumps(column_definitions),
            "primary_key_columns": json.dumps([]),
            "created_by": created_by,
        },
    )


def _undiscovered_sources(cur) -> list[dict]:
    cur.execute(
        """
        select ss.id, ss.friendly_name, ss.topic_name
        from streaming_source ss
        left join schema_registry sr
            on sr.controlling_object_id = ss.id
           and sr.controlling_object_type = 'streaming_source'
           and sr.is_current
        where ss.is_active = true and ss.schema_discovery_enabled = true and sr.id is null
        order by ss.friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def discover_source(source: dict, sample_size: int = 5) -> bool:
    # sample_size defaults to 5, matching seed_source_messages()'s own
    # default `count` -- setup() only ever seeds 5 messages per source
    # before calling this, so a higher sample_size just polls needlessly
    # (confirmed live: sample_size=20 against 5 real messages made this
    # run the full 100-attempt/2.0s-timeout loop, ~200s, before giving up
    # with 5 messages anyway).
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"streaming-testing-discovery-{source['id']}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([source["topic_name"]])
    messages = []
    try:
        for _ in range(sample_size * 10):
            if len(messages) >= sample_size:
                break
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            try:
                messages.append(json.loads(msg.value()))
            except json.JSONDecodeError:
                continue
    finally:
        consumer.close()

    if not messages:
        print(f"SKIPPED '{source['friendly_name']}': no messages on topic '{source['topic_name']}' yet")
        return False

    column_definitions = infer_column_definitions(pl.DataFrame(messages))
    column_names = {c["name"] for c in column_definitions}
    with psycopg.connect(**POSTGRES_CONN) as conn, conn.cursor() as cur:
        _write_schema_registry_version(
            cur,
            controlling_object_id=source["id"],
            column_definitions=column_definitions,
            created_by="streaming_testing_run_py",
        )
        if "event_timestamp" in column_names:
            cur.execute(
                "update streaming_source set event_timestamp_column = 'event_timestamp' "
                "where id = %s and event_timestamp_column is null",
                (source["id"],),
            )
        else:
            print(
                f"  NOTE: '{source['friendly_name']}' has no 'event_timestamp' column -- "
                "set event_timestamp_column by hand via the frontend."
            )
        conn.commit()
    print(f"Discovered {len(column_definitions)} column(s) for '{source['friendly_name']}' from {len(messages)} message(s).")
    return True


def discover_schemas() -> None:
    with psycopg.connect(**POSTGRES_CONN) as conn, conn.cursor() as cur:
        sources = _undiscovered_sources(cur)

    if not sources:
        print("Every active streaming_source already has a current schema_registry entry.")
        return

    failures = [s["friendly_name"] for s in sources if not discover_source(s)]
    if failures:
        raise SystemExit(f"Discovery failed for: {', '.join(failures)}")


def setup() -> None:
    ensure_fixtures()
    produced = seed_messages()
    if not produced:
        raise SystemExit("No source had a message fixture to seed with -- cannot discover any schema.")
    print("Waiting briefly for seeded messages to land before discovery...")
    time.sleep(3)
    discover_schemas()


# ---------------------------------------------------------------------------
# Verification


def _ready_sources(cur) -> list[dict]:
    cur.execute(
        """
        select ss.friendly_name, ss.topic_name, ss.table_name, ss.model_schema
        from streaming_source ss
        join schema_registry sr
            on sr.controlling_object_id = ss.id
           and sr.controlling_object_type = 'streaming_source'
           and sr.is_current
        where ss.is_active = true and ss.event_timestamp_column is not null
        order by ss.friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _row_count(cur, schema: str, table: str) -> int:
    try:
        cur.execute(f"select count(*) from iceberg.{schema}.{table}")
        (count,) = cur.fetchone()
        return count
    except Exception as e:
        if "does not exist" in str(e) or "not found" in str(e):
            return 0
        raise


def verify_raw() -> None:
    with psycopg.connect(**POSTGRES_CONN) as conn, conn.cursor() as cur:
        sources = _ready_sources(cur)
    if not sources:
        raise SystemExit("No ready streaming_source rows to verify -- nothing deployed?")

    trino_conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="streaming_testing", catalog="iceberg")
    cur = trino_conn.cursor()

    baseline = {s["table_name"]: _row_count(cur, "streaming", s["table_name"]) for s in sources}
    produced = [s["friendly_name"] for s in sources if seed_source_messages(s["friendly_name"], s["topic_name"], count=3)]
    if not produced:
        raise SystemExit("No ready source had a message fixture to produce with -- cannot verify growth.")

    print("Waiting for Flink's checkpoint interval (10s) to commit the new rows...")
    time.sleep(20)

    failures = []
    for s in sources:
        if s["friendly_name"] not in produced:
            continue
        before = baseline[s["table_name"]]
        after = _row_count(cur, "streaming", s["table_name"])
        if after <= before:
            failures.append(f"{s['friendly_name']}: row count did not grow ({before} -> {after})")
        else:
            print(f"  {s['friendly_name']}: iceberg.streaming.{s['table_name']} {before} -> {after} rows (growth confirmed).")

    if failures:
        raise SystemExit("verify-raw FAILED:\n  " + "\n  ".join(failures))
    print(f"verify-raw passed for {len(produced)} source(s).")


def verify_serve() -> None:
    with psycopg.connect(**POSTGRES_CONN) as conn, conn.cursor() as cur:
        sources = _ready_sources(cur)
    if not sources:
        raise SystemExit("No ready streaming_source rows to verify -- nothing deployed?")

    trino_conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="streaming_testing", catalog="iceberg")
    cur = trino_conn.cursor()

    failures = []
    for s in sources:
        count = _row_count(cur, "serve", s["table_name"])
        if count == 0:
            failures.append(f"{s['friendly_name']}: serve.{s['table_name']} has zero rows after dbt build")
        else:
            print(f"  {s['friendly_name']}: iceberg.serve.{s['table_name']} has {count} row(s).")

    if failures:
        raise SystemExit("verify-serve FAILED:\n  " + "\n  ".join(failures))
    print(f"verify-serve passed for {len(sources)} source(s).")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    {
        "setup": setup,
        "verify-raw": verify_raw,
        "verify-serve": verify_serve,
    }.get(mode, lambda: (_ for _ in ()).throw(SystemExit(f"Unknown mode '{mode}' -- expected setup|verify-raw|verify-serve")))()


if __name__ == "__main__":
    main()
