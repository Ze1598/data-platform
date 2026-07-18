"""Generates the streaming module's ingestion plumbing (Flink SQL sink
scripts, one FlinkDeployment CR per source, and the Kafka topic-creation
Job) from `streaming_source` + its current `schema_registry` entry
(`controlling_object_type='streaming_source'`) -- Roadmap Phase 11
generalization, mirroring generate_serve_views.py's own build-time-script,
fully-regenerated-every-run posture. Unlike generate_model_scaffolds.py,
there is zero user business logic anywhere in this output -- the generated
SQL is pure mechanical plumbing (topic/schema/sink DDL, a passthrough
INSERT ... SELECT with the event-timestamp CAST); all enrichment happens
downstream in the hand-authored serve view instead (see
generate_streaming_serve_scaffolds.py). Safe to wipe-and-regenerate on
every run, same as generate_serve_views.py's own `generated/` directories.

One shared Flink image is built from this output (see
streaming/flink/module.just) -- every source's generated .sql script gets
baked into the same image; only the FlinkDeployment CR's `args` differs
per source, selecting which script that job runs.

A `streaming_source` row is skipped (with a clear message, not a crash)
if it has no current schema_registry entry yet (schema discovery hasn't
run) or no event_timestamp_column set -- both are real prerequisites, not
bugs to route around.
"""

import os
import shutil
from pathlib import Path

import psycopg

CONN_KWARGS = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    user=os.environ.get("POSTGRES_USER", "platform"),
    password=os.environ.get("POSTGRES_PASSWORD", "platform"),
    dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
)

REPO_ROOT = Path(__file__).resolve().parents[1]
STREAMING_DIR = REPO_ROOT / "streaming"

KAFKA_BOOTSTRAP_SERVERS = "kafka.streaming.svc.cluster.local:9092"
POLARIS_URI = "http://polaris.query-engine.svc.cluster.local:8181/api/catalog"
ICEBERG_WAREHOUSE = "data_platform"
MINIO_ENDPOINT = "http://minio.query-engine.svc.cluster.local:9000"
# Hardcoded local-dev credentials -- matches this repo's own existing
# precedent (query-engine/polaris/deployment.yaml, polaris_client/bootstrap.py)
# rather than needing env-var substitution the vendored SqlRunner has no
# mechanism for (see streaming/flink/sql-runner/, Learnings.md).
POLARIS_CREDENTIAL = "root:s3cr3t"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"

# schema_registry.column_definitions -> Flink SQL type. Every column reads
# as STRING from Kafka's JSON deserializer regardless of its "real" type
# (matches the proven sales_events_sink.sql pattern) except where a Flink
# native type is safe to declare directly on the source table.
_FLINK_TYPE_MAP = {
    "string": "STRING",
    "long": "BIGINT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    # timestamp columns still come across Kafka's JSON deserializer as
    # plain strings -- declared STRING on the source table, CAST to
    # TIMESTAMP(6) only for the one designated event_timestamp_column, at
    # INSERT time (see _render_sink_sql below) -- exactly the
    # already-proven sales_events_sink.sql shape.
    "timestamp": "STRING",
}


def fetch_candidates(cur) -> tuple[list[dict], list[dict]]:
    """Returns (ready, skipped) -- ready rows have both a current
    schema_registry entry and event_timestamp_column set; skipped rows
    are missing one or both prerequisites, reported but not treated as an
    error (a streaming_source mid-onboarding is a normal, expected state)."""
    cur.execute(
        """
        select ss.id, ss.friendly_name, ss.topic_name, ss.table_name, ss.model_schema,
               ss.event_timestamp_column, ss.jobmanager_memory, ss.taskmanager_memory,
               ss.taskmanager_cpu, ss.parallelism, ss.autoscaler_enabled,
               sr.column_definitions
        from streaming_source ss
        left join schema_registry sr
            on sr.controlling_object_id = ss.id
           and sr.controlling_object_type = 'streaming_source'
           and sr.is_current
        where ss.is_active = true
        order by ss.friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    ready, skipped = [], []
    for row in rows:
        if row["column_definitions"] is None:
            skipped.append({**row, "reason": "no schema_registry entry yet -- run 'Discover Schema' first"})
        elif not row["event_timestamp_column"]:
            skipped.append({**row, "reason": "event_timestamp_column not set"})
        else:
            ready.append(row)
    return ready, skipped


def _render_sink_sql(row: dict) -> str:
    columns = row["column_definitions"]
    kafka_cols = ",\n".join(f"    {c['name']} {_FLINK_TYPE_MAP[c['data_type']]}" for c in columns)
    iceberg_cols = ",\n".join(
        f"    {c['name']} {'TIMESTAMP(6)' if c['name'] == row['event_timestamp_column'] else _FLINK_TYPE_MAP[c['data_type']]}"
        for c in columns
    )
    select_cols = ",\n    ".join(
        f"CAST({c['name']} AS TIMESTAMP(6))" if c["name"] == row["event_timestamp_column"] else c["name"]
        for c in columns
    )
    kafka_table = f"kafka_{row['table_name']}"

    return f"""-- Generated by scripts/generate_streaming_ingestion.py -- DO NOT EDIT BY HAND.
-- Source: streaming_source '{row['friendly_name']}' (table_name={row['table_name']}).
-- Pure mechanical plumbing, no business logic -- enrichment happens in the
-- hand-authored serve view instead (see
-- dbt/domains/{row['model_schema']}/models/serve/streaming/{row['table_name']}.sql).

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = '{POLARIS_URI}',
    'warehouse' = '{ICEBERG_WAREHOUSE}',
    'credential' = '{POLARIS_CREDENTIAL}',
    'oauth2-server-uri' = '{POLARIS_URI}/v1/oauth/tokens',
    'scope' = 'PRINCIPAL_ROLE:ALL',
    'header.X-Iceberg-Access-Delegation' = '',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = '{MINIO_ENDPOINT}',
    's3.access-key-id' = '{MINIO_ACCESS_KEY}',
    's3.secret-access-key' = '{MINIO_SECRET_KEY}',
    's3.path-style-access' = 'true',
    's3.region' = 'us-east-1'
);

CREATE TABLE IF NOT EXISTS iceberg_catalog.streaming.{row['table_name']} (
{iceberg_cols}
) WITH ('format-version' = '2');

CREATE TABLE {kafka_table} (
{kafka_cols}
) WITH (
    'connector' = 'kafka',
    'topic' = '{row['topic_name']}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-{row['table_name']}-sink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

INSERT INTO iceberg_catalog.streaming.{row['table_name']}
SELECT
    {select_cols}
FROM {kafka_table};
"""


def k8s_name(table_name: str) -> str:
    """table_name (e.g. "sales_events") may contain underscores, which are
    valid Iceberg/SQL identifiers but NOT valid Kubernetes resource names
    (RFC 1123 subdomain -- lowercase alphanumeric and '-' only). Confirmed
    live: a FlinkDeployment named "sales_events-sink" is rejected outright
    by the API server. Used for every k8s-facing name derived from
    table_name (FlinkDeployment metadata.name, its generated filename) --
    SQL/filesystem-facing names (the sink script's own filename, the Kafka
    consumer group.id) keep the real table_name unchanged, since neither
    of those has this restriction."""
    return table_name.replace("_", "-")


def _render_flinkdeployment(row: dict) -> str:
    jm_memory = row["jobmanager_memory"] or "1024m"
    tm_memory = row["taskmanager_memory"] or "1024m"
    tm_cpu = float(row["taskmanager_cpu"]) if row["taskmanager_cpu"] else 0.5
    parallelism = row["parallelism"] or 1
    autoscaler_line = (
        "\n    job.autoscaler.enabled: \"true\"" if row["autoscaler_enabled"] else ""
    )

    return f"""# Generated by scripts/generate_streaming_ingestion.py -- DO NOT EDIT BY HAND.
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: {k8s_name(row['table_name'])}-sink
  namespace: streaming
spec:
  image: data-platform-streaming-flink:latest
  imagePullPolicy: IfNotPresent
  flinkVersion: v2_1
  serviceAccount: flink
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "1"
    execution.checkpointing.interval: "10s"
    execution.checkpointing.mode: EXACTLY_ONCE
    state.checkpoints.dir: file:///flink-checkpoints{autoscaler_line}
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: AWS_REGION
              value: us-east-1
            - name: AWS_ACCESS_KEY_ID
              value: {MINIO_ACCESS_KEY}
            - name: AWS_SECRET_ACCESS_KEY
              value: {MINIO_SECRET_KEY}
            - name: AWS_ENDPOINT_URL_S3
              value: {MINIO_ENDPOINT}
          volumeMounts:
            - name: checkpoints
              mountPath: /flink-checkpoints
      volumes:
        - name: checkpoints
          emptyDir: {{}}
  jobManager:
    resource:
      memory: "{jm_memory}"
      cpu: 0.5
  taskManager:
    resource:
      memory: "{tm_memory}"
      cpu: {tm_cpu}
  job:
    jarURI: local:///opt/flink/usrlib/sql-runner.jar
    args: ["/opt/flink/usrlib/sql-scripts/generated/{row['table_name']}_sink.sql"]
    parallelism: {parallelism}
    upgradeMode: stateless
"""


def _render_create_topics_job(rows: list[dict]) -> str:
    commands = "\n".join(
        f"              /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \\\n"
        f"                --bootstrap-server {KAFKA_BOOTSTRAP_SERVERS} \\\n"
        f"                --topic {row['topic_name']} --partitions 1 --replication-factor 1"
        for row in rows
    )
    return f"""# Generated by scripts/generate_streaming_ingestion.py -- DO NOT EDIT BY HAND.
# One partition/replication-factor 1 per topic -- matches the single Kafka
# broker and each source's own Flink job parallelism default.
apiVersion: batch/v1
kind: Job
metadata:
  name: kafka-create-topics
  namespace: streaming
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: create-topics
          image: apache/kafka:latest
          command:
            - sh
            - -c
            - |
{commands}
"""


def generate(ready: list[dict]) -> list[Path]:
    sql_dir = STREAMING_DIR / "flink" / "sql-scripts" / "generated"
    cr_dir = STREAMING_DIR / "flink" / "generated"
    kafka_dir = STREAMING_DIR / "kafka" / "generated"
    for d in (sql_dir, cr_dir, kafka_dir):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    written = []
    for row in ready:
        sql_path = sql_dir / f"{row['table_name']}_sink.sql"
        sql_path.write_text(_render_sink_sql(row))
        written.append(sql_path)

        cr_path = cr_dir / f"{k8s_name(row['table_name'])}-flinkdeployment.yaml"
        cr_path.write_text(_render_flinkdeployment(row))
        written.append(cr_path)

    if ready:
        topics_path = kafka_dir / "create-topics-job.yaml"
        topics_path.write_text(_render_create_topics_job(ready))
        written.append(topics_path)

    return written


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        ready, skipped = fetch_candidates(cur)

    written = generate(ready)
    print(f"Generated {len(written)} file(s) for {len(ready)} ready streaming_source row(s).")
    for p in written:
        print(f"  {p.relative_to(REPO_ROOT)}")
    for row in skipped:
        print(f"  SKIPPED '{row['friendly_name']}': {row['reason']}")


if __name__ == "__main__":
    main()
