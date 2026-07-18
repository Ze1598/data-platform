-- Roadmap Phase 11 build plan, Step 4: the real, continuous Kafka -> Iceberg
-- sink. Step 3 (a hardcoded INSERT, no Kafka source) already proved the
-- Flink -> Polaris/MinIO write path works in isolation -- see Learnings.md
-- ("Flink + Kafka + Iceberg (streaming/ module)") for the two real gotchas
-- found getting there (the Hadoop classpath dependency, the AWS region/
-- credential env vars). This version adds the real Kafka source and turns
-- the one-shot proof into a genuinely continuous job.
--
-- Catalog properties are the Flink-SQL equivalent of
-- processing/raw_to_clean/raw_to_clean/catalog.py's `load_iceberg_catalog`
-- -- same underlying Iceberg REST catalog protocol/S3FileIO property
-- names, already proven live against this exact Polaris+MinIO setup by
-- every other layer of this platform (Trino, PyIceberg).
-- header.X-Iceberg-Access-Delegation='' is the Java-REST-catalog-client
-- equivalent of Trino's vended-credentials-enabled=false and PyIceberg's
-- own use of the same header -- disables server-vended S3 credentials,
-- which this catalog's stsUnavailable storage config can't satisfy anyway
-- (see Learnings.md, "Polaris `S3` storage type against MinIO").
--
-- Credentials hardcoded directly here rather than substituted from the
-- environment -- SqlRunner.java is vendored unmodified from the Flink
-- Kubernetes Operator's own example (a deliberate choice: zero
-- maintenance risk from patching someone else's reference code) and has
-- no env-var-substitution mechanism for a raw SQL file. Matches this
-- repo's own existing precedent for local-dev credentials in tracked
-- files (query-engine/polaris/deployment.yaml hardcodes MinIO credentials
-- directly; query-engine/polaris_client/polaris_client/bootstrap.py
-- hardcodes the OAuth2 client secret as a literal constant) -- same
-- root/s3cr3t, admin/password local-cluster credentials used everywhere
-- else in this repo, not a new secret.

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://polaris.query-engine.svc.cluster.local:8181/api/catalog',
    'warehouse' = 'data_platform',
    'credential' = 'root:s3cr3t',
    'oauth2-server-uri' = 'http://polaris.query-engine.svc.cluster.local:8181/api/catalog/v1/oauth/tokens',
    'scope' = 'PRINCIPAL_ROLE:ALL',
    'header.X-Iceberg-Access-Delegation' = '',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio.query-engine.svc.cluster.local:9000',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'password',
    's3.path-style-access' = 'true',
    's3.region' = 'us-east-1'
);

CREATE TABLE IF NOT EXISTS iceberg_catalog.streaming.sales_events (
    event_id STRING,
    event_type STRING,
    branch STRING,
    city STRING,
    product_line STRING,
    amount DOUBLE,
    event_timestamp TIMESTAMP(6)
) WITH ('format-version' = '2');

-- Event shape matches streaming/producer/'s synthetic generator (Step 5),
-- which itself mirrors orchestration/dagster_data_platform's real
-- sales_assets.py branch/city/product_line vocabulary -- so branch values
-- here genuinely match sales_dim_branch's actual rows for Step 6's join,
-- not an unrelated fictional set.
CREATE TABLE kafka_sales_events (
    event_id STRING,
    event_type STRING,
    branch STRING,
    city STRING,
    product_line STRING,
    amount DOUBLE,
    event_timestamp STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales-events',
    'properties.bootstrap.servers' = 'kafka.streaming.svc.cluster.local:9092',
    'properties.group.id' = 'flink-sales-events-sink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- CAST to TIMESTAMP(6): Iceberg only accepts microsecond-precision
-- timestamps (the same constraint Roadmap.md documents for dbt's
-- current_timestamp macro) -- the Kafka JSON deserializer only produces
-- STRING for the source column above, cast explicitly rather than relying
-- on an implicit conversion.
-- CONFIRMED LIVE GOTCHA: this CAST expects the SQL-standard
-- 'yyyy-MM-dd HH:mm:ss.SSSSSS' format (space separator) -- an ISO-8601
-- 'T'-separated string (e.g. '2026-07-18T14:40:00.000000') fails the CAST
-- *silently*: no exception, no log line, the row is just dropped (observed
-- directly: IcebergFilesCommitter logged a real commit attempt with
-- dataFilesCount=0 for that checkpoint). streaming/producer/ must emit
-- the space-separated format. See Learnings.md.
INSERT INTO iceberg_catalog.streaming.sales_events
SELECT
    event_id,
    event_type,
    branch,
    city,
    product_line,
    amount,
    CAST(event_timestamp AS TIMESTAMP(6))
FROM kafka_sales_events;
