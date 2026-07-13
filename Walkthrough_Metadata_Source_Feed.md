# Walkthrough: Ingesting the Platform's Own Metadata DB as a Source

A second worked example, deliberately different in kind from
`Walkthrough_New_Feed.md`: the source this time is `platform_metadata`
itself — the platform ingesting its own operational history. There's not
much analytical substance here (it's the platform's own housekeeping data,
not a business dataset), but it exercises two things the CSV walkthrough
didn't:

1. **A genuinely new landing-asset pattern.** Every existing feed with
   `system_type='database'` (`customers`, `sales`) is actually a synthetic
   in-memory stub — nothing in this codebase today runs a real SQL query
   against a live database as its extraction step. This is the first one
   that does.
2. **A role-playing dimension.** The fact table here (`data_processing_runs`)
   has *two* possible dimension joins per row — a feed-run row references
   `data_feed`, a model-run row references `lakehouse_models`, never both
   (enforced by a real `CHECK` constraint on the source table). The
   downstream fact has to tolerate exactly one of its two dimension keys
   being null on any given row — a genuine, common Kimball pattern
   (conditional/role-playing dimensions), not a contrivance for this
   exercise.

Target shape: `fct_metadata_runs` (fact) + `dim_metadata_feed` +
`dim_metadata_model` (the two dims, joined conditionally).

Same tagging convention as the first walkthrough: **[UI]** / **[Code]** /
**[Command]**.

---

## 0. (Optional) Platform startup

```bash
just start
```

### Accessing running services

| Service | URL / address | Notes |
|---|---|---|
| Streamlit (frontend CRUD) | `http://localhost:8501` | Local process (`frontend::start`), not in-cluster — just a normal localhost port |
| Dagster UI (Dagit) | `http://localhost:3000` | Local process (`orchestration::start`), same as above |
| Postgres (`platform_metadata`) | `localhost:5432` | In-cluster, but NodePort-mapped straight through by kind (see `platform/kind/kind-cluster.yaml`) — no port-forward needed. `psql -h localhost -U platform -d platform_metadata` (password `platform`, or your `.env` values). This is also the source you're extracting *from* in this walkthrough — same address, same credentials, the platform querying itself |
| Trino | `localhost:8080` | Same NodePort setup as Postgres — no port-forward needed |
| Polaris (Iceberg REST catalog) | in-cluster only | Not NodePort-mapped — `kubectl port-forward -n query-engine svc/polaris 8181:8181 &` first if you need to reach it directly |
| MinIO (S3-compatible storage) | in-cluster only | Same as Polaris — `kubectl port-forward -n query-engine svc/minio 9000:9000 &` (add `9001:9001` for the console) if you need to inspect it |

Anything that's a **local process** (Streamlit, Dagit) is just a normal port
on your machine. Anything **in-cluster with a NodePort** (Postgres, Trino)
is mapped straight through by the kind cluster config — also just
`localhost:<port>`, no forwarding step. Anything **in-cluster without a
NodePort** (Polaris, MinIO) needs an explicit `kubectl port-forward` — see
`platform/DebugReference.md` for the general pattern.

## 1. Register the source system **[UI]**

Streamlit → **Source Systems** → *Add new*.

| Field | Value |
|---|---|
| Code | `platform_metadata_self` |
| Name | `Platform metadata DB (self)` |
| System type | `database` — the first *real* one; `connection_config`/`base_location` document a live Postgres, not a stub |
| Base location | `postgres.metadata.svc.cluster.local:5432/platform_metadata` |
| Connection user | `platform` |
| Connection secret | `POSTGRES_PASSWORD env var (K8sRunLauncher already injects it into every launched pod)` |

No new credentials to provision: every pod `K8sRunLauncher` launches
already has `POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_USER`/
`POSTGRES_PASSWORD`/`POSTGRES_DB` set (see `dagster.yaml`'s
`run_launcher.config.env_vars` — the same env vars
`PostgresMetadataResource` itself uses). The new landing asset just reads
those directly; `connection_user`/`connection_secret` above are
documentation, not something the code reads (same as every other feed).

## 2. Register the data feed **[UI]**

Streamlit → **Data Feeds** → *Add new*.

| Field | Value |
|---|---|
| Source system | `platform_metadata_self` |
| Friendly name | `metadata_runs` |
| Source object name | `data_processing_runs (joined to data_feed, lakehouse_models)` |
| Batch group | `<New batch>` → `metadata_runs` |
| Batch feed hierarchy | `0` |
| Extraction type | `full` (simplest for a learning exercise — re-pulls the whole join each run; could become `incremental` on `job_started_timestamp` later) |
| Source PK columns (JSON array) | `["run_id"]` |
| Processing engine | `polars` |

## 3. Register the expected schema **[Command]**

The landing query (step 4) flattens `data_processing_runs` joined to
`data_feed`/`lakehouse_models` into one wide row — the schema describes
that flattened shape, not the raw table layout:

```bash
kubectl exec -n metadata postgres-0 -- psql -U platform -d platform_metadata -c "
INSERT INTO schema_registry (data_feed_id, version, column_definitions, is_current, created_by)
SELECT id, 1, '[
  {\"name\": \"run_id\",                        \"data_type\": \"string\",  \"nullable\": false, \"ordinal\": 1,  \"description\": \"Business key\"},
  {\"name\": \"data_feed_id\",                   \"data_type\": \"string\",  \"nullable\": true,  \"ordinal\": 2,  \"description\": \"Set for feed-run rows only\"},
  {\"name\": \"model_key\",                      \"data_type\": \"string\",  \"nullable\": true,  \"ordinal\": 3,  \"description\": \"Set for model-run rows only\"},
  {\"name\": \"tracking_group\",                 \"data_type\": \"string\",  \"nullable\": false, \"ordinal\": 4,  \"description\": \"batch_group or model_schema value\"},
  {\"name\": \"tracking_group_type\",             \"data_type\": \"string\",  \"nullable\": false, \"ordinal\": 5,  \"description\": \"batch_group | model_schema\"},
  {\"name\": \"dagster_run_id\",                  \"data_type\": \"string\",  \"nullable\": false, \"ordinal\": 6,  \"description\": \"Dagster run id\"},
  {\"name\": \"job_started_timestamp\",           \"data_type\": \"timestamp\", \"nullable\": false, \"ordinal\": 7,  \"description\": \"Run start\"},
  {\"name\": \"job_ended_timestamp\",             \"data_type\": \"timestamp\", \"nullable\": true,  \"ordinal\": 8,  \"description\": \"Run end\"},
  {\"name\": \"job_successful\",                  \"data_type\": \"boolean\", \"nullable\": true,  \"ordinal\": 9,  \"description\": \"Overall success\"},
  {\"name\": \"landing_rows_read\",                \"data_type\": \"long\",    \"nullable\": true,  \"ordinal\": 10, \"description\": \"Landing stage row count\"},
  {\"name\": \"raw_rows_read\",                    \"data_type\": \"long\",    \"nullable\": true,  \"ordinal\": 11, \"description\": \"Raw stage row count\"},
  {\"name\": \"clean_rows_inserted\",               \"data_type\": \"long\",    \"nullable\": true,  \"ordinal\": 12, \"description\": \"Clean stage rows written\"},
  {\"name\": \"staging_rows_updated\",              \"data_type\": \"long\",    \"nullable\": true,  \"ordinal\": 13, \"description\": \"Staging stage rows affected\"},
  {\"name\": \"model_rows_updated\",                \"data_type\": \"long\",    \"nullable\": true,  \"ordinal\": 14, \"description\": \"Model stage rows affected\"},
  {\"name\": \"serve_rows_read\",                   \"data_type\": \"long\",    \"nullable\": true,  \"ordinal\": 15, \"description\": \"Serve stage row count\"},
  {\"name\": \"feed_friendly_name\",                \"data_type\": \"string\",  \"nullable\": true,  \"ordinal\": 16, \"description\": \"data_feed.friendly_name, null on model-run rows\"},
  {\"name\": \"feed_batch_group_friendly_name\",    \"data_type\": \"string\",  \"nullable\": true,  \"ordinal\": 17, \"description\": \"data_feed.batch_group_friendly_name\"},
  {\"name\": \"feed_extraction_type\",              \"data_type\": \"string\",  \"nullable\": true,  \"ordinal\": 18, \"description\": \"data_feed.extraction_type\"},
  {\"name\": \"feed_processing_engine\",            \"data_type\": \"string\",  \"nullable\": true,  \"ordinal\": 19, \"description\": \"data_feed.processing_engine\"},
  {\"name\": \"feed_is_active\",                    \"data_type\": \"boolean\", \"nullable\": true,  \"ordinal\": 20, \"description\": \"data_feed.is_active\"},
  {\"name\": \"model_friendly_name\",               \"data_type\": \"string\",  \"nullable\": true,  \"ordinal\": 21, \"description\": \"lakehouse_models.friendly_name, null on feed-run rows\"},
  {\"name\": \"model_model_schema\",                \"data_type\": \"string\",  \"nullable\": true,  \"ordinal\": 22, \"description\": \"lakehouse_models.model_schema\"},
  {\"name\": \"model_table_type\",                  \"data_type\": \"string\",  \"nullable\": true,  \"ordinal\": 23, \"description\": \"lakehouse_models.table_type\"},
  {\"name\": \"model_scd_type\",                    \"data_type\": \"long\",    \"nullable\": true,  \"ordinal\": 24, \"description\": \"lakehouse_models.scd_type\"},
  {\"name\": \"model_updates_enabled\",              \"data_type\": \"boolean\", \"nullable\": true,  \"ordinal\": 25, \"description\": \"lakehouse_models.updates_enabled\"},
  {\"name\": \"model_deletes_enabled\",              \"data_type\": \"boolean\", \"nullable\": true,  \"ordinal\": 26, \"description\": \"lakehouse_models.deletes_enabled\"}
]'::jsonb, true, 'walkthrough'
FROM data_feed WHERE friendly_name = 'metadata_runs';
"
```

## 4. Write the landing/raw/clean Dagster assets **[Code]**

The new part: `landing_metadata_runs` runs a **real SQL query**, not a
stub or a file read — the first genuine live-database extraction in this
codebase. `raw_metadata_runs` still writes a durable copy (no file to
copy this time, since there was never a landing *file* — this feed is a
direct-connect source, same category as `police_crimes`'s API, not a
file-drop like `financial_transactions`: it dumps the queried DataFrame
itself to parquet).

New file:
`orchestration/dagster_data_platform/dagster_data_platform/assets/metadata_runs_assets.py`

```python
import os
from pathlib import Path

import polars as pl
import psycopg
from dagster import AssetExecutionContext, Output, asset

from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

FEED_FRIENDLY_NAME = "metadata_runs"
FEED_POOL = f"feed:{FEED_FRIENDLY_NAME}"
RAW_SUBDIR = "raw/metadata_runs"

REPO_ROOT = Path(__file__).resolve().parents[4]

_QUERY = """
    select
        r.run_id::text as run_id, r.data_feed_id::text as data_feed_id, r.model_key,
        r.tracking_group, r.tracking_group_type, r.dagster_run_id,
        r.job_started_timestamp, r.job_ended_timestamp, r.job_successful,
        r.landing_rows_read, r.raw_rows_read, r.clean_rows_inserted,
        r.staging_rows_updated, r.model_rows_updated, r.serve_rows_read,
        df.friendly_name as feed_friendly_name,
        df.batch_group_friendly_name as feed_batch_group_friendly_name,
        df.extraction_type as feed_extraction_type,
        df.processing_engine as feed_processing_engine,
        df.is_active as feed_is_active,
        lm.friendly_name as model_friendly_name,
        lm.model_schema as model_model_schema,
        lm.table_type as model_table_type,
        lm.scd_type as model_scd_type,
        lm.updates_enabled as model_updates_enabled,
        lm.deletes_enabled as model_deletes_enabled
    from data_processing_runs r
    left join data_feed df on df.id = r.data_feed_id
    left join lakehouse_models lm on lm.friendly_name = r.model_key
"""


def _data_lake_dir() -> Path:
    return Path(os.environ["DATA_LAKE_PATH"]) if "DATA_LAKE_PATH" in os.environ else REPO_ROOT / "data-lake"


def _raw_dir() -> Path:
    return _data_lake_dir() / RAW_SUBDIR


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def landing_metadata_runs(
    context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="landing",
        dagster_run_id=context.run_id,
    ) as log:
        # A real live-database query -- same connection env vars every
        # other resource in this codebase already uses (K8sRunLauncher
        # injects them into every launched pod), no new credentials.
        with psycopg.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ.get("POSTGRES_USER", "platform"),
            password=os.environ.get("POSTGRES_PASSWORD", "platform"),
            dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
        ) as conn, conn.cursor() as cur:
            cur.execute(_QUERY)
            columns = [d.name for d in cur.description]
            rows = cur.fetchall()
        df = pl.DataFrame(rows, schema=columns, orient="row") if rows else pl.DataFrame()
        log.set_counts(rows_read=df.height)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def raw_metadata_runs(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_metadata_runs: pl.DataFrame,
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="raw",
        dagster_run_id=context.run_id,
    ) as log:
        df = landing_metadata_runs
        if not df.is_empty():
            raw_run_dir = _raw_dir() / f"run_id={context.run_id}"
            raw_run_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(raw_run_dir / "metadata_runs.parquet")
        log.set_counts(rows_read=df.height)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def clean_metadata_runs(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_metadata_runs: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    df = raw_metadata_runs
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="clean",
        dagster_run_id=context.run_id,
    ) as log:
        if not df.is_empty():
            column_definitions = postgres_metadata.get_current_schema(str(data_feed["id"]))
            reconciliation = reconcile_schema(df, column_definitions)
            df = reconciliation.df
            schema_changed = reconciliation.updated_column_definitions is not None
            if schema_changed:
                postgres_metadata.update_schema_registry(
                    data_feed_id=str(data_feed["id"]),
                    column_definitions=reconciliation.updated_column_definitions,
                    created_by="clean_metadata_runs",
                )
                column_definitions = reconciliation.updated_column_definitions

            validate_schema(df, column_definitions)

            catalog = iceberg_catalog.get_catalog()
            write_clean_snapshot(
                catalog,
                namespace="clean",
                table_name="metadata_runs",
                df=df,
                column_definitions=column_definitions,
                schema_changed=schema_changed,
            )
        log.set_counts(rows_inserted=df.height)

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": df.height})
```

Register the 3 assets in `definitions.py`, and add `"metadata_runs"` to
`dbt_assets.py`'s `_CLEAN_SOURCE_TABLES` — same two edits as the first
walkthrough's step 4.

## 5. Declare the dbt source **[Code]**

Add to `_sources.yml`:

```yaml
      - name: metadata_runs
```

## 6. Write the staging model **[Code]**

`dbt/data_platform/models/staging/stg_metadata_runs.sql` — same shape as
every other staging model, just a wider column list:

```sql
{{
  config(
    unique_key='_key_hash',
    alias='metadata_runs',
    tags=['metadata_runs']
  )
}}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with source_raw as (

    select
        cast(run_id as varchar) as run_id,
        cast(data_feed_id as varchar) as data_feed_id,
        cast(model_key as varchar) as model_key,
        cast(tracking_group as varchar) as tracking_group,
        cast(tracking_group_type as varchar) as tracking_group_type,
        cast(dagster_run_id as varchar) as dagster_run_id,
        cast(job_started_timestamp as timestamp(6) with time zone) as job_started_timestamp,
        cast(job_ended_timestamp as timestamp(6) with time zone) as job_ended_timestamp,
        cast(job_successful as boolean) as job_successful,
        cast(landing_rows_read as bigint) as landing_rows_read,
        cast(raw_rows_read as bigint) as raw_rows_read,
        cast(clean_rows_inserted as bigint) as clean_rows_inserted,
        cast(staging_rows_updated as bigint) as staging_rows_updated,
        cast(model_rows_updated as bigint) as model_rows_updated,
        cast(serve_rows_read as bigint) as serve_rows_read,
        cast(feed_friendly_name as varchar) as feed_friendly_name,
        cast(feed_batch_group_friendly_name as varchar) as feed_batch_group_friendly_name,
        cast(feed_extraction_type as varchar) as feed_extraction_type,
        cast(feed_processing_engine as varchar) as feed_processing_engine,
        cast(feed_is_active as boolean) as feed_is_active,
        cast(model_friendly_name as varchar) as model_friendly_name,
        cast(model_model_schema as varchar) as model_model_schema,
        cast(model_table_type as varchar) as model_table_type,
        cast(model_scd_type as bigint) as model_scd_type,
        cast(model_updates_enabled as boolean) as model_updates_enabled,
        cast(model_deletes_enabled as boolean) as model_deletes_enabled,
        {{ row_hash(['run_id']) }} as _key_hash,
        {{ row_hash(['job_successful', 'job_ended_timestamp', 'landing_rows_read', 'raw_rows_read', 'clean_rows_inserted', 'staging_rows_updated', 'model_rows_updated', 'serve_rows_read']) }} as _attr_hash
    from {{ source('clean', 'metadata_runs') }}

)

{% if is_incremental() %}

, source as (
    {{ classify_changes('source_raw', updates_enabled) }}
)

{% endif %}

select *, {{ dbt.current_timestamp() }} as _loaded_at
from {{ 'source' if is_incremental() else 'source_raw' }}
```

`updates_enabled = true` here on purpose — unlike the CSV walkthrough's
immutable snapshot rows, a `data_processing_runs` row genuinely mutates
across its own lifecycle (each stage's columns fill in as the run
progresses), so this feed is one of the few where attribute-hash change
tracking is actually doing real work, not just satisfying a default.

## 7. Write the model-layer dbt models **[Code]**

Two dims, each derived from the *distinct*, non-null slice of
`stg_metadata_runs` relevant to it — every feed-run row leaves
`model_friendly_name` null, every model-run row leaves `feed_friendly_name`
null, so each dim's `where` clause is what keeps the other kind of row out.

`dbt/data_platform/models/model/dimensions/dim_metadata_feed.sql`:

```sql
{{ config(schema='model', unique_key='_key_hash', alias='dim_metadata_feed', tags=['metadata_runs']) }}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (
    select distinct
        feed_friendly_name, feed_batch_group_friendly_name,
        feed_extraction_type, feed_processing_engine, feed_is_active,
        false as is_deleted
    from {{ ref('stg_metadata_runs') }}
    where feed_friendly_name is not null
),
hashed as (
    select *,
        {{ row_hash(['feed_friendly_name']) }} as _key_hash,
        {{ row_hash(['feed_batch_group_friendly_name', 'feed_extraction_type', 'feed_processing_engine', 'feed_is_active', 'is_deleted']) }} as _attr_hash
    from base
)
{% if is_incremental() %}
, to_merge as ({{ classify_changes('hashed', updates_enabled) }})
{% endif %}
select *,
    cast(null as varchar) as _scd_id, cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to, {{ dbt.current_timestamp() }} as _updated_at
from {{ 'to_merge' if is_incremental() else 'hashed' }}
```

`dbt/data_platform/models/model/dimensions/dim_metadata_model.sql` —
identical shape to `dim_metadata_feed.sql`, filtered to the other side of
the role-playing split (`where model_friendly_name is not null` instead of
`feed_friendly_name`), business key `model_friendly_name`, tracking
`model_model_schema`/`model_table_type`/`model_scd_type`/
`model_updates_enabled`/`model_deletes_enabled`:

```sql
{{ config(schema='model', unique_key='_key_hash', alias='dim_metadata_model', tags=['metadata_runs']) }}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (
    select distinct
        model_friendly_name, model_model_schema, model_table_type,
        model_scd_type, model_updates_enabled, model_deletes_enabled,
        false as is_deleted
    from {{ ref('stg_metadata_runs') }}
    where model_friendly_name is not null
),
hashed as (
    select *,
        {{ row_hash(['model_friendly_name']) }} as _key_hash,
        {{ row_hash(['model_model_schema', 'model_table_type', 'model_scd_type', 'model_updates_enabled', 'model_deletes_enabled', 'is_deleted']) }} as _attr_hash
    from base
)
{% if is_incremental() %}
, to_merge as ({{ classify_changes('hashed', updates_enabled) }})
{% endif %}
select *,
    cast(null as varchar) as _scd_id, cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to, {{ dbt.current_timestamp() }} as _updated_at
from {{ 'to_merge' if is_incremental() else 'hashed' }}
```

`dbt/data_platform/models/model/facts/fct_metadata_runs.sql` — the
role-playing join: **both** dimension joins are `left join`, and exactly
one of `feed_key`/`lakehouse_model_key` is non-null on any given row,
mirroring the source's own `data_feed_id`/`model_key` exclusivity:

```sql
{{ config(schema='model', unique_key='_key_hash', alias='fct_metadata_runs', tags=['metadata_runs']) }}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (
    select
        s.run_id, s.dagster_run_id, s.tracking_group, s.tracking_group_type,
        s.job_started_timestamp, s.job_ended_timestamp, s.job_successful,
        s.landing_rows_read, s.raw_rows_read, s.clean_rows_inserted,
        s.staging_rows_updated, s.model_rows_updated, s.serve_rows_read,
        f._key_hash as feed_key,
        m._key_hash as lakehouse_model_key,
        false as is_deleted
    from {{ ref('stg_metadata_runs') }} s
    left join {{ ref('dim_metadata_feed') }} f on s.feed_friendly_name = f.feed_friendly_name
    left join {{ ref('dim_metadata_model') }} m on s.model_friendly_name = m.model_friendly_name
),
hashed as (
    select *,
        {{ row_hash(['run_id']) }} as _key_hash,
        {{ row_hash(['job_successful', 'job_ended_timestamp', 'landing_rows_read', 'raw_rows_read', 'clean_rows_inserted', 'staging_rows_updated', 'model_rows_updated', 'serve_rows_read', 'is_deleted']) }} as _attr_hash
    from base
)
{% if is_incremental() %}
, to_merge as ({{ classify_changes('hashed', updates_enabled) }})
{% endif %}
select *,
    cast(null as varchar) as _scd_id, cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to, {{ dbt.current_timestamp() }} as _updated_at
from {{ 'to_merge' if is_incremental() else 'hashed' }}
```

Add `schema.yml` entries for all three (same `_key_hash`/`_attr_hash`
`not_null`/`unique` pattern as before).

## 8. Register the 3 lakehouse models **[UI]**

Streamlit → **Lakehouse Models** → *Add new*, three times:

| Field | `dim_metadata_feed` | `dim_metadata_model` | `fct_metadata_runs` |
|---|---|---|---|
| Model schema | `model` | `model` | `model` |
| Table type | `dimension` | `dimension` | `fact` |
| Depends on feeds | `metadata_runs` | `metadata_runs` | `metadata_runs` |
| Owning feed | `metadata_runs` | `metadata_runs` | `metadata_runs` |
| Business key columns | `["feed_friendly_name"]` | `["model_friendly_name"]` | `["run_id"]` |
| Tracked columns | `["feed_batch_group_friendly_name", "feed_extraction_type", "feed_processing_engine", "feed_is_active"]` | `["model_model_schema", "model_table_type", "model_scd_type", "model_updates_enabled", "model_deletes_enabled"]` | `["job_successful", "landing_rows_read", "raw_rows_read", "clean_rows_inserted", "staging_rows_updated", "model_rows_updated", "serve_rows_read"]` |
| SCD type | `1` | `1` | `1` |
| Updates enabled | **on** | **on** | **on** |
| Deletes enabled | off | off | off |
| Load type | `full` | `full` | `full` |

`Updates enabled` is **on** for all three here, unlike the CSV
walkthrough's off-by-default snapshot rows — the source rows genuinely
mutate as a run progresses through its stages (step 6's note), so
attribute-hash change tracking is doing real, load-bearing work for this
feed, not just satisfying a default. Tracked columns above match each
model's own `row_hash([...])` `_attr_hash` list from step 7 exactly — if
you add/remove a tracked column later, keep both in sync by hand (nothing
cross-checks them yet, same caveat as `Learnings.md`'s note on the
`owning_feed_id`/dbt-tag pairing).

## 9. Regenerate the pipeline + restart **[Command]**

```bash
just orchestration::start
```

## 10. Trigger the run

No file to drop this time — it's a direct-connect source. Launch
`metadata_runs_job` the same way as the first walkthrough's step 10 (Dagit
Launchpad, or `dagster job launch -j metadata_runs_job -m dagster_data_platform.definitions`).

## 11. Verify

```sql
-- every row has exactly one dimension key set, never both, never neither
select
  count(*) filter (where feed_key is not null and lakehouse_model_key is null) as feed_rows,
  count(*) filter (where lakehouse_model_key is not null and feed_key is null) as model_rows,
  count(*) filter (where feed_key is not null and lakehouse_model_key is not null) as both_set_should_be_zero,
  count(*) filter (where feed_key is null and lakehouse_model_key is null) as neither_set_should_be_zero
from iceberg.model.fct_metadata_runs;
```

If `both_set_should_be_zero`/`neither_set_should_be_zero` are actually
zero, the role-playing join worked as intended — every fact row picked up
exactly the one dimension it should have.
