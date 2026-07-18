# Walkthrough: Ingesting the Platform's Own Metadata DB

A complete, copy-paste-able walkthrough of the platform's real architecture
— from writing metadata configuration, through triggering a real pipeline
run, to querying the resulting serve-layer views. The source is
`platform_metadata` itself: the platform ingesting its own operational run
history (`data_processing_runs`) as a feed called `metadata_runs`.

This feed already exists on any platform that's been through a normal
`just start` (it's seeded automatically by `scripts/seed_metadata_db.py`) —
so the values below describe both **how it's actually configured today**
and **exactly what to enter to reproduce it from a fresh, unseeded
platform**. If you follow the UI steps below against an already-seeded
platform, the "Add new" inserts will fail on a duplicate `friendly_name`/
`code` — that's expected, and means you can skip straight to
[step 5](#5-regenerate--rebuild--restart-command) and onward.

This exercise is worth doing specifically for one thing: **a real,
live-database extraction with zero hand-written Python.** Every step of
extraction (fetch, durable raw copy, schema-validated write to `clean`) is
handled generically by `processing/connectors/`'s `PostgresConnector`,
driven entirely by metadata. The only thing you write by hand is dbt SQL.

`data_feed.extraction_config` is left empty here — this is the normal case,
not a simplification for the walkthrough's sake. A plain table source needs
no extraction-time configuration at all: the connector runs
`SELECT * FROM <source_object_name>` on its own. `extraction_config` exists
for sources that genuinely need extra parameters a generic connector can't
infer — a REST feed's pagination rules, a CSV feed's encoding/delimiter
override — not for embedding joins or business logic into extraction (see
`metadata/DataModel.md`, `data_feed.extraction_config`, for the full design
and why: `raw` is a verbatim copy, never a transformation — see `Roadmap.md`,
"Layer Model" — and this walkthrough's whole point is to demonstrate that
transformation happening in dbt, where it belongs).

Target shape: a single fact, `metadata_fct_runs`, inside the `metadata`
domain (`dbt/domains/metadata/`).

Steps marked **[UI]** are done in the Streamlit frontend. Steps marked
**[Code]** require writing a file by hand — this platform's staging/model/
serve *codegen scaffolding* is metadata-driven, but the actual business-logic
SQL inside those files is not (see `Roadmap.md`, "dbt project scope"). Steps
marked **[Command]** are run from the repo root, `/Users/josecosta/Documents/projects/data-platform/`.

---

## 0. Bring up the platform

```bash
just start
```

### Accessing running services

Every one of these runs **fully in-cluster** — none are local processes —
and every one is reachable at `localhost:<port>` with no
`kubectl port-forward` needed, since the kind cluster maps each port straight
through (`platform/kind/kind-cluster.yaml`):

| Service | Address | Notes |
|---|---|---|
| Streamlit (frontend CRUD) | `http://localhost:8501` | In-cluster `Deployment`, `frontend/k8s/` |
| Dagster webserver (Dagit) | `http://localhost:3000` | In-cluster `Deployment`, `orchestration/k8s/` |
| Postgres (`platform_metadata`) | `localhost:5432` | `psql -h localhost -U platform -d platform_metadata` (password `platform`, or your `.env` values) — this is also the source you're extracting *from* in this walkthrough, same address, same credentials |
| Trino | `localhost:8080` | Any Trino client pointed here works; this walkthrough uses `kubectl exec` instead (no client install needed) |
| Polaris (Iceberg REST catalog) | in-cluster only | `kubectl port-forward -n query-engine svc/polaris 8181:8181 &` if you need to reach it directly |
| MinIO (S3-compatible storage) | in-cluster only | `kubectl port-forward -n query-engine svc/minio 9000:9000 &` (add `9001:9001` for the console) if you need to inspect it |

---

## 1. Register the source system **[UI]**

Streamlit (`http://localhost:8501`) → **Source Systems** → *Add new*.

| Field | Value |
|---|---|
| Code | `platform_metadata_db` |
| Name | `Platform metadata database` |
| Description | `This platform's own platform_metadata Postgres instance, queried as a source` |
| System type | `database` |
| Connector kind | `postgres` — this is what makes extraction fully generic; no hand-written asset file needed |
| Base location | *(leave blank)* |
| Connection user | *(leave blank)* |
| Connection secret | *(leave blank)* |
| Connection config (JSON) | `{}` (default — leave as-is) |

No credentials to provision here at all: every pod `K8sRunLauncher` launches
already has `POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_USER`/
`POSTGRES_PASSWORD`/`POSTGRES_DB` injected as env vars (the same ones
`PostgresMetadataResource` itself uses), and the generic `PostgresConnector`
reads those directly. `base_location`/`connection_user`/`connection_secret`
are only needed when a connection detail genuinely isn't already available
this way.

## 2. Register the data feed **[UI]**

Streamlit → **Data Feeds** → *Add new*.

| Field | Value |
|---|---|
| Source system | `platform_metadata_db` |
| Friendly name | `metadata_runs` |
| Source object name | `data_processing_runs` |
| Batch group | `<New batch>` → type `metadata_runs` |
| Batch feed hierarchy | `0` |
| Extraction type | `full` |
| Watermark column | *(leave blank — only required for `incremental`)* |
| Extraction config (JSON) | `{}` (default — leave as-is; see the note above) |
| Source PK columns (comma-separated) | `run_id` |
| Processing engine | `polars` |
| Pipeline steps | leave all three selected (`extraction`, `transformation`, `serving`) |
| ODS enabled | leave unchecked — this feed gets a real hand-modeled fact, not an automatic ODS table |

Leaving **Extraction config** at its default `{}` is the point: `source_object_name`
above (`data_processing_runs`) is all the generic `PostgresConnector` needs —
it runs `SELECT * FROM data_processing_runs` on its own, no query to write.

**No `schema_registry` step needed** — schema discovery bootstraps
`schema_registry` automatically on this feed's first real run
(`connectors/schema_registry_sync.py`), from the query's own result set.
There's nothing to hand-seed.

## 3. Write the dbt staging model **[Code]**

Directory: `dbt/domains/metadata/models/staging/`

New file: `dbt/domains/metadata/models/staging/stg_metadata_runs.sql`

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
        cast(master_dagster_run_id as varchar) as master_dagster_run_id,
        cast(extraction_dagster_run_id as varchar) as extraction_dagster_run_id,
        cast(transformation_dagster_run_id as varchar) as transformation_dagster_run_id,
        cast(serving_dagster_run_id as varchar) as serving_dagster_run_id,
        cast(job_started_timestamp as timestamp(6) with time zone) as job_started_timestamp,
        cast(job_ended_timestamp as timestamp(6) with time zone) as job_ended_timestamp,
        cast(job_successful as boolean) as job_successful,
        cast(raw_rows_read as bigint) as raw_rows_read,
        cast(clean_rows_inserted as bigint) as clean_rows_inserted,
        cast(staging_rows_updated as bigint) as staging_rows_updated,
        cast(model_rows_updated as bigint) as model_rows_updated,
        cast(serve_rows_read as bigint) as serve_rows_read,
        {{ row_hash(['run_id']) }} as _key_hash,
        {{ row_hash(['job_successful', 'job_ended_timestamp', 'raw_rows_read', 'clean_rows_inserted', 'staging_rows_updated', 'model_rows_updated', 'serve_rows_read']) }} as _attr_hash
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

Every column here is a real, direct column of `data_processing_runs` itself
— nothing joined in. `updates_enabled = true` is deliberate: unlike an
immutable snapshot feed, a `data_processing_runs` row genuinely mutates
across its own lifecycle (each stage's columns fill in as the run
progresses), so this is one of the few feeds where attribute-hash change
tracking does real, load-bearing work.

You don't need to hand-write `dbt/domains/metadata/models/staging/_sources.yml`
— it's fully generated (`scripts/generate_sources.py`) once the data feed is
seeded, picked up automatically in [step 5](#5-regenerate--rebuild--restart-command).

## 4. Write the model-layer dbt model **[Code]**

Directory: `dbt/domains/metadata/models/model/facts/`

New file: `dbt/domains/metadata/models/model/facts/metadata_fct_runs.sql`

```sql
{{ config(schema='model', unique_key='_key_hash', alias='metadata_fct_runs', tags=['metadata_runs']) }}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (
    select
        run_id, data_feed_id, model_key, master_dagster_run_id,
        extraction_dagster_run_id, transformation_dagster_run_id,
        serving_dagster_run_id, tracking_group, tracking_group_type,
        job_started_timestamp, job_ended_timestamp, job_successful,
        raw_rows_read, clean_rows_inserted,
        staging_rows_updated, model_rows_updated, serve_rows_read,
        false as is_deleted
    from {{ ref('stg_metadata_runs') }}
),
hashed as (
    select *,
        {{ row_hash(['run_id']) }} as _key_hash,
        {{ row_hash(['job_successful', 'job_ended_timestamp', 'raw_rows_read', 'clean_rows_inserted', 'staging_rows_updated', 'model_rows_updated', 'serve_rows_read', 'is_deleted']) }} as _attr_hash
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

`data_feed_id`/`model_key` carry straight through from staging as plain
attributes — a real `data_processing_runs` row has exactly one of the two
set, never both (a `CHECK` constraint on the source table enforces this),
so this fact reflects that directly without needing a dimensional join to
express it.

Add the matching test-companion file:

`dbt/domains/metadata/models/model/facts/metadata_fct_runs.yml`

```yaml
version: 2

models:
  - name: metadata_fct_runs
    columns:
      - name: _key_hash
        tests: [not_null, unique]
      - name: _attr_hash
        tests: [not_null]
      - name: is_deleted
        tests: [not_null]
```

**Tip**: once the `lakehouse_models` row in the next step exists, running
`just orchestration::generate-model-scaffolds` (already part of
[step 5](#5-regenerate--rebuild--restart-command)'s sequence) will
auto-generate a starting skeleton for this file if it doesn't exist yet on
disk — pre-filled `config()`/`row_hash()`/technical-column boilerplate plus
a `TODO` marker around the `base` CTE, listing exactly the business-key/
tracked columns you register in step 5. It never touches a file that
already exists.

## 5. Register the lakehouse model **[UI]**

Streamlit → **Lakehouse Models** → *Add new*.

| Field | Value |
|---|---|
| Friendly name | `fct_metadata_runs` |
| Table name | `metadata_fct_runs` |
| Model schema (domain) | `<New schema>` → type `metadata` |
| Batch hierarchy | `0` |
| Table type | `fact` |
| Depends on feeds | `metadata_runs` |
| Owning feed | `metadata_runs` |
| Business key columns (comma-separated) | `run_id` |
| Tracked columns (comma-separated) | `job_successful, job_ended_timestamp, raw_rows_read, clean_rows_inserted, staging_rows_updated, model_rows_updated, serve_rows_read` |
| SCD type | `1` |
| Updates enabled | **on** |
| Deletes enabled | off |
| Watermark column | *(leave blank)* |
| Load type | `full` |
| Pipeline steps | leave both selected (`transformation`, `serving`) |

`Updates enabled` is **on** — the source rows genuinely mutate as a run
progresses through its stages (see step 3's note), so attribute-hash change
tracking is doing real, load-bearing work here, not just satisfying a
default. **Tracked columns above must match `metadata_fct_runs.sql`'s own
`row_hash([...])` `_attr_hash` list from step 4 exactly** — nothing
cross-checks this automatically today if you add/remove a tracked column
later (see `Backlog.md`'s note on the dbt-tag/`owning_feed_id` check, a
related but separate gap).

## 6. Regenerate + rebuild + restart **[Command]**

```bash
just orchestration::start
```

This one command, in order: scaffolds `dbt/domains/metadata/` if it doesn't
exist yet (`scripts/generate_domain_projects.py`), regenerates
`dbt/domains/metadata/models/staging/_sources.yml`
(`scripts/generate_sources.py`), generates the serve-layer `_latest`/
`_historical` views (`scripts/generate_serve_views.py`), generates any
missing model-scaffold files (`scripts/generate_model_scaffolds.py` — a
no-op here since you already wrote the file by hand in step 4), rebuilds
the shared Docker image plus the `metadata` domain's own image, and restarts
Dagster's code-server/webserver/daemon to pick everything up.

## 7. Trigger the pipeline **[Command]**

```bash
cd orchestration/dagster_data_platform
export DAGSTER_HOME="$(pwd)/../dagster_home"
uv run python -m dagster_data_platform.trigger_master_pipeline \
  --orchestration-kind model_schema --orchestration-value metadata
```

`--orchestration-kind model_schema --orchestration-value metadata` resolves
every `lakehouse_models` row in the `metadata` domain, unions their
`depends_on_feeds` (just `metadata_runs` here), extracts it, then builds
`metadata`'s staging → model → serve layers — all through the same single
`master_pipeline` entry point every schedule/sensor/manual trigger goes
through. Prints the launched run's id to stdout on success; requires the
Dagster webserver already reachable at `http://localhost:3000` (step 0).

## 8. Verify the run **[Command]**

```bash
kubectl exec -n metadata postgres-0 -- psql -U platform -d platform_metadata -c \
  "select data_feed_id, model_key, job_successful from data_processing_runs order by job_started_timestamp desc limit 5;"
```

You should see one feed-run row (`data_feed_id` set, `model_key` null) for
`metadata_runs`'s own extraction, and one model-run row (`model_key` set to
`metadata`, `data_feed_id` null) for the domain build — both with
`job_successful = t`.

## 9. Query the serving views **[Command]**

```bash
kubectl exec -n query-engine deployment/trino-coordinator -- trino --execute \
  "select * from iceberg.serve.metadata_fct_runs_latest limit 10"
```

```bash
kubectl exec -n query-engine deployment/trino-coordinator -- trino --execute \
  "select tracking_group_type, count(*) from iceberg.model.metadata_fct_runs group by 1"
```

The second query is a quick sanity check on the source's own role-playing
shape: `data_processing_runs` rows are either `batch_group`-tracked
(feed-runs) or `model_schema`-tracked (domain builds) — you should see both
`tracking_group_type` values present, one row-count each, growing every
time you re-run [step 7](#7-trigger-the-pipeline-command).

---

## 10. (Optional) Turn this into a recurring schedule instead of a one-off trigger

Step 7 launched `metadata_runs` manually, once. To have the platform pick it
up on its own going forward — a cron schedule, or (for a file-drop-style
feed) a storage sensor — register it in Streamlit → **Ingestion Triggers**
→ *Add new* instead of re-running step 7 by hand:

| Field | Value |
|---|---|
| Controls a | `model` |
| Target | `fct_metadata_runs` |
| Trigger type | `schedule` |
| Cron schedule | `0 7 * * *` (daily at 07:00 — any standard 5-field cron string) |
| Active | **on** |

Re-run `just orchestration::start` once more to generate the real Dagster
`ScheduleDefinition` from this row (`scripts/generate_dagster_pipeline.py`).
Every generated schedule defaults to **stopped** in Dagster regardless of
this row's `Active` value — turn it on from the Dagit UI
(`http://localhost:3000` → Schedules) once you're ready for it to actually
fire. `Sensor`-type triggers are only valid for a `feed` target whose source
system's connector kind is `csv` or `json_file` (a sensor watches a landing
directory) — `metadata_runs` is `postgres`-kind and pull-based, so schedule
is the only applicable trigger type for it.
