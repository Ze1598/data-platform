# Walkthrough: Onboarding a Brand-New Feed (Worked Example)

A step-by-step guide for a platform user adding a new CSV-sourced feed that
lands in its own model layer: **1 fact + 2 dimensions**, **historical
one-time load** (`extraction_type='full'`, triggered manually once — no
recurring schedule/sensor needed).

Worked example used throughout: a warehouse management system exports a
one-time inventory snapshot, `inventory_snapshot.csv`:

```csv
snapshot_date,warehouse_code,warehouse_name,warehouse_region,product_sku,product_name,product_category,quantity_on_hand,unit_cost
2026-07-01,WH-01,Newark DC,US-East,SKU-1001,Widget A,Hardware,4200,3.15
2026-07-01,WH-01,Newark DC,US-East,SKU-1002,Widget B,Hardware,1800,5.40
2026-07-01,WH-02,Reno DC,US-West,SKU-1001,Widget A,Hardware,3100,3.15
...
```

Target model layer: `dim_warehouse` (Type 1), `dim_product` (Type 1),
`fct_inventory_snapshot` (fact, joined to both dims).

Steps marked **[UI]** are done in the Streamlit frontend. Steps marked
**[Code]** require writing a file by hand — this platform's staging/model/
serve layers are metadata-driven codegen, but *landing/raw/clean* and the
actual dbt SQL are not (see `Roadmap.md`, "dbt project scope"). Steps
marked **[Command]** are run from the repo root.

---

## 0. (Optional) Platform startup

Skip this section if the platform is already running.

```bash
just start          # brings up the full stack: kind cluster, Postgres,
                     # MinIO, Polaris, Trino, Dagster, Streamlit
```

Or bring up only what's needed to work through this guide's early steps
(metadata registration doesn't need Trino/dbt yet):

```bash
just start platform
just start metadata
just start frontend
```

### Accessing running services

| Service | URL / address | Notes |
|---|---|---|
| Streamlit (frontend CRUD) | `http://localhost:8501` | Local process (`frontend::start`), not in-cluster — just a normal localhost port |
| Dagster UI (Dagit) | `http://localhost:3000` | Local process (`orchestration::start`), same as above |
| Postgres (`platform_metadata`) | `localhost:5432` | In-cluster, but NodePort-mapped straight through by kind (see `platform/kind/kind-cluster.yaml`) — no port-forward needed. `psql -h localhost -U platform -d platform_metadata` (password `platform`, or your `.env` values) |
| Trino | `localhost:8080` | Same NodePort setup as Postgres — no port-forward needed. Any Trino CLI/client pointed at `localhost:8080` works |
| Polaris (Iceberg REST catalog) | in-cluster only | Not NodePort-mapped — only needed if you're debugging the catalog directly (Trino already talks to it internally). `kubectl port-forward -n query-engine svc/polaris 8181:8181 &` first if you need it |
| MinIO (S3-compatible storage) | in-cluster only | Same as Polaris — backing object storage, not something you query directly. `kubectl port-forward -n query-engine svc/minio 9000:9000 &` (add `9001:9001` for the web console) if you need to inspect it |

The pattern: anything that's a **local process** (Streamlit, Dagit) is just a normal port on your machine. Anything **in-cluster with a NodePort** (Postgres, Trino) is mapped straight through by the kind cluster config, so it's also just `localhost:<port>`, no forwarding step. Anything **in-cluster without a NodePort** (Polaris, MinIO) needs an explicit `kubectl port-forward` if you want to reach it directly from the host — see `platform/DebugReference.md` for the general pattern, and each module's own `DebugReference.md` for specifics.

---

## 1. Register the source system **[UI]**

Streamlit → **Source Systems** → *Add new*. Skip this if reusing an
existing source system (e.g. another file-drop system already registered).

| Field | Value |
|---|---|
| Code | `wms_export` |
| Name | `Warehouse Management System export` |
| System type | `file_drop` |
| Base location | *(optional — a description of where exports come from)* |
| Connection user / secret | *(leave blank — no live connection for a file drop)* |

## 2. Register the data feed **[UI]**

Streamlit → **Data Feeds** → *Add new*.

| Field | Value |
|---|---|
| Source system | `wms_export` |
| Friendly name | `inventory_snapshot` |
| Source object name | `inventory_snapshot.csv` |
| Batch group | `<New batch>` → `inventory_snapshot` (its own singleton batch — no other feed needs to run alongside it) |
| Batch feed hierarchy | `0` |
| Extraction type | `full` (a one-time historical load re-extracts everything each run, which here means "the one run it ever does") |
| Watermark column | *(leave blank — only required for `incremental`)* |
| Source PK columns (JSON array) | `["warehouse_code", "product_sku", "snapshot_date"]` |
| Processing engine | `polars` |

## 3. Register the expected schema **[Command]**

There's no frontend page for `schema_registry` yet (see `Backlog.md`) — add
it directly:

```bash
kubectl exec -n metadata postgres-0 -- psql -U platform -d platform_metadata -c "
INSERT INTO schema_registry (data_feed_id, version, column_definitions, is_current, created_by)
SELECT id, 1, '[
  {\"name\": \"snapshot_date\",      \"data_type\": \"string\", \"nullable\": false, \"ordinal\": 1,  \"description\": \"Snapshot date, YYYY-MM-DD\"},
  {\"name\": \"warehouse_code\",     \"data_type\": \"string\", \"nullable\": false, \"ordinal\": 2,  \"description\": \"Business key\"},
  {\"name\": \"warehouse_name\",     \"data_type\": \"string\", \"nullable\": false, \"ordinal\": 3,  \"description\": \"Warehouse display name\"},
  {\"name\": \"warehouse_region\",   \"data_type\": \"string\", \"nullable\": false, \"ordinal\": 4,  \"description\": \"Region code\"},
  {\"name\": \"product_sku\",        \"data_type\": \"string\", \"nullable\": false, \"ordinal\": 5,  \"description\": \"Business key\"},
  {\"name\": \"product_name\",       \"data_type\": \"string\", \"nullable\": false, \"ordinal\": 6,  \"description\": \"Product display name\"},
  {\"name\": \"product_category\",   \"data_type\": \"string\", \"nullable\": false, \"ordinal\": 7,  \"description\": \"Product category\"},
  {\"name\": \"quantity_on_hand\",   \"data_type\": \"long\",   \"nullable\": false, \"ordinal\": 8,  \"description\": \"Units on hand at snapshot time\"},
  {\"name\": \"unit_cost\",          \"data_type\": \"double\", \"nullable\": false, \"ordinal\": 9,  \"description\": \"Cost per unit\"}
]'::jsonb, true, 'walkthrough'
FROM data_feed WHERE friendly_name = 'inventory_snapshot';
"
```

## 4. Write the landing/raw/clean Dagster assets **[Code]**

New file: `orchestration/dagster_data_platform/dagster_data_platform/assets/inventory_snapshot_assets.py`.
Follows `financial_assets.py`'s established pattern exactly (the only other
real file-drop feed): a landing directory read, a durable verbatim raw
copy, then schema-validated Iceberg write.

```python
import os
from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext, Output, asset

from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

FEED_FRIENDLY_NAME = "inventory_snapshot"
FEED_POOL = f"feed:{FEED_FRIENDLY_NAME}"
LANDING_SUBDIR = "landing/inventory_snapshot"
RAW_SUBDIR = "raw/inventory_snapshot"

REPO_ROOT = Path(__file__).resolve().parents[4]


def _data_lake_dir() -> Path:
    return Path(os.environ["DATA_LAKE_PATH"]) if "DATA_LAKE_PATH" in os.environ else REPO_ROOT / "data-lake"


def _landing_dir() -> Path:
    return _data_lake_dir() / LANDING_SUBDIR


def _raw_dir() -> Path:
    return _data_lake_dir() / RAW_SUBDIR


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def landing_inventory_snapshot(
    context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="landing",
        dagster_run_id=context.run_id,
    ) as log:
        landing_dir = _landing_dir()
        csv_files = sorted(landing_dir.glob("*.csv")) if landing_dir.exists() else []
        df = pl.concat([pl.read_csv(f, infer_schema_length=None) for f in csv_files], how="diagonal_relaxed") if csv_files else pl.DataFrame()
        log.set_counts(rows_read=df.height)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def raw_inventory_snapshot(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_inventory_snapshot: pl.DataFrame,
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="raw",
        dagster_run_id=context.run_id,
    ) as log:
        df = landing_inventory_snapshot
        if not df.is_empty():
            raw_run_dir = _raw_dir() / f"run_id={context.run_id}"
            raw_run_dir.mkdir(parents=True, exist_ok=True)
            landing_dir = _landing_dir()
            import shutil
            for f in sorted(landing_dir.glob("*.csv")):
                shutil.copy2(f, raw_run_dir / f.name)
        log.set_counts(rows_read=df.height)

    return Output(df, metadata={"audit_run_id": log.run_id, "row_count": df.height})


@asset(pool=FEED_POOL, group_name=FEED_FRIENDLY_NAME)
def clean_inventory_snapshot(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_inventory_snapshot: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed(FEED_FRIENDLY_NAME)
    df = raw_inventory_snapshot
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
                    created_by="clean_inventory_snapshot",
                )
                column_definitions = reconciliation.updated_column_definitions

            validate_schema(df, column_definitions)

            catalog = iceberg_catalog.get_catalog()
            write_clean_snapshot(
                catalog,
                namespace="clean",
                table_name="inventory_snapshot",
                df=df,
                column_definitions=column_definitions,
                schema_changed=schema_changed,
            )
        log.set_counts(rows_inserted=df.height)

    return Output(None, metadata={"audit_run_id": log.run_id, "rows_inserted": df.height})
```

Register these 3 assets in `definitions.py`'s `assets=[...]` list (the
`dbt_assets` entries for this feed come in automatically once metadata is
seeded — see step 9):

```python
from dagster_data_platform.assets.inventory_snapshot_assets import (
    clean_inventory_snapshot,
    landing_inventory_snapshot,
    raw_inventory_snapshot,
)
# ... add landing_inventory_snapshot, raw_inventory_snapshot, clean_inventory_snapshot to assets=[...]
```

Add `"inventory_snapshot"` to `dbt_assets.py`'s `_CLEAN_SOURCE_TABLES` set —
this is what maps `source('clean', 'inventory_snapshot')` in dbt onto
`AssetKey("clean_inventory_snapshot")` above:

```python
_CLEAN_SOURCE_TABLES = {"customers", "sales", "financial_transactions", "police_crimes", "inventory_snapshot"}
```

## 5. Declare the dbt source **[Code]**

Add to `dbt/data_platform/models/staging/_sources.yml` (append under the
existing `clean` source's `tables:` list):

```yaml
      - name: inventory_snapshot
```

## 6. Write the staging model **[Code]**

New file: `dbt/data_platform/models/staging/stg_inventory_snapshot.sql` —
same insert/update-split pattern as every other staging model.

```sql
{{
  config(
    unique_key='_key_hash',
    alias='inventory_snapshot',
    tags=['inventory_snapshot']
  )
}}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with source_raw as (

    select
        cast(snapshot_date as varchar) as snapshot_date,
        cast(warehouse_code as varchar) as warehouse_code,
        cast(warehouse_name as varchar) as warehouse_name,
        cast(warehouse_region as varchar) as warehouse_region,
        cast(product_sku as varchar) as product_sku,
        cast(product_name as varchar) as product_name,
        cast(product_category as varchar) as product_category,
        cast(quantity_on_hand as bigint) as quantity_on_hand,
        cast(unit_cost as double) as unit_cost,
        {{ row_hash(['warehouse_code', 'product_sku', 'snapshot_date']) }} as _key_hash,
        {{ row_hash(['warehouse_name', 'warehouse_region', 'product_name', 'product_category', 'quantity_on_hand', 'unit_cost']) }} as _attr_hash
    from {{ source('clean', 'inventory_snapshot') }}

)

{% if is_incremental() %}

, source as (
    {{ classify_changes('source_raw', updates_enabled) }}
)

{% endif %}

select
    *,
    {{ dbt.current_timestamp() }} as _loaded_at
from {{ 'source' if is_incremental() else 'source_raw' }}
```

## 7. Write the model-layer dbt models (2 dims + 1 fact) **[Code]**

`dbt/data_platform/models/model/dimensions/dim_warehouse.sql`:

```sql
{{ config(schema='model', unique_key='_key_hash', alias='dim_warehouse', tags=['inventory_snapshot']) }}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (
    select distinct
        warehouse_code, warehouse_name, warehouse_region, false as is_deleted
    from {{ ref('stg_inventory_snapshot') }}
),
hashed as (
    select *,
        {{ row_hash(['warehouse_code']) }} as _key_hash,
        {{ row_hash(['warehouse_name', 'warehouse_region', 'is_deleted']) }} as _attr_hash
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

`dbt/data_platform/models/model/dimensions/dim_product.sql` — identical
shape, keyed on `product_sku`, tracking `product_name`/`product_category`.

`dbt/data_platform/models/model/facts/fct_inventory_snapshot.sql`:

```sql
{{ config(schema='model', unique_key='_key_hash', alias='fct_inventory_snapshot', tags=['inventory_snapshot']) }}

{% set updates_enabled = var('updates_enabled_by_model', {}).get(model.name, true) %}

with base as (
    select
        s.snapshot_date, s.quantity_on_hand, s.unit_cost,
        w._key_hash as warehouse_key, p._key_hash as product_key,
        false as is_deleted
    from {{ ref('stg_inventory_snapshot') }} s
    left join {{ ref('dim_warehouse') }} w on s.warehouse_code = w.warehouse_code
    left join {{ ref('dim_product') }} p on s.product_sku = p.product_sku
),
hashed as (
    select *,
        {{ row_hash(['warehouse_key', 'product_key', 'snapshot_date']) }} as _key_hash,
        {{ row_hash(['quantity_on_hand', 'unit_cost', 'is_deleted']) }} as _attr_hash
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

Add matching entries to `dbt/data_platform/models/model/schema.yml`
(`not_null`/`unique` on `_key_hash`, `not_null` on `_attr_hash` — see
existing entries for the pattern).

## 8. Register the 3 lakehouse models **[UI]**

Streamlit → **Lakehouse Models** → *Add new*, three times:

| Field | `dim_warehouse` | `dim_product` | `fct_inventory_snapshot` |
|---|---|---|---|
| Model schema | `model` | `model` | `model` |
| Table type | `dimension` | `dimension` | `fact` |
| Depends on feeds | `inventory_snapshot` | `inventory_snapshot` | `inventory_snapshot` |
| Owning feed | `inventory_snapshot` | `inventory_snapshot` | `inventory_snapshot` |
| Business key columns | `["warehouse_code"]` | `["product_sku"]` | `["warehouse_code", "product_sku", "snapshot_date"]` |
| Tracked columns | `["warehouse_name", "warehouse_region"]` | `["product_name", "product_category"]` | `["quantity_on_hand", "unit_cost"]` |
| SCD type | `1` | `1` | `1` |
| Updates enabled | off (immutable snapshot) | off | off |
| Deletes enabled | off | off | off |
| Load type | `full` | `full` | `full` |

(All 3 are single-feed here, so "Owning feed" trivially matches — see
`Learnings.md`, "A dbt model tagged with two feed tags..." for when this
matters.)

## 9. Regenerate the pipeline + restart **[Command]**

```bash
just orchestration::start
```

This re-runs, in order: the pipeline-jobs codegen (creates
`inventory_snapshot_job`, now that the feed is seeded), the serve-view
codegen (creates `dim_warehouse_latest`/`_historical` etc.), rebuilds the
Docker image (bakes in your new asset file + dbt models), and restarts
`dagster dev` to pick up the new Python code.

## 10. Drop the CSV and trigger the run

```bash
mkdir -p data-lake/landing/inventory_snapshot
cp inventory_snapshot.csv data-lake/landing/inventory_snapshot/
```

Trigger the new feed's job — this is a one-time load, so no
schedule/sensor is needed, just a manual launch. Either via the Dagit UI
(`http://localhost:3000` → Jobs → `inventory_snapshot_job` → Launchpad →
Launch Run), or from the CLI:

```bash
cd orchestration/dagster_data_platform
export DAGSTER_HOME="$(pwd)/../dagster_home"
dagster job launch -j inventory_snapshot_job -m dagster_data_platform.definitions
```

## 11. Verify

```bash
# data_processing_runs shows the feed-run + model-run rows succeeded
kubectl exec -n metadata postgres-0 -- psql -U platform -d platform_metadata -c \
  "select data_feed_id, model_key, job_successful from data_processing_runs order by job_started_timestamp desc limit 5;"
```

Query the new tables via Trino (`just query-engine::trino` or any Trino
client pointed at `localhost:8080`):

```sql
select * from iceberg.model.dim_warehouse;
select * from iceberg.model.dim_product;
select * from iceberg.serve.fct_inventory_snapshot_latest f
  join iceberg.serve.dim_warehouse_latest w on f.warehouse_key = w._key_hash
  join iceberg.serve.dim_product_latest p on f.product_key = p._key_hash;
```

Since this was a one-time historical load, there's nothing further to
schedule — `inventory_snapshot_job` simply sits idle until (or unless)
manually launched again.
