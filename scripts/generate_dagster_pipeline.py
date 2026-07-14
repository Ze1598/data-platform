"""Generates the dynamic, metadata-driven pipeline foundation -- per-feed
dbt assets, per-feed connector-driven landing/raw/clean assets, per-feed
jobs, and real Dagster schedules from `data_feed` + `source_system` (+
`schedule` + `lakehouse_models` for model-type schedules) -- as
`orchestration/dagster_data_platform/dagster_data_platform/pipeline_generated.py`.

Deliberately a standalone build-time script, not a Dagster op or a live
Postgres read folded into module import: this project's established rule
(see Learnings.md, and generate_serve_views.py/
generate_deletion_synthesis_views.py, the two existing precedents) is that
anything determining Dagster's *static object graph* -- which
assets/jobs/schedules exist, their cron, their target -- must be resolved
before `dagster dev`/Docker image build, never at Python import time. What
an asset/schedule's *execution function* looks up live (is_active, a
feed's current watermark, connector fetch() results) is a different phase
of Dagster's lifecycle and is deliberately NOT baked in here.

Landing/raw/clean asset generation (added alongside the pre-existing
dbt/job/schedule generation) is scoped to feeds whose source_system has a
non-null `connector_kind` -- see the connector library plan
(.claude/plans/). A feed with connector_kind IS NULL (customers/sales'
synthetic stub generators) keeps a fully hand-written asset file; this
script only ever generates for a *covered* connector kind, it never
silently skips a feed that needs one.

Fully regenerates `pipeline_generated.py` on every run (clears/overwrites,
not additive) so it never drifts from current metadata state.
"""

import os
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
OUTPUT_PATH = REPO_ROOT / "orchestration" / "dagster_data_platform" / "dagster_data_platform" / "pipeline_generated.py"

# Both categories exist because tabular and nested-JSON sources genuinely
# need a different asset shape -- see connectors/base.py and the plan.
_TABULAR_KINDS = ("postgres", "csv")
_JSON_KINDS = ("rest", "json_file")


def fetch_active_feeds(cur) -> list[str]:
    cur.execute("SELECT friendly_name FROM data_feed WHERE is_active = true ORDER BY friendly_name")
    return [row[0] for row in cur.fetchall()]


def fetch_connector_feeds(cur) -> list[dict]:
    cur.execute(
        """
        SELECT df.friendly_name AS feed_friendly_name, ss.connector_kind, ss.base_location
        FROM data_feed df
        JOIN source_system ss ON ss.id = df.source_system_id
        WHERE df.is_active = true AND ss.connector_kind IS NOT NULL
        ORDER BY df.friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_feed_schedules(cur) -> list[dict]:
    cur.execute(
        """
        SELECT s.id::text AS schedule_id, s.cron, df.friendly_name AS feed_friendly_name
        FROM schedule s
        JOIN data_feed df ON df.id = s.controlling_object_id
        WHERE s.is_active AND s.controlling_object_type = 'feed' AND df.is_active
        ORDER BY s.id
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_model_schedules(cur) -> list[dict]:
    # One row per (schedule, dependent feed) -- a schedule binds to exactly
    # one Dagster job, and a model has no standalone job of its own (models
    # only ever build as a side effect of a feed's dbt build), so a
    # model-type schedule expands into one generated schedule per feed in
    # that model's depends_on_feeds.
    cur.execute(
        """
        SELECT s.id::text AS schedule_id, s.cron,
               lm.friendly_name AS model_friendly_name, df.friendly_name AS feed_friendly_name
        FROM schedule s
        JOIN lakehouse_models lm ON lm.id = s.controlling_object_id
        JOIN data_feed df ON df.id::text = ANY(string_to_array(lm.depends_on_feeds, ','))
        WHERE s.is_active AND s.controlling_object_type = 'model' AND lm.is_active AND df.is_active
        ORDER BY s.id, df.friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _schedule_python_name(schedule_id: str, feed_friendly_name: str) -> str:
    return f"schedule_{schedule_id.replace('-', '')}_{feed_friendly_name}"


def _connector_build_expr(kind: str, feed: str, *, with_watermark: bool) -> str:
    """The Python source expression that constructs this feed's connector
    instance. `with_watermark=True` is the landing-time construction (REST/
    json_file connectors need last_watermark for their own fetch()
    catch-up logic); clean-time construction never needs it (flatten()/
    discover_schema() don't touch the source)."""
    if kind == "postgres":
        return (
            'PostgresConnector(\n'
            '            host=os.environ.get("POSTGRES_HOST", "localhost"),\n'
            '            port=int(os.environ.get("POSTGRES_PORT", "5432")),\n'
            '            dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),\n'
            '            user=os.environ.get("POSTGRES_USER", "platform"),\n'
            '            password=os.environ.get("POSTGRES_PASSWORD", "platform"),\n'
            '            query=data_feed["extraction_config"]["query"],\n'
            '        )'
        )
    if kind == "csv":
        return f'CSVConnector(landing_dir=_data_lake_dir() / "landing" / "{feed}")'
    if kind == "json_file":
        watermark_kwarg = ', last_watermark=data_feed.get("last_watermark_value")' if with_watermark else ""
        return f'_{feed}_Connector(landing_dir=_data_lake_dir() / "landing" / "{feed}"{watermark_kwarg})'
    if kind == "rest":
        watermark_kwarg = ', last_watermark=data_feed.get("last_watermark_value")' if with_watermark else ""
        return f'_{feed}_Connector(base_url=source_system_base_location["{feed}"]{watermark_kwarg})'
    raise ValueError(f"unknown connector_kind {kind!r}")


def _render_tabular_assets(feed: dict) -> str:
    friendly_name = feed["feed_friendly_name"]
    kind = feed["connector_kind"]
    landing_expr = _connector_build_expr(kind, friendly_name, with_watermark=False)
    return f'''
@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def landing_{friendly_name}(
    context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    # Explicit master-pipeline extraction-start step -- guaranteed before
    # connector.fetch() runs, not just an incidental side effect of the
    # stage-logging context below. Matters for any feed that queries
    # data_processing_runs as its own source (metadata_runs).
    postgres_metadata.record_run_started(
        data_feed_id=str(data_feed["id"]),
        dagster_run_id=context.run_id,
        tracking_group=data_feed["batch_group_friendly_name"],
    )
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="landing",
        dagster_run_id=context.run_id,
    ) as log:
        connector = {landing_expr}
        df = connector.fetch()
        watermark_column = data_feed.get("watermark_column")
        last_watermark = data_feed.get("last_watermark_value")
        if not df.is_empty() and watermark_column and last_watermark is not None:
            df = df.filter(pl.col(watermark_column) > last_watermark)
        if not df.is_empty():
            discovered = connector.discover_schema(df)
            postgres_metadata.sync_schema_registry(
                data_feed_id=str(data_feed["id"]),
                discovered_column_definitions=discovered,
                created_by="landing_{friendly_name}",
            )
        log.set_counts(
            rows_read=df.height,
            watermark_value_start=last_watermark,
            watermark_value_end=(
                str(df[watermark_column].max()) if (not df.is_empty() and watermark_column) else last_watermark
            ),
        )
    return Output(df, metadata={{"audit_run_id": log.run_id, "row_count": df.height}})


@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def raw_{friendly_name}(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_{friendly_name}: pl.DataFrame,
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="raw",
        dagster_run_id=context.run_id,
    ) as log:
        df = landing_{friendly_name}
        if not df.is_empty():
            raw_run_dir = _data_lake_dir() / "raw" / "{friendly_name}" / f"run_id={{context.run_id}}"
            raw_run_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(raw_run_dir / "{friendly_name}.parquet")
        log.set_counts(rows_read=df.height)
    return Output(df, metadata={{"audit_run_id": log.run_id, "row_count": df.height}})


@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def clean_{friendly_name}(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_{friendly_name}: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    df = raw_{friendly_name}
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="clean",
        dagster_run_id=context.run_id,
    ) as log:
        if not df.is_empty():
            column_definitions = postgres_metadata.get_current_schema(str(data_feed["id"]))
            df = reconcile_schema(df, column_definitions)
            validate_schema(df, column_definitions)
            catalog = iceberg_catalog.get_catalog()
            write_clean_snapshot(
                catalog, namespace="clean", table_name="{friendly_name}", df=df, column_definitions=column_definitions,
            )
        log.set_counts(rows_inserted=df.height)

    watermark_column = data_feed.get("watermark_column")
    if not df.is_empty() and watermark_column:
        postgres_metadata.update_watermark(
            data_feed_id=str(data_feed["id"]), watermark_value=str(df[watermark_column].max()), run_id=context.run_id
        )

    return Output(None, metadata={{"audit_run_id": log.run_id, "rows_inserted": df.height}})
'''


def _render_json_assets(feed: dict) -> str:
    friendly_name = feed["feed_friendly_name"]
    kind = feed["connector_kind"]
    landing_expr = _connector_build_expr(kind, friendly_name, with_watermark=True)
    clean_expr = _connector_build_expr(kind, friendly_name, with_watermark=False)
    return f'''
@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def landing_{friendly_name}(
    context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    # Explicit master-pipeline extraction-start step -- guaranteed before
    # connector.fetch() runs, not just an incidental side effect of the
    # stage-logging context below. Matters for any feed that queries
    # data_processing_runs as its own source (metadata_runs).
    postgres_metadata.record_run_started(
        data_feed_id=str(data_feed["id"]),
        dagster_run_id=context.run_id,
        tracking_group=data_feed["batch_group_friendly_name"],
    )
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="landing",
        dagster_run_id=context.run_id,
    ) as log:
        connector = {landing_expr}
        df = connector.fetch()
        log.set_counts(rows_read=df.height, watermark_value_start=data_feed.get("last_watermark_value"))
    return Output(df, metadata={{"audit_run_id": log.run_id, "row_count": df.height}})


@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def raw_{friendly_name}(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    landing_{friendly_name}: pl.DataFrame,
) -> Output[pl.DataFrame]:
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="raw",
        dagster_run_id=context.run_id,
    ) as log:
        df = landing_{friendly_name}
        if not df.is_empty():
            raw_run_dir = _data_lake_dir() / "raw" / "{friendly_name}" / f"run_id={{context.run_id}}"
            raw_run_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(raw_run_dir / "{friendly_name}.parquet")
        log.set_counts(rows_read=df.height)
    return Output(df, metadata={{"audit_run_id": log.run_id, "row_count": df.height}})


@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def clean_{friendly_name}(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
    raw_{friendly_name}: pl.DataFrame,
) -> Output[None]:
    # Extraction and validation combine into this one stage for nested-JSON
    # sources -- flattening is inseparable from establishing the real
    # (flat) schema contract, see connectors/base.py's JsonConnector.
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    df = raw_{friendly_name}
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        tracking_group=data_feed["batch_group_friendly_name"],
        stage="clean",
        dagster_run_id=context.run_id,
    ) as log:
        if not df.is_empty():
            connector = {clean_expr}
            df = connector.flatten(df)
            discovered = connector.discover_schema(df)
            sync_result = postgres_metadata.sync_schema_registry(
                data_feed_id=str(data_feed["id"]),
                discovered_column_definitions=discovered,
                created_by="clean_{friendly_name}",
            )
            df = reconcile_schema(df, sync_result.column_definitions)
            validate_schema(df, sync_result.column_definitions)
            catalog = iceberg_catalog.get_catalog()
            write_clean_snapshot(
                catalog,
                namespace="clean",
                table_name="{friendly_name}",
                df=df,
                column_definitions=sync_result.column_definitions,
            )
        log.set_counts(rows_inserted=df.height)

    watermark_column = data_feed.get("watermark_column")
    if not df.is_empty() and watermark_column:
        postgres_metadata.update_watermark(
            data_feed_id=str(data_feed["id"]), watermark_value=str(df[watermark_column].max()), run_id=context.run_id
        )

    return Output(None, metadata={{"audit_run_id": log.run_id, "rows_inserted": df.height}})
'''


def render(feeds: list[str], connector_feeds: list[dict], feed_schedules: list[dict], model_schedules: list[dict]) -> str:
    feeds_repr = ", ".join(f'"{f}"' for f in feeds)

    connector_imports = []
    base_locations = {}
    connector_asset_blocks = []
    connector_asset_names = []
    for feed in connector_feeds:
        friendly_name = feed["feed_friendly_name"]
        kind = feed["connector_kind"]
        if kind in _JSON_KINDS:
            connector_imports.append(
                f'from dagster_data_platform.connectors.{friendly_name}_connector import Connector as _{friendly_name}_Connector'
            )
        if kind == "rest":
            base_locations[friendly_name] = feed["base_location"]
        if kind in _TABULAR_KINDS:
            connector_asset_blocks.append(_render_tabular_assets(feed))
        else:
            connector_asset_blocks.append(_render_json_assets(feed))
        connector_asset_names += [f"landing_{friendly_name}", f"raw_{friendly_name}", f"clean_{friendly_name}"]

    connector_imports_block = "\n".join(sorted(set(connector_imports)))
    base_locations_repr = "{" + ", ".join(f'"{k}": "{v}"' for k, v in base_locations.items()) + "}"
    connector_assets_block = "\n".join(connector_asset_blocks)
    connector_asset_names_block = ", ".join(connector_asset_names)

    schedule_calls = []
    for row in feed_schedules:
        schedule_calls.append(
            "    _make_feed_schedule(\n"
            f'        python_name="{_schedule_python_name(row["schedule_id"], row["feed_friendly_name"])}",\n'
            f'        schedule_id="{row["schedule_id"]}",\n'
            f'        cron="{row["cron"]}",\n'
            f'        feed_friendly_name="{row["feed_friendly_name"]}",\n'
            '        controlling_object_type="feed",\n'
            f'        controlling_object_friendly_name="{row["feed_friendly_name"]}",\n'
            "    ),"
        )
    for row in model_schedules:
        schedule_calls.append(
            "    _make_feed_schedule(\n"
            f'        python_name="{_schedule_python_name(row["schedule_id"], row["feed_friendly_name"])}",\n'
            f'        schedule_id="{row["schedule_id"]}",\n'
            f'        cron="{row["cron"]}",\n'
            f'        feed_friendly_name="{row["feed_friendly_name"]}",\n'
            '        controlling_object_type="model",\n'
            f'        controlling_object_friendly_name="{row["model_friendly_name"]}",\n'
            "    ),"
        )
    schedules_block = "\n".join(schedule_calls) if schedule_calls else "    # no active schedule rows"

    return f'''"""GENERATED by scripts/generate_dagster_pipeline.py -- DO NOT EDIT BY HAND.

Regenerate via `just orchestration::generate-pipeline-jobs`. See that
script's module docstring for why this is a build-time codegen artifact
rather than a live Postgres read folded into module import.
"""

import os
from pathlib import Path

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DefaultScheduleStatus,
    Output,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    schedule,
)

from connectors import CSVConnector, PostgresConnector
{connector_imports_block}
from dagster_data_platform.assets.dbt_assets import _build_dbt_assets_for_feed
from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from raw_to_clean import reconcile_schema, validate_schema, write_clean_snapshot

# source_system.base_location per REST-connector-backed feed, resolved at
# codegen time (structural -- which feeds are REST-backed -- not a live
# lookup; the base URL itself is stable per feed, unlike a watermark).
source_system_base_location = {base_locations_repr}


# .../orchestration/dagster_data_platform/dagster_data_platform/pipeline_generated.py
# -> repo root is 3 parents up, same convention as the hand-written asset
# files' REPO_ROOT (financial_assets.py etc., 4 parents up from one
# directory deeper).
REPO_ROOT = Path(__file__).resolve().parents[3]


def _data_lake_dir() -> Path:
    return Path(os.environ["DATA_LAKE_PATH"]) if "DATA_LAKE_PATH" in os.environ else REPO_ROOT / "data-lake"


# One _build_dbt_assets_for_feed(...) call per active feed, and only here --
# calling this factory twice for the same feed would construct two @dbt_assets
# defs both claiming the same AssetKeys, which Dagster rejects.
DBT_ASSETS = {{f: _build_dbt_assets_for_feed(f) for f in [{feeds_repr}]}}
ALL_DBT_ASSETS = list(DBT_ASSETS.values())

# --- connector-driven landing/raw/clean assets ------------------------------
# One block per feed whose source_system.connector_kind is set (see
# scripts/generate_dagster_pipeline.py's module docstring) -- a feed with
# connector_kind IS NULL keeps a fully hand-written asset file instead.
{connector_assets_block}

ALL_CONNECTOR_ASSETS = [{connector_asset_names_block}]

# .upstream() (not a bare .groups(feed)) so a feed's job also pulls in
# whatever cross-feed Dagster dependencies its own assets actually have --
# e.g. fct_daily_financial_activity is tagged only 'sales' for dbt-asset
# ownership (see that model's own comment for why), but sales_job still
# needs to build stg_financial_transactions first when run standalone, not
# just under the full __ASSET_JOB. A no-op for every feed with no
# cross-feed dependents.
FEED_JOBS = {{
    f: define_asset_job(f"{{f}}_job", selection=AssetSelection.groups(f).upstream())
    for f in [{feeds_repr}]
}}
ALL_FEED_JOBS = list(FEED_JOBS.values())


def _make_feed_schedule(
    *, python_name, schedule_id, cron, feed_friendly_name, controlling_object_type, controlling_object_friendly_name
):
    job = FEED_JOBS[feed_friendly_name]

    # postgres_metadata is declared as a direct parameter (not pulled from
    # context.resources) -- Dagster infers the resource requirement from
    # the parameter name itself; passing required_resource_keys= as well
    # is rejected outright ("Cannot specify resource requirements in both
    # @schedule decorator and as arguments to the decorated function"),
    # confirmed for real against the installed Dagster version.
    @schedule(
        name=python_name,
        cron_schedule=cron,
        job=job,
        default_status=DefaultScheduleStatus.STOPPED,
    )
    def _fn(context, postgres_metadata: PostgresMetadataResource):
        pg = postgres_metadata
        if not pg.is_schedule_active(schedule_id):
            return SkipReason(f"schedule {{schedule_id}} is no longer active")
        data_feed = pg.get_data_feed(feed_friendly_name)
        return RunRequest(
            tags={{
                "schedule_id": schedule_id,
                "controlling_object_type": controlling_object_type,
                "controlling_object_friendly_name": controlling_object_friendly_name,
                "feed_friendly_name": feed_friendly_name,
                "batch_group_friendly_name": data_feed["batch_group_friendly_name"],
            }}
        )

    return _fn


ALL_SCHEDULES = [
{schedules_block}
]
'''


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        feeds = fetch_active_feeds(cur)
        connector_feeds = fetch_connector_feeds(cur)
        feed_schedules = fetch_feed_schedules(cur)
        model_schedules = fetch_model_schedules(cur)

    OUTPUT_PATH.write_text(render(feeds, connector_feeds, feed_schedules, model_schedules))
    print(
        f"Generated {OUTPUT_PATH} -- {len(feeds)} feed job(s), {len(connector_feeds)} connector-driven feed(s), "
        f"{len(feed_schedules) + len(model_schedules)} schedule(s)."
    )


if __name__ == "__main__":
    main()
