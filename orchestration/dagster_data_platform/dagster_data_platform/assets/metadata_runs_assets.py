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
