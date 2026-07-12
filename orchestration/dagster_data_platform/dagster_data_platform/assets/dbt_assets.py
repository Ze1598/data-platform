import json
from pathlib import Path
from typing import Any, Mapping

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource

# .../orchestration/dagster_data_platform/dagster_data_platform/assets/dbt_assets.py
# -> repo root is 4 parents up. Kept as a repo-relative path (not an
# absolute one) so it resolves the same way both in local dev and inside
# the orchestration Docker image, which preserves this same relative
# layout (see orchestration/Dockerfile).
REPO_ROOT = Path(__file__).resolve().parents[4]
_DBT_PROJECT_DIR = REPO_ROOT / "dbt" / "data_platform"
dbt_project = DbtProject(project_dir=_DBT_PROJECT_DIR, profiles_dir=_DBT_PROJECT_DIR / "profiles")
dbt_project.prepare_if_dev()

# Every feed with dbt models needs its `clean.<table>` source mapped onto
# the matching stub asset key (extraction_assets.py / sales_assets.py) so
# the landing -> raw -> clean chain and dbt's staging model share one asset
# graph instead of two coincidentally-ordered ones. Add an entry here when
# a new feed's staging model is added.
_CLEAN_SOURCE_TABLES = {"customers", "sales", "financial_transactions", "police_crimes"}

# `dbt build --select tag:<feed>` builds staging, model-layer, AND serve
# objects together in one DAG-ordered invocation (Phase 7 added dim_*/
# fct_*/snapshots tagged the same way as their feed; Phase 8's generated
# _latest/_historical views inherit their tags from lakehouse_models'
# depends_on_feeds, see scripts/generate_serve_views.py) -- no separate dbt
# invocation needed, but data_processing_runs tracks them as distinct
# stages, so results get split by which of these a node's name matches. Generic test
# unique_ids embed their target's name (e.g. "not_null_stg_customers_id"),
# so a substring/suffix check on the whole unique_id (not just the node
# name itself) classifies tests correctly too.
_STAGING_MODEL_NAMES = {f"stg_{table}" for table in _CLEAN_SOURCE_TABLES}
_SERVE_MODEL_SUFFIXES = ("_latest", "_historical")
_DATA_MODEL_STAGES_BUILT = ("staging", "model", "serve")


def _stage_for_dbt_node(unique_id: str) -> str:
    if any(name in unique_id for name in _STAGING_MODEL_NAMES):
        return "staging"
    if any(suffix in unique_id for suffix in _SERVE_MODEL_SUFFIXES):
        return "serve"
    return "model"


class DataPlatformDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if (
            dbt_resource_props["resource_type"] == "source"
            and dbt_resource_props["source_name"] == "clean"
            and dbt_resource_props["name"] in _CLEAN_SOURCE_TABLES
        ):
            return AssetKey(f"clean_{dbt_resource_props['name']}")
        return super().get_asset_key(dbt_resource_props)


def _build_dbt_assets_for_feed(feed_friendly_name: str):
    """One @dbt_assets function per feed, not one for the whole project.

    Necessary, not just tidier: a single @dbt_assets function running
    `dbt build` across every feed's models would have to sit under one
    concurrency pool (see extraction_assets.py FEED_POOL) — which would
    wrongly serialize unrelated feeds against each other the moment a
    second feed got its own dbt model. Flagged as a known boundary back
    in Phase 5 (Learnings.md); this is that boundary being hit.

    `select=f"tag:{feed_friendly_name}"` scopes which assets this function
    owns in Dagster's graph; `dbt.cli(["build"], context=context)` derives
    the matching dbt `--select` automatically from that same context — no
    need to pass --select twice.
    """

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=DataPlatformDbtTranslator(),
        select=f"tag:{feed_friendly_name}",
        pool=f"feed:{feed_friendly_name}",
        name=f"dbt_{feed_friendly_name}_assets",
    )
    def _dbt_assets_for_feed(
        context: AssetExecutionContext, dbt: DbtCliResource, postgres_metadata: PostgresMetadataResource
    ):
        # updates_enabled_by_model: resolved from Postgres here, not inside
        # the dbt SQL itself (Trino has no catalog federating into
        # platform_metadata) -- see PostgresMetadataResource
        # .get_updates_enabled_map()'s docstring and stg_customers.sql for
        # how each model consumes it.
        updates_enabled_map = postgres_metadata.get_updates_enabled_map(feed_friendly_name)
        invocation = dbt.cli(
            ["build", "--vars", json.dumps({"updates_enabled_by_model": updates_enabled_map})],
            context=context,
        )
        yield from invocation.stream()

        try:
            run_results = invocation.get_artifact("run_results.json")
        except Exception:
            run_results = {"results": []}

        # Per-node status, not the single overall is_successful() flag --
        # a model-layer failure shouldn't mark staging as failed too (dbt
        # build runs both in one DAG-ordered invocation; they're genuinely
        # independent outcomes even though they share one process).
        stage_rows: dict[str, int] = {}
        stage_ok = {stage: True for stage in _DATA_MODEL_STAGES_BUILT}
        stage_error: dict[str, str] = {}
        for result in run_results.get("results", []):
            stage = _stage_for_dbt_node(result["unique_id"])
            if result.get("status") not in ("success", "pass"):
                stage_ok[stage] = False
                stage_error[stage] = result.get("message") or f"{result['unique_id']} failed"
            adapter_response = result.get("adapter_response") or {}
            if "rows_affected" in adapter_response:
                stage_rows[stage] = stage_rows.get(stage, 0) + adapter_response["rows_affected"]

        # A build-level failure with zero per-node results (e.g. a parse
        # error before execution started) -- neither stage's nodes ever
        # got a chance to run; attribute it to staging as the earliest one.
        if not run_results.get("results") and not invocation.is_successful():
            stage_ok["staging"] = False
            stage_error["staging"] = str(invocation.get_error() or f"dbt build failed for feed '{feed_friendly_name}' before any node ran")

        for stage in _DATA_MODEL_STAGES_BUILT:
            with postgres_metadata.log_data_model_stage(
                model_key=feed_friendly_name,
                uses_feeds=feed_friendly_name,
                # Every lakehouse_models row built today uses model_schema
                # 'model' (staging/serve are naming-convention-only, not
                # separately tracked lakehouse_models rows) -- hardcoded
                # rather than looked up per stage since a single
                # data_processing_runs row spans all three stages
                # (staging/model/serve) and needs one tracking_group value.
                # Revisit once a second model_schema is actually in use.
                tracking_group="model",
                stage=stage,
                dagster_run_id=context.run_id,
            ) as log:
                if not stage_ok[stage]:
                    raise RuntimeError(stage_error[stage])
                log.set_counts(rows_updated=stage_rows.get(stage))

    return _dbt_assets_for_feed


dbt_customers_assets = _build_dbt_assets_for_feed("customers")
dbt_sales_assets = _build_dbt_assets_for_feed("sales")
dbt_financial_transactions_assets = _build_dbt_assets_for_feed("financial_transactions")
dbt_police_crimes_assets = _build_dbt_assets_for_feed("police_crimes")
