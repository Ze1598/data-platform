import json
from pathlib import Path
from typing import Any, Mapping

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from dagster_data_platform.clean_source_tables_generated import CLEAN_SOURCE_TABLES
from dagster_data_platform.pipeline_steps import parse_selected_steps
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

# CLEAN_SOURCE_TABLES is generated (scripts/generate_dagster_pipeline.py,
# clean_source_tables_generated.py) from every active data_feed row --
# previously a hardcoded set here requiring a manual edit per new feed
# (the exact same category of gap _sources.yml's own codegen closed, see
# generate_sources.py). Maps `clean.<table>` sources onto their matching
# Dagster asset key so the landing -> raw -> clean chain and dbt's
# staging/ODS models share one asset graph instead of two
# coincidentally-ordered ones.
_CLEAN_SOURCE_TABLES = CLEAN_SOURCE_TABLES

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
    def __init__(self, group_name: str | None = None):
        super().__init__()
        self._group_name = group_name

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if (
            dbt_resource_props["resource_type"] == "source"
            and dbt_resource_props["source_name"] == "clean"
            and dbt_resource_props["name"] in _CLEAN_SOURCE_TABLES
        ):
            return AssetKey(f"clean_{dbt_resource_props['name']}")
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        # Lets every dbt-layer asset for a feed share group_name=<feed
        # friendly_name> with that feed's hand-written landing/raw/clean
        # assets (see extraction_assets.py etc.) -- AssetSelection.groups()
        # then selects a feed's entire chain purely from a metadata string,
        # no hardcoded per-feed asset lists needed (see
        # scripts/generate_dagster_pipeline.py). None keeps superclass
        # behavior, so a bare DataPlatformDbtTranslator() (e.g.
        # tests/test_dbt_assets.py) is unaffected.
        if self._group_name is not None:
            return self._group_name
        return super().get_group_name(dbt_resource_props)


# `path:` selectors don't work reliably here -- @dbt_assets resolves
# select=/exclude= *in-process* (dagster_dbt.utils._select_unique_ids_from_manifest),
# building a synthetic Manifest object directly from the JSON dict with no
# real project root, so a `path:` selector (which needs to resolve a
# relative filesystem path) silently matches zero nodes there even though
# it resolves correctly via the real `dbt ls`/`dbt build` CLI (a
# completely different code path with real project context) -- confirmed
# directly, `just smoketest` failed with "does not match any enabled
# nodes" against every `path:` variant tried, while `dbt ls --select`
# against the exact same string succeeded every time. `tag:` selectors
# don't have this problem (pure attribute matching on the manifest, no
# filesystem/project context needed) -- generate_serve_views.py tags every
# generated view with this, alongside its owning feed tag.
_SERVING_LAYER_TAG = "tag:serving_layer"


def _run_dbt_build_and_log_stages(
    *,
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    postgres_metadata: PostgresMetadataResource,
    feed_friendly_name: str,
    stages: tuple[str, ...],
    earliest_stage: str,
):
    """Shared body for both the transformation and serving dbt asset
    factories below -- same dbt-build-then-log-per-stage shape as before
    the split, just parameterized by which stage subset each one owns.
    `dbt.cli(..., context=context)` derives the matching dbt `--select`/
    `--exclude` automatically from the decorator's own select=/exclude=
    (see _build_transformation_assets_for_feed/_build_serving_assets_for_feed
    below) -- no need to pass them again here.
    """
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

    # Per-node status, not the single overall is_successful() flag -- a
    # model-layer failure shouldn't mark staging as failed too (a single
    # dbt build invocation still runs its nodes DAG-ordered in one process;
    # they're genuinely independent outcomes).
    stage_rows: dict[str, int] = {}
    stage_ok = {stage: True for stage in stages}
    stage_error: dict[str, str] = {}
    for result in run_results.get("results", []):
        stage = _stage_for_dbt_node(result["unique_id"])
        if stage not in stages:
            continue
        if result.get("status") not in ("success", "pass"):
            stage_ok[stage] = False
            stage_error[stage] = result.get("message") or f"{result['unique_id']} failed"
        adapter_response = result.get("adapter_response") or {}
        if "rows_affected" in adapter_response:
            stage_rows[stage] = stage_rows.get(stage, 0) + adapter_response["rows_affected"]

    # A build-level failure with zero per-node results (e.g. a parse error
    # before execution started) -- none of this invocation's nodes ever got
    # a chance to run; attribute it to the earliest stage this invocation owns.
    if not run_results.get("results") and not invocation.is_successful():
        stage_ok[earliest_stage] = False
        stage_error[earliest_stage] = str(
            invocation.get_error() or f"dbt build failed for feed '{feed_friendly_name}' before any node ran"
        )

    for stage in stages:
        with postgres_metadata.log_data_model_stage(
            model_key=feed_friendly_name,
            uses_feeds=feed_friendly_name,
            # Every lakehouse_models row built today uses model_schema
            # 'model' (staging/serve are naming-convention-only, not
            # separately tracked lakehouse_models rows) -- hardcoded rather
            # than looked up per stage since a single data_processing_runs
            # row spans all three stages and needs one tracking_group
            # value. Revisit once a second model_schema is actually in use.
            tracking_group="model",
            stage=stage,
            dagster_run_id=context.run_id,
        ) as log:
            if not stage_ok[stage]:
                raise RuntimeError(stage_error[stage])
            log.set_counts(rows_updated=stage_rows.get(stage))


def _build_transformation_assets_for_feed(feed_friendly_name: str):
    """clean -> staging -> model for one feed -- the 'transformation'
    pipeline step. Excludes the generated serve views (see
    _build_serving_assets_for_feed) so transformation and serving are two
    independently selectable dbt build invocations, not one bundled
    command -- required for a feed to genuinely skip serving while still
    running transformation (see the master pipeline / cherry-picking
    design, metadata/DataModel.md's `pipeline_steps` section).

    One @dbt_assets function per feed, not one for the whole project, same
    reasoning as before the split: a single function running `dbt build`
    across every feed's models would have to sit under one concurrency
    pool, wrongly serializing unrelated feeds (Learnings.md, Phase 5).
    """

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=DataPlatformDbtTranslator(group_name=feed_friendly_name),
        select=f"tag:{feed_friendly_name}",
        exclude=_SERVING_LAYER_TAG,
        pool=f"feed:{feed_friendly_name}",
        name=f"dbt_{feed_friendly_name}_transformation_assets",
    )
    def _transformation_assets_for_feed(
        context: AssetExecutionContext, dbt: DbtCliResource, postgres_metadata: PostgresMetadataResource
    ):
        data_feed = postgres_metadata.get_data_feed(feed_friendly_name)
        if "transformation" not in parse_selected_steps(data_feed["pipeline_steps"]):
            return
        yield from _run_dbt_build_and_log_stages(
            context=context,
            dbt=dbt,
            postgres_metadata=postgres_metadata,
            feed_friendly_name=feed_friendly_name,
            stages=("staging", "model"),
            earliest_stage="staging",
        )

    return _transformation_assets_for_feed


def _build_serving_assets_for_feed(feed_friendly_name: str):
    """model -> serve for one feed -- the 'serving' pipeline step.
    Intersection select (comma = AND in dbt's selector syntax): only this
    feed's generated `_latest`/`_historical` views, nothing else."""

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=DataPlatformDbtTranslator(group_name=feed_friendly_name),
        select=f"tag:{feed_friendly_name},{_SERVING_LAYER_TAG}",
        pool=f"feed:{feed_friendly_name}",
        name=f"dbt_{feed_friendly_name}_serving_assets",
    )
    def _serving_assets_for_feed(
        context: AssetExecutionContext, dbt: DbtCliResource, postgres_metadata: PostgresMetadataResource
    ):
        data_feed = postgres_metadata.get_data_feed(feed_friendly_name)
        if "serving" not in parse_selected_steps(data_feed["pipeline_steps"]):
            return
        yield from _run_dbt_build_and_log_stages(
            context=context,
            dbt=dbt,
            postgres_metadata=postgres_metadata,
            feed_friendly_name=feed_friendly_name,
            stages=("serve",),
            earliest_stage="serve",
        )

    return _serving_assets_for_feed


# No hardcoded per-feed calls here anymore -- scripts/generate_dagster_pipeline.py
# calls _build_transformation_assets_for_feed(...)/_build_serving_assets_for_feed(...)
# once per active data_feed row (a live Postgres read at build/start time, not
# at this module's import time) and writes the results into
# pipeline_generated.TRANSFORMATION_ASSETS/SERVING_ASSETS. Call each factory at most once per feed
# anywhere in the codebase -- calling either twice for the same feed would
# construct two different @dbt_assets defs both claiming the same AssetKeys,
# which Dagster rejects.
