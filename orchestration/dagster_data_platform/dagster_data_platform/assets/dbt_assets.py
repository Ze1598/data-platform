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
_CLEAN_SOURCE_TABLES = {"customers", "sales"}


class DataPlatformDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if (
            dbt_resource_props["resource_type"] == "source"
            and dbt_resource_props["source_name"] == "clean"
            and dbt_resource_props["name"] in _CLEAN_SOURCE_TABLES
        ):
            return AssetKey(f"clean_{dbt_resource_props['name']}")
        return super().get_asset_key(dbt_resource_props)


def _build_dbt_assets_for_feed(feed_code: str):
    """One @dbt_assets function per feed, not one for the whole project.

    Necessary, not just tidier: a single @dbt_assets function running
    `dbt build` across every feed's models would have to sit under one
    concurrency pool (see extraction_assets.py FEED_POOL) — which would
    wrongly serialize unrelated feeds against each other the moment a
    second feed got its own dbt model. Flagged as a known boundary back
    in Phase 5 (Learnings.md); this is that boundary being hit.

    `select=f"tag:{feed_code}"` scopes which assets this function owns in
    Dagster's graph; `dbt.cli(["build"], context=context)` derives the
    matching dbt `--select` automatically from that same context — no
    need to pass --select twice.
    """

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=DataPlatformDbtTranslator(),
        select=f"tag:{feed_code}",
        pool=f"feed:{feed_code}",
        name=f"dbt_{feed_code}_assets",
    )
    def _dbt_assets_for_feed(
        context: AssetExecutionContext, dbt: DbtCliResource, postgres_metadata: PostgresMetadataResource
    ):
        # No get_data_feed() lookup needed here (unlike extraction_assets.py) --
        # log_data_model_stage() only needs the feed *code*, which is already
        # in scope, not its data_feed row.
        with postgres_metadata.log_data_model_stage(
            model_key=feed_code,
            uses_feeds=feed_code,
            stage="staging",
            dagster_run_id=context.run_id,
        ) as log:
            invocation = dbt.cli(["build"], context=context)
            yield from invocation.stream()

            if not invocation.is_successful():
                raise invocation.get_error() or RuntimeError(f"dbt build failed for feed '{feed_code}'")

            rows_affected = None
            run_results = invocation.get_artifact("run_results.json")
            for result in run_results.get("results", []):
                adapter_response = result.get("adapter_response") or {}
                if "rows_affected" in adapter_response:
                    rows_affected = (rows_affected or 0) + adapter_response["rows_affected"]
            log.set_counts(rows_updated=rows_affected)

    return _dbt_assets_for_feed


dbt_customers_assets = _build_dbt_assets_for_feed("customers")
dbt_sales_assets = _build_dbt_assets_for_feed("sales")
