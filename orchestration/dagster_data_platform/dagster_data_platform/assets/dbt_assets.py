import json
from typing import Any, Mapping

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from dagster_data_platform.clean_source_tables_generated import CLEAN_SOURCE_TABLES
from dagster_data_platform.pipeline_steps import parse_selected_steps
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource

# No module-level single DbtProject anymore -- each domain
# (dbt/domains/<domain>/, see Roadmap.md "multi-project dbt split") is its
# own dbt project with its own manifest, genuinely compile-isolated from
# every other domain. definitions.py discovers one DbtProject per domain
# directory (glob + prepare_if_dev() per instance) and passes it into
# _build_transformation_assets_for_domain/_build_serving_assets_for_domain
# below, rather than this module owning one hardcoded project.

# CLEAN_SOURCE_TABLES is generated (scripts/generate_dagster_pipeline.py,
# clean_source_tables_generated.py) from every active data_feed row --
# previously a hardcoded set here requiring a manual edit per new feed
# (the exact same category of gap _sources.yml's own codegen closed, see
# generate_sources.py). Maps `clean.<table>` sources onto their matching
# Dagster asset key so the extraction -> raw -> clean chain and dbt's
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
        # group_name=domain_group_name(domain) (NOT the owning feed anymore,
        # and NOT the bare domain string either -- see domain_group_name()'s
        # own docstring for why the prefix is required) is what lets
        # AssetSelection.groups(domain_group_name(domain)) select a whole
        # domain's transformation+serving dbt assets purely from a metadata
        # string, no hardcoded per-domain asset lists needed. Feed-level
        # extraction/raw/clean assets (extraction_assets.py etc.) keep
        # group_name=<feed friendly_name>, unchanged -- these are two
        # different, deliberately-disambiguated namespaces now (see
        # Roadmap.md "multi-project dbt split"). None keeps superclass
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

# Every streaming/ serve view (dbt/domains/<schema>/models/serve/streaming/,
# see generate_streaming_serve_scaffolds.py) carries this blanket tag,
# regardless of model_schema/streaming_source. Confirmed live: without this
# exclusion, a plain `sales_modeling_job` run (the ordinary batch
# transformation step, unrelated to streaming) tried to build
# serve.sales_events/serve.inventory_events and failed with
# TrinoUserError TABLE_NOT_FOUND -- those views select from
# iceberg.streaming.* tables that only exist once Flink has done its first
# checkpoint, which nothing in the batch pipeline waits for or should have
# to. Batch and streaming are deliberately independent build graphs now
# (the only real coupling is streaming serve views joining OUT to
# already-persisted model-layer tables, which are assumed to already exist
# -- see Roadmap.md) -- this tag is what keeps master_pipeline's
# MODELING_JOBS/SERVING_JOBS from ever touching a streaming serve view.
_STREAMING_TAG = "tag:streaming"


def _run_dbt_build_and_log_stages(
    *,
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    postgres_metadata: PostgresMetadataResource,
    domain: str,
    feeds: list[str],
    stages: tuple[str, ...],
    earliest_stage: str,
    step_name: str,
):
    """Shared body for both the transformation and serving dbt asset
    factories below -- same dbt-build-then-log-per-stage shape as before
    the domain split, now parameterized by domain + the feeds that domain
    spans (a domain job can bundle several feeds' models, see Roadmap.md
    "multi-project dbt split"). `dbt.cli(..., context=context)` derives the
    matching dbt `--select`/`--exclude` automatically from the decorator's
    own select=/exclude= (see _build_transformation_assets_for_domain/
    _build_serving_assets_for_domain below) -- no need to pass them again
    here, only the per-feed cherry-pick excludes below are extra.

    Per-feed cherry-picking: pipeline_steps gating used to be a static
    per-feed check before this ever ran (one feed, one @dbt_assets def).
    Now that a domain job can span multiple feeds, which of them have
    `step_name` deselected has to be resolved live, per run, and passed as
    `--exclude tag:<feed>` extra CLI args -- the per-feed `tag:<feed>` tag
    generate_model_scaffolds.py/generate_ods_models.py/
    generate_serve_views.py already stamp on every generated file is what
    makes this possible without any new dbt tagging convention.
    """
    data_feeds = {feed: postgres_metadata.get_data_feed(feed) for feed in feeds}
    excluded_feeds = [
        feed for feed, data_feed in data_feeds.items()
        if step_name not in parse_selected_steps(data_feed["pipeline_steps"])
    ]
    if len(excluded_feeds) == len(feeds):
        # Every feed in this domain has this step deselected this run --
        # skip dbt entirely, same no-op behavior the old static per-feed
        # check gave for a single-feed build, just resolved dynamically.
        return

    updates_enabled_map: dict[str, bool] = {}
    for feed in feeds:
        updates_enabled_map.update(postgres_metadata.get_updates_enabled_map(feed))

    extra_args = []
    for feed in excluded_feeds:
        extra_args += ["--exclude", f"tag:{feed}"]

    invocation = dbt.cli(
        ["build", "--vars", json.dumps({"updates_enabled_by_model": updates_enabled_map}), *extra_args],
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
            invocation.get_error() or f"dbt build failed for domain '{domain}' before any node ran"
        )

    master_dagster_run_id = context.run.tags["master_dagster_run_id"]
    for stage in stages:
        with postgres_metadata.log_data_model_stage(
            model_key=domain,
            stage=stage,
            master_dagster_run_id=master_dagster_run_id,
            dagster_run_id=context.run_id,
        ) as log:
            if not stage_ok[stage]:
                raise RuntimeError(stage_error[stage])
            log.set_counts(rows_updated=stage_rows.get(stage))


def domain_group_name(domain: str) -> str:
    """The Dagster group_name a domain's dbt assets carry -- 'domain_<domain>',
    NOT the bare domain string. Confirmed live, the hard way: domain names
    and feed friendly_names share the same flat group_name string space in
    the asset graph, so a bare group_name=domain collides whenever a
    domain happens to be named the same as a feed (e.g. the 'sales' domain
    vs. the 'sales' feed; the 'police_crimes' ODS domain vs. the
    'police_crimes' feed it defaults its batch_ods_name from) -- a
    group-based AssetSelection.groups(domain) would then silently pull that
    feed's own extraction/raw/clean assets in alongside the intended dbt
    steps. MODELING_JOBS/SERVING_JOBS (scripts/generate_dagster_pipeline.py)
    now select by the exact AssetsDefinition object each domain factory
    returns rather than by group, sidestepping this specific collision --
    but the prefix stays: it's still what disambiguates the two namespaces
    for Dagit's own asset-graph grouping/display, and matches the
    'domain_<domain>' convention already used for each domain's own dbt
    project/profile name (scripts/generate_domain_projects.py)."""
    return f"domain_{domain}"


def _build_transformation_assets_for_domain(domain: str, feeds: list[str], dbt_project: DbtProject):
    """clean -> staging -> model for one domain -- the 'transformation'
    pipeline step. Excludes the generated serve views (see
    _build_serving_assets_for_domain) so transformation and serving are two
    independently selectable dbt build invocations, not one bundled
    command -- required for a domain to genuinely skip serving while still
    running transformation (see the master pipeline / cherry-picking
    design, metadata/DataModel.md's `pipeline_steps` section).

    select=/exclude= no longer need a `tag:<feed>` term to scope down to
    one feed out of a shared manifest -- this domain's manifest ONLY ever
    contains this domain's own models (compile isolation, see Roadmap.md
    "multi-project dbt split"), so exclude=_SERVING_LAYER_TAG is already
    the full transformation set modulo streaming. Per-feed tags still
    matter for the cherry-picking done inside _run_dbt_build_and_log_stages,
    just not for this selector.

    Also excludes _STREAMING_TAG -- streaming/ serve views (see its own
    docstring) are a deliberately separate build graph from batch, not
    part of MODELING_JOBS/SERVING_JOBS at all; they're built directly via
    the streaming module's own tooling once their source Iceberg table
    exists, never through master_pipeline.

    One @dbt_assets function per domain, not one for the whole platform,
    same reasoning as before the split: a single function running
    `dbt build` across every domain's models would have to sit under one
    concurrency pool, wrongly serializing unrelated domains (Learnings.md,
    Phase 5).

    dbt_cli is constructed HERE, directly (`DbtCliResource(project_dir=dbt_project)`,
    a plain object, not injected via Dagster's `resources={}` DI), and closed
    over by the returned function rather than taken as a `dbt: DbtCliResource`
    parameter. This deliberately diverges from the multi-project-dbt-split
    plan's original proposal (N uniquely-keyed DbtCliResource resources in
    Definitions) -- confirmed against the installed dagster-dbt source that
    Dagster resolves a function's resource parameters by NAME against the
    Definitions-level resources={} dict, so every domain's function using the
    literal parameter name `dbt` would collide on one shared key/project dir.
    DbtCliResource is a plain ConfigurableResource (pydantic model with a
    .cli() method) and works identically whether Dagster-injected or
    hand-constructed, so closing over a per-domain instance sidesteps the
    naming collision entirely with no resources={} wiring needed for it at
    all -- only postgres_metadata (still genuinely shared/singleton) stays a
    real injected resource parameter.
    """
    dbt_cli = DbtCliResource(project_dir=dbt_project)

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=DataPlatformDbtTranslator(group_name=domain_group_name(domain)),
        exclude=f"{_SERVING_LAYER_TAG} {_STREAMING_TAG}",
        pool=f"domain:{domain}",
        name=f"dbt_{domain}_transformation_assets",
    )
    def _transformation_assets_for_domain(
        context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
    ):
        yield from _run_dbt_build_and_log_stages(
            context=context,
            dbt=dbt_cli,
            postgres_metadata=postgres_metadata,
            domain=domain,
            feeds=feeds,
            stages=("staging", "model"),
            earliest_stage="staging",
            step_name="transformation",
        )

    return _transformation_assets_for_domain


def _build_serving_assets_for_domain(domain: str, feeds: list[str], dbt_project: DbtProject):
    """model -> serve for one domain -- the 'serving' pipeline step.
    select=_SERVING_LAYER_TAG alone (no `tag:<feed>` intersection needed,
    same compile-isolation reasoning as the transformation factory above):
    every generated `_latest`/`_historical` view in this domain's own
    manifest, nothing else. Same closed-over dbt_cli construction as
    _build_transformation_assets_for_domain above -- see its docstring for
    why this isn't a Dagster-injected resource parameter."""
    dbt_cli = DbtCliResource(project_dir=dbt_project)

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=DataPlatformDbtTranslator(group_name=domain_group_name(domain)),
        select=_SERVING_LAYER_TAG,
        pool=f"domain:{domain}",
        name=f"dbt_{domain}_serving_assets",
    )
    def _serving_assets_for_domain(
        context: AssetExecutionContext, postgres_metadata: PostgresMetadataResource
    ):
        yield from _run_dbt_build_and_log_stages(
            context=context,
            dbt=dbt_cli,
            postgres_metadata=postgres_metadata,
            domain=domain,
            feeds=feeds,
            stages=("serve",),
            earliest_stage="serve",
            step_name="serving",
        )

    return _serving_assets_for_domain


# No hardcoded per-domain calls here anymore -- scripts/generate_dagster_pipeline.py
# calls _build_transformation_assets_for_domain(...)/_build_serving_assets_for_domain(...)
# once per resolved domain (a live Postgres read at build/start time, not
# at this module's import time) and writes the results into
# pipeline_generated.TRANSFORMATION_ASSETS/SERVING_ASSETS, constructing one
# DbtProject per domain itself (see that script for why, not definitions.py --
# a deliberate deviation from the original plan). Call each factory at most
# once per domain anywhere in the codebase -- calling either twice for the
# same domain would construct two different @dbt_assets defs both claiming
# the same AssetKeys, which Dagster rejects.
