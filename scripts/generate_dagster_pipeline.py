"""Generates the dynamic, metadata-driven pipeline foundation -- per-feed
connector-driven extraction/raw/clean assets + EXTRACTION_JOBS, per-domain
transformation/serving dbt assets + MODELING_JOBS/SERVING_JOBS, the single
`master_pipeline` job (parameterized by `orchestration_kind`/
`orchestration_value`, launching those child jobs itself via
dagster_launch.launch_and_wait), and real Dagster schedules (every schedule
now targets `master_pipeline` uniformly, differing only in which
orchestration_kind/value it resolves) -- as
`orchestration/dagster_data_platform/dagster_data_platform/pipeline_generated.py`.

Deliberately a standalone build-time script, not a Dagster op or a live
Postgres read folded into module import: this project's established rule
(see Learnings.md, and generate_serve_views.py/
generate_deletion_synthesis_views.py, the two existing precedents) is that
anything determining Dagster's *static object graph* -- which
assets/jobs/schedules exist, their cron, their target -- must be resolved
before `dagster dev`/Docker image build, never at Python import time. What
an asset/schedule/the master pipeline op's *execution function* looks up
live (is_active, a feed's current watermark, connector fetch() results,
which feeds/domain a given orchestration_kind/value run actually needs) is
a different phase of Dagster's lifecycle and is deliberately NOT baked in
here.

Extraction/raw/clean asset generation (added alongside the pre-existing
dbt/job/schedule generation) is scoped to feeds whose source_system has a
non-null `connector_kind` -- see the connector library plan
(.claude/plans/). A feed with connector_kind IS NULL (customers/sales'
synthetic stub generators) keeps a fully hand-written asset file; this
script only ever generates for a *covered* connector kind, it never
silently skips a feed that needs one.

One job per pipeline stage per feed/domain -- EXTRACTION_JOBS[feed] (raw+
clean bundled as one job/pod, see Roadmap.md "Master pipeline
orchestration" -- "raw and clean are so closely tied together they might
as well be under the same pipeline"), MODELING_JOBS[domain] (staging+
model), SERVING_JOBS[domain] (serve) -- each independently launched as its
own K8sRunLauncher-managed pod by the master pipeline, never nested inside
it. No k8s_job_executor anywhere in this file: once every pipeline stage is
already its own top-level job/pod, splitting a job's own steps into further
pods would just be a third level of nesting nobody asked for (dropped
entirely, not fixed -- see Roadmap.md).

Fully regenerates `pipeline_generated.py` on every run (clears/overwrites,
not additive) so it never drifts from current metadata state.
"""

import os
from pathlib import Path

import psycopg

from generate_domain_projects import slugify_domain

CONN_KWARGS = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    user=os.environ.get("POSTGRES_USER", "platform"),
    password=os.environ.get("POSTGRES_PASSWORD", "platform"),
    dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
)

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = REPO_ROOT / "orchestration" / "dagster_data_platform" / "dagster_data_platform" / "pipeline_generated.py"
# A separate, tiny generated file (not folded into pipeline_generated.py
# itself) specifically to avoid a circular import: pipeline_generated.py
# already imports _build_transformation_assets_for_feed/
# _build_serving_assets_for_feed FROM dbt_assets.py, so dbt_assets.py
# importing CLEAN_SOURCE_TABLES back from pipeline_generated.py would
# create a cycle. This leaf module has no imports of its own, so both
# sides can safely import from it.
CLEAN_SOURCE_TABLES_OUTPUT_PATH = (
    REPO_ROOT / "orchestration" / "dagster_data_platform" / "dagster_data_platform" / "clean_source_tables_generated.py"
)

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


def fetch_domain_dependent_feeds(cur) -> dict[str, list[str]]:
    """domain -> sorted list of feed friendly_names that domain's
    transformation+serving jobs span (Roadmap.md "multi-project dbt
    split"). Real domains: union of depends_on_feeds across that domain's
    lakehouse_models rows. ODS domains: feeds sharing that batch_ods_name.
    Same union-query shape as generate_sources.py::fetch_domain_feeds(),
    mirrored (not imported -- these live in the same scripts/ package
    already, but each generator stays self-contained on purpose, matching
    the rest of this module's existing standalone-fetch-function style)."""
    domain_feeds: dict[str, set[str]] = {}

    cur.execute(
        """
        SELECT lm.model_schema, df.friendly_name
        FROM lakehouse_models lm
        JOIN data_feed df ON df.id::text = ANY(string_to_array(lm.depends_on_feeds, ','))
        WHERE lm.is_active = true AND df.is_active = true
        """
    )
    for model_schema, feed_name in cur.fetchall():
        domain_feeds.setdefault(slugify_domain(model_schema), set()).add(feed_name)

    cur.execute(
        """
        SELECT batch_ods_name, friendly_name
        FROM data_feed
        WHERE ods_enabled = true AND batch_ods_name IS NOT NULL AND is_active = true
        """
    )
    for batch_ods_name, feed_name in cur.fetchall():
        domain_feeds.setdefault(slugify_domain(batch_ods_name), set()).add(feed_name)

    return {domain: sorted(feeds) for domain, feeds in domain_feeds.items()}


def fetch_feed_schedules(cur) -> list[dict]:
    # One row per feed-type schedule -- controlling_object_type='feed' maps
    # 1:1 to one data_feed row already, no expansion needed. orchestration_value
    # (that feed's own batch_group_friendly_name) is baked in here as a
    # structural literal, same "resolved from Postgres at codegen time"
    # rule as DOMAIN_FEEDS below -- not a live per-tick lookup, since which
    # batch a feed belongs to is metadata, not runtime state.
    cur.execute(
        """
        SELECT s.id::text AS schedule_id, s.cron, df.batch_group_friendly_name
        FROM schedule s
        JOIN data_feed df ON df.id = s.controlling_object_id
        WHERE s.is_active AND s.controlling_object_type = 'feed' AND df.is_active
        ORDER BY s.id
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_model_schedules(cur) -> list[dict]:
    # One row per model-type schedule -- controlling_object_type='model'
    # maps 1:1 to one lakehouse_models row, whose own model_schema is what
    # master_pipeline needs as orchestration_value (raw, unslugified --
    # PostgresMetadataResource.get_domain_feeds_for_model_schema() matches
    # against lakehouse_models.model_schema directly). No per-feed
    # expansion anymore -- master_pipeline reverse-engineers the domain's
    # dependent feeds itself, live, from orchestration_value alone (see
    # Roadmap.md "Master pipeline orchestration").
    cur.execute(
        """
        SELECT s.id::text AS schedule_id, s.cron, lm.model_schema
        FROM schedule s
        JOIN lakehouse_models lm ON lm.id = s.controlling_object_id
        WHERE s.is_active AND s.controlling_object_type = 'model' AND lm.is_active
        ORDER BY s.id
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _schedule_python_name(schedule_id: str) -> str:
    return f"schedule_{schedule_id.replace('-', '')}"


def _connector_build_expr(kind: str, feed: str, *, with_watermark: bool) -> str:
    """The Python source expression that constructs this feed's connector
    instance. `with_watermark=True` is passed for REST/json_file connectors,
    which need `last_watermark` for their own fetch() catch-up logic;
    tabular kinds (postgres/csv) never need it, since their watermark
    filtering happens after fetch(), on the caller's side."""
    if kind == "postgres":
        return (
            'PostgresConnector(\n'
            '            host=os.environ.get("POSTGRES_HOST", "localhost"),\n'
            '            port=int(os.environ.get("POSTGRES_PORT", "5432")),\n'
            '            dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),\n'
            '            user=os.environ.get("POSTGRES_USER", "platform"),\n'
            '            password=os.environ.get("POSTGRES_PASSWORD", "platform"),\n'
            '            query=data_feed["extraction_config"]["query"],\n'
            '            table_name=data_feed["extraction_config"].get("table_name"),\n'
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
    extraction_expr = _connector_build_expr(kind, friendly_name, with_watermark=False)
    # Real catalog-based PK discovery only exists on PostgresConnector
    # (see connectors/postgres.py) -- every other tabular kind has no live
    # catalog to introspect, so their generated code never even calls a
    # discovery method, rather than calling one that's always a no-op.
    discovered_pk_line = "        discovered_pk = connector.discover_primary_key()\n" if kind == "postgres" else ""
    discovered_pk_arg = "discovered_pk" if kind == "postgres" else "None"
    return f'''
@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def extraction_{friendly_name}(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
) -> Output[pl.DataFrame]:
    # Tabular sources (postgres/csv) need no pre-processing before their
    # schema is discoverable, so this step only fetches + discovers/syncs
    # schema_registry -- clean_{friendly_name} still owns the write, since
    # there's no flattening cost to save by combining the two steps here
    # (contrast with the JSON/REST connector kinds' extraction step). No
    # stage-log call of its own -- there is no schema-level stage left to
    # attribute a bare fetch to now that `landing` is gone (folded into
    # `raw`, see Roadmap.md); raw_{friendly_name} logs stage="raw" for the
    # fetch-through-durable-write outcome as a whole.
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    connector = {extraction_expr}
    df = connector.fetch()
    watermark_column = data_feed.get("watermark_column")
    last_watermark = data_feed.get("last_watermark_value")
    if not df.is_empty() and watermark_column and last_watermark is not None:
        df = df.filter(pl.col(watermark_column) > last_watermark)
    if not df.is_empty():
        discovered = connector.discover_schema(df)
{discovered_pk_line}        postgres_metadata.sync_schema_registry(
            data_feed_id=str(data_feed["id"]),
            discovered_column_definitions=discovered,
            metadata_source_pk=data_feed["source_pk"],
            discovered_primary_key_columns={discovered_pk_arg},
            created_by="extraction_{friendly_name}",
        )
    return Output(
        df,
        metadata={{
            "row_count": df.height,
            "watermark_value_end": (
                str(df[watermark_column].max()) if (not df.is_empty() and watermark_column) else last_watermark
            ),
        }},
    )


@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def raw_{friendly_name}(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    extraction_{friendly_name}: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    master_dagster_run_id = context.run.tags["master_dagster_run_id"]
    df = extraction_{friendly_name}
    watermark_column = data_feed.get("watermark_column")
    last_watermark = data_feed.get("last_watermark_value")
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="raw",
        master_dagster_run_id=master_dagster_run_id,
        dagster_run_id=context.run_id,
    ) as log:
        write_raw_snapshot("{friendly_name}", context.run_id, df)
        log.set_counts(
            rows_read=df.height,
            output_path=str(raw_snapshot_path("{friendly_name}", context.run_id)) if not df.is_empty() else None,
            watermark_value_start=last_watermark,
            watermark_value_end=(str(df[watermark_column].max()) if (not df.is_empty() and watermark_column) else last_watermark),
        )
    if not df.is_empty() and watermark_column:
        postgres_metadata.update_watermark(
            data_feed_id=str(data_feed["id"]), watermark_value=str(df[watermark_column].max()), run_id=context.run_id
        )
    return Output(None, metadata={{"audit_run_id": log.run_id, "row_count": df.height}})


@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}", deps=["raw_{friendly_name}"])
def clean_{friendly_name}(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
) -> Output[None]:
    # Reads raw_{friendly_name}'s durable parquet file back from disk,
    # rather than accepting its DataFrame as an in-memory asset-dependency
    # value -- "clean can read from that raw storage to do its clean layer
    # work", applied uniformly even though raw+clean share one job/pod here
    # (Roadmap.md "Master pipeline orchestration"). raw_{friendly_name} is
    # an order-only `deps=` entry, not a function parameter -- confirmed
    # live that declaring it as a plain `raw_{friendly_name}: None`
    # parameter instead crashes with "missing 1 required positional
    # argument": Dagster's IO manager treats an upstream Output(None) as
    # "nothing to load" and doesn't pass a value at all, rather than
    # passing None itself.
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    master_dagster_run_id = context.run.tags["master_dagster_run_id"]
    df = read_raw_snapshot("{friendly_name}", context.run_id)
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="clean",
        master_dagster_run_id=master_dagster_run_id,
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

    return Output(None, metadata={{"audit_run_id": log.run_id, "rows_inserted": df.height}})
'''


def _render_json_assets(feed: dict) -> str:
    friendly_name = feed["feed_friendly_name"]
    kind = feed["connector_kind"]
    extraction_expr = _connector_build_expr(kind, friendly_name, with_watermark=True)
    return f'''
@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def extraction_{friendly_name}(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    iceberg_catalog: IcebergCatalogResource,
) -> Output[pl.DataFrame]:
    # Nested-JSON sources (REST/json_file) need pre-processing (flattening)
    # before their schema is discoverable at all -- see connectors/base.py's
    # JsonConnector. Rather than flatten twice (once here to discover, once
    # in clean_{friendly_name} to write), this step does the clean-layer
    # write itself, reusing the one flattened DataFrame -- clean_{friendly_name}
    # below becomes a pure pass-through, kept only so clean.{friendly_name}
    # has a stable AssetKey for dbt source lineage. raw_{friendly_name} still
    # persists the untouched *nested* fetch result returned here, unrelated
    # to this flattened copy -- raw's own contract (verbatim, zero
    # transformation) is unaffected. Logs stage="clean" (not "landing",
    # which no longer exists -- folded into raw, see Roadmap.md) since the
    # clean-layer write is this step's real outcome; raw_{friendly_name}
    # separately logs stage="raw" for its own durable copy.
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    master_dagster_run_id = context.run.tags["master_dagster_run_id"]
    connector = {extraction_expr}
    df = connector.fetch()
    rows_inserted = 0
    watermark_column = data_feed.get("watermark_column")
    last_watermark = data_feed.get("last_watermark_value")
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="clean",
        master_dagster_run_id=master_dagster_run_id,
        dagster_run_id=context.run_id,
    ) as log:
        if not df.is_empty():
            flat = connector.flatten(df)
            discovered = connector.discover_schema(flat)
            sync_result = postgres_metadata.sync_schema_registry(
                data_feed_id=str(data_feed["id"]),
                discovered_column_definitions=discovered,
                metadata_source_pk=data_feed["source_pk"],
                discovered_primary_key_columns=None,
                created_by="extraction_{friendly_name}",
            )
            flat = reconcile_schema(flat, sync_result.column_definitions)
            validate_schema(flat, sync_result.column_definitions)
            catalog = iceberg_catalog.get_catalog()
            write_clean_snapshot(
                catalog,
                namespace="clean",
                table_name="{friendly_name}",
                df=flat,
                column_definitions=sync_result.column_definitions,
            )
            rows_inserted = flat.height
        log.set_counts(
            rows_read=df.height,
            rows_inserted=rows_inserted,
            watermark_value_start=last_watermark,
            watermark_value_end=(str(df[watermark_column].max()) if (not df.is_empty() and watermark_column) else last_watermark),
        )

    if not df.is_empty() and watermark_column:
        postgres_metadata.update_watermark(
            data_feed_id=str(data_feed["id"]), watermark_value=str(df[watermark_column].max()), run_id=context.run_id
        )

    return Output(df, metadata={{"audit_run_id": log.run_id, "row_count": df.height}})


@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}")
def raw_{friendly_name}(
    context: AssetExecutionContext,
    postgres_metadata: PostgresMetadataResource,
    extraction_{friendly_name}: pl.DataFrame,
) -> Output[None]:
    data_feed = postgres_metadata.get_data_feed("{friendly_name}")
    master_dagster_run_id = context.run.tags["master_dagster_run_id"]
    df = extraction_{friendly_name}
    with postgres_metadata.log_data_feed_stage(
        data_feed_id=str(data_feed["id"]),
        stage="raw",
        master_dagster_run_id=master_dagster_run_id,
        dagster_run_id=context.run_id,
    ) as log:
        write_raw_snapshot("{friendly_name}", context.run_id, df)
        log.set_counts(
            rows_read=df.height,
            output_path=str(raw_snapshot_path("{friendly_name}", context.run_id)) if not df.is_empty() else None,
        )
    return Output(None, metadata={{"audit_run_id": log.run_id, "row_count": df.height}})


@asset(pool=f"feed:{friendly_name}", group_name="{friendly_name}", deps=["raw_{friendly_name}"])
def clean_{friendly_name}() -> Output[None]:
    # Pure pass-through: extraction_{friendly_name} already performed the
    # clean-layer write (see its own comment) to avoid flattening this
    # feed's nested source data twice. This asset exists only so
    # clean.{friendly_name} keeps a stable AssetKey for dbt source lineage
    # -- there is no remaining clean-stage work for this connector kind.
    return Output(None, metadata={{"skipped": True, "reason": "clean-layer write already performed by extraction for this connector kind"}})
'''


def render(
    feeds: list[str], connector_feeds: list[dict], feed_schedules: list[dict], model_schedules: list[dict],
    domain_feeds: dict[str, list[str]],
) -> str:
    connector_imports = []
    base_locations = {}
    connector_asset_blocks = []
    connector_asset_names = []
    connector_feed_names = []
    for feed in connector_feeds:
        friendly_name = feed["feed_friendly_name"]
        kind = feed["connector_kind"]
        connector_feed_names.append(friendly_name)
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
        connector_asset_names += [f"extraction_{friendly_name}", f"raw_{friendly_name}", f"clean_{friendly_name}"]

    connector_imports_block = "\n".join(sorted(set(connector_imports)))
    base_locations_repr = "{" + ", ".join(f'"{k}": "{v}"' for k, v in base_locations.items()) + "}"
    connector_assets_block = "\n".join(connector_asset_blocks)
    connector_asset_names_block = ", ".join(connector_asset_names)

    # Per DOMAIN, not per feed -- a domain's transformation/serving dbt
    # assets are compile-isolated in their own dbt project
    # (dbt/domains/<domain>/, see Roadmap.md "multi-project dbt split") and
    # can span several feeds' models. DOMAIN_FEEDS is baked in as a literal
    # dict (structural: which domains exist and which feeds each spans,
    # resolved from Postgres at codegen time) -- DBT_PROJECTS itself is
    # NOT baked in here, it's built by the rendered module body via its own
    # filesystem glob over dbt/domains/*/ at Dagster-import time (mirrors
    # definitions.py's own independent glob -- both must agree, enforced by
    # `just start`'s ordering: generate-domain-projects always runs before
    # either glob ever executes, not by a shared source at runtime, see
    # that script for why).
    def _domain_feeds_entry(domain: str, feeds_for_domain: list[str]) -> str:
        feed_list_repr = "[" + ", ".join(f'"{f}"' for f in feeds_for_domain) + "]"
        return f'"{domain}": {feed_list_repr}'

    domain_feeds_repr = "{" + ", ".join(
        _domain_feeds_entry(domain, feeds_for_domain) for domain, feeds_for_domain in domain_feeds.items()
    ) + "}"

    # Every schedule now targets `master_pipeline` uniformly -- the only
    # difference between a feed-type and model-type schedule is which
    # orchestration_kind/orchestration_value pair it resolves
    # (batch_group_friendly_name vs. raw model_schema, both baked in as
    # codegen-time literals -- structural metadata, not runtime state, same
    # reasoning as DOMAIN_FEEDS above).
    schedule_calls = []
    for row in feed_schedules:
        schedule_calls.append(
            "    _make_master_pipeline_schedule(\n"
            f'        python_name="{_schedule_python_name(row["schedule_id"])}",\n'
            f'        schedule_id="{row["schedule_id"]}",\n'
            f'        cron="{row["cron"]}",\n'
            '        orchestration_kind="batch_group",\n'
            f'        orchestration_value="{row["batch_group_friendly_name"]}",\n'
            "    ),"
        )
    for row in model_schedules:
        schedule_calls.append(
            "    _make_master_pipeline_schedule(\n"
            f'        python_name="{_schedule_python_name(row["schedule_id"])}",\n'
            f'        schedule_id="{row["schedule_id"]}",\n'
            f'        cron="{row["cron"]}",\n'
            '        orchestration_kind="model_schema",\n'
            f'        orchestration_value="{row["model_schema"]}",\n'
            "    ),"
        )
    schedules_block = "\n".join(schedule_calls) if schedule_calls else "    # no active schedule rows"

    # python_name (== the generated ScheduleDefinition's own .name) ->
    # (orchestration_kind, orchestration_value) -- every generated
    # ScheduleDefinition now targets the same master_pipeline job (see
    # _make_master_pipeline_schedule below), so a caller that needs to find
    # "the schedule for feed X" (trigger_schedule_run.py) can no longer
    # look one up by job_name; this is the structural lookup that replaces
    # it. Keyed by python_name, not the raw schedule_id -- _schedule_python_name()
    # strips schedule_id's dashes, which would make recovering the original
    # schedule_id from a ScheduleDefinition's own .name lossy.
    schedule_orchestration_entries = []
    for row in feed_schedules:
        schedule_orchestration_entries.append(
            f'"{_schedule_python_name(row["schedule_id"])}": ("batch_group", "{row["batch_group_friendly_name"]}")'
        )
    for row in model_schedules:
        schedule_orchestration_entries.append(
            f'"{_schedule_python_name(row["schedule_id"])}": ("model_schema", "{row["model_schema"]}")'
        )
    schedule_orchestration_repr = "{" + ", ".join(schedule_orchestration_entries) + "}"

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
    Config,
    DefaultScheduleStatus,
    Failure,
    OpExecutionContext,
    Output,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    job,
    op,
    schedule,
)
from dagster_dbt import DbtProject

from connectors import CSVConnector, PostgresConnector
{connector_imports_block}
from dagster_data_platform.assets.dbt_assets import (
    _build_serving_assets_for_domain,
    _build_transformation_assets_for_domain,
    domain_group_name,
)
from dagster_data_platform.dagster_launch import launch_and_wait
from dagster_data_platform.pipeline_steps import parse_selected_steps
from dagster_data_platform.raw_storage import raw_snapshot_path, read_raw_snapshot, write_raw_snapshot
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


# DOMAIN_FEEDS: which domains exist and which feeds each spans (structural,
# resolved from Postgres at codegen time -- see
# scripts/generate_dagster_pipeline.py::fetch_domain_dependent_feeds()).
DOMAIN_FEEDS = {domain_feeds_repr}

# DBT_PROJECTS: one DbtProject per domain, built HERE via a filesystem glob
# over dbt/domains/*/ at Dagster-import time -- NOT baked in as codegen
# output, since it's filesystem state (which domain directories actually
# exist on disk), not Postgres state. definitions.py does its own
# independent identical glob for the SAME reason dbt_assets.py's
# _build_transformation_assets_for_domain closes over its own DbtCliResource
# instead of taking one via Dagster resource injection (see that function's
# docstring) -- two independently-computed lists that must agree, enforced
# by `just start`'s ordering (generate-domain-projects always runs before
# either glob ever executes), not by a shared source at runtime.
DOMAINS_DIR = REPO_ROOT / "dbt" / "domains"
DBT_PROJECTS = {{}}
for _domain_dir in sorted(DOMAINS_DIR.glob("*")):
    if not (_domain_dir / "dbt_project.yml").exists():
        continue
    _project = DbtProject(project_dir=_domain_dir, profiles_dir=_domain_dir / "profiles")
    _project.prepare_if_dev()
    DBT_PROJECTS[_domain_dir.name] = _project

# One _build_transformation_assets_for_domain(...)/_build_serving_assets_for_domain(...)
# call per resolved domain, and only here -- calling either factory twice for
# the same domain would construct two @dbt_assets defs both claiming the
# same AssetKeys, which Dagster rejects.
TRANSFORMATION_ASSETS = {{
    domain: _build_transformation_assets_for_domain(domain, feeds, DBT_PROJECTS[domain])
    for domain, feeds in DOMAIN_FEEDS.items()
}}
SERVING_ASSETS = {{
    domain: _build_serving_assets_for_domain(domain, feeds, DBT_PROJECTS[domain])
    for domain, feeds in DOMAIN_FEEDS.items()
}}
ALL_DBT_ASSETS = list(TRANSFORMATION_ASSETS.values()) + list(SERVING_ASSETS.values())

# --- connector-driven extraction/raw/clean assets ---------------------------
# One block per feed whose source_system.connector_kind is set (see
# scripts/generate_dagster_pipeline.py's module docstring) -- a feed with
# connector_kind IS NULL keeps a fully hand-written asset file instead.
{connector_assets_block}

ALL_CONNECTOR_ASSETS = [{connector_asset_names_block}]

# --- EXTRACTION_JOBS[feed] ---------------------------------------------------
# One job per active feed (connector-driven or hand-written), bundling
# extraction+raw+clean as ONE job/pod -- there is no separate "validation"
# job anymore (Roadmap.md "Master pipeline orchestration"). Whether a given
# run actually launches this job for a feed at all is decided live by
# master_pipeline itself (data_feed.pipeline_steps' "extraction" gate), not
# baked in here or checked inside the assets -- see run_master_pipeline
# below.
EXTRACTION_JOBS = {{
    f: define_asset_job(f"extraction_{{f}}_job", selection=AssetSelection.groups(f))
    for f in [{", ".join(f'"{f}"' for f in feeds)}]
}}
ALL_EXTRACTION_JOBS = list(EXTRACTION_JOBS.values())

# --- MODELING_JOBS[domain] / SERVING_JOBS[domain] ---------------------------
# One pair per resolved domain -- MODELING_JOBS covers clean -> staging ->
# model (the 'transformation' pipeline step); SERVING_JOBS covers model ->
# serve. Selected via the exact AssetsDefinition object each domain factory
# returned above (TRANSFORMATION_ASSETS[domain]/SERVING_ASSETS[domain]), not
# AssetSelection.groups(...) -- both dbt-assets defs for one domain share
# the same group_name (domain_group_name(domain), see dbt_assets.py), so a
# group-based selection would pull both into either job; selecting the
# AssetsDefinition object directly is unambiguous. No k8s_job_executor here
# -- each of these is already its own top-level job/pod (dropped entirely,
# see this script's module docstring).
MODELING_JOBS = {{
    domain: define_asset_job(f"{{domain}}_modeling_job", selection=[TRANSFORMATION_ASSETS[domain]])
    for domain in DOMAIN_FEEDS
}}
SERVING_JOBS = {{
    domain: define_asset_job(f"{{domain}}_serving_job", selection=[SERVING_ASSETS[domain]])
    for domain in DOMAIN_FEEDS
}}
ALL_MODELING_JOBS = list(MODELING_JOBS.values())
ALL_SERVING_JOBS = list(SERVING_JOBS.values())


# --- master_pipeline: the single entry point --------------------------------
# Every trigger path (schedule, manual launch) goes through this one job,
# parameterized by orchestration_kind ('batch_group' or 'model_schema') +
# orchestration_value -- never a per-feed/per-domain job of its own (see
# Roadmap.md "Master pipeline orchestration" for the full confirmed
# design). Everything else (which feeds, which domain, which steps are
# selected) is derived live from Postgres inside run_master_pipeline, not
# baked in here.
class MasterPipelineConfig(Config):
    orchestration_kind: str
    orchestration_value: str


@op(name="run_master_pipeline")
def run_master_pipeline(
    context: OpExecutionContext, config: MasterPipelineConfig, postgres_metadata: PostgresMetadataResource
) -> None:
    master_dagster_run_id = context.run_id
    kind = config.orchestration_kind
    value = config.orchestration_value

    # batch_group-triggered runs always produce ODS output only (never a
    # hand-modeled domain); model_schema-triggered runs reverse-engineer
    # the feeds they need from lakehouse_models.depends_on_feeds, and build
    # the hand-modeled domain (confirmed design, see Roadmap.md). A
    # batch_group is expected to map to exactly one ODS domain (1:1, not
    # enforced -- see Backlog.md); ods_domain is None for an
    # extraction-only batch (no feed in it is ODS-enabled), which is valid.
    if kind == "batch_group":
        feeds = postgres_metadata.get_batch_group_feeds(value)
        ods_domain = postgres_metadata.get_batch_group_ods_domain(value)
        domains = [ods_domain] if ods_domain else []
    elif kind == "model_schema":
        domain, feeds = postgres_metadata.get_domain_feeds_for_model_schema(value)
        domains = [domain]
    else:
        raise Failure(f"Unknown orchestration_kind {{kind!r}} -- expected 'batch_group' or 'model_schema'")

    if not feeds:
        raise Failure(f"No active feeds resolved for orchestration_kind={{kind!r}} orchestration_value={{value!r}}")

    # The master pipeline's own first action: create every feed-run/
    # model-run row up front, keyed by ITS OWN run id -- each child job
    # looks its row up by (identity, master_dagster_run_id) and only
    # updates it, it never creates one itself (see
    # PostgresMetadataResource.record_run_started/record_model_run_started).
    for feed in feeds:
        data_feed = postgres_metadata.get_data_feed(feed)
        postgres_metadata.record_run_started(
            data_feed_id=str(data_feed["id"]),
            master_dagster_run_id=master_dagster_run_id,
            tracking_group=data_feed["batch_group_friendly_name"],
        )
    for domain in domains:
        postgres_metadata.record_model_run_started(
            model_key=domain,
            uses_feeds=",".join(feeds),
            master_dagster_run_id=master_dagster_run_id,
            tracking_group=domain,
        )

    # Extraction: per-feed gating happens HERE, once, rather than inside
    # each asset -- a feed with "extraction" deselected in its own
    # pipeline_steps simply never gets its job launched this run. A child
    # job failure raises dagster.Failure (see dagster_launch.launch_and_wait),
    # which propagates straight out of this op and fails the master
    # pipeline itself, stopping here -- fail-fast, not best-effort across
    # the remaining feeds (Roadmap.md: "propagates the failure to the
    # master parent pipeline so it stops there").
    for feed in feeds:
        data_feed = postgres_metadata.get_data_feed(feed)
        if "extraction" not in parse_selected_steps(data_feed["pipeline_steps"]):
            context.log.info(f"Skipping extraction for feed {{feed!r}} -- not selected in pipeline_steps")
            continue
        launch_and_wait(EXTRACTION_JOBS[feed].name, tags={{"master_dagster_run_id": master_dagster_run_id}})

    # Modeling/serving always launch for every resolved domain -- per-feed
    # cherry-picking for these two steps stays inside the domain's own dbt
    # build (dbt_assets.py::_run_dbt_build_and_log_stages), since a domain
    # job can span multiple feeds with different pipeline_steps selections.
    for domain in domains:
        launch_and_wait(MODELING_JOBS[domain].name, tags={{"master_dagster_run_id": master_dagster_run_id}})
    for domain in domains:
        launch_and_wait(SERVING_JOBS[domain].name, tags={{"master_dagster_run_id": master_dagster_run_id}})


@job(name="master_pipeline")
def master_pipeline():
    run_master_pipeline()


def _make_master_pipeline_schedule(*, python_name, schedule_id, cron, orchestration_kind, orchestration_value):
    # postgres_metadata is declared as a direct parameter (not pulled from
    # context.resources) -- Dagster infers the resource requirement from
    # the parameter name itself; passing required_resource_keys= as well
    # is rejected outright ("Cannot specify resource requirements in both
    # @schedule decorator and as arguments to the decorated function"),
    # confirmed for real against the installed Dagster version.
    @schedule(
        name=python_name,
        cron_schedule=cron,
        job=master_pipeline,
        default_status=DefaultScheduleStatus.STOPPED,
    )
    def _fn(context, postgres_metadata: PostgresMetadataResource):
        if not postgres_metadata.is_schedule_active(schedule_id):
            return SkipReason(f"schedule {{schedule_id}} is no longer active")
        return RunRequest(
            tags={{
                "schedule_id": schedule_id,
                "orchestration_kind": orchestration_kind,
                "orchestration_value": orchestration_value,
            }},
            run_config={{
                "ops": {{
                    "run_master_pipeline": {{
                        "config": {{"orchestration_kind": orchestration_kind, "orchestration_value": orchestration_value}}
                    }}
                }}
            }},
        )

    return _fn


ALL_SCHEDULES = [
{schedules_block}
]

# ScheduleDefinition.name -> (orchestration_kind, orchestration_value) --
# see scripts/generate_dagster_pipeline.py's render() for why this exists
# (every schedule targets the same master_pipeline job now, so looking one
# up "for feed X" needs this rather than a job_name match).
SCHEDULE_ORCHESTRATION = {schedule_orchestration_repr}
'''


def _render_clean_source_tables(feeds: list[str]) -> str:
    feeds_repr = ", ".join(f'"{f}"' for f in feeds)
    return f'''"""GENERATED by scripts/generate_dagster_pipeline.py -- DO NOT EDIT BY HAND.

Every feed with dbt models needs its `clean.<table>` source mapped onto
the matching Dagster asset key (dbt_assets.py's DataPlatformDbtTranslator.
get_asset_key()) so the extraction -> raw -> clean chain and dbt's staging/
ODS model share one asset graph instead of two coincidentally-ordered
ones -- also used to classify which dbt nodes are "staging" vs. "model"/
"serve" (dbt_assets.py's _stage_for_dbt_node()). A standalone leaf module,
not folded into pipeline_generated.py itself, specifically to avoid a
circular import (pipeline_generated.py already imports from dbt_assets.py).

Regenerate via `just orchestration::generate-pipeline-jobs`.
"""

CLEAN_SOURCE_TABLES = {{{feeds_repr}}}
'''


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        feeds = fetch_active_feeds(cur)
        connector_feeds = fetch_connector_feeds(cur)
        feed_schedules = fetch_feed_schedules(cur)
        model_schedules = fetch_model_schedules(cur)
        domain_feeds = fetch_domain_dependent_feeds(cur)

    OUTPUT_PATH.write_text(render(feeds, connector_feeds, feed_schedules, model_schedules, domain_feeds))
    CLEAN_SOURCE_TABLES_OUTPUT_PATH.write_text(_render_clean_source_tables(feeds))
    print(
        f"Generated {OUTPUT_PATH} -- {len(feeds)} extraction job(s), {len(domain_feeds)} modeling/serving job pair(s), "
        f"{len(connector_feeds)} connector-driven feed(s), {len(feed_schedules) + len(model_schedules)} schedule(s), "
        f"1 master_pipeline job."
    )
    print(f"Generated {CLEAN_SOURCE_TABLES_OUTPUT_PATH} -- {len(feeds)} clean source table(s).")


if __name__ == "__main__":
    main()
