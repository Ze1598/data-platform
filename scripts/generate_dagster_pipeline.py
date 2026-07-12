"""Generates the dynamic, metadata-driven pipeline foundation -- per-feed
dbt assets, per-feed jobs, and real Dagster schedules from `data_feed` +
`schedule` (+ `lakehouse_models` for model-type schedules) -- as
`orchestration/dagster_data_platform/dagster_data_platform/pipeline_generated.py`.

Deliberately a standalone build-time script, not a Dagster op or a live
Postgres read folded into module import: this project's established rule
(see Learnings.md, and generate_serve_views.py/
generate_deletion_synthesis_views.py, the two existing precedents) is that
anything determining Dagster's *static object graph* -- which jobs/schedules
exist, their cron, their target -- must be resolved before `dagster dev`/
Docker image build, never at Python import time. What a schedule's
*execution function* looks up live at each tick (is_active, the target
feed's current batch_group_friendly_name) is a different phase of Dagster's
lifecycle (the daemon process, at tick time) and is deliberately NOT baked
in here -- see the generated `_make_feed_schedule()` helper.

Fully regenerates `pipeline_generated.py` on every run (clears/overwrites,
not additive) so it never drifts from current `data_feed`/`schedule`/
`lakehouse_models` state.
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


def fetch_active_feeds(cur) -> list[str]:
    cur.execute("SELECT friendly_name FROM data_feed WHERE is_active = true ORDER BY friendly_name")
    return [row[0] for row in cur.fetchall()]


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


def render(feeds: list[str], feed_schedules: list[dict], model_schedules: list[dict]) -> str:
    feeds_repr = ", ".join(f'"{f}"' for f in feeds)

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

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    SkipReason,
    define_asset_job,
    schedule,
)

from dagster_data_platform.assets.dbt_assets import _build_dbt_assets_for_feed
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource

# One _build_dbt_assets_for_feed(...) call per active feed, and only here --
# calling this factory twice for the same feed would construct two @dbt_assets
# defs both claiming the same AssetKeys, which Dagster rejects.
DBT_ASSETS = {{f: _build_dbt_assets_for_feed(f) for f in [{feeds_repr}]}}
ALL_DBT_ASSETS = list(DBT_ASSETS.values())

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
        feed_schedules = fetch_feed_schedules(cur)
        model_schedules = fetch_model_schedules(cur)

    OUTPUT_PATH.write_text(render(feeds, feed_schedules, model_schedules))
    print(
        f"Generated {OUTPUT_PATH} -- {len(feeds)} feed job(s), "
        f"{len(feed_schedules) + len(model_schedules)} schedule(s)."
    )


if __name__ == "__main__":
    main()
