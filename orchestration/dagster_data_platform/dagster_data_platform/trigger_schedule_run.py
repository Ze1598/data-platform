"""Fires a real generated schedule (pipeline_generated.ALL_SCHEDULES) end to
end: evaluates its execution function via a real ScheduleEvaluationContext
(no cron wait -- see dagster.build_schedule_context), then launches the
resulting RunRequest through the real webserver -> daemon -> K8sRunLauncher
path via dagster_launch.launch_and_wait -- the same launch-and-wait
mechanism master_pipeline's own op and trigger_master_pipeline.py use,
reused here for consistency (submits via GraphQL since `dagster job
launch` has no --tags flag and there's no "fire a schedule now" CLI
command; blocks on the run's own Dagster-level status rather than a
`kubectl wait`, which can report a launched pod's k8s Job as "Complete"
even when the Dagster run inside it failed -- see Learnings.md). Prints the
launched run's id to stdout on success -- used by orchestration/module.just's
verify-schedule recipe (`python -m dagster_data_platform.trigger_schedule_run
--feed <name>`), and find_schedule_for_feed() is reused directly by
tests/test_schedules.py.

Every generated schedule now targets the same `master_pipeline` job (see
Roadmap.md "Master pipeline orchestration") -- find_schedule_for_feed() can
no longer find "the schedule for feed X" by matching job_name (every
schedule shares one), so it resolves the feed's own batch_group_friendly_name
and matches against SCHEDULE_ORCHESTRATION instead (ScheduleDefinition.name ->
(orchestration_kind, orchestration_value), generated alongside ALL_SCHEDULES
for exactly this purpose).

Requires dagster dev's webserver already running at localhost:3000 (same
precondition as trigger_master_pipeline.py) and a real DagsterInstance
(DAGSTER_HOME set) for build_schedule_context.
"""

import argparse
import os

from dagster import DagsterInstance, build_schedule_context

from dagster_data_platform.dagster_launch import launch_and_wait
from dagster_data_platform.pipeline_generated import ALL_SCHEDULES, SCHEDULE_ORCHESTRATION
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource


def _postgres_metadata_resource() -> PostgresMetadataResource:
    return PostgresMetadataResource(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "platform"),
        password=os.environ.get("POSTGRES_PASSWORD", "platform"),
        dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
    )


def find_schedule_for_feed(feed_friendly_name: str, postgres_metadata: PostgresMetadataResource):
    batch_group_friendly_name = postgres_metadata.get_data_feed(feed_friendly_name)["batch_group_friendly_name"]
    for schedule_def in ALL_SCHEDULES:
        kind, value = SCHEDULE_ORCHESTRATION.get(schedule_def.name, (None, None))
        if kind == "batch_group" and value == batch_group_friendly_name:
            return schedule_def
    raise ValueError(
        f"No generated schedule targets feed {feed_friendly_name!r}'s batch_group ({batch_group_friendly_name!r})"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed", required=True, help="feed friendly_name whose schedule should fire")
    args = parser.parse_args()

    postgres_metadata = _postgres_metadata_resource()
    schedule_def = find_schedule_for_feed(args.feed, postgres_metadata)

    context = build_schedule_context(
        instance=DagsterInstance.get(),
        resources={"postgres_metadata": postgres_metadata},
    )
    result = schedule_def(context)
    # A @schedule function may return a single RunRequest, a list of them,
    # or a SkipReason -- normalize to a list of RunRequests for submission.
    run_requests = result if isinstance(result, list) else [result]

    for request in run_requests:
        if not hasattr(request, "tags"):
            raise RuntimeError(f"Schedule {schedule_def.name!r} skipped: {request}")
        run_id = launch_and_wait(
            schedule_def.job_name,
            tags=dict(request.tags),
            run_config=request.run_config,
        )
        print(run_id)


if __name__ == "__main__":
    main()
