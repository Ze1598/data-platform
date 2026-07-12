"""Fires a real generated schedule (pipeline_generated.ALL_SCHEDULES) end to
end: evaluates its execution function via a real ScheduleEvaluationContext
(no cron wait -- see dagster.build_schedule_context), then submits the
resulting RunRequest through the real webserver -> daemon -> K8sRunLauncher
path via dagster_graphql.DagsterGraphQLClient, since `dagster job launch`
(the CLI) has no --tags flag and there's no "fire a schedule now" CLI
command. Prints the launched run's id to stdout -- used by
orchestration/module.just's verify-schedule recipe (`python -m
dagster_data_platform.trigger_schedule_run --feed <name>`), and
find_schedule_for_feed() is reused directly by tests/test_schedules.py.

Requires dagster dev's webserver already running at localhost:3000 (same
precondition as verify-pipeline's `dagster job launch`) and a real
DagsterInstance (DAGSTER_HOME set) for build_schedule_context.
"""

import argparse
import os

from dagster import DagsterInstance, build_schedule_context
from dagster_graphql import DagsterGraphQLClient

from dagster_data_platform.pipeline_generated import ALL_SCHEDULES, FEED_JOBS
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource


def _postgres_metadata_resource() -> PostgresMetadataResource:
    return PostgresMetadataResource(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "platform"),
        password=os.environ.get("POSTGRES_PASSWORD", "platform"),
        dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
    )


def find_schedule_for_feed(feed_friendly_name: str):
    target_job_name = FEED_JOBS[feed_friendly_name].name
    for schedule_def in ALL_SCHEDULES:
        if schedule_def.job_name == target_job_name:
            return schedule_def
    raise ValueError(f"No generated schedule targets feed {feed_friendly_name!r}'s job ({target_job_name!r})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed", required=True, help="feed friendly_name whose schedule should fire")
    args = parser.parse_args()

    schedule_def = find_schedule_for_feed(args.feed)

    context = build_schedule_context(
        instance=DagsterInstance.get(),
        resources={"postgres_metadata": _postgres_metadata_resource()},
    )
    result = schedule_def(context)
    # A @schedule function may return a single RunRequest, a list of them,
    # or a SkipReason -- normalize to a list of RunRequests for submission.
    run_requests = result if isinstance(result, list) else [result]

    client = DagsterGraphQLClient("localhost", port_number=3000)
    for request in run_requests:
        if not hasattr(request, "tags"):
            raise RuntimeError(f"Schedule {schedule_def.name!r} skipped: {request}")
        run_id = client.submit_job_execution(
            job_name=schedule_def.job_name,
            tags=dict(request.tags),
        )
        print(run_id)


if __name__ == "__main__":
    main()
