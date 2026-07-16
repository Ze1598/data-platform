"""Launches `master_pipeline` directly for a given orchestration_kind/
orchestration_value, waits for it to reach a terminal status, and raises if
it didn't succeed -- via dagster_launch.launch_and_wait, the same
launch-and-wait mechanism master_pipeline's own op uses for its child jobs.
Used by orchestration/module.just's verify-pipeline recipe (`python -m
dagster_data_platform.trigger_master_pipeline --orchestration-kind
batch_group --orchestration-value police_crimes`) as the real end-to-end
replacement for the old `dagster job launch -j '__ASSET_JOB'` path, which
is no longer compatible with the master pipeline architecture (every
extraction/raw/clean/dbt asset now expects a master_dagster_run_id run tag
and a pre-created data_processing_runs row -- see Roadmap.md "Master
pipeline orchestration").

Prints the launched run's id to stdout on success; a non-zero exit (via the
propagated dagster.Failure) means the run itself failed or timed out.

Requires dagster dev's webserver already running at localhost:3000, same
precondition as trigger_schedule_run.py.
"""

import argparse

from dagster_data_platform.dagster_launch import launch_and_wait


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orchestration-kind", required=True, choices=["batch_group", "model_schema"])
    parser.add_argument("--orchestration-value", required=True)
    args = parser.parse_args()

    run_id = launch_and_wait(
        "master_pipeline",
        tags={},
        run_config={
            "ops": {
                "run_master_pipeline": {
                    "config": {
                        "orchestration_kind": args.orchestration_kind,
                        "orchestration_value": args.orchestration_value,
                    }
                }
            }
        },
    )
    print(run_id)


if __name__ == "__main__":
    main()
