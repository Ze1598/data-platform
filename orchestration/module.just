# Dagster (dagster dev runs locally, launches real pods in kind via
# K8sRunLauncher). See DebugReference.md for the manual-command equivalents
# this wraps, including *why* DAGSTER_HOME/PATH/the dagster-instance
# ConfigMap all matter. POSTGRES_* env vars come from the root Justfile's
# `set dotenv-load`.

# Build + load the image, regenerate the dagster-instance ConfigMap, then
# launch `dagster dev` in the background (webserver + daemon together --
# the daemon is what actually launches queued runs, see DebugReference.md
# "Why a launched run seemed to hang in QUEUED").
start:
    #!/usr/bin/env bash
    set -euo pipefail
    just orchestration::build-image
    kubectl create configmap dagster-instance \
        --from-file=dagster.yaml=dagster_home/dagster.yaml \
        -n orchestration --dry-run=client -o yaml | kubectl apply -f -

    export DAGSTER_HOME="{{justfile_directory()}}/orchestration/dagster_home"
    export PATH="{{justfile_directory()}}/.venv/bin:$PATH"
    cd dagster_data_platform
    nohup dagster dev -m dagster_data_platform.definitions \
        > "$DAGSTER_HOME/dagster-dev.log" 2>&1 &
    disown

    echo "Waiting for the webserver to come up on localhost:3000..."
    until curl -s -o /dev/null http://localhost:3000; do sleep 2; done

    # The webserver responding doesn't mean dagster_db's schema is fully
    # created yet, let alone that the daemon (a separate thread pool within
    # the same process) has finished its own startup. Two-stage race,
    # both observed for real this session:
    #  1. dagster_db's ~22 tables (jobs, runs, event_logs, schedules, ...)
    #     get created as a batch at first-connect, not instantly -- a
    #     daemon thread that starts querying its own table (e.g.
    #     SchedulerDaemon querying `jobs`) before the *whole* batch lands
    #     hits `UndefinedTable`, which was observed to send some daemon
    #     threads into a crash/restart loop (visible as a churn of
    #     different daemon_ids fighting over heartbeat ownership).
    #  2. Even once the schema exists, the daemon still needs a moment to
    #     register all 7 daemon types' heartbeats.
    # Checking daemon_heartbeats alone (stage 2) wasn't sufficient --
    # it can already have rows while `jobs` et al still don't exist, since
    # heartbeats is evidently created early in the batch. Wait for the
    # full table count first, then heartbeats.
    echo "Waiting for dagster_db's schema to be fully created..."
    for i in $(seq 1 30); do
        tables=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
            -U "$POSTGRES_USER" -d dagster_db -tAc \
            "select count(*) from information_schema.tables where table_schema = 'public';" 2>/dev/null || echo 0)
        [ "$tables" -ge 22 ] && break
        sleep 2
    done
    if [ "$tables" -lt 22 ]; then
        echo "dagster_db schema incomplete after 60s (only $tables/22 tables created)" >&2
        exit 1
    fi

    echo "Waiting for the dagster daemon to fully initialize..."
    for i in $(seq 1 30); do
        count=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
            -U "$POSTGRES_USER" -d dagster_db -tAc "select count(*) from daemon_heartbeats;" 2>/dev/null || echo 0)
        [ "$count" -ge 7 ] && break
        sleep 2
    done
    if [ "$count" -lt 7 ]; then
        echo "Dagster daemon didn't fully initialize after 60s (only $count/7 daemon types reporting)" >&2
        exit 1
    fi
    echo "dagster dev up (logs: $DAGSTER_HOME/dagster-dev.log)"

# Kill the whole dagster dev process tree: the top-level wrapper, plus its
# `dagster._daemon run` and `dagster_webserver` children *individually* --
# NOT just the wrapper. Those children have their own separate command
# lines (neither contains the substring "dagster dev" at all), and
# `dagster dev`'s graceful-shutdown handshake (which normally cascades a
# clean stop to them) only runs if the wrapper gets a chance to react to
# SIGTERM -- a SIGKILL (or any kill that races ahead of the handshake)
# leaves them orphaned (reparented to pid 1), still connected to Postgres,
# still writing heartbeats. Confirmed for real this session: three
# generations of orphaned daemon/webserver pairs accumulated silently
# across nuke-and-rebuild cycles, all colliding with each fresh run's
# legitimate daemon over heartbeat ownership (visible as a churn of
# different daemon_ids fighting each other) and ultimately preventing any
# run's k8s Job from ever getting created. Match on a path substring
# shared by every process in *this* project's dagster tree specifically
# (not just the wrapper's own invocation), so already-orphaned children
# get caught too, not just a live wrapper.
kill:
    #!/usr/bin/env bash
    set -euo pipefail
    patterns=("dagster dev -m dagster_data_platform.definitions" "orchestration/dagster_home")
    any_running() {
        for p in "${patterns[@]}"; do
            pgrep -f "$p" > /dev/null && return 0
        done
        return 1
    }
    if ! any_running; then
        echo "dagster dev not running"
        exit 0
    fi
    for p in "${patterns[@]}"; do
        pkill -f "$p" || true
    done
    for i in $(seq 1 10); do
        any_running || exit 0
        sleep 1
    done
    echo "dagster dev didn't fully exit after SIGTERM, sending SIGKILL"
    for p in "${patterns[@]}"; do
        pkill -9 -f "$p" || true
    done

# Rebuild the orchestration image and reload it into kind, without
# restarting dagster dev -- for the "changed code, need launched pods to
# see it" case. Build context must be repo root, not orchestration/.
build-image:
    docker build -f Dockerfile -t data-platform-orchestration:latest {{justfile_directory()}}
    kind load docker-image data-platform-orchestration:latest --name data-platform

# Launch the full pipeline via the real K8sRunLauncher (not `dagster asset
# materialize`, which never touches it -- see DebugReference.md) and verify
# it actually succeeded: finds the newly-created run Job, waits for it to
# complete, then checks data_feed_run/data_model_run show 2 successful rows
# each (customers/sales x landing->raw->clean, and customers/sales staging).
# This is the check that actually caught both bugs found during this
# session's full nuke-and-rebuild -- a narrower check (like tests/integration
# alone) would not have.
verify-pipeline:
    #!/usr/bin/env bash
    set -euo pipefail
    export DAGSTER_HOME="{{justfile_directory()}}/orchestration/dagster_home"
    export PATH="{{justfile_directory()}}/.venv/bin:$PATH"

    before=$(kubectl get jobs -n orchestration -o jsonpath='{.items[*].metadata.name}')
    (cd dagster_data_platform && dagster job launch -j '__ASSET_JOB' -m dagster_data_platform.definitions)

    # Space-separated set-difference rather than jsonpath `[-1]` indexing --
    # the latter throws on an empty job list, which is exactly the state
    # right after `dagster job launch` returns (it only submits to the
    # queue; the daemon launches the actual k8s Job a moment later).
    job_name=""
    for i in $(seq 1 30); do
        after=$(kubectl get jobs -n orchestration -o jsonpath='{.items[*].metadata.name}')
        for name in $after; do
            if [[ ! " $before " =~ " $name " ]]; then
                job_name="$name"
                break 2
            fi
        done
        sleep 2
    done
    if [[ -z "$job_name" ]]; then
        echo "Could not find the newly-launched run Job after 60s" >&2
        exit 1
    fi
    echo "Watching job/$job_name..."
    kubectl wait --for=condition=complete "job/$job_name" -n orchestration --timeout=180s

    run_id="${job_name#dagster-run-}"
    feed_ok=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "select count(*) from data_feed_run where dagster_run_id = '$run_id' and job_successful = true;")
    model_ok=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "select count(*) from data_model_run where dagster_run_id = '$run_id' and job_successful = true;")
    if [[ "$feed_ok" -ne 2 || "$model_ok" -ne 2 ]]; then
        echo "Expected 2 successful data_feed_run + 2 successful data_model_run rows for run $run_id, got $feed_ok + $model_ok" >&2
        exit 1
    fi
    echo "Pipeline run $run_id succeeded: 2/2 data_feed_run + 2/2 data_model_run rows successful."

# Unit tests -- pure logic, no live cluster needed.
test:
    cd dagster_data_platform && uv run pytest
