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
    echo "dagster dev up (logs: $DAGSTER_HOME/dagster-dev.log)"

# Kill the whole dagster dev process tree (webserver, daemon, code server --
# killing the parent cleanly brings down all of them, no orphans, when it
# actually dies). SIGTERM alone isn't always enough: if Postgres/the k8s
# cluster already went away first (e.g. this ran after query-engine/
# metadata were already torn down), dagster dev can get stuck in a
# blocking call that never reaches its own signal handler -- observed this
# session, where a plain `pkill -f` reported success but two instances sat
# alive for 10+ seconds afterward. Escalate to SIGKILL if it doesn't exit.
kill:
    #!/usr/bin/env bash
    set -euo pipefail
    pattern="dagster dev -m dagster_data_platform.definitions"
    if ! pgrep -f "$pattern" > /dev/null; then
        echo "dagster dev not running"
        exit 0
    fi
    pkill -f "$pattern"
    for i in $(seq 1 10); do
        pgrep -f "$pattern" > /dev/null || exit 0
        sleep 1
    done
    echo "dagster dev didn't exit after SIGTERM, sending SIGKILL"
    pkill -9 -f "$pattern" || true

# Rebuild the orchestration image and reload it into kind, without
# restarting dagster dev -- for the "changed code, need launched pods to
# see it" case. Build context must be repo root, not orchestration/.
build-image:
    docker build -f Dockerfile -t data-platform-orchestration:latest {{justfile_directory()}}
    kind load docker-image data-platform-orchestration:latest --name data-platform

# Launch the full pipeline via the real K8sRunLauncher (not `dagster asset
# materialize`, which never touches it -- see DebugReference.md) and verify
# it actually succeeded: finds the newly-created run Job, waits for it to
# complete, then checks run_audit_log shows all 8 rows (landing/raw/clean/
# staging x 2 feeds) as `success`. This is the check that actually caught
# both bugs found during this session's full nuke-and-rebuild -- a narrower
# check (like tests/integration alone) would not have.
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
    rows=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "select count(*) from run_audit_log where dagster_run_id = '$run_id' and status = 'success';")
    if [[ "$rows" -ne 8 ]]; then
        echo "Expected 8 successful run_audit_log rows for run $run_id, got $rows" >&2
        exit 1
    fi
    echo "Pipeline run $run_id succeeded: 8/8 run_audit_log rows success."

# Unit tests -- pure logic, no live cluster needed.
test:
    cd dagster_data_platform && uv run pytest
