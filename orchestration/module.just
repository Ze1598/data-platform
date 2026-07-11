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
    just _ensure-venv-visible
    just orchestration::generate-serve-views
    just orchestration::generate-deletion-synthesis-views
    just orchestration::build-image
    kubectl create configmap dagster-instance \
        --from-file=dagster.yaml=dagster_home/dagster.yaml \
        -n orchestration --dry-run=client -o yaml | kubectl apply -f -

    export DAGSTER_HOME="{{justfile_directory()}}/orchestration/dagster_home"
    export PATH="{{justfile_directory()}}/.venv/bin:$PATH"
    cd dagster_data_platform

    # Sweep again immediately before this specific subprocess spawn, not
    # just once at the top of the recipe -- build-image + the configmap
    # apply above are enough real elapsed time for the files to get
    # re-hidden again before dagster dev's own fresh Python startup hits
    # them (confirmed recurring on a seconds-to-low-minutes cadence, not
    # tied to any uv write -- see Learnings.md).
    #
    # Retried on the known signature, same reasoning as verify-pipeline: a
    # dagster dev process that fails to import its own code location dies
    # (or never brings up the webserver) before doing anything else, so
    # nothing is partially started -- safe to kill and relaunch. The
    # webserver-wait loop is bounded now (it used to be unbounded, which
    # would have hung forever against a crashed process instead of
    # detecting and retrying).
    just _ensure-venv-visible
    dagster_up=false
    for attempt in 1 2 3; do
        nohup dagster dev -m dagster_data_platform.definitions \
            > "$DAGSTER_HOME/dagster-dev.log" 2>&1 &
        disown

        echo "Waiting for the webserver to come up on localhost:3000..."
        for i in $(seq 1 30); do
            curl -s -o /dev/null http://localhost:3000 && { dagster_up=true; break; }
            sleep 2
        done
        [ "$dagster_up" = true ] && break

        if ! grep -q "ModuleNotFoundError" "$DAGSTER_HOME/dagster-dev.log" 2>/dev/null; then
            echo "dagster dev didn't come up after 60s, and not the known .pth/UF_HIDDEN signature -- see $DAGSTER_HOME/dagster-dev.log" >&2
            tail -30 "$DAGSTER_HOME/dagster-dev.log" >&2
            exit 1
        fi
        echo ">>> dagster dev hit the known .pth/UF_HIDDEN race (attempt $attempt/3) -- killing, re-sweeping, and retrying" >&2
        just orchestration::kill
        just _ensure-venv-visible
        # Widened on macOS specifically -- see verify-pipeline's identical
        # comment for the full reasoning (iCloud sync cycle timing).
        [ "$(uname)" = "Darwin" ] && sleep 10 || sleep 1
    done
    if [ "$dagster_up" != true ]; then
        echo ">>> dagster dev failed 3 times with the .pth/UF_HIDDEN signature -- giving up" >&2
        tail -30 "$DAGSTER_HOME/dagster-dev.log" >&2
        exit 1
    fi

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

# Regenerate the serve layer's `_latest`/`_historical` dbt view models from
# model_feed (scripts/generate_serve_views.py) -- must run before
# build-image/dagster dev, not as a Dagster op: dagster-dbt's @dbt_assets
# reads target/manifest.json at Python-import time, before any run
# executes, so these files have to exist on disk before `dbt parse` builds
# that manifest (Learnings.md, Phase 5 "manifest must be pre-built into the
# image"). Requires metadata already seeded (model_feed populated) -- true
# by the time `start` reaches this step, since metadata::start runs first
# in the root Justfile's sequencing. Also runnable standalone after a
# model_feed change, without a full image rebuild (same pattern as
# `just metadata::seed`). Own .pth sweep, not just inherited from `start`
# -- reachable standalone (`just orchestration::generate-serve-views`),
# same reasoning as the sweep in `test` below.
generate-serve-views:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_serve_views.py

# Regenerate the model layer's deletion-synthesis intermediate models
# (int_<feed>_with_deletes.sql) from model_feed + schema_registry
# (scripts/generate_deletion_synthesis_views.py) -- same "must run before
# build-image/dagster dev" reasoning as generate-serve-views above (the
# manifest needs these files to already exist on disk). Also runnable
# standalone after a model_feed.deletions_enabled change.
generate-deletion-synthesis-views:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_deletion_synthesis_views.py

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
    just _ensure-venv-visible
    export DAGSTER_HOME="{{justfile_directory()}}/orchestration/dagster_home"
    export PATH="{{justfile_directory()}}/.venv/bin:$PATH"

    before=$(kubectl get jobs -n orchestration -o jsonpath='{.items[*].metadata.name}')

    # Sweep again immediately before this specific subprocess spawn, not
    # just once at the top of the recipe -- the kubectl round-trip above is
    # enough real elapsed time for the files to get re-hidden again before
    # `dagster job launch`'s own fresh Python startup hits them. Confirmed
    # for real: this exact gap caused a `No module named 'raw_to_clean'`
    # failure here even with the top-of-recipe sweep already in place.
    #
    # Retry, don't just sweep-and-hope: the underlying cause (Learnings.md,
    # ".pth files getting UF_HIDDEN re-applied") is outside this repo's
    # control and intermittent, not fixable from here -- but this specific
    # failure always happens at code-location *import* time, before
    # `dagster job launch` submits anything, so nothing is partially
    # applied on failure and a retry is genuinely safe, not a hack. Scoped
    # to the exact known signature (ModuleNotFoundError) so a real bug
    # still fails on the first attempt instead of being masked.
    just _ensure-venv-visible
    launch_ok=false
    for attempt in 1 2 3; do
        if launch_output=$(cd dagster_data_platform && dagster job launch -j '__ASSET_JOB' -m dagster_data_platform.definitions 2>&1); then
            launch_ok=true
            break
        fi
        if ! grep -q "ModuleNotFoundError" <<< "$launch_output"; then
            echo "$launch_output" >&2
            exit 1
        fi
        echo ">>> dagster job launch hit the known .pth/UF_HIDDEN race (attempt $attempt/3) -- re-sweeping and retrying" >&2
        just _ensure-venv-visible
        # Widened on macOS specifically: the underlying cause (Learnings.md
        # -- iCloud Desktop & Documents Folders sync interfering with uv's
        # editable installs, astral-sh/uv#9902) runs on roughly a 10s
        # break/recover cycle, not sub-second -- a 1s gap was retrying
        # inside the same bad window instead of waiting past it. No reason
        # to wait this long on a platform where the bug can't occur.
        [ "$(uname)" = "Darwin" ] && sleep 10 || sleep 1
    done
    echo "$launch_output"
    if [ "$launch_ok" != true ]; then
        echo ">>> dagster job launch failed 3 times with the .pth/UF_HIDDEN signature -- giving up" >&2
        exit 1
    fi

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
    # 900s, not 180s -- a from-scratch metadata DB (the normal post-nuke
    # state this recipe runs against) means police_crimes' watermark is
    # empty, so landing_police_crimes does a full historical backfill
    # (every available month since the UK Police API's start, one
    # sequential HTTP call per month) instead of a normal incremental
    # catch-up. Confirmed for real: this alone took ~2.5 minutes against
    # the live API on top of everything else in __ASSET_JOB, blowing past
    # the old 180s budget even though the run was genuinely healthy the
    # whole time -- kubectl wait timing out here is a recipe bug, not a
    # pipeline bug.
    kubectl wait --for=condition=complete "job/$job_name" -n orchestration --timeout=900s

    run_id="${job_name#dagster-run-}"
    feed_ok=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "select count(*) from data_feed_run where dagster_run_id = '$run_id' and job_successful = true;")
    model_ok=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "select count(*) from data_model_run where dagster_run_id = '$run_id' and job_successful = true;")
    # 4 + 4, not 2 + 2 -- Phase 9 added financial_transactions and
    # police_crimes as two more feeds __ASSET_JOB always includes
    # alongside customers/sales.
    if [[ "$feed_ok" -ne 4 || "$model_ok" -ne 4 ]]; then
        echo "Expected 4 successful data_feed_run + 4 successful data_model_run rows for run $run_id, got $feed_ok + $model_ok" >&2
        exit 1
    fi
    echo "Pipeline run $run_id succeeded: 4/4 data_feed_run + 4/4 data_model_run rows successful."

# Unit tests -- pure logic, no live cluster needed.
# Defensive .pth sweep here too, not just in `start`/`verify-pipeline` --
# this recipe is reachable standalone (`just orchestration::test`), bypassing
# the root Justfile's `test` recipe dependency. See Learnings.md for the
# .pth/UF_HIDDEN investigation -- confirmed recurring within a single
# `just smoketest` run, minutes after a prior sweep already ran. Retried on
# the known signature only (test *collection* failing on import, before any
# test runs -- safe to retry, nothing partially executed) -- see
# verify-pipeline's identical pattern for the full reasoning.
test:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    test_ok=false
    for attempt in 1 2 3; do
        if test_output=$(cd dagster_data_platform && uv run pytest 2>&1); then
            test_ok=true
            break
        fi
        if ! grep -q "ModuleNotFoundError" <<< "$test_output"; then
            echo "$test_output" >&2
            exit 1
        fi
        echo ">>> pytest hit the known .pth/UF_HIDDEN race (attempt $attempt/3) -- re-sweeping and retrying" >&2
        just _ensure-venv-visible
        # Widened on macOS specifically -- see verify-pipeline's identical
        # comment for the full reasoning (iCloud sync cycle timing).
        [ "$(uname)" = "Darwin" ] && sleep 10 || sleep 1
    done
    echo "$test_output"
    if [ "$test_ok" != true ]; then
        echo ">>> pytest failed 3 times with the .pth/UF_HIDDEN signature -- giving up" >&2
        exit 1
    fi
