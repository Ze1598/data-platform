# Dagster runs fully in-cluster now (webserver + daemon + code-server, all
# real Deployments in orchestration/k8s/ -- see Roadmap.md "Master pipeline
# orchestration"), not `dagster dev` on the host. This was forced, not a
# style preference: master_pipeline's own pod calls back into the
# webserver's GraphQL API to launch its child jobs (dagster_launch.py),
# and "localhost" from inside a pod doesn't resolve to anything -- there
# was no address inside the cluster for it to reach until this existed.
# See DebugReference.md for manual-command equivalents.

# Build + load the image, regenerate both dagster-instance ConfigMaps
# (run-pod config, unchanged; the new in-cluster webserver/daemon config +
# workspace.yaml), apply the K8s manifests, and wait for everything to
# actually be ready -- not just "kubectl apply returned", which says
# nothing about whether the pods came up healthy.
start:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    just orchestration::seed-financial-landing
    just orchestration::generate-pipeline-jobs
    just orchestration::generate-domain-projects
    just orchestration::generate-sources
    just orchestration::generate-serve-views
    just orchestration::generate-deletion-synthesis-views
    just orchestration::generate-model-scaffolds
    just orchestration::generate-streaming-serve-scaffolds
    just orchestration::generate-ods-models
    just orchestration::build-image
    just orchestration::build-domain-images

    # dagster-instance: unchanged, mounted by K8sRunLauncher into every
    # *launched run* pod (load_incluster_config: false -- these pods never
    # call instance.run_launcher themselves, see dagster_launch.py).
    kubectl create configmap dagster-instance \
        --from-file=dagster.yaml=dagster_home/dagster.yaml \
        -n orchestration --dry-run=client -o yaml | kubectl apply -f -

    # dagster-instance-incluster: the webserver/daemon's own config
    # (load_incluster_config: true -- the daemon is what actually calls
    # K8sRunLauncher.launch_run(), from inside its own pod) plus
    # workspace.yaml (points both at dagster-code-server's gRPC address).
    # Two keys, one ConfigMap -- mounted as a directory, so DAGSTER_HOME
    # ends up containing both dagster.yaml and workspace.yaml.
    kubectl create configmap dagster-instance-incluster \
        --from-file=dagster.yaml=dagster_home/dagster-incluster.yaml \
        --from-file=workspace.yaml=dagster_home/workspace.yaml \
        -n orchestration --dry-run=client -o yaml | kubectl apply -f -

    kubectl apply -f k8s/postgres-credentials-secret.yaml -f k8s/rbac.yaml -f k8s/rbac-frontend-wake.yaml
    kubectl apply -f k8s/code-server-deployment.yaml -f k8s/code-server-service.yaml
    kubectl apply -f k8s/webserver-deployment.yaml -f k8s/webserver-service.yaml
    kubectl apply -f k8s/daemon-deployment.yaml

    # A rebuilt image reloaded into kind (build-image, above) does NOT
    # itself restart an already-running Deployment pod -- the Deployment
    # spec's `image: ...:latest` didn't change, so `kubectl apply` sees no
    # diff and Kubernetes has no reason to recreate the pod. Confirmed live
    # to matter concretely, not just in theory: dagster-code-server is what
    # reports the asset graph's *structure* (which assets exist, their
    # dependencies) to the webserver/daemon -- a structural code change
    # (e.g. an asset's deps=) left stale here would make a freshly
    # submitted run's execution plan reflect the OLD structure even though
    # the run's own freshly-launched pod has the NEW image. Force it every
    # `start`, not just when a structural change is suspected -- cheap, and
    # distinguishing "this code change was structural" from "just asset
    # body logic" isn't reliable enough to skip it.
    kubectl rollout restart deployment/dagster-code-server -n orchestration

    echo "Waiting for dagster-code-server/webserver/daemon to roll out..."
    kubectl rollout status deployment/dagster-code-server -n orchestration --timeout=120s
    kubectl rollout status deployment/dagster-webserver -n orchestration --timeout=120s
    kubectl rollout status deployment/dagster-daemon -n orchestration --timeout=120s

    # NodePort-mapped to localhost:3000 (platform/kind/kind-cluster.yaml) --
    # a rolled-out Deployment doesn't mean the webserver process inside it
    # has finished starting up yet.
    echo "Waiting for the webserver to come up on localhost:3000..."
    webserver_up=false
    for i in $(seq 1 30); do
        curl -s -o /dev/null http://localhost:3000 && { webserver_up=true; break; }
        sleep 2
    done
    if [ "$webserver_up" != true ]; then
        echo "Webserver didn't come up on localhost:3000 after 60s -- check: kubectl logs -n orchestration deployment/dagster-webserver" >&2
        exit 1
    fi

    # The webserver responding doesn't mean dagster_db's schema is fully
    # created yet, let alone that the daemon has finished its own startup.
    # Two-stage race, both observed for real against this exact instance
    # config (see Learnings.md):
    #  1. dagster_db's ~22 tables (jobs, runs, event_logs, schedules, ...)
    #     get created as a batch at first-connect, not instantly -- a
    #     daemon thread that starts querying its own table before the
    #     *whole* batch lands hits `UndefinedTable`, observed to send some
    #     daemon threads into a crash/restart loop.
    #  2. Even once the schema exists, the daemon still needs a moment to
    #     register all 7 daemon types' heartbeats.
    # Checking daemon_heartbeats alone (stage 2) wasn't sufficient -- it
    # can already have rows while `jobs` et al still don't exist. Wait for
    # the full table count first, then heartbeats.
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
        echo "Dagster daemon didn't fully initialize after 60s (only $count/7 daemon types reporting) -- check: kubectl logs -n orchestration deployment/dagster-daemon" >&2
        exit 1
    fi
    echo "Dagster up in-cluster (webserver: localhost:3000, or 'kubectl logs -n orchestration deployment/dagster-webserver' / 'deployment/dagster-daemon' / 'deployment/dagster-code-server')."

    # Applied LAST, deliberately -- after every health check above already
    # confirmed webserver/daemon/code-server came up healthy at their
    # normal replicas:1. KEDA will start driving webserver/code-server to 0
    # outside the configured cron windows shortly after this applies (its
    # own polling interval, ~30s default) -- fine, since it only happens
    # once `start` has already proven the deployment healthy, not during
    # the bring-up sequence itself.
    just orchestration::install-keda
    kubectl apply -f k8s/keda-scaledobjects.yaml

# KEDA (core only, no HTTP Add-on -- see Learnings.md's "Kubernetes scaling
# options compared" for why) -- installed from this module since it's the
# first (and so far only) consumer, mirrors streaming/flink/module.just's
# own precedent of installing the Flink Kubernetes Operator from the
# module that needs it. `helm upgrade --install` is idempotent, so a
# future second KEDA consumer wouldn't need to reinstall it.
install-keda:
    helm repo add kedacore https://kedacore.github.io/charts
    helm repo update kedacore
    helm upgrade --install keda kedacore/keda -n keda
    kubectl rollout status deployment/keda-operator -n keda --timeout=120s

# Cooperative wake -- webserver/code-server may be scaled to 0 by KEDA's
# Cron scaler outside the configured schedule windows (see
# k8s/keda-scaledobjects.yaml). Every recipe below that talks to Dagster
# needs them at replicas:1 first; this is the same "scale up, wait for
# ready, then proceed" pattern Backlog.md's planned Streamlit trigger
# feature will eventually need too, applied here to this project's own
# verification tooling first. Safe to call when already at replicas:1 --
# the annotate and rollout status calls both no-op quickly.
#
# NOT a plain `kubectl scale`: once a ScaledObject targets a Deployment,
# KEDA installs an HPA that owns that Deployment's replica count and
# continuously reconciles it back to the trigger's computed value -- 0
# outside the configured Cron windows. Confirmed live: a manual `kubectl
# scale --replicas=1` got reverted to 0 by KEDA within ~15s, before the
# GraphQL client could even connect, surfacing as a bare connection-reset
# error that looked like a readiness-probe race but wasn't one. The
# `autoscaling.keda.sh/paused-replicas` annotation is KEDA's own supported
# override for exactly this: setting it forces the target to that replica
# count and makes KEDA stop reconciling it until the annotation is
# removed (see _sleep-orchestration). See Learnings.md.
_wake-orchestration:
    #!/usr/bin/env bash
    set -euo pipefail
    # data-platform.internal/last-woken-at stamped alongside the pause
    # annotation -- wake_sleep_sensor.py's grace period keys off this to
    # stop a stale sleep-sensor tick (e.g. an OLDER master_pipeline run's
    # terminal-status event, or the gap between this recipe's own
    # sequential master_pipeline invocations in verify-pipeline) from
    # unpausing before whichever new run this wake is for actually exists
    # in Dagster's run storage. Found live, 2026-07-20: without this,
    # verify-pipeline itself hit the identical race
    # frontend/dagster_wake.py was originally fixed for -- webserver
    # killed mid-poll, right as verify-pipeline moved from one
    # master_pipeline invocation to the next. Every wake path needs this
    # stamp, not just Streamlit's -- see wake_sleep_sensor.py.
    woken_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    kubectl annotate scaledobject dagster-webserver-scaler dagster-code-server-scaler -n orchestration \
        autoscaling.keda.sh/paused-replicas=1 "data-platform.internal/last-woken-at=${woken_at}" --overwrite
    kubectl rollout status deployment/dagster-webserver -n orchestration --timeout=120s
    kubectl rollout status deployment/dagster-code-server -n orchestration --timeout=120s

# Hands replica control back to KEDA once ad hoc work is done -- removing
# the pause annotation resumes normal Cron-trigger reconciliation, which
# scales back to 0 on its own (respecting cooldownPeriod) since this is
# called outside the configured windows. Mirrors this project's existing
# "kill every process/container spun up for a phase's work once it's done"
# convention (see CLAUDE.md), applied to a KEDA-paused replica count
# instead of a local process.
_sleep-orchestration:
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl annotate scaledobject dagster-webserver-scaler dagster-code-server-scaler -n orchestration \
        autoscaling.keda.sh/paused-replicas- --overwrite 2>/dev/null || true

# Tears down every in-cluster Dagster piece start brought up -- mirrors
# start exactly (same manifest list), same "full teardown" convention as
# every other k8s-hosted module's kill recipe (see metadata::kill,
# polaris::kill). No local process to pkill anymore -- Kubernetes itself
# now guarantees exactly one daemon pod at a time (replicas: 1), so the
# "multiple dagster dev instances fighting over heartbeat ownership"
# problem this recipe used to guard against is structurally impossible
# now, not just handled better.
kill:
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl delete -f k8s/keda-scaledobjects.yaml --ignore-not-found
    helm uninstall keda -n keda || true
    kubectl delete -f k8s/daemon-deployment.yaml \
        -f k8s/webserver-service.yaml -f k8s/webserver-deployment.yaml \
        -f k8s/code-server-service.yaml -f k8s/code-server-deployment.yaml \
        -f k8s/rbac.yaml -f k8s/rbac-frontend-wake.yaml -f k8s/postgres-credentials-secret.yaml \
        --ignore-not-found
    kubectl delete configmap dagster-instance dagster-instance-incluster -n orchestration --ignore-not-found

# Rebuild the orchestration image and reload it into kind, without
# restarting dagster dev -- for the "changed code, need launched pods to
# see it" case. Build context must be repo root, not orchestration/.
build-image:
    docker build -f Dockerfile -t data-platform-orchestration:latest {{justfile_directory()}}
    kind load docker-image data-platform-orchestration:latest --name data-platform

# One image per scaffolded domain (dbt/domains/<domain>/, discovered from
# disk the same way generate_domain_projects.py itself derives them from
# Postgres) -- MODELING_JOBS[domain]/SERVING_JOBS[domain] (the only jobs
# that actually run domain-specific dbt code) are tagged to use
# data-platform-domain-<domain> instead of the shared
# data-platform-orchestration image (scripts/generate_dagster_pipeline.py's
# dagster-k8s/config tag). Same Dockerfile, one different DOMAIN build
# arg -- every domain's build shares the same cached uv-sync layer, only
# the final `dbt parse` layer differs, so a change to one domain's models
# never needs another domain's image rebuilt. ODS-derived domains (e.g.
# police_crimes) get identical treatment -- no special-casing, since this
# loop only ever sees real scaffolded directories on disk, the same way
# every other domain-scoped recipe here does.
build-domain-images:
    #!/usr/bin/env bash
    set -euo pipefail
    for d in {{justfile_directory()}}/dbt/domains/*/; do
        dir="${d%/}"
        domain="$(basename "$dir")"
        [ -f "$dir/dbt_project.yml" ] || continue
        echo "Building data-platform-domain-$domain..."
        docker build -f Dockerfile --build-arg "DOMAIN=$domain" -t "data-platform-domain-$domain:latest" {{justfile_directory()}}
        kind load docker-image "data-platform-domain-$domain:latest" --name data-platform
    done

# Ensure at least one financial_transactions landing batch exists before the
# pipeline's first run. clean_financial_transactions deliberately skips its
# Iceberg write when there's nothing new to process (so an empty run doesn't
# crash) -- which means the very first run against an empty landing/ never
# creates iceberg.clean.financial_transactions at all, and
# stg_financial_transactions.sql's `source('clean', 'financial_transactions')`
# then fails with TABLE_NOT_FOUND (confirmed for real: this is exactly what a
# from-scratch `just smoketest` hit once this session's landing/ cleanup
# emptied the directory with nothing to regenerate it). Only generates when
# empty -- a `just start` on top of already-live landing data (the normal,
# non-smoketest case) shouldn't silently inject an extra synthetic batch
# nobody asked for.
seed-financial-landing:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    landing_dir="{{justfile_directory()}}/data-lake/landing/financial_transactions"
    if [ -z "$(find "$landing_dir" -name '*.csv' -print -quit 2>/dev/null)" ]; then
        echo "No financial_transactions landing batches found -- seeding one."
        cd {{justfile_directory()}} && uv run python scripts/generate_financial_reports.py
    fi

# Regenerate the dynamic pipeline foundation (per-feed dbt asset factory
# calls, per-feed jobs, real Dagster schedules) from data_feed/schedule/
# lakehouse_models (scripts/generate_dagster_pipeline.py) as
# pipeline_generated.py -- must run before build-image/dagster dev, same
# "structural decisions are a build-time codegen artifact, never a live
# Postgres read at Python import time" rule as generate-serve-views below.
# financial_assets.py imports from pipeline_generated.py directly (as do
# the connector-driven extraction/raw/clean assets pipeline_generated.py
# itself renders, e.g. for police_crimes/metadata_runs), so it must exist
# on disk before dagster dev's own import even starts. Also writes the
# small companion clean_source_tables_generated.py (dbt_assets.py imports
# CLEAN_SOURCE_TABLES from it -- a separate file specifically to avoid a
# circular import with pipeline_generated.py, which itself imports from
# dbt_assets.py). Also runnable standalone after a schedule/data_feed
# change.
generate-pipeline-jobs:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_dagster_pipeline.py

# Scaffold any new domain's dbt project skeleton (dbt/domains/<domain>/) from
# lakehouse_models.model_schema + data_feed.batch_ods_name
# (scripts/generate_domain_projects.py) -- must run before every other
# generate-* recipe below, since they all write INTO a domain's own
# directory tree (models/staging/, models/model/, etc.) and expect it to
# already exist. Create-if-missing, same philosophy as
# generate-model-scaffolds -- an existing domain's dbt_project.yml may have
# been hand-edited (materialization overrides, etc.), so it's never
# touched, only genuinely new domains get scaffolded. Also runnable
# standalone after a new model_schema/batch_ods_name value appears.
generate-domain-projects:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_domain_projects.py

# Regenerate the dbt `clean` source declarations
# (models/staging/_sources.yml) from every active data_feed row
# (scripts/generate_sources.py) -- same "must exist on disk before dbt
# parse builds target/manifest.json" reasoning as generate-serve-views
# below. Previously a hand-maintained file requiring a manual edit per new
# feed; any new feed's staging (hand-written or ODS-generated, see
# generate-ods-models below) reading source('clean', '<feed>') would
# otherwise fail `dbt parse` until someone remembered to add it by hand.
# Also runnable standalone after a data_feed change.
generate-sources:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_sources.py

# Regenerate the serve layer's `_latest`/`_historical` dbt view models from
# lakehouse_models (scripts/generate_serve_views.py) -- must run before
# build-image/dagster dev, not as a Dagster op: dagster-dbt's @dbt_assets
# reads target/manifest.json at Python-import time, before any run
# executes, so these files have to exist on disk before `dbt parse` builds
# that manifest (Learnings.md, Phase 5 "manifest must be pre-built into the
# image"). Requires metadata already seeded (lakehouse_models populated) --
# true by the time `start` reaches this step, since metadata::start runs
# first in the root Justfile's sequencing. Also runnable standalone after a
# lakehouse_models change, without a full image rebuild (same pattern as
# `just metadata::seed`). Own .pth sweep, not just inherited from `start`
# -- reachable standalone (`just orchestration::generate-serve-views`),
# same reasoning as the sweep in `test` below.
generate-serve-views:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_serve_views.py

# Regenerate the model layer's deletion-synthesis intermediate models
# (int_<feed>_with_deletes.sql) from lakehouse_models + schema_registry
# (scripts/generate_deletion_synthesis_views.py) -- same "must run before
# build-image/dagster dev" reasoning as generate-serve-views above (the
# manifest needs these files to already exist on disk). Also runnable
# standalone after a lakehouse_models.deletes_enabled change.
generate-deletion-synthesis-views:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_deletion_synthesis_views.py

# Scaffold new model-layer .sql/.yml files (Type 1 dimension/fact models,
# Type 2 dimension snapshots) from lakehouse_models
# (scripts/generate_model_scaffolds.py) -- must run before build-image/
# dagster dev, same "must exist on disk before dbt parse builds
# target/manifest.json" reasoning as generate-serve-views above. Unlike
# every other generate-* recipe here, this one NEVER overwrites an
# existing file -- only creates scaffolds for lakehouse_models rows whose
# target file is missing, so a hand-filled-in model is always safe to
# re-run this against. Also runnable standalone after adding a new
# lakehouse_models row.
generate-model-scaffolds:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_model_scaffolds.py

# Scaffold new streaming serve-view .sql/.yml files (Roadmap Phase 11
# generalization) from streaming_source (scripts/generate_streaming_serve_scaffolds.py)
# -- same "must exist on disk before dbt parse builds target/manifest.json"
# reasoning as generate-serve-views above, and same never-overwrite-an-
# existing-file behavior as generate-model-scaffolds (the streaming analog
# for the exact same reason -- a hand-filled-in join is real business
# logic, not something to regenerate over). Also runnable standalone
# after adding a new streaming_source row.
generate-streaming-serve-scaffolds:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_streaming_serve_scaffolds.py

# Regenerate the ODS (Operational Data Store) layer's staging + model-layer
# .sql files from data_feed.ods_enabled + schema_registry
# (scripts/generate_ods_models.py) -- same "must exist on disk before dbt
# parse builds target/manifest.json" reasoning as generate-serve-views
# above. Unlike generate-model-scaffolds, this ALWAYS fully regenerates
# (wipe-and-rewrite) -- ODS output has zero hand-written content ever, by
# design, so there's nothing to protect from being overwritten when
# schema_registry evolves. Also runnable standalone after an ods_enabled
# flip.
generate-ods-models:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    cd {{justfile_directory()}} && uv run python scripts/generate_ods_models.py

# Launches the real master_pipeline job (not `dagster asset materialize`,
# which never touches K8sRunLauncher -- see DebugReference.md; not
# `__ASSET_JOB` either -- that bypasses the master pipeline entirely and is
# no longer compatible with it, see Learnings.md "A feed whose source is
# the pipeline's own run history..." fourth occurrence) three times --
# model_schema=sales, model_schema=metadata, batch_group=police_crimes --
# the minimum set of orchestration_kind/value pairs that together exercise
# every feed and every domain exactly once, the same coverage the old
# single __ASSET_JOB run gave (5 feed-run rows + 3 domain model-run rows),
# but now via real per-stage pod isolation and both orchestration_kind
# values. Each launch is via trigger_master_pipeline.py, which blocks on
# the run's own Dagster-level status (dagster_launch.launch_and_wait) --
# NOT `kubectl wait --for=condition=complete`, confirmed live to be
# unreliable here: a launched pod's k8s Job can report "Complete" even
# though the Dagster run inside it failed (the run worker process's exit
# code doesn't reflect step failures) -- see Learnings.md.
verify-pipeline:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    just orchestration::_wake-orchestration
    trap 'just orchestration::_sleep-orchestration' EXIT
    export DAGSTER_HOME="{{justfile_directory()}}/orchestration/dagster_home"
    export PATH="{{justfile_directory()}}/.venv/bin:$PATH"

    # Sweep again immediately before each launch, not just once at the top
    # of the recipe, and retry on the known .pth/UF_HIDDEN signature only
    # (ModuleNotFoundError at code-location import time, before anything is
    # submitted -- safe to retry, nothing partially applied) -- same
    # reasoning as every other recipe here, see Learnings.md.
    launch_master_pipeline() {
        local kind="$1" value="$2"
        just _ensure-venv-visible >&2
        for attempt in 1 2 3; do
            if output=$(cd dagster_data_platform && uv run python -m dagster_data_platform.trigger_master_pipeline --orchestration-kind "$kind" --orchestration-value "$value" 2>&1); then
                tail -1 <<< "$output"
                return 0
            fi
            if ! grep -q "ModuleNotFoundError" <<< "$output"; then
                echo "$output" >&2
                exit 1
            fi
            echo ">>> trigger_master_pipeline (kind=$kind value=$value) hit the known .pth/UF_HIDDEN race (attempt $attempt/3) -- re-sweeping and retrying" >&2
            just _ensure-venv-visible >&2
            [ "$(uname)" = "Darwin" ] && sleep 10 || sleep 1
        done
        echo ">>> trigger_master_pipeline (kind=$kind value=$value) failed 3 times with the .pth/UF_HIDDEN signature -- giving up" >&2
        exit 1
    }

    # 900s timeout is baked into dagster_launch.launch_and_wait's own
    # default, not repeated here -- sized for a from-scratch metadata DB's
    # police_crimes full historical backfill (every available month since
    # the UK Police API's start), confirmed to take ~2.5 minutes on top of
    # everything else in a run. See Learnings.md.
    sales_run_id=$(launch_master_pipeline model_schema sales)
    echo "master_pipeline (model_schema=sales) succeeded: $sales_run_id"
    metadata_run_id=$(launch_master_pipeline model_schema metadata)
    echo "master_pipeline (model_schema=metadata) succeeded: $metadata_run_id"
    police_crimes_run_id=$(launch_master_pipeline batch_group police_crimes)
    echo "master_pipeline (batch_group=police_crimes) succeeded: $police_crimes_run_id"

    run_ids="'$sales_run_id','$metadata_run_id','$police_crimes_run_id'"
    feed_ok=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "select count(*) from data_processing_runs where master_dagster_run_id in ($run_ids) and data_feed_id is not null and job_successful = true;")
    model_ok=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "select count(*) from data_processing_runs where master_dagster_run_id in ($run_ids) and model_key is not null and job_successful = true;")
    # 5 feed-run rows across the 3 master_pipeline runs: customers, sales,
    # financial_transactions (via model_schema=sales), metadata_runs (via
    # model_schema=metadata), police_crimes (via batch_group=police_crimes).
    # 3 model-run rows: "sales", "metadata", "police_crimes" (its ODS
    # domain, see data_feed.batch_ods_name) -- one per MODELING_JOBS/
    # SERVING_JOBS pair actually launched.
    if [[ "$feed_ok" -ne 5 || "$model_ok" -ne 3 ]]; then
        echo "Expected 5 successful feed-run + 3 successful model-run rows across these 3 master_pipeline runs, got $feed_ok + $model_ok" >&2
        exit 1
    fi
    echo "Pipeline verified: 5/5 feed-run + 3/3 domain model-run rows successful across 3 master_pipeline invocations."

    # customers/sales raw durability fix -- confirms raw_customers/raw_sales
    # actually write a verbatim file now, not just pass the DataFrame
    # through in memory (see extraction_assets.py/sales_assets.py). Looked
    # up via each feed's own storage_watermark (Roadmap.md "Incremental
    # Loading & Watermarks") -- generated once by record_run_started() at
    # row-creation time and shared by both the raw-writing and
    # clean-reading steps of that feed's own EXTRACTION_JOBS[feed] job/pod,
    # not by extraction_dagster_run_id (which raw_storage.py's path no
    # longer keys by) and not assumed to equal $sales_run_id the way a
    # single __ASSET_JOB run used to allow.
    for feed in customers sales; do
        storage_watermark=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
            -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
            "select r.storage_watermark from data_processing_runs r join data_feed df on df.id = r.data_feed_id where r.master_dagster_run_id = '$sales_run_id' and df.friendly_name = '$feed';")
        f="data-lake/raw/$feed/$storage_watermark/$feed.parquet"
        if [ ! -f "{{justfile_directory()}}/$f" ]; then
            echo "Expected durable raw file missing: $f" >&2
            exit 1
        fi
    done
    echo "Durable raw files confirmed for customers and sales."

# Real end-to-end test for a schedule-fired run: evaluates a real generated
# ScheduleDefinition (trigger_schedule_run.py, via build_schedule_context --
# no cron wait), launches the resulting RunRequest through the actual
# webserver -> daemon -> K8sRunLauncher path and blocks on its own
# Dagster-level run status (dagster_launch.launch_and_wait, via
# trigger_schedule_run.py -- `dagster job launch` has no --tags flag and
# there's no "fire a schedule now" CLI command; NOT `kubectl wait`, which
# can report a launched pod's k8s Job as "Complete" even when the Dagster
# run inside it failed, see Learnings.md), then checks data_processing_runs
# the same way verify-pipeline does. Targets the police_crimes
# feed-schedule specifically (schedule_id looked up live, not hardcoded --
# see trigger_schedule_run.py); the schedule now targets master_pipeline
# with orchestration_kind='batch_group', so $run_id below is
# master_dagster_run_id directly.
verify-schedule:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    just orchestration::_wake-orchestration
    trap 'just orchestration::_sleep-orchestration' EXIT
    export DAGSTER_HOME="{{justfile_directory()}}/orchestration/dagster_home"
    export PATH="{{justfile_directory()}}/.venv/bin:$PATH"
    cd dagster_data_platform

    # `tail -1`, not a bare capture -- importing pipeline_generated.py
    # (trigger_schedule_run.py's own import chain) evaluates every feed's
    # @dbt_assets select=/exclude= at import time, and a feed genuinely
    # owning no lakehouse_models rows (metadata_runs, police_crimes) makes
    # dbt print a "does not match any enabled nodes" WARNING straight to
    # stdout -- confirmed for real: this corrupted a bare `run_id=$(...)`
    # capture into multi-line garbage even though the actual run completed
    # successfully every time. print(run_id) is always the script's last
    # statement for a feed-type schedule (exactly one RunRequest), so the
    # true id is always the last line. launch_and_wait already blocks until
    # master_pipeline reaches a terminal status (raising if it didn't
    # succeed), so nothing further to wait on here.
    run_id=$(uv run python -m dagster_data_platform.trigger_schedule_run --feed police_crimes | tail -1)
    echo "Schedule-triggered master_pipeline run $run_id succeeded."

    ok=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "select count(*) from data_processing_runs where master_dagster_run_id = '$run_id' and data_feed_id is not null and job_successful = true;")
    if [[ "$ok" -lt 1 ]]; then
        echo "Schedule-triggered run $run_id did not produce a successful feed-run row in data_processing_runs" >&2
        exit 1
    fi
    echo "Schedule-triggered run $run_id verified: successful feed-run row confirmed in data_processing_runs."

# Real end-to-end test for the sensor-triggered path (not just its
# evaluation logic, which tests/test_sensors.py already covers in-process):
# starts the generated sensor for financial_transactions (--feed defaults
# to it; every generated sensor is STOPPED by default -- see
# scripts/generate_dagster_pipeline.py's _make_master_pipeline_sensor),
# drops a genuine CSV into the landing directory, waits for the daemon's
# own sensor-tick loop to detect it and launch master_pipeline, waits for
# that run to finish, then always stops the sensor again afterward
# (verify_sensor_trigger.py's own finally block) -- closes the gap noted
# in Backlog.md: this exact trigger path bypassed the master pipeline
# entirely earlier in this project's history, and nothing exercised it
# live until now.
verify-sensor:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    just orchestration::_wake-orchestration
    trap 'just orchestration::_sleep-orchestration' EXIT
    export DAGSTER_HOME="{{justfile_directory()}}/orchestration/dagster_home"
    export PATH="{{justfile_directory()}}/.venv/bin:$PATH"
    cd dagster_data_platform

    run_id=$(uv run python -m dagster_data_platform.verify_sensor_trigger | tail -1)
    echo "Sensor-triggered master_pipeline run $run_id succeeded."

    ok=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
        "select count(*) from data_processing_runs where master_dagster_run_id = '$run_id' and data_feed_id is not null and job_successful = true;")
    if [[ "$ok" -lt 1 ]]; then
        echo "Sensor-triggered run $run_id did not produce a successful feed-run row in data_processing_runs" >&2
        exit 1
    fi
    echo "Sensor-triggered run $run_id verified: successful feed-run row confirmed in data_processing_runs."

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
    # test_schedules.py's build_schedule_context(instance=DagsterInstance.get())
    # needs a real DAGSTER_HOME, same as verify-pipeline/verify-schedule --
    # it's a no-op for every other test file, which don't touch DagsterInstance.
    export DAGSTER_HOME="{{justfile_directory()}}/orchestration/dagster_home"
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
