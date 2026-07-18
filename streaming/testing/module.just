# Isolated streaming tests -- proves each active streaming_source works
# end-to-end (Kafka -> Flink -> Iceberg -> dbt serve view) without
# requiring the batch pipeline to have run first. A dummy model-layer
# dependency table is created if missing (run.py's ensure_fixtures/
# _MODEL_LAYER_FIXTURES) specifically so a streaming serve view's join
# (e.g. sales_events -> sales_dim_branch) doesn't need a real
# master_pipeline run first -- these validate streaming in isolation, not
# as an integration test gated on batch having already run.
#
# Runs as one-shot Kubernetes Jobs inside the cluster, not from the host --
# Kafka's Service is ClusterIP-only (see Learnings.md's "Host-side tools
# need a NodePort" entry), so anything touching Kafka has to run in-cluster;
# Postgres/Trino are reached the same way here (in-cluster DNS) for one
# consistent execution path rather than splitting steps across host-NodePort
# and in-cluster. Jobs, not `kubectl run --rm`, for the same reason
# kafka-create-topics is a Job -- reliable exit-code/log inspection, not
# dependent on `kubectl run --attach`'s exit-code quirks across kubectl
# versions. backoffLimit: 0 -- a test script retrying silently on failure
# would mask a real bug rather than surface it.
#
# See ../DebugReference.md for the manual-command equivalents this wraps.

build:
    #!/usr/bin/env bash
    set -euo pipefail
    docker build -f Dockerfile -t data-platform-streaming-testing:latest {{justfile_directory()}}
    kind load docker-image data-platform-streaming-testing:latest --name data-platform

# Runs `python run.py <mode>` as a one-shot Job in the streaming namespace,
# waits for it to finish, always prints its logs, and fails the recipe if
# the Job didn't succeed. Job name is fixed per mode (not per-invocation
# unique) and deleted before re-creating -- same delete-then-recreate
# reasoning as kafka::start's own create-topics Job (immutable Job spec,
# and this mode may genuinely be re-run in the same session).
_run mode:
    #!/usr/bin/env bash
    set -euo pipefail
    job="streaming-testing-{{mode}}"
    kubectl delete job "$job" -n streaming --ignore-not-found --wait=true >/dev/null 2>&1 || true
    cat <<YAML | kubectl apply -f -
    apiVersion: batch/v1
    kind: Job
    metadata:
      name: $job
      namespace: streaming
    spec:
      backoffLimit: 0
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: runner
              image: data-platform-streaming-testing:latest
              imagePullPolicy: IfNotPresent
              command: ["python", "run.py", "{{mode}}"]
    YAML

    succeeded="" failed=""
    for i in $(seq 1 90); do
        succeeded=$(kubectl get job "$job" -n streaming -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "")
        failed=$(kubectl get job "$job" -n streaming -o jsonpath='{.status.failed}' 2>/dev/null || echo "")
        [ "$succeeded" = "1" ] && break
        [ "$failed" = "1" ] && break
        sleep 5
    done

    echo "--- logs: $job ---"
    kubectl logs "job/$job" -n streaming --all-containers 2>&1 || true

    if [ "$succeeded" != "1" ]; then
        echo "streaming-testing job '$job' did not complete successfully (succeeded='$succeeded' failed='$failed')" >&2
        exit 1
    fi

setup:
    just streaming-testing::_run setup

verify-raw:
    just streaming-testing::_run verify-raw

verify-serve:
    just streaming-testing::_run verify-serve

# Full isolated streaming test cycle: build the runner image, prime every
# active source (dummy model fixtures + sample messages + schema
# discovery), regenerate + redeploy the real ingestion plumbing now that
# sources are "ready" (see scripts/generate_streaming_ingestion.py),
# confirm the raw Iceberg sink tables actually grow, build the streaming
# serve views via dbt, then confirm those views return real rows too.
#
# Deliberately does its own kafka/flink sequencing rather than calling
# `streaming::start` -- start's fixed ordering (kafka -> flink -> producer)
# assumes discovery already happened; this recipe does discovery itself,
# in between, which start's sequence has no hook for.
test:
    #!/usr/bin/env bash
    set -euo pipefail
    just streaming-testing::build
    just streaming-testing::setup
    just streaming::generate-streaming-ingestion
    just kafka::start
    just flink::start
    just streaming-testing::verify-raw

    echo "=== Building streaming serve views (dbt) ==="
    schemas=$(PGPASSWORD="${POSTGRES_PASSWORD:-platform}" psql -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5432}" \
        -U "${POSTGRES_USER:-platform}" -d "${POSTGRES_DB:-platform_metadata}" -tAc \
        "select distinct model_schema from streaming_source where is_active = true")
    for schema in $schemas; do
        (cd {{justfile_directory()}}/dbt/domains/"$schema" && uv run dbt build --profiles-dir profiles --select tag:streaming)
    done

    just streaming-testing::verify-serve
    just producer::start
    echo "Streaming tests passed."

kill:
    #!/usr/bin/env bash
    set -euo pipefail
    for mode in setup verify-raw verify-serve; do
        kubectl delete job "streaming-testing-$mode" -n streaming --ignore-not-found
    done
