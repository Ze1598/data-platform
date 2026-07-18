# Kafka (KRaft mode, single broker) -- the streaming/ module's ingest broker.
# See ../DebugReference.md for the manual-command equivalents this wraps.
#
# Topic creation comes from generated/create-topics-job.yaml (one topic per
# ready streaming_source row -- see scripts/generate_streaming_ingestion.py,
# run by streaming/module.just::start before this). Skipped gracefully if
# that file doesn't exist yet (zero streaming_source rows configured is a
# normal state, not an error, on a fresh setup).

start:
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl apply -f pvc.yaml -f deployment.yaml -f service.yaml
    kubectl rollout status deployment/kafka -n streaming --timeout=120s
    if [ -f generated/create-topics-job.yaml ]; then
        # Job specs are immutable -- confirmed live: re-applying with a
        # different topic list (a new streaming_source row became ready)
        # fails with "field is immutable" against the previous run's Job.
        # Delete-then-recreate, not apply, since this Job's content
        # genuinely changes as sources are added.
        kubectl delete -f generated/create-topics-job.yaml --ignore-not-found
        kubectl apply -f generated/create-topics-job.yaml
        kubectl wait --for=condition=complete job/kafka-create-topics -n streaming --timeout=60s
    else
        echo "No streaming_source rows ready yet (generated/create-topics-job.yaml missing) -- skipping topic creation."
    fi

kill:
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl delete -f pvc.yaml -f deployment.yaml -f service.yaml --ignore-not-found
    [ -f generated/create-topics-job.yaml ] && kubectl delete -f generated/create-topics-job.yaml --ignore-not-found || true
