# Flink Kubernetes Operator + the sales-events-sink FlinkDeployment.
# See ../DebugReference.md for the manual-command equivalents this wraps.
#
# Cluster memory is genuinely tight with this module added on top of the
# rest of the stack -- confirmed live at ~87% of Docker Desktop's 7.65GB
# VM budget with everything running simultaneously (Postgres, Polaris,
# MinIO, Trino, 3 Dagster deployments, Streamlit, Kafka, the Flink
# operator, JobManager+TaskManager, and the producer, all up at once). It
# fits, but with limited headroom -- a heavier concurrent batch workload
# could push it over. Watch for OOMKilled pods if that happens.

install-operator:
    helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
    helm repo update flink-operator-repo
    # webhook.create=false: the operator's default webhook needs
    # cert-manager (Certificate/Issuer CRDs), not installed anywhere else
    # in this cluster. The webhook only does admission validation/
    # defaulting on FlinkDeployment CRs -- not required for jobs to run,
    # and adding a whole new cluster-wide dependency for it wasn't judged
    # worth it for a single local dev cluster.
    helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
        -n streaming --set webhook.create=false
    kubectl rollout status deployment/flink-kubernetes-operator -n streaming --timeout=120s

start:
    #!/usr/bin/env bash
    set -euo pipefail
    just flink::install-operator
    docker build --provenance=false -f Dockerfile -t data-platform-streaming-flink:latest .
    kind load docker-image data-platform-streaming-flink:latest --name data-platform
    kubectl apply -f flinkdeployment.yaml
    echo "Waiting for the sales-events-sink job to start running..."
    for i in $(seq 1 30); do
        state=$(kubectl get flinkdeployment sales-events-sink -n streaming -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || echo "")
        [ "$state" = "RUNNING" ] && break
        sleep 5
    done
    kubectl get flinkdeployment sales-events-sink -n streaming -o jsonpath='{.status.jobStatus.state}{"\n"}'

kill:
    kubectl delete -f flinkdeployment.yaml --ignore-not-found
    helm uninstall flink-kubernetes-operator -n streaming --ignore-not-found
