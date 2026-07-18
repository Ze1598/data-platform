# Flink Kubernetes Operator + one FlinkDeployment per active streaming_source
# row (generated/*.yaml -- see scripts/generate_streaming_ingestion.py, run
# by streaming/module.just::start before this). One FlinkDeployment per
# source (Application Mode), not a shared session cluster -- confirmed
# against Flink's own docs this is the real production-recommended
# isolation pattern, see Learnings.md/Roadmap.md. All sources share one
# Docker image (built once); only which generated .sql script a source's
# FlinkDeployment runs differs (its own `args`).
# See ../DebugReference.md for the manual-command equivalents this wraps.
#
# Cluster memory is genuinely tight with this module added on top of the
# rest of the stack -- confirmed live at ~87% of Docker Desktop's 7.65GB
# VM budget with everything running simultaneously (Postgres, Polaris,
# MinIO, Trino, 3 Dagster deployments, Streamlit, Kafka, the Flink
# operator, and one JobManager+TaskManager pair per source, all up at
# once). Bump Docker Desktop's memory allocation before onboarding more
# than one or two streaming sources locally -- this is a local dev
# environment ceiling, not an architecture flaw (a real deployment target
# runs multi-node with autoscaling). Per-source jobmanager_memory/
# taskmanager_memory/taskmanager_cpu (streaming_source metadata) is the
# real dial to turn locally.

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

    shopt -s nullglob
    crs=(generated/*.yaml)
    if [ ${#crs[@]} -eq 0 ]; then
        echo "No streaming_source rows ready yet (generated/*.yaml missing) -- nothing to deploy."
        exit 0
    fi

    # Resource name read back from kubectl apply's own output, not guessed
    # from the filename -- the generated filename and the FlinkDeployment's
    # own metadata.name are related but not identical (table_name's
    # underscores become hyphens for the k8s-facing name, see
    # generate_streaming_ingestion.py's k8s_name()), so deriving one from
    # the other via basename was a real bug, confirmed live.
    names=()
    for cr in "${crs[@]}"; do
        name=$(kubectl apply -f "$cr" -o jsonpath='{.metadata.name}')
        names+=("$name")
    done

    for name in "${names[@]}"; do
        echo "Waiting for FlinkDeployment $name to start running..."
        for i in $(seq 1 30); do
            state=$(kubectl get flinkdeployment "$name" -n streaming -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || echo "")
            [ "$state" = "RUNNING" ] && break
            sleep 5
        done
        kubectl get flinkdeployment "$name" -n streaming -o jsonpath="$name: "'{.status.jobStatus.state}{"\n"}'
    done

kill:
    #!/usr/bin/env bash
    set -euo pipefail
    shopt -s nullglob
    crs=(generated/*.yaml)
    if [ ${#crs[@]} -gt 0 ]; then
        kubectl delete -f generated/ --ignore-not-found
    fi
    helm uninstall flink-kubernetes-operator -n streaming --ignore-not-found
