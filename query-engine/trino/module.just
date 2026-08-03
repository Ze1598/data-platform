# Trino (Iceberg REST catalog client, SQL engine for dbt). Depends on
# polaris (catalog must be registered before Trino can query anything
# useful, though the Helm release itself doesn't strictly require it to
# install). See ../DebugReference.md for the manual-command equivalents.

# Chart version pinned (2026-08-03) -- see Learnings.md's "Dagster control
# plane scaling on Kubernetes" entry (why every off-the-shelf
# image/chart in this project got pinned to its then-current version).
start:
    helm upgrade --install trino trino/trino -n query-engine -f values.yaml --version 1.42.2
    kubectl rollout status deployment/trino-coordinator -n query-engine --timeout=180s

kill:
    helm uninstall trino -n query-engine --ignore-not-found
