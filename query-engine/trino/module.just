# Trino (Iceberg REST catalog client, SQL engine for dbt). Depends on
# polaris (catalog must be registered before Trino can query anything
# useful, though the Helm release itself doesn't strictly require it to
# install). See ../DebugReference.md for the manual-command equivalents.

start:
    helm upgrade --install trino trino/trino -n query-engine -f values.yaml
    kubectl rollout status deployment/trino-coordinator -n query-engine --timeout=180s

kill:
    helm uninstall trino -n query-engine --ignore-not-found
