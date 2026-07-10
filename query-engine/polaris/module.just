# Apache Polaris (Iceberg REST catalog). Depends on minio (bucket must
# exist before catalog registration can succeed). See ../DebugReference.md
# for the manual-command equivalents this wraps.

start:
    kubectl apply -f secret.yaml -f deployment.yaml -f service.yaml
    kubectl rollout status deployment/polaris -n query-engine --timeout=120s
    kubectl apply -f bootstrap-job.yaml
    kubectl wait --for=condition=complete job/polaris-bootstrap -n query-engine --timeout=60s
    ./register-catalog.sh

kill:
    kubectl delete -f secret.yaml -f deployment.yaml -f service.yaml -f bootstrap-job.yaml --ignore-not-found
