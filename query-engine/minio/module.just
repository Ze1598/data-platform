# MinIO (S3-compatible object storage backing the Iceberg warehouse).
# See ../DebugReference.md for the manual-command equivalents this wraps.

start:
    kubectl apply -f secret.yaml -f pvc.yaml -f deployment.yaml -f service.yaml
    kubectl rollout status deployment/minio -n query-engine --timeout=120s
    kubectl apply -f create-bucket-job.yaml
    kubectl wait --for=condition=complete job/minio-create-bucket -n query-engine --timeout=60s

kill:
    kubectl delete -f secret.yaml -f pvc.yaml -f deployment.yaml -f service.yaml -f create-bucket-job.yaml --ignore-not-found
