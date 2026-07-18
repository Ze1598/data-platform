# Kafka (KRaft mode, single broker) -- the streaming/ module's ingest broker.
# See ../DebugReference.md for the manual-command equivalents this wraps.

start:
    kubectl apply -f pvc.yaml -f deployment.yaml -f service.yaml
    kubectl rollout status deployment/kafka -n streaming --timeout=120s
    kubectl apply -f create-topic-job.yaml
    kubectl wait --for=condition=complete job/kafka-create-topic -n streaming --timeout=60s

kill:
    kubectl delete -f pvc.yaml -f deployment.yaml -f service.yaml -f create-topic-job.yaml --ignore-not-found
