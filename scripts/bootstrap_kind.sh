#!/usr/bin/env bash
# Creates the local kind cluster (if it doesn't already exist), applies
# namespaces, and deploys Postgres in-cluster. Idempotent — safe to re-run.
set -euo pipefail

cd "$(dirname "$0")/.."  # repo root, so the kind-cluster.yaml hostPath resolves correctly

mkdir -p data-lake/{landing,raw,clean,staging,model,iceberg-warehouse}

if kind get clusters 2>/dev/null | grep -qx "data-platform"; then
  echo "kind cluster 'data-platform' already exists, skipping creation"
else
  kind create cluster --config platform/kind/kind-cluster.yaml
fi

kubectl apply -f platform/namespaces/

# Generated from metadata/db/init/ rather than hand-duplicated in a static
# manifest, so the schema has a single source of truth.
kubectl create configmap postgres-init-scripts \
  --from-file=metadata/db/init/ \
  -n metadata \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f metadata/k8s/secret.yaml
kubectl apply -f metadata/k8s/statefulset.yaml
kubectl apply -f metadata/k8s/service.yaml

echo "Waiting for postgres StatefulSet to become ready..."
kubectl rollout status statefulset/postgres -n metadata --timeout=180s

echo "Done. Postgres reachable at localhost:5432 (credentials: metadata/k8s/secret.yaml)."
