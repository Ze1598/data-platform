#!/usr/bin/env bash
# Creates the local kind cluster (if it doesn't already exist, or resumes it
# if it exists but was `docker stop`-ped), and applies namespaces. Idempotent
# — safe to re-run. This is the `platform` module's own start action (see
# platform/module.just); Postgres and every other module deploy themselves
# via their own module.just, not from here — this script only owns the
# cluster + namespace foundation.
set -euo pipefail

cd "$(dirname "$0")/.."  # repo root, so the kind-cluster.yaml hostPath resolves correctly

mkdir -p data-lake/{landing,raw,clean,staging,model,iceberg-warehouse}

if kind get clusters 2>/dev/null | grep -qx "data-platform"; then
  if [ "$(docker inspect -f '{{.State.Running}}' data-platform-control-plane 2>/dev/null)" != "true" ]; then
    echo "kind cluster 'data-platform' exists but is stopped, resuming..."
    docker start data-platform-control-plane
  else
    echo "kind cluster 'data-platform' container already running"
  fi
  # The container reporting "running" doesn't mean the API server inside is
  # accepting connections yet -- e.g. Docker itself can restart a container
  # with a restart policy (seen in practice: `on-failure`) independently of
  # whatever state it was left in, so "already running" isn't a reliable
  # signal that it's been up for a while. `kubectl wait` itself doesn't
  # retry a bare connection-refused -- it expects the API server to already
  # be reachable to poll conditions at all, and fails immediately instead
  # of treating "not listening yet" as "not ready yet, keep waiting".
  # Poll for the API server actually accepting connections first.
  echo "Waiting for the API server to accept connections..."
  for i in $(seq 1 30); do
    kubectl cluster-info --request-timeout=2s > /dev/null 2>&1 && break
    sleep 2
  done
  echo "Waiting for the node to become ready..."
  kubectl wait --for=condition=Ready node/data-platform-control-plane --timeout=120s
else
  kind create cluster --config platform/kind/kind-cluster.yaml
fi

kubectl apply -f platform/namespaces/

echo "Done. Cluster up, namespaces applied."
