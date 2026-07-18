# Synthetic sales-event producer Deployment. See ../DebugReference.md for
# the manual-command equivalents this wraps.
#
# Build context is the repo root ({{justfile_directory()}}, which resolves
# to root even inside a `mod`-imported submodule -- see Dockerfile's own
# comment) so the workspace lockfile is available for `uv sync --frozen`,
# same pattern as frontend/module.just and orchestration/module.just.

start:
    #!/usr/bin/env bash
    set -euo pipefail
    docker build -f Dockerfile --provenance=false -t data-platform-streaming-producer:latest {{justfile_directory()}}
    kind load docker-image data-platform-streaming-producer:latest --name data-platform
    kubectl apply -f k8s/deployment.yaml
    kubectl rollout status deployment/sales-events-producer -n streaming --timeout=60s

kill:
    kubectl delete -f k8s/deployment.yaml --ignore-not-found
