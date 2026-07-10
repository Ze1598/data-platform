# Cluster-wide foundation: the kind cluster itself and namespace scaffolding.
# See DebugReference.md for the manual-command equivalents this wraps.

# Create (or resume) the kind cluster and apply namespaces.
start:
    {{justfile_directory()}}/scripts/bootstrap_kind.sh

# Pause the cluster (docker stop, not delete) — reversible via `just start platform`.
# For a full, non-reversible teardown use `just nuke` instead.
kill:
    #!/usr/bin/env bash
    set -euo pipefail
    if docker ps --format '{{{{.Names}}' | grep -qx "data-platform-control-plane"; then
        docker stop data-platform-control-plane
    else
        echo "data-platform-control-plane not running, nothing to stop"
    fi

# Quick sanity check that the cluster is actually up.
status:
    kubectl get nodes
    kubectl get pods -A
