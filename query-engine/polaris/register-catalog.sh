#!/usr/bin/env bash
# Provisions (or verifies) the "data_platform" Iceberg catalog in Polaris,
# via polaris_client (query-engine/polaris_client) — the project's own
# reusable Python wrapper around apache-polaris's generated Management API
# SDK, not the CLI directly. The CLI was the right tool while this was a
# one-off bootstrap script (see git history / Learnings.md for that phase);
# now that the same catalog/privilege operations need to be reusable by
# other Python tooling (Dagster, eventually), the actual logic lives in
# polaris_client.bootstrap and this script is just its entrypoint.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

# Defensive, not a full fix -- see Learnings.md for the .pth/UF_HIDDEN
# investigation. Something beyond uv's own write path re-hides already-good
# .pth files well within a single `just smoketest` run, not just hours
# apart, so this needs to run immediately before the import it's protecting,
# not just once upstream. Delegates to the root Justfile's
# _ensure-venv-visible (single source of truth, including the macOS-only
# gate -- chflags/UF_HIDDEN is a macOS/BSD-specific mechanism, confirmed
# not an issue elsewhere) rather than duplicating the raw command here.
#
# Retried on the known signature: polaris_client.bootstrap is itself
# idempotent (create-or-update against the catalog/namespaces, see its own
# module docs), and the failure happens at import time before any bootstrap
# logic runs, so retrying is safe -- not masking a real bug, since a
# genuinely different failure exits immediately on attempt 1.
just _ensure-venv-visible
bootstrap_ok=false
for attempt in 1 2 3; do
    if bootstrap_output=$(uv run python -m polaris_client.bootstrap 2>&1); then
        bootstrap_ok=true
        break
    fi
    if ! grep -q "ModuleNotFoundError" <<< "$bootstrap_output"; then
        echo "$bootstrap_output" >&2
        exit 1
    fi
    echo ">>> polaris_client.bootstrap hit the known .pth/UF_HIDDEN race (attempt $attempt/3) -- re-sweeping and retrying" >&2
    just _ensure-venv-visible
    # Widened on macOS specifically -- see orchestration/module.just's
    # verify-pipeline for the full reasoning (iCloud sync cycle timing).
    [ "$(uname)" = "Darwin" ] && sleep 10 || sleep 1
done
echo "$bootstrap_output"
if [ "$bootstrap_ok" != true ]; then
    echo ">>> polaris_client.bootstrap failed 3 times with the .pth/UF_HIDDEN signature -- giving up" >&2
    exit 1
fi
