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
uv run python -m polaris_client.bootstrap
