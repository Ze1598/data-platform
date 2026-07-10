# raw_to_clean (PyIceberg + Polars raw->clean library). No `start`/`kill` --
# this is a library imported by orchestration's op pods, not a standalone
# deployment.

# Unit tests -- pure logic (validate_schema/schema mapping), no live
# cluster or I/O needed.
# Defensive .pth sweep here too, not just in `start`/`verify-pipeline` --
# this recipe is reachable standalone (`just processing::test`), bypassing
# the root Justfile's `test` recipe dependency. See Learnings.md for the
# .pth/UF_HIDDEN investigation -- confirmed recurring within a single
# `just smoketest` run, minutes after a prior sweep already ran.
# Retried on the known .pth/UF_HIDDEN signature only, same reasoning as
# orchestration::test / verify-pipeline -- failure happens at import time
# (test collection), before any test runs, so retrying is safe.
test:
    #!/usr/bin/env bash
    set -euo pipefail
    just _ensure-venv-visible
    test_ok=false
    for attempt in 1 2 3; do
        if test_output=$(cd raw_to_clean && uv run pytest 2>&1); then
            test_ok=true
            break
        fi
        if ! grep -q "ModuleNotFoundError" <<< "$test_output"; then
            echo "$test_output" >&2
            exit 1
        fi
        echo ">>> pytest hit the known .pth/UF_HIDDEN race (attempt $attempt/3) -- re-sweeping and retrying" >&2
        just _ensure-venv-visible
        # Widened on macOS specifically -- see orchestration::verify-pipeline's
        # identical comment for the full reasoning (iCloud sync cycle timing).
        [ "$(uname)" = "Darwin" ] && sleep 10 || sleep 1
    done
    echo "$test_output"
    if [ "$test_ok" != true ]; then
        echo ">>> pytest failed 3 times with the .pth/UF_HIDDEN signature -- giving up" >&2
        exit 1
    fi
