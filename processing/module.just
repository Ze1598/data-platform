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
test:
    #!/usr/bin/env bash
    set -euo pipefail
    find "{{justfile_directory()}}/.venv/lib" -name "*.pth" -flags +hidden -exec chflags nohidden {} \; 2>/dev/null || true
    cd raw_to_clean && uv run pytest
