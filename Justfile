# Operational entrypoint for the whole platform. See each module's own
# module.just (and DebugReference.md) for what a given module's recipes
# actually do; this file only owns cross-module sequencing/dispatch.
#
# Module names match their directory names exactly (query-engine, not
# query_engine) -- `just` accepts hyphens in mod/recipe names.

set dotenv-load

mod platform 'platform/module.just'
mod metadata 'metadata/module.just'
mod minio 'query-engine/minio/module.just'
mod polaris 'query-engine/polaris/module.just'
mod trino 'query-engine/trino/module.just'
mod query-engine 'query-engine/module.just'
mod kafka 'streaming/kafka/module.just'
mod flink 'streaming/flink/module.just'
mod producer 'streaming/producer/module.just'
mod streaming 'streaming/module.just'
mod orchestration 'orchestration/module.just'
mod processing 'processing/module.just'
mod dbt 'dbt/module.just'
mod frontend 'frontend/module.just'
mod tests-integration 'tests/integration/module.just'

default:
    @just --list

# Defensive mitigation, not a full fix: `link-mode = "copy"` (pyproject.toml)
# stops *new* uv writes from getting the macOS UF_HIDDEN flag (see
# Learnings.md for the full investigation -- Python 3.13's site.py skips
# hidden .pth files, silently breaking editable-install imports). But
# already-written, previously-good .pth files have been observed getting
# re-hidden minutes later with no rewrite (same mtime) -- something beyond
# uv's write path is involved, not fully root-caused. `UF_HIDDEN`/`chflags`
# is macOS/BSD-specific (confirmed, not just untested elsewhere) -- gated
# on `uname` so this is a no-op on Linux (CI runners, other dev machines),
# not just harmlessly-erroring per-file. Single source of truth: every
# other sweep site in this repo (orchestration/processing module.just,
# register-catalog.sh) calls `just _ensure-venv-visible` rather than
# duplicating this command, so the OS gate only needs to live here.
_ensure-venv-visible:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "$(uname)" = "Darwin" ] && [ -d .venv/lib ]; then
        find .venv/lib -name "*.pth" -flags +hidden -exec chflags nohidden {} \; 2>/dev/null || true
    fi

# Bring up every module in dependency order, or just one: `just start orchestration`.
start module="": _ensure-venv-visible
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "{{module}}" ]; then
        just platform::start
        just metadata::start
        just query-engine::start
        just streaming::start
        just orchestration::start
        just frontend::start
    else
        just "{{module}}::start"
    fi

# Tear down every module (reverse order), or just one: `just kill frontend`.
# For k8s-hosted modules this deletes that module's manifests (and, for
# PVC-backed ones, its persisted data) -- see the plan addendum for why
# this is a scoped-nuke, not a gentle pause, for anything but `platform`.
kill module="":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "{{module}}" ]; then
        just frontend::kill
        just orchestration::kill
        just streaming::kill
        just query-engine::kill
        just metadata::kill
        just platform::kill
    else
        just "{{module}}::kill"
    fi

# Full, non-reversible teardown: delete the kind cluster entirely and kill
# every local process. Does NOT rebuild -- that's `just smoketest`.
nuke:
    #!/usr/bin/env bash
    set -euo pipefail
    just orchestration::kill || true
    just frontend::kill || true
    pkill -f "kubectl port-forward" || true
    kind delete cluster --name data-platform || true
    docker stop data-platform-control-plane 2>/dev/null || true
    echo "Nuked. Run 'just start' to rebuild from scratch."

# Drop the whole solution, rebuild from zero, and prove it actually works:
# nuke -> start -> live pipeline verification -> full test suite. This is
# the project's standing regression-testing methodology (see Learnings.md,
# "Phase 6 (continued)") as one command instead of a hand-run sequence.
smoketest:
    just nuke
    just start
    just orchestration::verify-pipeline
    just orchestration::verify-schedule
    just orchestration::verify-sensor
    just test

# Run tests: no arg runs everything; a known module name scopes to that
# module's unit tests; anything else is treated as a dbt tag (feed code),
# e.g. `just test customers` runs that feed's dbt schema tests.
test module="": _ensure-venv-visible
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{module}}" in
        "")
            just processing::test
            just dbt::test
            just orchestration::test
            just frontend::test
            just tests-integration::test
            ;;
        processing|raw_to_clean) just processing::test ;;
        dagster|orchestration) just orchestration::test ;;
        frontend) just frontend::test ;;
        integration) just tests-integration::test ;;
        *) just dbt::test "{{module}}" ;;
    esac
