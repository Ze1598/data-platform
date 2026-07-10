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
mod orchestration 'orchestration/module.just'
mod processing 'processing/module.just'
mod dbt 'dbt/module.just'
mod frontend 'frontend/module.just'
mod tests-integration 'tests/integration/module.just'

default:
    @just --list

# Bring up every module in dependency order, or just one: `just start orchestration`.
start module="":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "{{module}}" ]; then
        just platform::start
        just metadata::start
        just query-engine::start
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
    just test

# Run tests: no arg runs everything; a known module name scopes to that
# module's unit tests; anything else is treated as a dbt tag (feed code),
# e.g. `just test customers` runs that feed's dbt schema tests.
test module="":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{module}}" in
        "")
            just processing::test
            just dbt::test
            just orchestration::test
            just tests-integration::test
            ;;
        processing|raw_to_clean) just processing::test ;;
        dagster|orchestration) just orchestration::test ;;
        integration) just tests-integration::test ;;
        *) just dbt::test "{{module}}" ;;
    esac
