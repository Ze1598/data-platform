# dbt schema/data tests against the live staging models. Needs Trino
# reachable (localhost:8080 by default, see
# domains/<domain>/profiles/profiles.yml) and the models already
# materialized -- these are "batch"/model tests, not pure-logic unit tests.
# Loops over every scaffolded domain project (see Roadmap.md "multi-project
# dbt split") -- domains are compile-isolated (separate manifests), so this
# is N separate `dbt test` invocations, not one shared-project invocation.

# Run dbt tests. With no tag, runs every test in every domain; with a tag
# (a feed code, e.g. `just test customers`), scopes to that feed's models
# only, in whichever domain(s) actually have that tag -- metadata-driven the
# same way the per-feed dbt asset factory and tests/integration are, no
# hardcoded feed/domain list to maintain here either. A domain with zero
# matching nodes for a given tag just logs dbt's own harmless "does not
# match any enabled nodes" warning and moves on -- confirmed non-fatal (see
# orchestration/module.just's verify-schedule comment for the same
# behavior observed elsewhere).
test tag="":
    #!/usr/bin/env bash
    set -euo pipefail
    for d in domains/*/; do
        dir="${d%/}"
        [ -f "$dir/dbt_project.yml" ] || continue
        echo "=== $(basename "$dir") ==="
        if [ -z "{{tag}}" ]; then
            (cd "$dir" && uv run dbt test --profiles-dir profiles)
        else
            (cd "$dir" && uv run dbt test --profiles-dir profiles --select "tag:{{tag}}")
        fi
    done
    # Cross-checks every lakehouse_models.owning_feed_id against the actual
    # tags=[...] on its compiled model/snapshot node -- a live-Postgres
    # check, so it can't be a dbt-side test (see scripts/check_model_feed_tags.py's
    # own docstring for why). Runs unconditionally, even for a scoped
    # `just test <tag>` -- it's one query plus a few manifest reads, cheap
    # enough that the signal is worth it regardless of which tag was asked for.
    cd {{justfile_directory()}} && uv run python scripts/check_model_feed_tags.py
