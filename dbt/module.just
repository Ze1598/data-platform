# dbt schema/data tests against the live staging models. Needs Trino
# reachable (localhost:8080 by default, see data_platform/profiles/profiles.yml)
# and the models already materialized -- these are "batch"/model tests, not
# pure-logic unit tests.

# Run dbt tests. With no tag, runs every test; with a tag (a feed code,
# e.g. `just test customers`), scopes to that feed's models only --
# metadata-driven the same way the per-feed dbt asset factory and
# tests/integration are, no hardcoded feed list to maintain here either.
test tag="":
    #!/usr/bin/env bash
    set -euo pipefail
    cd data_platform
    if [ -z "{{tag}}" ]; then
        uv run dbt test --profiles-dir profiles
    else
        uv run dbt test --profiles-dir profiles --select "tag:{{tag}}"
    fi
