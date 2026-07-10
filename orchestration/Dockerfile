# Image for orchestration's Dagster webserver/daemon and the per-run Job
# pods K8sRunLauncher launches from it. Built from the repo root (not this
# directory) so the workspace lockfile and the dbt project are both in the
# build context — see the `docker build -f orchestration/Dockerfile .`
# invocation this expects. Repo-root-relative layout is preserved inside
# the image so dbt_assets.py's REPO_ROOT-relative path to dbt/data_platform
# resolves identically to local dev.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
COPY . /app

RUN uv sync --frozen --package dagster-data-platform

ENV PATH="/app/.venv/bin:$PATH"

# DbtProject.prepare_if_dev() (dbt_assets.py) only regenerates the
# manifest under `dagster dev` — everywhere else (this image's run pods)
# it's a no-op and expects target/manifest.json to already exist. `dbt
# parse` doesn't need a live Trino connection, just a syntactically valid
# profiles.yml, so this is safe at build time.
RUN cd dbt/data_platform && dbt parse --profiles-dir profiles
