# Image for orchestration's Dagster webserver/daemon and the per-run Job
# pods K8sRunLauncher launches from it. Built from the repo root (not this
# directory) so the workspace lockfile and every domain's dbt project are
# both in the build context — see the `docker build -f
# orchestration/Dockerfile .` invocation this expects. Repo-root-relative
# layout is preserved inside the image so pipeline_generated.py's
# REPO_ROOT-relative glob over dbt/domains/*/ resolves identically to local
# dev.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
COPY . /app

RUN uv sync --frozen --package dagster-data-platform

ENV PATH="/app/.venv/bin:$PATH"

# DbtProject.prepare_if_dev() (pipeline_generated.py) only regenerates a
# manifest under `dagster dev` — everywhere else (this image's run pods)
# it's a no-op and expects each domain's own target/manifest.json to
# already exist. Loops over every scaffolded domain project (see
# Roadmap.md "multi-project dbt split") rather than one hardcoded project
# dir — `dbt deps` is a new required step here (was never needed for the
# single shared project, since it had no package dependencies of its own;
# every domain now installs dbt/_shared/ as a local-path package, and `dbt
# parse` does NOT auto-install packages, confirmed against the installed
# dbt-core source). Neither command needs a live Trino connection, just a
# syntactically valid profiles.yml, so this is safe at build time.
RUN for d in dbt/domains/*/; do \
      dir="${d%/}"; \
      [ -f "$dir/dbt_project.yml" ] || continue; \
      (cd "$dir" && dbt deps --profiles-dir profiles && dbt parse --profiles-dir profiles) || exit 1; \
    done
