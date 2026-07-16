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
# dir. No `dbt deps` step: dbt/_shared/ is not installed as a package
# dependency — scripts/generate_domain_projects.py physically copies its
# macros into every domain's own macros/ directory instead (confirmed live
# that package-based sharing breaks unpredictably after a project's first
# build in this dbt version — see that script's own module docstring), so
# there's no dbt_packages/ to install here. `dbt parse` doesn't need a live
# Trino connection, just a syntactically valid profiles.yml, so this is
# safe at build time.
RUN for d in dbt/domains/*/; do \
      dir="${d%/}"; \
      [ -f "$dir/dbt_project.yml" ] || continue; \
      (cd "$dir" && dbt parse --profiles-dir profiles) || exit 1; \
    done
