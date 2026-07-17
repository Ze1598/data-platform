# Image for orchestration's Dagster webserver/daemon/code-server and the
# per-run Job pods K8sRunLauncher launches from it. Built from the repo
# root (not this directory) so the workspace lockfile and every domain's
# dbt project are both in the build context — see the `docker build -f
# orchestration/Dockerfile .` invocation this expects. Repo-root-relative
# layout is preserved inside the image so pipeline_generated.py's
# REPO_ROOT-relative glob over dbt/domains/*/ resolves identically to local
# dev.
#
# DOMAIN build arg (optional, unset by default): when unset, every
# scaffolded domain gets its manifest built here, producing the one shared
# `data-platform-orchestration` image every job uses by default (unchanged
# behavior). When set to one domain's name, only that domain's `dbt parse`
# runs — every other domain's directory is still present on disk (this
# COPY layer doesn't change) but has no target/manifest.json, so
# pipeline_generated.py's DBT_PROJECTS loop silently skips it (see that
# script's module docstring) rather than failing to import. This is what
# `data-platform-domain-<domain>` images (built via `just
# orchestration::build-domain-images`, tagged onto that domain's
# MODELING_JOBS/SERVING_JOBS via a dagster-k8s/config tag override) are —
# same image, one different build arg, most layers shared/cached across
# every domain's build.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
COPY . /app

RUN uv sync --frozen --package dagster-data-platform

ENV PATH="/app/.venv/bin:$PATH"

# DbtProject.prepare_if_dev() (pipeline_generated.py) only regenerates a
# manifest under `dagster dev` — everywhere else (this image's run pods)
# it's a no-op and expects each domain's own target/manifest.json to
# already exist. No `dbt deps` step: dbt/_shared/ is not installed as a
# package dependency — scripts/generate_domain_projects.py physically
# copies its macros into every domain's own macros/ directory instead
# (confirmed live that package-based sharing breaks unpredictably after a
# project's first build in this dbt version — see that script's own module
# docstring), so there's no dbt_packages/ to install here. `dbt parse`
# doesn't need a live Trino connection, just a syntactically valid
# profiles.yml, so this is safe at build time.
ARG DOMAIN=""
RUN for d in dbt/domains/*/; do \
      dir="${d%/}"; \
      name="$(basename "$dir")"; \
      [ -f "$dir/dbt_project.yml" ] || continue; \
      if [ -n "$DOMAIN" ] && [ "$name" != "$DOMAIN" ]; then continue; fi; \
      (cd "$dir" && dbt parse --profiles-dir profiles) || exit 1; \
    done
