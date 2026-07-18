# Image for the Streamlit CRUD frontend's in-cluster Deployment
# (frontend/k8s/) -- replaces the local `nohup streamlit run app.py`
# process (Roadmap.md "Kubernetes Hosting Model"). Built from the repo
# root (not this directory) so the workspace lockfile is in the build
# context -- see the `docker build -f frontend/Dockerfile .` invocation
# this expects.
#
# `uv sync --frozen --package frontend`, not `--all-packages` -- a scoped
# sync installs only frontend's own dependency set (streamlit/sqlalchemy/
# psycopg/pandas/confluent-kafka, plus the connectors workspace package
# used by 4_Streaming_Sources.py's schema-discovery action), a meaningfully
# smaller image than pulling in dagster/dbt/trino too. `frontend` is a
# real, valid `--package` target despite `[tool.uv] package = false` in
# its own pyproject.toml -- that flag means "don't build/install this as
# an importable wheel," not "this isn't a real workspace member uv can
# scope a sync to." `uv sync --package frontend` still resolves and
# installs frontend's own declared workspace dependencies (connectors),
# it just skips every OTHER workspace member frontend doesn't depend on.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
COPY . /app

RUN uv sync --frozen --package frontend

ENV PATH="/app/.venv/bin:$PATH"
WORKDIR /app/frontend

# --server.address 0.0.0.0, not the default -- a container's own loopback
# interface isn't reachable from the Service/NodePort in front of it.
CMD ["streamlit", "run", "app.py", "--server.headless", "true", "--server.port", "8501", "--server.address", "0.0.0.0"]
