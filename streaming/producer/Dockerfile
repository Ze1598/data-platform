# Synthetic sales-event producer, streaming/ module. Built from the repo
# root (not this directory) so the workspace lockfile is in the build
# context -- see the `docker build -f streaming/producer/Dockerfile .`
# invocation this expects, same pattern as frontend/Dockerfile.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
COPY . /app

RUN uv sync --frozen --package streaming-producer

ENV PATH="/app/.venv/bin:$PATH"
WORKDIR /app/streaming/producer

CMD ["python", "-m", "producer.main"]
