# Isolated streaming test runner, streaming/ module. Built from the repo
# root (not this directory) so the workspace lockfile is in the build
# context -- see streaming/producer/Dockerfile's identical pattern, which
# this mirrors exactly.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
COPY . /app

RUN uv sync --frozen --package streaming-testing

ENV PATH="/app/.venv/bin:$PATH"
WORKDIR /app/streaming/testing
