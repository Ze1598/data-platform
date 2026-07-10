# Live-platform regression suite (Trino/Postgres, not mocks). TRINO_*/
# POSTGRES_* env vars come from the root Justfile's `set dotenv-load` plus
# TRINO_HOST/TRINO_PORT defaults matching dbt's own profile.

test:
    TRINO_HOST="${TRINO_HOST:-localhost}" TRINO_PORT="${TRINO_PORT:-8080}" uv run pytest -v
