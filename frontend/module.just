# Streamlit CRUD app. POSTGRES_* env vars come from the root Justfile's
# `set dotenv-load`. See DebugReference.md for the manual-command
# equivalents this wraps.

start:
    #!/usr/bin/env bash
    set -euo pipefail
    nohup uv run streamlit run app.py --server.headless true --server.port 8501 \
        > /tmp/data-platform-streamlit.log 2>&1 &
    disown
    echo "Waiting for the app to come up on localhost:8501..."
    until curl -s -o /dev/null http://localhost:8501; do sleep 2; done
    echo "streamlit up (logs: /tmp/data-platform-streamlit.log)"

kill:
    pkill -f "streamlit run app.py" || echo "streamlit not running"

# Tests against the live Postgres (not mocks) -- needs the cluster up.
test:
    uv run pytest
