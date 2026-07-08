# Debug Reference: frontend (Streamlit)

Commands for running and verifying the Streamlit CRUD app. See [../metadata/DebugReference.md](../metadata/DebugReference.md) for checking the Postgres state this app reads/writes, and [../Learnings.md](../Learnings.md) for the `uv sync`/canvas-rendering gotchas referenced below.

---

### Run the app locally against the in-cluster Postgres
**Scenario**: after any frontend code change, or after Postgres moves/changes, confirm the app still actually works end-to-end, not just that it starts.
```bash
set -a && source .env && set +a
nohup uv run streamlit run frontend/app.py --server.headless true --server.port 8501 > /tmp/streamlit.log 2>&1 &
sleep 6
curl -s -o /dev/null -w "http:%{http_code}\n" http://localhost:8501
```
`set -a && source .env && set +a` exports every variable from `.env` into the shell (so the app picks up `POSTGRES_HOST` etc.) without needing a separate env-loading library. Check `ps aux | grep streamlit` afterward if something seems off — `uv sync --all-packages` (not plain `uv sync`) is required at least once for the workspace member's dependencies to actually be installed; otherwise the command can silently fall back to a stray global streamlit install (see Learnings.md).

### Drive it with a headless browser (not just curl)
**Scenario**: proving an actual UI flow works (create/edit/delete through real form interactions), not just that the server responds to HTTP.
```bash
uv run --with playwright python3 -c "
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    b = p.chromium.launch()
    page = b.new_page(viewport={'width':1400,'height':1000})
    page.goto('http://localhost:8501', wait_until='networkidle')
    page.screenshot(path='home.png')
    b.close()
"
```
`uv run --with playwright` pulls in the `playwright` package for a one-off script without adding it to any `pyproject.toml` — good for throwaway verification tooling. First use on a machine needs `uv run --with playwright playwright install chromium` once to fetch the actual browser binary.

**Important gotcha**: `st.dataframe`/`st.data_editor` render via `<canvas>`, so `page.inner_text()` cannot see table contents — assert against the database directly (see [../metadata/DebugReference.md](../metadata/DebugReference.md)) after driving the UI, not by scraping the rendered grid.
