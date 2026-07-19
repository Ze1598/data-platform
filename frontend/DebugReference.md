# Debug Reference: frontend (Streamlit)

**Update 2026-07-17 — the frontend now runs fully in-cluster** (`frontend/k8s/`, a real `Deployment`+`Service`, NodePort-mapped to `localhost:8501` same as before — see Roadmap.md "Master pipeline orchestration"). The **"Run the app locally" entry below describes the OLD workflow (`nohup streamlit run app.py` on the host) and is no longer the standard operating mode** — kept here, not deleted, since a local run is still occasionally useful for fast iteration on frontend code without a full image rebuild. Annotated inline rather than removed.

Commands for running and verifying the Streamlit CRUD app. See [../metadata/DebugReference.md](../metadata/DebugReference.md) for checking the Postgres state this app reads/writes, and [../Learnings.md](../Learnings.md) for the `uv sync`/canvas-rendering gotchas referenced below.

---

## In-cluster (current)

### Rebuild + reload after a code change, and check pod health
**Scenario**: same "kind doesn't pull from a registry" reasoning as every other module's image — a code change needs an explicit rebuild+reload before the running pod sees it, and (unlike `orchestration`'s code-server) the frontend Deployment DOES need a rollout restart too, for the same reason: the image tag itself (`data-platform-frontend:latest`) doesn't change, so `kubectl apply` sees no spec diff and won't recreate the pod on its own.

**Update 2026-07-19**: this was documented here but `frontend/module.just`'s `start` recipe never actually included the `kubectl rollout restart` step until now — confirmed live, a real page fix sat rebuilt-and-loaded-but-unserved for a full session, `kubectl exec`-ing into the "redeployed" pod showed old file content still running. `just frontend::start` now genuinely does what this section always said it did (see `Learnings.md`, "`kubectl apply` on an unchanged Deployment spec never restarts a `:latest`-tagged pod").
```bash
just frontend::start   # rebuilds, reloads, applies manifests, and waits for rollout in one step
# or, if iterating without a full start:
docker build -f frontend/Dockerfile -t data-platform-frontend:latest .
kind load docker-image data-platform-frontend:latest --name data-platform
kubectl rollout restart deployment/frontend -n frontend
kubectl rollout status deployment/frontend -n frontend
kubectl logs -n frontend deployment/frontend --tail=100
```

### Reach it
```bash
curl -s -o /dev/null -w "http:%{http_code}\n" http://localhost:8501
open http://localhost:8501
```

---

## Local dev (superseded — the app no longer runs as a local process day to day)

### Run the app locally against the in-cluster Postgres
**Scenario**: after any frontend code change, or after Postgres moves/changes, confirm the app still actually works end-to-end, not just that it starts.
```bash
set -a && source .env && set +a
nohup uv run streamlit run frontend/app.py --server.headless true --server.port 8501 > /tmp/streamlit.log 2>&1 &
sleep 6
curl -s -o /dev/null -w "http:%{http_code}\n" http://localhost:8501
```
`set -a && source .env && set +a` exports every variable from `.env` into the shell (so the app picks up `POSTGRES_HOST` etc.) without needing a separate env-loading library. Check `ps aux | grep streamlit` afterward if something seems off — `uv sync --all-packages` (not plain `uv sync`) is required at least once for the workspace member's dependencies to actually be installed; otherwise the command can silently fall back to a stray global streamlit install (see Learnings.md). **If the in-cluster Deployment is also running, you'll have two Streamlit instances up at once** — harmless for this app specifically (it has no daemon/heartbeat concept the way Dagster does), but stop the local one (`pkill -f "streamlit run app.py"`) once done so `localhost:8501` unambiguously points at one instance.

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
