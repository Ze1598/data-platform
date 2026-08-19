# Backlog

Things explicitly deferred, not forgotten. Unlike `Roadmap.md` (planned phases) or `Progress/` (what's done), this is the catch-all for smaller items raised in passing — a want mentioned but not requested, a sharp edge found while building something else, a convention that only lives in a comment today. Nothing here is authorized for implementation; it's a list to point at, not a queue to work through unprompted. Resolved items are deleted from this file once done, not kept as history — `Progress/` is the permanent build record.

---

### QA agent's retroactive coverage audit — not yet run

Once the QA agent (`.claude/agents/qa.md`) is actually operational, its first real task should be auditing the 15 already-completed phases' current test coverage against the same qualitative bar new work is held to going forward (does the existing test suite cover each phase's stated functionality plus reasonable edge cases — see CLAUDE.md's "Multi-agent development workflow"). Not run as part of standing up the multi-agent workflow itself — queued here so it isn't forgotten. Scope is assess-and-report gaps per phase, not writing new Gherkin specs for already-shipped functionality unless a real gap is found worth closing.

---

### Superseded design: single shared dbt project + `tag:<model_schema>` selectors, as a fallback if full domain isolation proves premature

Before landing on genuine per-domain dbt project isolation (separate `dbt_project.yml`/manifest/image per `model_schema` domain — see `README.md`'s Repo Structure `dbt/` line), a lighter-weight alternative was fully designed and explicitly rejected in favor of real isolation, not because it doesn't work: **one shared dbt project** (today's structure, unchanged), with each domain's `staging`/`model`/`serve` models tagged `tag:<model_schema>` — the exact same mechanism already splitting transformation from serving today — giving independently-triggerable `dbt build` invocations per domain, physical naming-convention differentiation (`<model_schema>_<fct|dim>_<name>`) instead of separate schemas, and Dagster `pool=` to avoid concurrency races between domains. Real, viable, much less infrastructure than full isolation.

**Rejected because**: it doesn't solve `dbt parse`'s full-project compile cost (paid by every domain's build regardless of `--select` scope) or deployment blast radius (one shared manifest/image means every domain's build/deploy is coupled). Given this platform is explicitly meant to validate feasibility at real enterprise scale, those were judged worth solving now rather than deferring.

Noted here in case genuine per-domain project isolation turns out to be more than is needed and this lighter mechanism becomes the better fit after all — a complete, ready-to-build fallback, not abandoned reasoning.

### Non-dbt staging engine (PyIceberg) for complex staging logic — investigated, not built

For staging business logic too complex or cumbersome to express in SQL. dbt Python models are not an available path (`dbt-trino` has no Python model execution backend — a day-one architectural decision, not a gap). The alternative would be a separate Dagster asset writing `staging` directly, structurally identical to how `raw_to_clean` already writes `clean`.

Verified feasible against the actual installed library (PyIceberg 0.11.1, read from source, not docs): `Table.upsert(df, join_cols=[...])` exists and does a predicate-pushed scan for matched rows only (confirmed via its own source comment, "so we don't have to load the entire target table") — it does *not* reintroduce the whole-table-read failure mode that caused this project's real OOM incident. Real limitation, also code-confirmed: its row-changed comparison (`upsert_util.get_rows_to_update`) is an unvectorized, one-row-at-a-time Python loop — a genuine throughput ceiling versus Trino's vectorized anti-join at the row counts this project has already exercised (millions of rows).

Would stay opt-in only if built — a deliberate throughput trade for feeds that genuinely need Python-expressible logic, not a peer-performance alternative to dbt. Staging → model would always stay dbt regardless of which engine built staging.

### `schema_registry`'s type vocabulary is flat/scalar-only — no unified policy for jsonb/nested columns

`TYPE_MAP` in `processing/raw_to_clean/raw_to_clean/schema_validation.py` is 5 scalar types only (`string`/`long`/`double`/`boolean`/`timestamp`) — no jsonb/struct/array type exists in the vocabulary itself. But the two concrete cases that hit this today already have working, in-production resolutions, applied ad hoc per source rather than as a chosen platform-wide policy: Postgres `jsonb` columns are stringified (`processing/connectors/connectors/postgres.py`'s `_POSTGRES_TYPE_MAP`), and `police_crimes`'s nested API response is flattened before registry mapping (`orchestration/dagster_data_platform/dagster_data_platform/connectors/police_crimes_connector.py`'s `flatten()`, via the `JsonConnector` base). Open question is no longer "what do we do about this" — both answers already exist in code — it's whether to formalize one/both as the standard pattern for future sources, or keep resolving it case-by-case.

### Postgres/Trino/Polaris vertical scaling based on demand

**Corrected scope (2026-07-20)** — the original wording here talked about scale-to-zero risk; that was never the intent for these three. Postgres/Trino/Polaris are foundational, always-on services (Postgres is Polaris's own catalog DB and everything else's metadata store; Trino/Polaris serve live query traffic) — the goal is **vertical** scaling, right-sizing CPU/memory requests/limits to real observed demand, not scaling replicas toward zero. Streamlit is deliberately out of scope here — it's a small, always-on Deployment with no scale-to-zero ambitions of its own, not a vertical-resourcing concern.

Trino/Polaris's demand is not a single clean trigger — it's the union of Dagster's dbt builds *and* any streaming source's serve-view reads, so a naive single metric won't capture it. The likely mechanism is a Vertical Pod Autoscaler, the same category of tool already scoped for this — worth carrying forward one confirmed-live technical detail regardless of which service it's applied to: in-place Pod resource resizing (changing a running container's requests/limits without recreating the pod) is stable as of Kubernetes v1.36 (this cluster already runs v1.36.1), but **memory resizing still defaults to requiring a container restart** (`resizePolicy: RestartContainer` is memory's default; only CPU resizes in-place by default) — and memory is the dimension actually constrained on this platform. Whether VPA's own controller exploits the in-place primitive for memory, or still defaults to evict-and-recreate (a real disruption cost for stateful Postgres), is unverified. Not yet designed — needs its own dedicated plan, not tackled alongside the `orchestration`-scoped KEDA work already done.
