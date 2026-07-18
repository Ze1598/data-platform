# Backlog

Things explicitly deferred, not forgotten. Unlike `Roadmap.md` (planned phases) or `Progress.md` (what's done), this is the catch-all for smaller items raised in passing — a want mentioned but not requested, a sharp edge found while building something else, a convention that only lives in a comment today. Nothing here is authorized for implementation; it's a list to point at, not a queue to work through unprompted. Resolved items are deleted from this file once done, not kept as history — `Progress.md` is the permanent build record.

---

### Superseded design: single shared dbt project + `tag:<model_schema>` selectors, as a fallback if full domain isolation proves premature

Before landing on genuine per-domain dbt project isolation (separate `dbt_project.yml`/manifest/image per `model_schema` domain — see `Roadmap.md`), a lighter-weight alternative was fully designed and explicitly rejected in favor of real isolation, not because it doesn't work: **one shared dbt project** (today's structure, unchanged), with each domain's `staging`/`model`/`serve` models tagged `tag:<model_schema>` — the exact same mechanism already splitting transformation from serving today — giving independently-triggerable `dbt build` invocations per domain, physical naming-convention differentiation (`<model_schema>_<fct|dim>_<name>`) instead of separate schemas, and Dagster `pool=` to avoid concurrency races between domains. Real, viable, much less infrastructure than full isolation.

**Rejected because**: it doesn't solve `dbt parse`'s full-project compile cost (paid by every domain's build regardless of `--select` scope) or deployment blast radius (one shared manifest/image means every domain's build/deploy is coupled). Given this platform is explicitly meant to validate feasibility at real enterprise scale, those were judged worth solving now rather than deferring.

Noted here in case genuine per-domain project isolation turns out to be more than is needed and this lighter mechanism becomes the better fit after all — a complete, ready-to-build fallback, not abandoned reasoning.

### `batch_group`/`batch_feed_hierarchy` are metadata-only

Every `data_feed` has a batch (enforced not-null), but nothing in Dagster groups or orders execution by batch/hierarchy yet — it's pure data with no behavioral consumer. Whether this becomes real (e.g. a batch-scoped job/schedule, parallel-within-tier execution) depends on whether a real multi-feed batch ever gets configured; today every feed is still its own singleton batch.

### `customers`/`sales` are still synthetic in-memory stub generators

Their `raw_*` assets write real durable files, but the data itself is still generated fresh in-process each run, not pulled from any real source the way `financial_transactions` (CSV file-drop) and `police_crimes` (live API) are. Turning either into a real source isn't planned — noted only because it's the one remaining asymmetry between the four feeds.

### Non-dbt staging engine (PyIceberg) for complex staging logic — investigated, not built

For staging business logic too complex or cumbersome to express in SQL. dbt Python models are not an available path (`dbt-trino` has no Python model execution backend — a day-one architectural decision, not a gap). The alternative would be a separate Dagster asset writing `staging` directly, structurally identical to how `raw_to_clean` already writes `clean`.

Verified feasible against the actual installed library (PyIceberg 0.11.1, read from source, not docs): `Table.upsert(df, join_cols=[...])` exists and does a predicate-pushed scan for matched rows only (confirmed via its own source comment, "so we don't have to load the entire target table") — it does *not* reintroduce the whole-table-read failure mode that caused this project's real OOM incident. Real limitation, also code-confirmed: its row-changed comparison (`upsert_util.get_rows_to_update`) is an unvectorized, one-row-at-a-time Python loop — a genuine throughput ceiling versus Trino's vectorized anti-join at the row counts this project has already exercised (millions of rows).

Would stay opt-in only if built — a deliberate throughput trade for feeds that genuinely need Python-expressible logic, not a peer-performance alternative to dbt. Staging → model would always stay dbt regardless of which engine built staging.

### Frontend page for defining model tables — proposed, not built

A metadata-driven, codegen-to-dbt UX alternative to hand-writing SQL directly: define columns, types, constraints, and partitions for a model table through the CRUD frontend instead of authoring the `.sql` file by hand. Would be an optional on-ramp, not a requirement — users could still author dbt/Python by hand regardless.

### Catalog-based schema discovery for Postgres — not built

Schema discovery (`infer_column_definitions()`) is sample-based for every connector kind today, including Postgres — true catalog-based discovery (`information_schema`/`pg_catalog`) for a single-table Postgres source is scoped but not built.

### `schema_registry`'s type vocabulary is flat/scalar-only — jsonb/nested columns unmapped

A Postgres `jsonb` column or a nested JSON API response has nowhere obvious to map to in `schema_registry`'s current type vocabulary. Real options: flatten, stringify, or extend the vocabulary — an open design decision, not yet made.

### Dagster's authoring/observability fit — two gaps, not resolved by the master pipeline rebuild

Pipeline authoring is 100% code — no visual authoring surface for someone who isn't hand-writing Python asset files and dbt models. And `data_processing_runs` (this platform's own run-tracking) already duplicates Dagster's own run-history/observability layer, so Dagster's UI isn't earning much beyond what's already custom-built. Neither gap is closed by the master pipeline rebuild (`Roadmap.md`'s "Master pipeline orchestration") — that solved *sequencing*, not these two. Not scheduled; revisit deliberately if either becomes a real pain point, not reflexively.

### Generalizing streaming source onboarding beyond the first hand-built slice

`streaming/` (Roadmap Phase 11) is deliberately a first, bounded slice: one hardcoded Kafka topic, one hardcoded Flink SQL script, one hand-authored serve view — static config, not metadata-driven the way a batch feed is. A second real streaming source would currently mean hand-writing all of that again. Whether this becomes a real metadata-driven onboarding system (a new `source_system`/topic registration path, codegen'd Flink SQL scripts and `FlinkDeployment` CRs the way `generate_dagster_pipeline.py` does for batch jobs) depends on whether a second real stream ever actually shows up — deliberately not built ahead of that need, per the same reasoning that kept every other codegen script in this project metadata-driven only once a second real consumer existed.

### Streaming table fold-in to `staging`/`model` — not built

`streaming.sales_events` stays a serve-only real-time view for now (joined directly into `model.sales_dim_branch` via a hand-authored dbt view) — it never flows through `staging`'s hash-gated SCD merge logic the way batch feeds do. Folding it in would mean reintroducing a "run" concept (a periodic micro-batch merge) for something that's supposed to be continuous — a real, harder design problem, deliberately deferred rather than solved as part of the first pass. See `Roadmap.md`'s Phase 11 entry.

### Roadmap phases not started

Not backlog items in the "found in passing" sense, but listed here for one-stop visibility — see `Roadmap.md` for the actual design content.

- **Phase 12 — Front-end data visualization module** (deprioritized): two open decisions — connectivity mechanism (a plain `trino` client, not ADBC — no `adbc-driver-trino` package on PyPI, would need a separate Go binary installer, inconsistent with this project's all-`uv` tooling) and chart-config persistence (ephemeral per-session vs. saveable/reloadable).
