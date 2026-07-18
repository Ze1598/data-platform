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

### Dagster's authoring/observability fit — two gaps, not resolved by the master pipeline rebuild

Pipeline authoring is 100% code — no visual authoring surface for someone who isn't hand-writing Python asset files and dbt models. And `data_processing_runs` (this platform's own run-tracking) already duplicates Dagster's own run-history/observability layer, so Dagster's UI isn't earning much beyond what's already custom-built. Neither gap is closed by the master pipeline rebuild (`Roadmap.md`'s "Master pipeline orchestration") — that solved *sequencing*, not these two. Not scheduled; revisit deliberately if either becomes a real pain point, not reflexively.

### Roadmap phases not started

Not backlog items in the "found in passing" sense, but listed here for one-stop visibility — see `Roadmap.md` for the actual design content.

- **Phase 11 — Streaming ingestion** (deprioritized): barely designed, rough shape only (Kafka/Flink or Kafka Connect Iceberg sink).
- **Phase 12 — Front-end data visualization module** (deprioritized): two open decisions — connectivity mechanism (a plain `trino` client, not ADBC — no `adbc-driver-trino` package on PyPI, would need a separate Go binary installer, inconsistent with this project's all-`uv` tooling) and chart-config persistence (ephemeral per-session vs. saveable/reloadable).
