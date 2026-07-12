# Backlog

Things explicitly deferred, not forgotten. Unlike `Roadmap.md` (planned phases) or `Progress.md` (what's done), this is the catch-all for smaller items raised in passing — a want mentioned but not requested, a sharp edge found while building something else, a convention that only lives in a comment today. Nothing here is authorized for implementation; it's a list to point at, not a queue to work through unprompted.

---

### Frontend CRUD for `schedule`

No page exists for the `schedule` table — `1_Source_Systems.py`/`2_Data_Feeds.py`/`3_Lakehouse_Models.py` establish the per-table CRUD pattern (one file per table, numbered); a future `4_Schedules.py` would follow that same shape. `load_type` almost certainly doesn't need one (fixed 4-row lookup, seeded once via DDL, never edited).

### `batch_group`/`batch_feed_hierarchy` are metadata-only

Every `data_feed` has a batch (enforced not-null), but nothing in Dagster groups or orders execution by batch/hierarchy yet — it's pure data with no behavioral consumer. Whether this becomes real (e.g. a batch-scoped job/schedule, parallel-within-tier execution) depends on whether a real multi-feed batch ever gets configured; today every feed is still its own singleton batch.

### Multi-feed dbt-asset-ownership convention is a manual, undocumented-outside-comments rule

A dbt model spanning >1 feed in `depends_on_feeds` must be tagged with exactly *one* feed (the alphabetically-first `depends_on_feeds` member, by convention — see `fct_daily_financial_activity.sql`'s and `generate_serve_views.py`'s comments) or Dagster rejects it as a duplicate `AssetKey`. This is enforced by nothing except two comments agreeing with each other. Worth eventually either codifying the rule in `generate_dagster_pipeline.py`/`generate_serve_views.py` itself (derive the base model's required tag and fail loudly if it doesn't match), or adding a CI-style check — right now a second multi-feed model could reintroduce the exact bug documented in `Learnings.md` ("A dbt model tagged with two feed tags...").

### `customers`/`sales` are still synthetic in-memory stub generators

Their `raw_*` assets now write real durable files (fixed this session), but the data itself is still generated fresh in-process each run, not pulled from any real source the way `financial_transactions` (CSV file-drop) and `police_crimes` (live API) are. Turning either into a real source isn't planned — noted only because it's the one remaining asymmetry between the four feeds.

### ADBC driver decision — paused, not resolved

Flagged during earlier research for the not-yet-built front-end data-viz module (Roadmap Phase 12): no `adbc-driver-trino` on PyPI, would need a separate Go-based `dbc install trino` binary installer, inconsistent with this project's all-`uv` tooling. Explicitly set aside by direct instruction — do not pick this back up without being asked; if Phase 12 resumes, the plain `trino` Python client is the fallback already identified.

### Roadmap phases not started

Not backlog items in the "found in passing" sense, but listed here for one-stop visibility — see `Roadmap.md` for the actual design content:
- **Phase 11 — Streaming ingestion**: barely designed, rough shape only (Kafka/Flink or Kafka Connect Iceberg sink).
- **Phase 12 — Front-end data visualization module**: two open decisions (ADBC vs. plain `trino` client — see above; ephemeral vs. saveable charts).
