# Backlog

Things explicitly deferred, not forgotten. Unlike `Roadmap.md` (planned phases) or `Progress.md` (what's done), this is the catch-all for smaller items raised in passing — a want mentioned but not requested, a sharp edge found while building something else, a convention that only lives in a comment today. Nothing here is authorized for implementation; it's a list to point at, not a queue to work through unprompted.

---

### Frontend CRUD for `schedule`

No page exists for the `schedule` table — `1_Source_Systems.py`/`2_Data_Feeds.py`/`3_Lakehouse_Models.py` establish the per-table CRUD pattern (one file per table, numbered); a future `4_Schedules.py` would follow that same shape. `load_type` almost certainly doesn't need one (fixed 4-row lookup, seeded once via DDL, never edited).

### `batch_group`/`batch_feed_hierarchy` are metadata-only

Every `data_feed` has a batch (enforced not-null), but nothing in Dagster groups or orders execution by batch/hierarchy yet — it's pure data with no behavioral consumer. Whether this becomes real (e.g. a batch-scoped job/schedule, parallel-within-tier execution) depends on whether a real multi-feed batch ever gets configured; today every feed is still its own singleton batch.

### No automated check that a dbt model's `tags=[...]` actually matches its `owning_feed_id`

Resolved the *shared-source-of-truth* problem (`lakehouse_models.owning_feed_id`, a real required column — see `Learnings.md`, "A dbt model tagged with two feed tags..."), but there's still one manual step left: a human has to write the matching single tag in the model's own `.sql` file by hand, and nothing cross-checks it against the metadata column. If they drift, the failure mode is still a loud `Definitions`-construction crash (not silent data corruption), just not caught until `just smoketest` runs. A real fix would read the compiled dbt manifest's node tags and assert they match `owning_feed_id` per row — `generate_dagster_pipeline.py` can't do this itself (it necessarily runs *before* `dbt parse`, so the manifest it would read is stale), so this would need to be a separate post-parse check, e.g. a small script run right after the Docker image's `dbt parse` step, or a dbt-side test.

### `customers`/`sales` are still synthetic in-memory stub generators

Their `raw_*` assets now write real durable files (fixed this session), but the data itself is still generated fresh in-process each run, not pulled from any real source the way `financial_transactions` (CSV file-drop) and `police_crimes` (live API) are. Turning either into a real source isn't planned — noted only because it's the one remaining asymmetry between the four feeds.

### ADBC driver decision — paused, not resolved

Flagged during earlier research for the not-yet-built front-end data-viz module (Roadmap Phase 12): no `adbc-driver-trino` on PyPI, would need a separate Go-based `dbc install trino` binary installer, inconsistent with this project's all-`uv` tooling. Explicitly set aside by direct instruction — do not pick this back up without being asked; if Phase 12 resumes, the plain `trino` Python client is the fallback already identified.

### JSON-list metadata fields should be comma-separated text in the frontend, not raw JSON

Fields like `business_key_columns`/`tracked_columns`/`depends_on_feed_friendly_names` (and `schema_registry.column_definitions`) currently ask the user to type a raw JSON array into the CRUD forms. Reported friction from actually working through `Walkthrough_Metadata_Source_Feed.md` end-to-end (2026-07-14) — reword these fields as plain comma-separated text, parsed into a JSON list in the frontend before the insert/update call. Small, isolated frontend-only change; no schema change needed.

### Per-model transformation gating (only serving is gated at the model level today)

`lakehouse_models.pipeline_steps` (Phase 13's cherry-picking work, built 2026-07-14) only gates that model's **serving** step — `generate_serve_views.py` simply skips generating a model's `_latest`/`_historical` views when serving isn't selected. Transformation stays all-or-nothing **per feed**: a lakehouse_model's `dim_*`/`fct_*` dbt code still builds as part of its owning feed's single tag-scoped transformation dbt invocation, with no independent per-model Dagster asset to gate against. Fully closing this would mean building a dynamic per-model `--exclude` list inside each feed's transformation `dbt build` invocation — real, but materially bigger and more fragile than what the concrete cases in scope (extraction-only, stop-after-clean, skip-serving) actually needed. Deliberate scope decision made when the master pipeline was designed, not an oversight; noted here per that plan's own explicit flag.

### Roadmap phases not started

Not backlog items in the "found in passing" sense, but listed here for one-stop visibility — see `Roadmap.md` for the actual design content.

**Current priority (2026-07-14)**: platform solidity over new capability surface area. Phase 13 is now substantially built (connector library, master pipeline, cherry-picking — see `Roadmap.md`/`Progress.md`); Phase 14 and working through this backlog are next; 11 and 12 remain deliberately deprioritized.

- **Phase 13 — Master pipeline architecture (extraction/validation/transformation/serving as child pipelines)**: substantially built 2026-07-14 — connector library (extraction/validation split across Postgres/CSV/REST/JSON sources), the master pipeline as a real Dagster construct (`pipeline_init_<feed>`), and cherry-picking (`pipeline_steps` lookup table + live per-run feed gating + codegen-time model-serving gating) are all implemented and `just smoketest`-verified. Remaining open item: per-model transformation gating (see above, new backlog item), and the still-open dbt-boilerplate-automation piece (per-model dbt config generation) noted in `Roadmap.md`'s transformation child-pipeline section.
- **Phase 14 — Reconsider Dagster as the orchestrator**: next up. Phase 13's build answered the narrower "can Dagster model the master/child-pipeline contract" question (yes — see `Roadmap.md`'s 2026-07-14 update on this phase), but the two original gaps motivating this phase (code-only authoring, duplicated observability) are still open and unaddressed.
- **Phase 11 — Streaming ingestion** (deprioritized): barely designed, rough shape only (Kafka/Flink or Kafka Connect Iceberg sink).
- **Phase 12 — Front-end data visualization module** (deprioritized): two open decisions (ADBC vs. plain `trino` client — see above; ephemeral vs. saveable charts).
