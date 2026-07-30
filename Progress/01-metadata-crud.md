# Phase 1 — Metadata + CRUD

- [x] Postgres running via docker-compose (no k8s yet)
- [x] `platform_metadata` schema created: `source_system`, `data_feed`, `schema_registry`, `model_feed`, `model_feed_source`, `run_audit_log`
- [x] Streamlit CRUD app scaffolded (`frontend/`, uv workspace member)
- [x] CRUD pages: `source_system`, `data_feed`, `model_feed`
- [x] **Verify**: create/edit rows for each entity via the app — browser-driven (Playwright) create/edit/delete flow run against all three pages, cross-checked directly against Postgres (not just UI text) for `source_system` → `data_feed` → `model_feed`, including FK correctness, default values (`scd_type=2`, `surrogate_key_column=_scd_id`), and FK-protected delete (deleting a referenced `source_system` correctly fails)

Notes / deviations:
- Delete is a real `DELETE` (not a soft-delete via `is_active`), relying on Postgres FK constraints (default `RESTRICT`) to block deleting a `source_system`/`data_feed` that still has dependent rows. The UI surfaces the resulting DB error rather than preventing the action pre-emptively.
- `model_feed.deletions_enabled=true` is validated in the UI at save time against the linked `data_feed.extraction_type == 'full'`, per the Roadmap's "Model Layer: SCD Design" section — not a DB constraint.
- Bugs found and fixed during implementation (none were roadmap/design issues, all straightforward coding bugs): (1) `try/except/elif` is invalid Python syntax — restructured as nested `if` inside `else`; (2) `uv sync` at the repo root doesn't pull in workspace member dependencies by default — must use `uv sync --all-packages`; (3) SQLAlchemy `text()` mishandles a `::jsonb` cast stuck directly to a named bind parameter — switched to `cast(:param as jsonb)`; (4) `st.dataframe` renders via canvas (glide-data-grid), so UUID columns returned as `uuid.UUID` objects serialize as unreadable byte-dicts — `fetch_table` now stringifies UUID columns before display.
