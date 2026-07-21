# Data Platform: Roadmap (phase status)

Architecture and design live in [README.md](README.md) — that's the permanent reference. This file tracks phase status and what's not yet built; it and `Progress.md`/`Backlog.md`/`Learnings.md` are working documents for this project's build-out, not meant to outlive it.

## Current priority (as of 2026-07-19)

Phases 1–11 and 13–15 are all done (see `Progress.md`). **Phase 12 remains deliberately deprioritized** — explicit decision to prioritize platform solidity over adding new capability surface area (data viz). Nothing has a "next" designation right now — see `Backlog.md` for genuinely open, unscheduled items to pull from.

## Phased Build Order (each phase independently runnable/testable)

Design detail for each phase lives in [README.md](README.md) and [metadata/DataModel.md](metadata/DataModel.md); full history (dates, bugs found and fixed, live-verification detail) lives in `Progress.md`, one section per phase. This list is status + a pointer, not a duplicate of either.

1. **Metadata + CRUD** — done. `Progress.md` Phase 1.
2. **kind cluster + local storage** — done. `Progress.md` Phase 2.
3. **Apache Polaris + Trino, manual MERGE proof** — done. `Progress.md` Phase 3.
4. **dbt clean→staging** — done. `Progress.md` Phase 4.
5. **Dagster wiring (stubbed extraction)** — done. `Progress.md` Phase 5.
6. **Real raw→clean** — done, scope changed from the original plan: Polars is the default processing engine (runs inline in the Dagster op, no separate CR/cluster), Spark is opt-in per feed (`data_feed.processing_engine`) once actually needed for volume — no standing `spark-operator` deployment. `Progress.md` Phase 6.
7. **Model layer: Type 1/Type 2 dims + facts** — done. `Progress.md` Phase 7.
8. **Serve layer** — done. `Progress.md` Phase 8.
9. **End-to-end hardening** — done. `Progress.md` Phase 9.
10. **Metadata data model review** — done; resulted in the current metadata schema (every dead column wired up or dropped, `data_feed_run`/`data_model_run` merged into `data_processing_runs`). `Progress.md` Phase 10.
11. **Real-time / streaming ingestion** — built, including metadata-driven onboarding (`streaming_source`, polymorphic `schema_registry`, codegen for ingestion + serve scaffolds) matching a batch feed's shape; two sources (`sales_events`, `inventory_events`) proven running concurrently. `Progress.md`'s Phase 11 and "Phase 11 (continued)" sections.
12. **Front-end data visualization module** — **deprioritized (2026-07-14)**, not built. Users would connect to a serve-layer view/table, pick a chart type/axes/legend, and the app renders via Plotly Express. Open decisions, not yet made: connectivity (ADBC vs. the plain `trino` client — the Trino ADBC driver isn't pip-installable, needs a separate binary install step, in tension with this project's all-`uv` tooling) and chart-config persistence (ephemeral per-session vs. saveable, which would need a new metadata table).
13. **Master pipeline architecture (child pipelines, cherry-picking, ODS layer)** — done; design in README.md's "Master Pipeline Architecture". `Progress.md` Phase 13 (parts 1–4).
14. **Master pipeline orchestration (`master_pipeline` itself)** — done; design in README.md's "Master Pipeline Architecture". `Progress.md` Phase 14.
15. **Multi-project dbt split — per-domain compile isolation** — done; why in README.md's Repo Structure `dbt/` line. `Progress.md` Phase 15.

**Azure portability** is a deliberate design intention behind this project (see README.md's "Storage" bullet) — real ADLS Gen2 isn't a phase on this build order, and isn't something to implement here. Noted for context only.
