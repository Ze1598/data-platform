# data-platform

A metadata-driven lakehouse platform: Dagster orchestrates extraction, validation, and dbt/Trino transformations over Apache Iceberg tables, all running on Kubernetes. The goal is that onboarding a new source or table is mostly a matter of configuring metadata and writing business logic — not writing platform code.

See `Roadmap.md` for the full design history and phased build order, `Progress.md` for what's actually been built and verified, and `Backlog.md` for known gaps and deferred items. This README is the entry-level orientation; those files are the detailed record.

## Architecture

```mermaid
flowchart TB
    UI["Streamlit CRUD\n(frontend/)"] --> META[("Postgres metadata\nsource_system, data_feed,\nlakehouse_models, schedule")]
    META --> CG["Codegen scripts\n(scripts/)"]
    CG -->|"generates assets, jobs,\nschedules, model scaffolds"| DAG["Dagster\n(orchestration/)"]

    DAG --> L[landing]
    L --> RA[raw]
    RA --> C[clean]
    C --> S[staging]
    S --> MO[model]
    MO --> SE[serve]

    CONN["processing/connectors\n(extraction)"] -.-> L
    CONN -.-> RA
    R2C["processing/raw_to_clean\n(validation)"] -.-> C
    DBT["dbt/\n(staging, model, serve SQL)"] -.-> S
    DBT -.-> MO
    DBT -.-> SE

    S --> ICE[("Apache Iceberg tables\nvia Trino + Polaris catalog\n(query-engine/)")]
    MO --> ICE
    SE --> ICE

    K8S["Kubernetes (platform/)"] -. hosts .-> DAG
    K8S -. hosts .-> ICE
    K8S -. hosts .-> UI
```

Six storage layers, one chain per feed: **landing** (raw file drop) → **raw** (verbatim durable copy) → **clean** (schema-validated) → **staging** (cumulative, upserted by business key) → **model** (Kimball facts/dimensions, SCD1/SCD2) → **serve** (query-facing views). A feed's `pipeline_steps` metadata can narrow this to just extraction+validation, or skip serving, etc. — resolved live per run by a generated `pipeline_init_<feed>` asset, the actual entry point of every feed's pipeline.

## Top-level folders

| Folder | Purpose |
|---|---|
| `metadata/` | The platform's own config database — Postgres DDL/init scripts and its Kubernetes manifests. Every other module reads from this DB; nothing here depends on another module. |
| `scripts/` | Build-time codegen: reads metadata and generates Dagster assets/jobs/schedules, dbt model/snapshot scaffolds, dbt serve-layer views, and deletion-synthesis models. Also seeds the metadata DB. This is the platform's generation engine — rarely touched day-to-day. |
| `orchestration/` | The Dagster project — resources, hand-written assets for non-connector feeds, the dbt-assets integration, and per-feed connector subclasses for bespoke extraction logic (e.g. REST pagination/flattening). Consumes what `scripts/` generates. |
| `processing/connectors/` | The generic, reusable extraction connector framework — base classes and the standard connector kinds (Postgres/CSV/JSON file/REST) plus generic schema discovery. |
| `processing/raw_to_clean/` | Generic raw→clean validation logic (schema coercion against the metadata-tracked schema registry) — one shared module every feed's `clean` step uses. |
| `dbt/` | The dbt project: staging/model/serve SQL, shared macros (`row_hash`, `classify_changes`), and Type 2 snapshots. This is where most day-to-day modeling work happens. |
| `query-engine/` | Trino (compute) and Apache Polaris (Iceberg REST catalog) — config and Kubernetes manifests, no custom application code. |
| `frontend/` | Streamlit CRUD app for all metadata tables (source systems, feeds, lakehouse models, schedules). |
| `platform/` | Cluster-wide concerns not owned by any one module: the local kind cluster definition and Kubernetes namespaces. |
| `tests/` | Cross-module integration tests (as opposed to each module's own unit tests, which live inside that module). |

## Platform features: who's responsible

**User** = configures metadata (via the Streamlit CRUD app) and writes modeling/business logic (dbt SQL) within the platform's existing structure. **Developer** = changes the platform's own code — a new connector, new codegen behavior, new shared mechanics.

| Feature | Standard behavior (automatic) | User-generated | Developer-generated |
|---|---|---|---|
| Pipeline structure (jobs, schedules, asset graph) | Fully codegen'd from metadata — never hand-authored | — | Changing how `scripts/generate_dagster_pipeline.py` codegens |
| Source/feed/model/schedule configuration | — | Streamlit CRUD forms | — |
| Extraction (landing → raw) for standard source shapes | Generic connectors (Postgres/CSV/JSON file/REST) handle fetch + durable write | — | A new generic connector kind, or a bespoke per-feed subclass for pagination/flattening |
| Schema discovery & drift tracking | Fully automatic — inferred and versioned into `schema_registry` on every extraction | — | Changing discovery/diffing logic itself |
| Validation (raw → clean) | Generic, schema-registry-driven — no per-feed code | — | Changing shared validation/coercion logic |
| Staging modeling (clean → staging) | Shared merge mechanics (`row_hash`, `classify_changes`) | The `stg_<feed>.sql` business-logic SELECT (columns, casts, joins) | — |
| Model-layer boilerplate (config, hashes, merge wrapper) | Scaffolded automatically for any new `lakehouse_models` row (`generate_model_scaffolds.py`) | — | Changing what the scaffold generates |
| Model-layer business logic (dimension/fact SELECTs) | — | The hand-filled `base` CTE in a scaffolded model/snapshot file | — |
| SCD Type 1 / Type 2 merge mechanics | Shared macros + dbt snapshot machinery, reused by every model | — | Changing the shared mechanics themselves |
| Deletion synthesis | Fully generated per `deletes_enabled` flag — no user code | — | Changing the generation logic |
| Serve layer — standard views (`_latest`/`_historical`) | Fully generated from `lakehouse_models`, zero code | — | Changing the view-generation template |
| Serve layer — custom views | — | Hand-authored views outside the generated subfolder | — |
| Cherry-picking (which pipeline steps run) | Resolved live from metadata, no code either way | Set via `pipeline_steps` on a feed or model | — |
| Run tracking / observability (`data_processing_runs`) | Fully automatic, written by every stage | — | Changing what's tracked or how |
