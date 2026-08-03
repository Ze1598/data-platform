# Backlog

Things explicitly deferred, not forgotten. Unlike `Roadmap.md` (planned phases) or `Progress.md` (what's done), this is the catch-all for smaller items raised in passing — a want mentioned but not requested, a sharp edge found while building something else, a convention that only lives in a comment today. Nothing here is authorized for implementation; it's a list to point at, not a queue to work through unprompted. Resolved items are deleted from this file once done, not kept as history — `Progress.md` is the permanent build record.

---

### QA agent's retroactive coverage audit — not yet run

Once the QA agent (`.claude/agents/qa.md`) is actually operational, its first real task should be auditing the 15 already-completed phases' current test coverage against the same qualitative bar new work is held to going forward (does the existing test suite cover each phase's stated functionality plus reasonable edge cases — see CLAUDE.md's "Multi-agent development workflow"). Not run as part of standing up the multi-agent workflow itself — queued here so it isn't forgotten. Scope is assess-and-report gaps per phase, not writing new Gherkin specs for already-shipped functionality unless a real gap is found worth closing.

---

### `lakehouse_model_columns`-driven dedicated staging generation doesn't integrate with `deletes_enabled`

`scripts/generate_model_scaffolds.py` (lines ~480-501) generates a dedicated `stg_<table_name>.sql` per model from `lakehouse_model_columns` rows (`is_business_key`/`is_tracked`/`data_type` casts) — this path is real and independently tested (`tests/integration/test_utc_consistency.py`). But when a model has both `lakehouse_model_columns` rows **and** `deletes_enabled=true`, the script skips dedicated-staging generation entirely and falls back to the original `business_key_columns`/`tracked_columns` flow, printing a warning at generation time rather than silently doing the wrong thing. Root cause: deletion synthesis (`generate_deletion_synthesis_views.py`'s `int_<table_name>_with_deletes`) still wraps the old per-feed `stg_<owning_feed>` model, not the new dedicated per-model staging table — the two codegen mechanisms were built independently and never integrated for this combination.

Not fixed here — deliberately deferred, not silently worked around. When this gets picked up: the deletion-synthesis intermediate needs to source from the dedicated staging table when one exists for that model, and this combination (`lakehouse_model_columns` + `deletes_enabled=true`) needs its own live-verified test case added to whatever test suite already covers dedicated-staging generation and deletion synthesis separately — neither currently exercises this combination together.

### Superseded design: single shared dbt project + `tag:<model_schema>` selectors, as a fallback if full domain isolation proves premature

Before landing on genuine per-domain dbt project isolation (separate `dbt_project.yml`/manifest/image per `model_schema` domain — see `README.md`'s Repo Structure `dbt/` line), a lighter-weight alternative was fully designed and explicitly rejected in favor of real isolation, not because it doesn't work: **one shared dbt project** (today's structure, unchanged), with each domain's `staging`/`model`/`serve` models tagged `tag:<model_schema>` — the exact same mechanism already splitting transformation from serving today — giving independently-triggerable `dbt build` invocations per domain, physical naming-convention differentiation (`<model_schema>_<fct|dim>_<name>`) instead of separate schemas, and Dagster `pool=` to avoid concurrency races between domains. Real, viable, much less infrastructure than full isolation.

**Rejected because**: it doesn't solve `dbt parse`'s full-project compile cost (paid by every domain's build regardless of `--select` scope) or deployment blast radius (one shared manifest/image means every domain's build/deploy is coupled). Given this platform is explicitly meant to validate feasibility at real enterprise scale, those were judged worth solving now rather than deferring.

Noted here in case genuine per-domain project isolation turns out to be more than is needed and this lighter mechanism becomes the better fit after all — a complete, ready-to-build fallback, not abandoned reasoning.

### `customers`/`sales` are still synthetic in-memory stub generators

Their `raw_*` assets write real durable files, but the data itself is still generated fresh in-process each run, not pulled from any real source the way `financial_transactions` (CSV file-drop) and `police_crimes` (live API) are. Turning either into a real source isn't planned — noted only because it's the one remaining asymmetry between the four feeds.

### Non-dbt staging engine (PyIceberg) for complex staging logic — investigated, not built

For staging business logic too complex or cumbersome to express in SQL. dbt Python models are not an available path (`dbt-trino` has no Python model execution backend — a day-one architectural decision, not a gap). The alternative would be a separate Dagster asset writing `staging` directly, structurally identical to how `raw_to_clean` already writes `clean`.

Verified feasible against the actual installed library (PyIceberg 0.11.1, read from source, not docs): `Table.upsert(df, join_cols=[...])` exists and does a predicate-pushed scan for matched rows only (confirmed via its own source comment, "so we don't have to load the entire target table") — it does *not* reintroduce the whole-table-read failure mode that caused this project's real OOM incident. Real limitation, also code-confirmed: its row-changed comparison (`upsert_util.get_rows_to_update`) is an unvectorized, one-row-at-a-time Python loop — a genuine throughput ceiling versus Trino's vectorized anti-join at the row counts this project has already exercised (millions of rows).

Would stay opt-in only if built — a deliberate throughput trade for feeds that genuinely need Python-expressible logic, not a peer-performance alternative to dbt. Staging → model would always stay dbt regardless of which engine built staging.

### Frontend page for defining model tables — first build rejected as not fit for purpose, needs a full rebuild

A first version of this page (`frontend/pages/7_Model_Table_Columns.py`, numbered `6_` at the time, see `Progress/2026-07-27-documentation-audit.md`'s page-renumbering entry) was built and live-verified, but rejected by the user as not fit for purpose. Exact requirements for the rebuild, verbatim, not paraphrased:

Rebuild the entire Model Table Columns page in the streamlit app. The page is aimed at both creating new tables for model tables defined in the lakehouse_models table, AND editing existing table. This page must allow the user to define the table structures and the following details for each column:
-Column name
-Column data Type (based on plain text data type options that need to be translated into corresponding valid data types in dbt: string, integer, decimal, boolean, timestamp)
-Column nullability (true means null, false means not null)
-Column is business key (true or false, this feeds into the calculation of the key hash)
-Column is tracked (true or false, this feeds into the calculation of the non-key hash, not applicable if Column is business key = true)

Upon saving the table definition, this is what generates the placeholder staging dbt script plus the table definition in the model schema, not saving the model entry in the Lakehouse models table. This is because this page only generates the placeholder script if the script does not yet exist, and likewise for the model table existence - it must not touch the code and/or table definition if they already exist because changing it risks losing existing business logic and/or corrupt model data. This calculation of code and table templates happens when the user clicks the save button only. By all means you are free to rethink the entire visual design of this page to avoid a crappy user experience.

Moreover, this page should start with a filter for the model_schema, followed by a second filter that allows selection of tables for the selected model_schema.

### `schema_registry`'s type vocabulary is flat/scalar-only — no unified policy for jsonb/nested columns

`TYPE_MAP` in `processing/raw_to_clean/raw_to_clean/schema_validation.py` is 5 scalar types only (`string`/`long`/`double`/`boolean`/`timestamp`) — no jsonb/struct/array type exists in the vocabulary itself. But the two concrete cases that hit this today already have working, in-production resolutions, applied ad hoc per source rather than as a chosen platform-wide policy: Postgres `jsonb` columns are stringified (`processing/connectors/connectors/postgres.py`'s `_POSTGRES_TYPE_MAP`), and `police_crimes`'s nested API response is flattened before registry mapping (`orchestration/dagster_data_platform/dagster_data_platform/connectors/police_crimes_connector.py`'s `flatten()`, via the `JsonConnector` base). Open question is no longer "what do we do about this" — both answers already exist in code — it's whether to formalize one/both as the standard pattern for future sources, or keep resolving it case-by-case.

### Dagster's authoring/observability fit — two gaps, not resolved by the master pipeline rebuild

Pipeline authoring is 100% code — no visual authoring surface for someone who isn't hand-writing Python asset files and dbt models. And `data_processing_runs` (this platform's own run-tracking) already duplicates Dagster's own run-history/observability layer, so Dagster's UI isn't earning much beyond what's already custom-built. Neither gap is closed by the master pipeline rebuild (`README.md`'s "Master Pipeline Architecture") — that solved *sequencing*, not these two. Not scheduled; revisit deliberately if either becomes a real pain point, not reflexively.

### Flink's built-in autoscaler (`autoscaler_enabled`) is wired but never exercised

`streaming_source.autoscaler_enabled` and its `job.autoscaler.enabled` line in the generated `FlinkDeployment` (`scripts/generate_streaming_ingestion.py`) exist end-to-end, but nobody has actually turned it on and watched it behave — it's flagged experimental in the Flink Kubernetes Operator's own current docs, same opt-in posture as `data_feed.processing_engine`'s Spark option. Untested, not scheduled.

### Postgres/Trino/Polaris vertical scaling based on demand

**Corrected scope (2026-07-20)** — the original wording here talked about scale-to-zero risk; that was never the intent for these three. Postgres/Trino/Polaris are foundational, always-on services (Postgres is Polaris's own catalog DB and everything else's metadata store; Trino/Polaris serve live query traffic) — the goal is **vertical** scaling, right-sizing CPU/memory requests/limits to real observed demand, not scaling replicas toward zero. Streamlit is deliberately out of scope here — it's a small, always-on Deployment with no scale-to-zero ambitions of its own, not a vertical-resourcing concern.

Trino/Polaris's demand is not a single clean trigger — it's the union of Dagster's dbt builds *and* any streaming source's serve-view reads, so a naive single metric won't capture it. The likely mechanism is a Vertical Pod Autoscaler, the same category of tool already scoped for this — worth carrying forward one confirmed-live technical detail regardless of which service it's applied to: in-place Pod resource resizing (changing a running container's requests/limits without recreating the pod) is stable as of Kubernetes v1.36 (this cluster already runs v1.36.1), but **memory resizing still defaults to requiring a container restart** (`resizePolicy: RestartContainer` is memory's default; only CPU resizes in-place by default) — and memory is the dimension actually constrained on this platform. Whether VPA's own controller exploits the in-place primitive for memory, or still defaults to evict-and-recreate (a real disruption cost for stateful Postgres), is unverified. Not yet designed — needs its own dedicated plan, not tackled alongside the `orchestration`-scoped KEDA work already done.
