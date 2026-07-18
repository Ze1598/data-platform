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

### Walkthrough doc: onboarding a new streaming source — not written

No walkthrough exists yet for the real, hands-on sequence of onboarding a new streaming source, unlike batch feeds (`Walkthrough_Metadata_Ingestion.md`). `streaming/testing/run.py`'s hand-maintained `_MESSAGE_FIXTURES`/discovery automation exists purely to test the platform, not to replace this step for a real user — a genuinely new source still needs a human to do the real onboarding by hand, and there's no document showing what that looks like. Needed: `Walkthrough_New_Streaming_Source.md`, one worked example (a single streaming object is enough) covering the actual user-facing sequence — create the `streaming_source` row via the frontend, get real messages flowing onto its Kafka topic, run "Discover schema," pick `event_timestamp_column`, then write the hand-authored serve-view join against an existing model-layer table (`scripts/generate_streaming_serve_scaffolds.py`'s scaffold is the starting point; the join itself is the user's business logic).

### Flink's built-in autoscaler (`autoscaler_enabled`) is wired but never exercised

`streaming_source.autoscaler_enabled` and its `job.autoscaler.enabled` line in the generated `FlinkDeployment` (`scripts/generate_streaming_ingestion.py`) exist end-to-end, but nobody has actually turned it on and watched it behave — it's flagged experimental in the Flink Kubernetes Operator's own current docs, same opt-in posture as `data_feed.processing_engine`'s Spark option. Untested, not scheduled.

### Streamlit: "Trigger a Dagster run" feature — not yet built

There's currently no UI path in the frontend to trigger a Dagster run at all — all triggering today happens via `just orchestration::verify-pipeline`/`verify-schedule`/`verify-sensor` or direct GraphQL/CLI calls, never through Streamlit. A real prerequisite for on-demand Dagster triggering (and for the cooperative wake-up mechanism below) to mean anything.

### Cooperative wake-up mechanism for Dagster from Streamlit

Once the trigger feature above exists: Streamlit's backend explicitly scales `dagster-webserver`/`dagster-code-server` to 1 replica (a real Kubernetes API call from Streamlit's own server-side code), polls for readiness, then submits the trigger — an application-aware "wake the thing I'm about to call" pattern, deliberately chosen to avoid needing KEDA's HTTP Add-on (see below) for this specific case, since Streamlit already knows exactly when it's about to need Dagster.

### Streamlit's own scale-to-zero — blocked on HTTP-triggered-scale-from-zero maturity

Scaling Streamlit itself to zero when idle needs something to wake it the instant a real browser request arrives — the same mechanism a manual Dagster trigger needs, but with no "cooperative" caller to lean on (a human opening a browser can't scale the pod up first). The two real options: KEDA's HTTP Add-on (confirmed live this session: still beta, not v1.0, undocumented WebSocket support — a real risk given Streamlit's live UI depends on WebSocket) or Knative Serving (CNCF Incubating, purpose-built for exactly this, but needs its own networking layer — Istio, Kourier, or Contour — a bigger architectural commitment). Revisit once either matures, not on a fixed timer.

### VPA-based dynamic (non-zero-floor) resourcing for Postgres/Streamlit

The alternative to scale-to-zero for foundational always-on services: never scale replicas to 0, but continuously right-size CPU/memory requests/limits based on real observed usage (Vertical Pod Autoscaler). Confirmed live this session: in-place Pod resource resizing (changing a running container's requests/limits without recreating the pod) is stable exactly as of Kubernetes v1.36, and this cluster already runs v1.36.1 — genuinely viable now, not a someday feature. But **memory resizing still defaults to requiring a container restart** (`resizePolicy: RestartContainer` is memory's default; only CPU resizes in-place by default), and memory is the dimension that's actually constrained this platform. Whether VPA's own controller has been updated to exploit the in-place primitive for memory, or still defaults to evict-and-recreate (a real disruption cost for a stateful Postgres), is unverified — a real spike to run before adopting this, not an assumption either way.

### Metadata-driven KEDA Cron windows, generated from `ingestion_triggers`

The first KEDA `ScaledObject` for `dagster-webserver`/`dagster-code-server` is deliberately hand-written for today's two real schedules (`police_crimes_schedule`, `fct_daily_financial_activity`), matching this project's own established pattern of proving a mechanism by hand before generalizing it. Once proven, generating the Cron trigger windows from `ingestion_triggers` (mirroring `scripts/generate_dagster_pipeline.py`'s own role for the schedules/sensors themselves) means a new schedule row doesn't also need a hand-edited `ScaledObject`.

### Postgres/Trino/Polaris's own demand-following scaling — deferred, higher risk

Postgres is foundational (Polaris's own catalog DB, and everything else's metadata store) and stateful — scaling it based on demand carries real cold-start-latency-for-everything and StatefulSet-scale-to-zero-maturity risk. Trino/Polaris's demand is also not a single clean trigger — it's the union of Dagster's dbt builds *and* any streaming source's serve-view reads. Deliberately not tackled alongside the `orchestration`-scoped first phase; needs its own dedicated design.

### Roadmap phases not started

Not backlog items in the "found in passing" sense, but listed here for one-stop visibility — see `Roadmap.md` for the actual design content.

- **Phase 12 — Front-end data visualization module** (deprioritized): two open decisions — connectivity mechanism (a plain `trino` client, not ADBC — no `adbc-driver-trino` package on PyPI, would need a separate Go binary installer, inconsistent with this project's all-`uv` tooling) and chart-config persistence (ephemeral per-session vs. saveable/reloadable).
