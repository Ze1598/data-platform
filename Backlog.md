# Backlog

Things explicitly deferred, not forgotten. Unlike `Roadmap.md` (planned phases) or `Progress.md` (what's done), this is the catch-all for smaller items raised in passing — a want mentioned but not requested, a sharp edge found while building something else, a convention that only lives in a comment today. Nothing here is authorized for implementation; it's a list to point at, not a queue to work through unprompted. Resolved items are deleted from this file once done, not kept as history — `Progress.md` is the permanent build record.

---

### `dagster-webserver`/`dagster-daemon` don't force a rollout restart on `orchestration::start` — only `dagster-code-server` does

Confirmed live (2026-07-19) while fixing the identical bug for `frontend`/`streaming/producer` (see `Learnings.md`): `orchestration/module.just`'s `start` recipe only runs `kubectl rollout restart deployment/dagster-code-server`, not `dagster-webserver`/`dagster-daemon` — both of which run from the exact same rebuilt image. The existing comment's reasoning is specifically about code-server reporting the asset graph's *structure* to the other two, which may genuinely be the only case that matters for correctness (the webserver/daemon might not need their own code reloaded for most changes) — but that's not verified, just the original reasoning as written. Worth confirming deliberately (does a webserver/daemon-only code change ever actually need a restart to take effect, or does everything meaningful route through code-server regardless) before either adding the same restart to both or documenting why it's genuinely unnecessary.

### Superseded design: single shared dbt project + `tag:<model_schema>` selectors, as a fallback if full domain isolation proves premature

Before landing on genuine per-domain dbt project isolation (separate `dbt_project.yml`/manifest/image per `model_schema` domain — see `README.md`'s Repo Structure `dbt/` line), a lighter-weight alternative was fully designed and explicitly rejected in favor of real isolation, not because it doesn't work: **one shared dbt project** (today's structure, unchanged), with each domain's `staging`/`model`/`serve` models tagged `tag:<model_schema>` — the exact same mechanism already splitting transformation from serving today — giving independently-triggerable `dbt build` invocations per domain, physical naming-convention differentiation (`<model_schema>_<fct|dim>_<name>`) instead of separate schemas, and Dagster `pool=` to avoid concurrency races between domains. Real, viable, much less infrastructure than full isolation.

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

### `schema_registry`'s type vocabulary is flat/scalar-only — jsonb/nested columns unmapped

A Postgres `jsonb` column or a nested JSON API response has nowhere obvious to map to in `schema_registry`'s current type vocabulary. Real options: flatten, stringify, or extend the vocabulary — an open design decision, not yet made.

### Dagster's authoring/observability fit — two gaps, not resolved by the master pipeline rebuild

Pipeline authoring is 100% code — no visual authoring surface for someone who isn't hand-writing Python asset files and dbt models. And `data_processing_runs` (this platform's own run-tracking) already duplicates Dagster's own run-history/observability layer, so Dagster's UI isn't earning much beyond what's already custom-built. Neither gap is closed by the master pipeline rebuild (`README.md`'s "Master Pipeline Architecture") — that solved *sequencing*, not these two. Not scheduled; revisit deliberately if either becomes a real pain point, not reflexively.

### Flink's built-in autoscaler (`autoscaler_enabled`) is wired but never exercised

`streaming_source.autoscaler_enabled` and its `job.autoscaler.enabled` line in the generated `FlinkDeployment` (`scripts/generate_streaming_ingestion.py`) exist end-to-end, but nobody has actually turned it on and watched it behave — it's flagged experimental in the Flink Kubernetes Operator's own current docs, same opt-in posture as `data_feed.processing_engine`'s Spark option. Untested, not scheduled.

### Metadata-driven KEDA Cron windows, generated from `ingestion_triggers`

The first KEDA `ScaledObject` for `dagster-webserver`/`dagster-code-server` is deliberately hand-written for today's two real schedules (`police_crimes_schedule`, `fct_daily_financial_activity`), matching this project's own established pattern of proving a mechanism by hand before generalizing it. Once proven, generating the Cron trigger windows from `ingestion_triggers` (mirroring `scripts/generate_dagster_pipeline.py`'s own role for the schedules/sensors themselves) means a new schedule row doesn't also need a hand-edited `ScaledObject`.

### Postgres/Trino/Polaris vertical scaling based on demand

**Corrected scope (2026-07-20)** — the original wording here talked about scale-to-zero risk; that was never the intent for these three. Postgres/Trino/Polaris are foundational, always-on services (Postgres is Polaris's own catalog DB and everything else's metadata store; Trino/Polaris serve live query traffic) — the goal is **vertical** scaling, right-sizing CPU/memory requests/limits to real observed demand, not scaling replicas toward zero. Streamlit is deliberately out of scope here — its scaling story is fully owned by the cooperative-wake item above, not a vertical-resourcing concern of its own.

Trino/Polaris's demand is not a single clean trigger — it's the union of Dagster's dbt builds *and* any streaming source's serve-view reads, so a naive single metric won't capture it. The likely mechanism is a Vertical Pod Autoscaler, the same category of tool already scoped for this — worth carrying forward one confirmed-live technical detail regardless of which service it's applied to: in-place Pod resource resizing (changing a running container's requests/limits without recreating the pod) is stable as of Kubernetes v1.36 (this cluster already runs v1.36.1), but **memory resizing still defaults to requiring a container restart** (`resizePolicy: RestartContainer` is memory's default; only CPU resizes in-place by default) — and memory is the dimension actually constrained on this platform. Whether VPA's own controller exploits the in-place primitive for memory, or still defaults to evict-and-recreate (a real disruption cost for stateful Postgres), is unverified. Not yet designed — needs its own dedicated plan, not tackled alongside the `orchestration`-scoped KEDA work already done.
