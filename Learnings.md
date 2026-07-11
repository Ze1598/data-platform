# Lessons Learned

Obscure, hard-won problems hit while building this platform, and how they were actually resolved — the stuff that isn't obvious from reading the docs or the manifests. Organized by system/component, not build order. Each entry: what broke (symptom/error), why, how it was fixed, and any residual caveat worth knowing.

If you landed here from a search engine: welcome. Every entry below was reproduced and fixed for real against a live system, not guessed from a forum post.

---

## Apache Polaris (Iceberg REST catalog)

### `FILE` storage type is effectively unusable on Polaris ≥1.6.0

**Symptom**: `IllegalStateException: Severe production readiness issues detected, startup aborted!` at Polaris startup, or — once past that — every catalog operation against a `FILE`-type catalog fails with `File IO implementation ... is considered insecure and must not be used`, even after explicitly allow-listing `FILE` storage.

**What was tried**: `SUPPORTED_CATALOG_STORAGE_TYPES` needs **JSON array syntax** in the env var, not a comma-separated string (`POLARIS_FEATURES__SUPPORTED_CATALOG_STORAGE_TYPES_='["S3","GCS","AZURE","FILE"]'` — a plain comma-separated value throws a Jackson `JsonParseException`). Even with that fixed, a separate request-time check (`ALLOW_INSECURE_STORAGE_TYPES`) still rejects every operation. The production-readiness abort can be bypassed with `POLARIS_READINESS_IGNORE_SEVERE_ISSUES=true`, but that disables *all* severe checks, not just this one. Confirmed via `kubectl exec ... printenv` that every relevant env var was actually reaching the pod correctly, and cross-referenced the exact `apache-polaris-1.6.0` tagged source (not `main`) — `ALLOW_INSECURE_STORAGE_TYPES` still silently failed to apply for reasons never fully identified.

**Resolution**: abandoned `FILE` storage entirely, pivoted to MinIO (S3-compatible) — Polaris's non-defense-gated path. If you're evaluating Polaris for a local/dev setup and considering `FILE` storage to avoid standing up an object store: don't, on any Polaris version in the 1.6.x line at least. Budget for MinIO (or real S3/ADLS) from the start.

### Quarkus/SmallRye env-var naming for quoted config map keys

**Symptom**: a Polaris feature-flag env var (e.g. `polaris.features."SOME_KEY"`) has no effect no matter what value is set.

**Cause**: a quoted map key (needed because the key itself contains dots/underscores) maps to an env var with a **doubled underscore** where the dot-then-quote or quote-then-dot collide: `polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"` → `POLARIS_FEATURES__SUPPORTED_CATALOG_STORAGE_TYPES_` (note both the double underscore after `FEATURES` and the trailing underscore). Easy to get wrong by pattern-matching a simpler, unquoted property name.

**Resolution**: verified against multiple real properties (`SUPPORTED_CATALOG_STORAGE_TYPES`, `ALLOW_INSECURE_STORAGE_TYPES`, `DROP_WITH_PURGE_ENABLED`) — the doubled-underscore pattern held consistently. If a Polaris (or any Quarkus/SmallRye-config-based service's) env var seems to have zero effect, check whether the underlying property is a quoted map key first.

### Polaris schema bootstrap is a separate, mandatory step from realm bootstrap

**Symptom**: `relation "polaris_schema.entities" does not exist` on the very first request to an otherwise cleanly-started Polaris server.

**Cause**: setting `POLARIS_BOOTSTRAP_CREDENTIALS` on the main server container self-bootstraps the *realm and root credential record* — but does **not** create the underlying database schema tables. The server starts up looking healthy either way.

**Resolution**: run `apache/polaris-admin-tool:latest bootstrap -r <realm> -c <realm>,<clientId>,<clientSecret>` as a one-off job (a Kubernetes `Job` works well) before the main server serves any real request. No error at server startup hints this step is missing — it only surfaces on the first actual query.

### Polaris catalog `storageConfigInfo` can only be set at creation time

**Symptom**: `PUT /api/management/v1/catalogs/<name>` to change `pathStyleAccess`/`stsUnavailable`/`roleArn` returns `200 OK`, but a follow-up `GET` shows the old values unchanged.

**Cause**: confirmed via the official `polaris` CLI's own `catalogs update --help` — the `update` command doesn't even expose `--path-style-access`/`--role-arn`/`--no-sts` (only `--region`, under AWS S3 options). This isn't a client bug on your end — there's no supported way to update `storageConfigInfo` on an existing catalog, CLI or raw REST.

**Resolution**: if a catalog's storage config needs to change, **delete and recreate it**. (A catalog with existing namespaces/tables can't be deleted until they're dropped first.) Distinct from the catalog `properties` map below, which genuinely *can* be updated post-creation.

### Polaris `S3` storage type against MinIO (or any non-AWS S3-compatible store)

**Symptom, in order encountered**:
1. `StsException: The security token included in the request is invalid` on catalog operations.
2. `Forbidden: Principal 'root' ... not authorized for op CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION`.
3. After fixing #2: `IllegalArgumentException: Credential vending was requested for table ..., but no credentials are available`.
4. `301 The bucket you are attempting to access must be addressed using the specified endpoint` — the actual blocker, and the one that cost the most time.

**Causes and fixes, one per symptom**:
1. MinIO has no real IAM/STS. Set `roleArn` in `storageConfigInfo` to any syntactically valid dummy ARN (`arn:aws:iam::000000000000:role/minio-polaris-role`), and set `stsUnavailable: true` — without it, Polaris attempts a real `AssumeRole` call against the dummy ARN and fails.
2. Looked like a deliberate CVE-hardening lockdown at first; it's actually a missing RBAC grant. See the dedicated RBAC entry below.
3. Once `vended-credentials-enabled=true` is re-enabled after the RBAC fix, it still fails for an unrelated reason: `stsUnavailable: true` means Polaris genuinely has no real temporary credentials to vend — there's nothing to hand out. Keep `vended-credentials-enabled=false` on the Trino (or PyIceberg) side and give the client static credentials directly. This is correct, permanent config for an STS-less backend like MinIO — not a workaround pending a future fix.
4. **The real blocker**: Polaris's own **server-side** `S3FileIO` client (used to validate/finalize table commits, independent of anything the query engine does) wasn't picking up the catalog's `s3.endpoint` property at all, and defaulted to real AWS S3. Confirmed via live MinIO traffic capture (`mc admin trace -v` from a throwaway in-cluster pod) that the failing request never reached MinIO — ruling out a MinIO-side or Trino-side cause definitively rather than guessing. **Fix**: set `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_ENDPOINT_URL_S3` (AWS SDK Java v2's env var for a custom S3 endpoint, supported since SDK 2.28.1) **directly on the Polaris deployment itself** — a completely separate credential/endpoint path from whatever the query engine's own catalog properties say.

**Dead end worth flagging**: a suspected Trino bug (`trinodb/trino#25187`, path-style-access not honored with Iceberg REST + S3) looked plausible for a while and led to switching Trino to the legacy Hadoop-S3A filesystem config. Confirmed a red herring later — switching back to the modern `fs.native-s3` + `s3.*` properties worked fine once the real (Polaris-side) fix above was in place.

### RBAC `Forbidden` errors are a missing grant, not a feature lockdown

**Symptom**: `Forbidden: Principal 'root' ... not authorized for op DROP_TABLE_WITH_PURGE` (or `CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION`) even for the root/`service_admin` principal.

**Cause**: easy to assume this is a deliberate CVE-hardening lockdown given the principal is already root — it isn't. Traced by reading the exact `apache-polaris-1.6.0`-tagged authorization source: `RbacOperationSemantics.java` registers `DROP_TABLE_WITH_PURGE` as needing `TABLE_DROP` **and** `TABLE_WRITE_DATA` (dropping a table *without* purge only needs `TABLE_DROP`; dropping a *view* only needs `VIEW_DROP` — no data-write privilege at all). `PolarisAuthorizerImpl.java`'s `SUPER_PRIVILEGES` map shows only `CATALOG_MANAGE_CONTENT` or `TABLE_WRITE_DATA` itself satisfy that requirement — notably **not** `CATALOG_MANAGE_METADATA`, which is what a default `catalog_admin` role actually has.

**Resolution**: grant `TABLE_WRITE_DATA` directly to the catalog role (`polaris privileges catalog grant --catalog <name> --catalog-role catalog_admin TABLE_WRITE_DATA`) — narrower and more correct than granting the broad `CATALOG_MANAGE_CONTENT`, which also happens to fix it but grants far more than needed (full namespace/table create/drop, not just data-write). A default `catalog_admin` role having only `CATALOG_MANAGE_ACCESS`/`CATALOG_MANAGE_METADATA` (neither implies `TABLE_WRITE_DATA`) is worth remembering for any fresh Polaris catalog role setup.

**Takeaway**: an Iceberg REST `ForbiddenException` naming a specific op is Polaris's RBAC authorizer, not a hardcoded feature wall — check `RbacOperationSemantics`/`PolarisAuthorizerImpl` for the *specific* privilege before assuming an operation is permanently unavailable.

### `DROP_WITH_PURGE_ENABLED`: REST `PUT` silently no-ops, the CLI actually works

**Symptom**: `Unable to purge entity ... set the Polaris configuration DROP_WITH_PURGE_ENABLED or the catalog configuration polaris.config.drop-with-purge.enabled`. Setting the realm-level env var flags (`POLARIS_FEATURES_DROP_WITH_PURGE_ENABLED=true` and the realm-override variant) has zero effect. A hand-rolled `curl -X PUT /api/management/v1/catalogs/<name>` to set the catalog-level property `polaris.config.drop-with-purge.enabled` returns `200 OK`, but a follow-up `GET` shows the property was never actually added.

**Why the realm-level env vars don't help**: `polaris.features."DROP_WITH_PURGE_ENABLED"` is a realm-wide *default* — the first mechanism named in the error — but a catalog that already exists with its own override isn't governed by it.

**Why the raw `curl PUT` silently no-ops**: almost certainly a client-request-shape problem, not a Polaris server bug. The official CLI does its own get-then-merge-then-`PUT` under the hood, including `currentEntityVersion` for optimistic concurrency — a hand-rolled request has to reproduce that exactly, and a naive one won't.

**Resolution**: use the official CLI, not raw REST, for catalog property updates:
```bash
uvx --from apache-polaris polaris --host localhost --port <port> \
  --client-id root --client-secret <secret> \
  catalogs update --set-property polaris.config.drop-with-purge.enabled=true <catalog>
```
(Auth needs `--host`/`--port`, not `--base-url` — the latter 404s on the token endpoint.) Confirmed durable across a Polaris pod restart, since the catalog entity lives in Postgres via `relational-jdbc`, not pod state.

**General rule for this class of problem**: catalog **`properties`** (the generic client-visible key/value map) genuinely can be updated post-creation, just not via a naively-constructed raw REST call — use the CLI. **`storageConfigInfo`** (the nested S3/Azure/GCS-specific config), by contrast, isn't exposed for update by the CLI at all — delete-and-recreate is the only path (see the entry above). `POST` (creation) with a complete, correct body is reliable by hand; `PUT` (update) is not.

### `apache-polaris` ships a real Python SDK, not just the CLI

If you're scripting or automating anything against Polaris beyond one-off CLI commands: the `apache-polaris` PyPI package (the same one the `polaris` CLI comes from) exposes `apache_polaris.sdk.management` — a full generated client (`PolarisDefaultApi`, typed request/response models) that the CLI itself is built on. Shelling out to the CLI as a subprocess and parsing text output is a materially worse pattern than importing this directly. Worth mirroring the CLI's own request-construction logic (`apache_polaris.cli.command.catalogs`/`privileges`, `apache_polaris.cli.api_client_builder`) rather than reinventing it — in particular, property updates need the same get-then-merge-then-`UpdateCatalogRequest(current_entity_version=..., properties=...)` sequence described above.

---

## Trino + Iceberg + object storage

### Iceberg's optimistic concurrency protects single commits, not multi-statement sequences

If your pipeline does two separate writes (e.g. `DELETE` then `INSERT`, or two separate Iceberg commits) against the same table where overlapping runs are possible: Iceberg's concurrency model is optimistic-concurrency-via-atomic-metadata-pointer-swap. A **single** commit (a Trino `MERGE`, a PyIceberg `overwrite()`) is genuinely safe under concurrent writers — conflicting commits get `CommitFailedException` rather than silent corruption, and non-conflicting ones auto-retry (4 attempts with exponential backoff by default). A **two-statement sequence** is not protected by that mechanism at all — nothing stops a second writer's statements from interleaving with the first's. If you need atomicity across what would otherwise be two separate writes, route through a single `MERGE`/`overwrite()` call instead of `DELETE` + `INSERT`.

### Trino/dbt-trino implicit namespace creation isn't atomic under concurrent sessions

**Symptom**: `TrinoQueryError: Cannot create namespace <name>. Namespace already exists`, thrown from inside one of two concurrently-running dbt invocations, immediately after the *other* one's log shows it just created that same namespace.

**Cause**: unlike PyIceberg's `catalog.create_namespace_if_not_exists()` (which is safe under this exact race — confirmed by direct testing), Trino/dbt-trino's implicit schema auto-creation throws a hard error on the loser of a race between two sessions both hitting a not-yet-existing namespace at nearly the same moment, rather than silently no-op'ing.

**Resolution**: don't rely on implicit creation for any namespace multiple independent, concurrently-running processes might write to. Pre-create every such namespace once, deterministically, as part of one-time bootstrap (e.g. via PyIceberg's safe `create_namespace_if_not_exists`), before any concurrent runtime code can race to create it itself.

### dbt-trino's `accepted_values` test doesn't coerce against a native boolean column

**Symptom**: `TrinoUserError(TYPE_MISMATCH: ... boolean and varchar(4))` running `accepted_values: [true, false]` against a genuinely `boolean`-typed column.

**Cause**: the generic test compares the column against string-typed literal values without coercing to the column's actual type.

**Resolution**: drop the test on native boolean columns — a boolean column's own type already rejects anything but `true`/`false`/`null`, so `accepted_values` adds nothing `not_null` doesn't already cover for that column.

### dbt's default `generate_schema_name` macro concatenates, it doesn't replace

**Symptom**: a model configured with `schema='model'` lands in a schema literally named `staging_model` (or `<target-schema>_model`), not `model`.

**Cause**: dbt's built-in `generate_schema_name` macro appends a model's custom `schema` config onto the target's default schema by default, rather than using it verbatim — surprising if you expect `schema=` to mean "use exactly this schema."

**Resolution**: override the macro with dbt's own documented pattern:
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```
Only affects models that set an explicit `schema` config.

### A passing `dbt build`/`dbt test` doesn't confirm objects landed in the intended schema

**Symptom**: a model builds successfully and all its tests pass, but querying the schema you expected it to be in shows nothing there.

**Cause**: dbt resolves `ref()`s and runs tests against wherever a model actually landed — it doesn't care whether that matches your intended namespace. If a `schema=` config is missing (e.g. set at the wrong level, or only in project-level YAML but not in the model's own `config()`), everything still builds and tests green, just in the wrong place.

**Resolution**: after any change to schema/materialization config, explicitly check `show tables from <intended_schema>` (or equivalent) — don't rely on a green test suite alone to confirm object placement.

### A locally-sized Trino coordinator OOMs on a real multi-million-row `MERGE` — and raising its memory limit has its own internal constraint

**Symptom, part 1**: Trino coordinator pod gets `OOMKilled` (container exit code 137) partway through a dbt incremental model's `MERGE`, on a coordinator sized for light dev workloads (e.g. a 2Gi container memory limit) processing a genuinely large batch (millions of new rows).

**Symptom, part 2, after naively raising the limit**: coordinator fails to start at all — `IllegalArgumentException: Invalid memory configuration. The sum of max query memory per node (...) and heap headroom (...) cannot be larger than the available heap memory (...)`.

**Cause, part 2**: Trino reserves roughly 30% of `jvm.maxHeapSize` automatically as headroom (`heap-headroom-per-node`); `query.maxMemoryPerNode` plus that headroom has to fit *inside* the heap or the coordinator refuses to boot. Bumping the container's memory limit alone (without checking how `maxMemoryPerNode` relates to the new heap size) reproduces this immediately if `maxMemoryPerNode` was set close to the old heap ceiling.

**Resolution**: when resizing for real data volume, change all three settings together and keep clear margin: container memory limit > `jvm.maxHeapSize` (leaves room for off-heap/native memory, direct buffers, metaspace — don't set the heap equal to the container limit), and `jvm.maxHeapSize` > `query.maxMemoryPerNode` + ~30% headroom (don't set `maxMemoryPerNode` close to the heap ceiling either). A rough working ratio that held for a several-million-row incremental load: 4Gi container limit / 3000M heap / 1800MB `maxMemoryPerNode`.

**Don't assume a doubled join (or any other query-shape inefficiency) is the cause without an A/B test proving it** — confirmed the hard way here. The original OOM happened under a query with a real, genuine inefficiency: an anti-join CTE pre-filtering "changed or new" rows, feeding into dbt's `merge` incremental strategy, which then did its *own* internal join against the same target table — two joins where one would do. Rewriting to eliminate that (see the insert/update-split entry below) was a real, worthwhile fix on its own merits, but a direct A/B test — reverting Trino to the exact original undersized limits and rerunning the rewritten, single-join, no-MERGE version against comparable data volume — still `OOMKilled` the coordinator, this time on a *smaller* row count (~1.3M vs. the original 3M). That's decisive: the double-join was a real but secondary inefficiency, not the dominant cause. The dominant cause was `server.workers: 0` (Trino's coordinator doing double duty as its only worker, deliberately, to fit a laptop) concentrating an entire multi-million-row query's memory footprint onto one node, something no query-shape rewrite fixes — only vertical (more memory per node) or horizontal (more nodes, unavailable here by design) scaling does. **Lesson**: when a query OOMs and you can see a real inefficiency in its shape, fixing the inefficiency is still worth doing, but don't declare victory on the resource question until you've re-run the *fixed* query against the *original* resource ceiling and confirmed it actually survives — "this looks more efficient now" and "this now fits in the memory budget" are different claims, and only a rerun proves the second one.

### Fixing a writer doesn't retroactively fix already-materialized tables

**Symptom**: after fixing a bug in code that writes a table's schema (e.g. a timestamp column written without timezone info), newly-written data is correct, but querying the table still shows the old, wrong column type.

**Cause**: an incremental `MERGE` (or any incremental/append strategy) merges *row data* into an existing table — it does not retroactively change that table's already-established column types. The table was created once, earlier, with the old (wrong) schema, and nothing about fixing the writer touches the table's existing DDL.

**Resolution**: `DROP TABLE` and let it get recreated from scratch by the now-fixed writer. Fixing the writer's *logic* being correct doesn't mean the *deployed table* is correct — these are two different things that are easy to conflate, especially since the code now "looks right."

---

## PyIceberg

### Credential vending fails against an STS-less catalog (MinIO or similar)

**Symptom**: `IllegalArgumentException: Credential vending was requested for table ..., but no credentials are available`.

**Cause**: PyIceberg's REST catalog client defaults to sending `X-Iceberg-Access-Delegation: vended-credentials`, which the catalog can't satisfy if it's backed by an S3-compatible store with no real STS (see the Polaris/MinIO entry above for why).

**Resolution**: disable the header and fall back to static credentials: `"header.X-Iceberg-Access-Delegation": ""` in the catalog config, with `s3.access-key-id`/`s3.secret-access-key` properties set directly.

### Polars `.to_arrow()` always produces nullable fields — declaring `required=True` breaks every write

**Symptom**: schema-mismatch errors writing a Polars DataFrame (converted via `.to_arrow()`) into an Iceberg table whose fields were declared `required=True`/non-nullable.

**Cause**: Polars' Arrow conversion always produces nullable columns, regardless of the DataFrame's own dtype nullability.

**Resolution**: declare Iceberg table fields as `required=False` when writing from Polars. If a genuine NOT NULL constraint is needed, enforce it as an application-level data-quality check *before* the write, not as the Iceberg field's own `required` flag — these are two different concerns (storage-level constraint vs. pipeline-level validation), and only one of them works cleanly with Polars' output.

### `TimestampType` (naive) and `TimestamptzType` (aware) are strictly incompatible, not just a formatting difference

**Symptom**: schema-mismatch error writing a tz-aware Python `datetime` (e.g. from `datetime.now(timezone.utc)`) into a table whose Iceberg schema declares a naive `TimestampType`.

**Cause**: Polars' `.to_arrow()` represents a tz-aware `datetime` as a tz-aware Arrow timestamp, and PyIceberg's schema validation treats `TimestampType` vs `TimestamptzType` as genuinely different, incompatible types — not something it'll coerce.

**Resolution**: if every timestamp your pipeline produces is tz-aware (the common and generally correct case), map to `TimestamptzType` unconditionally rather than trying to support both.

### A literal `datetime` value needs the stdlib, not a Polars expression function

**Symptom**: a column that should be a timestamp instead serializes as a meaningless `fixed_size_binary(8)` field.

**Cause**: `pl.datetime(2026, 1, 1)` is a **Polars expression constructor**, meant for use inside `select()`/`with_columns()` — using it as a plain literal value in a Python list (not inside a Polars expression context) doesn't do what it looks like it does.

**Resolution**: use `datetime.datetime(...)` from the standard library for actual literal values; reserve `pl.datetime()` for expression contexts.

---

## Polars data processing (raw_to_clean, landing/raw/clean assets)

### `pl.DataFrame()`/`pl.read_csv()` sample only the first N rows to infer a column's type — a late, real value can crash the build entirely

**Symptom**: `could not append value "..." to the builder; make sure that all rows have the same schema` constructing a DataFrame from a `list[dict]`, or a column silently inferring as the wrong type (e.g. an all-digit ID column inferring `Int64` instead of the intended string) reading a CSV.

**Cause**: Polars' schema inference (`infer_schema_length`, default 100 for `pl.DataFrame()`/CSV reading) only looks at the first N rows. A column that's null across every sampled row but has a real string value later in a multi-thousand-row batch breaks the builder the moment it hits that later row — Polars committed to a schema before ever seeing the value that contradicts it. A column that looks numeric in every row (e.g. `"5000"`, `"6100"`) infers as `Int64` even when it's semantically a string identifier.

**Resolution**: pass `infer_schema_length=None` to scan every row before committing to a schema — turns the crash-on-a-later-row failure mode into a correct (if occasionally surprising) upfront inference. Doesn't fully solve the "numeric-looking string" case (Polars will still legitimately infer `Int64` for an all-digit column if that's genuinely what every row's data looks like) — that's what a schema-registry-driven reconciliation step downstream is for (see below), not something CSV-read-time flags can fix on their own.

**Caveat**: a column that's *entirely* null across the whole file (not just the sample) still infers as Polars' `Null` type even with `infer_schema_length=None` — there's no data to infer a real type from. Handle this explicitly downstream (cast to a known/expected type, or default to a sensible fallback like `Utf8`) rather than assuming full-file scanning eliminates every null-related edge case.

### `pl.concat(how="vertical_relaxed")` requires an identical column set across frames — only dtype differences are tolerated, not column differences

**Symptom**: `SchemaError: schema lengths differ` concatenating several DataFrames (e.g. one CSV file per historical batch in a file-drop landing directory) where one file has a different column count than the others.

**Cause**: `"vertical_relaxed"` relaxes *type* mismatches between frames with the same columns (casting to a common supertype) — it does not union differing column sets. A file-drop source's historical files can genuinely gain/lose columns over time (the same schema-evolution scenario a downstream reconciliation step is built to handle), and the naive `vertical_relaxed` concat breaks on exactly that case instead of tolerating it.

**Resolution**: use `how="diagonal_relaxed"` instead — unions the columns across all frames and fills any frame missing a given column with null, while still relaxing dtype mismatches for columns that are present in both. This is the vectorized equivalent of what a plain `list[dict]`-based concatenation (e.g. `all_rows.extend(...)`) tolerates for free via `.get()`-style defensive access — `vertical_relaxed` is the wrong default the moment historical batches aren't guaranteed to have identical columns.

### Reactive schema-registry reconciliation vs. hardcoded per-column `schema_overrides` dicts

If a pipeline validates incoming data against a metadata-tracked expected schema (a `schema_registry`-style table) and that schema can legitimately drift over time (a source system adds a column, or changes a column's precision — e.g. starts sending an ID as an integer instead of a zero-padded string): don't hardcode a Python dict of Polars dtype overrides per feed as the fix for a schema-inference mismatch. It silently reintroduces exactly the problem the registry exists to track, and produces inconsistent per-feed special-casing across a codebase (some feeds go through the registry, others get a bespoke override dict).

**Better pattern**: reconcile the *inferred* schema against the *registered* schema at the point data enters the pipeline, before validation — null/all-null columns get cast to the already-known type from the registry (or a safe default like `Utf8` if genuinely new); a new column not yet in the registry gets added automatically (nullable, inferred type); a column whose *non-null* data now has a different concrete type than registered is treated as a legitimate upstream schema change — auto-evolve the registry to the new type and let the run proceed, don't fail it. Only a column *disappearing* entirely (present in the registry, absent from this run's data) should hard-fail — silently dropping a promised column is never safe to auto-heal, unlike the other three cases.

**A consequence worth knowing if the target table is a spec-strict format like Iceberg**: Iceberg's schema evolution rules only allow a narrow set of "safe" type promotions in place (`int`→`long`, `float`→`double`) — a change like `string`→`long` (a plausible real case: an ID field the source system suddenly sends as pure digits) isn't a legal in-place column-type change. If the target table has no cross-run history to preserve (e.g. it's overwritten fresh every run, not accumulated), the simplest correct response to *any* detected schema change is to drop and recreate the table under the new schema rather than attempting in-place evolution with a fallback path — there's nothing to lose by recreating it, and it sidesteps Iceberg's promotion restrictions entirely.

**A second consequence, one layer downstream**: if a *cumulative* table (e.g. a dbt incremental model built via `MERGE`) reads from the table whose schema just evolved, the cumulative table still has the *old* column type baked into its own already-materialized schema — `MERGE`'s source/target column types have to match, so the very next incremental run fails with a type-mismatch error even though the upstream schema change was itself handled correctly. Dropping+recreating the cumulative table isn't viable if it holds history nothing else can reconstruct (a full backing snapshot only ever holds the *current* run's batch, not the union of everything ever seen). The fix belongs in the cumulative model itself: cast every source column to a fixed, deliberately-chosen target type in the model's own `select`, decoupling the warehouse's stable contract from whatever type upstream happens to infer on any given run. This is standard dbt staging-layer practice (normalize/type-stabilize before anything durable) for exactly this reason, not something specific to a Polars/Iceberg source — a bare `select *`/unqualified column list in a staging model is implicitly betting the source's inferred types never change.

---

## Dagster + Kubernetes

### `dagster asset materialize` never touches the configured run launcher

**Symptom**: a `K8sRunLauncher` is configured, but `dagster asset materialize --select "*"` never creates any pods — `kubectl get pods` stays empty even though the command reports success.

**Cause**: `dagster asset materialize` executes every step in-process, locally, on whatever machine runs the command. It's a dev convenience command, not a real "submit a run" path.

**Resolution**: use `dagster job launch -j '__ASSET_JOB' -m <module>` (or the webserver's "Materialize" button, which triggers the same GraphQL mutation) — this goes through `instance.submit_run`, which respects the configured launcher.

### A submitted run sits in `QUEUED` forever without the daemon running

**Symptom**: `dagster job launch` reports success, but the run never progresses past `QUEUED` — no pod, no further events beyond `PIPELINE_ENQUEUED`.

**Cause**: Dagster's default `QueuedRunCoordinator` hands runs to a queue; only the **daemon** process (one of several `dagster dev` starts alongside the webserver, or a standalone `dagster-daemon run`) actually polls that queue and calls `run_launcher.launch_run()`. Launching from a bare one-shot CLI invocation with no daemon running anywhere leaves nothing to ever pick the run up.

**Resolution**: make sure a daemon process is actually running (via `dagster dev`, or `dagster-daemon run` in production) before submitting runs expecting them to execute.

### `K8sRunLauncher` needs Postgres-backed (or equivalent shared) instance storage

**Symptom**: a run launched via `K8sRunLauncher` creates a pod, but its status/events never show up wherever the submitting process is looking.

**Cause**: the whole point of `K8sRunLauncher` is that the launched run executes in a **different process, in a different location** than whatever submitted it. Dagster's default instance storage is a local SQLite file — invisible from inside the launched pod.

**Resolution**: point `storage:` in `dagster.yaml` at a shared Postgres instance reachable from both the submitting process and the launched pod. `dagster-postgres` auto-creates/migrates its own schema on first connection.

### `K8sRunLauncher`'s launched pod and the local submitting process need different hostnames for the same shared service

If the submitting process runs on a host machine (reaching Postgres via `localhost`/a NodePort) and the launched pod runs in-cluster (reaching the same Postgres via its in-cluster DNS name), a single static `dagster.yaml` can't serve both. Use a `dagster.yaml` template with `hostname: {env: POSTGRES_HOST}`, then provide two different values for that one env var: exported directly in the shell for the local process, and injected via `K8sRunLauncher`'s `env_vars` config (which supports bare `KEY` passthrough too, for values identical in both places) for the launched pod, mounted via `instance_config_map`. `instance_config_map` is a *required* config field, despite reading as though it might be optional — omitting it leaves a launched pod with no instance config at all.

### `K8sRunLauncher.service_account_name` is required by the constructor despite the config schema marking it optional

**Symptom**: `TypeError: K8sRunLauncher.__init__() missing 1 required positional argument: 'service_account_name'` at instance-load time, even though `is_required=False` in the actual config schema.

**Resolution**: always set it explicitly in `dagster.yaml`'s `run_launcher.config` (e.g. `service_account_name: default`) — a real mismatch between the declared config schema and the constructor in at least this `dagster-k8s` version, not a config mistake.

### `dagster-dbt`'s manifest must be pre-built into any image that isn't run via `dagster dev`

**Symptom**: a launched pod fails because `target/manifest.json` doesn't exist, even though the same dbt project works fine locally.

**Cause**: `DbtProject.prepare_if_dev()` (which regenerates the manifest) is a **documented no-op everywhere except under the `dagster dev` CLI specifically** — a launched pod never runs via that CLI.

**Resolution**: bake the manifest into the image at build time — `RUN dbt parse --profiles-dir <dir>` in the Dockerfile. `dbt parse` doesn't need a live warehouse connection, only a syntactically valid `profiles.yml`, so it's safe during image build.

### `DbtProject`'s `profiles_dir` doesn't inherit from `DbtCliResource`'s

**Symptom**: `prepare_if_dev()`'s manifest regeneration fails (or uses the wrong profile) even though `DbtCliResource` was configured with the correct `profiles_dir`.

**Cause**: if your `profiles.yml` lives somewhere other than the dbt project root, `DbtProject(project_dir=...)` defaults its *own* `profiles_dir` to `project_dir` unless told otherwise — it does not share whatever was passed to a separately-constructed `DbtCliResource`.

**Resolution**: pass the same explicit `profiles_dir=` to both `DbtProject` and `DbtCliResource`.

### dagster-dbt doesn't automatically connect a dbt source to the upstream asset that populates it

**Symptom**: a dbt source and the (non-dbt) asset that actually writes that data show up as two disconnected nodes in the asset graph, rather than one connected lineage chain — selecting the dbt asset doesn't pull in its real upstream.

**Cause**: by default, `dagster-dbt` assigns a dbt source its own auto-generated `AssetKey`, unrelated to whatever key your own upstream `@asset` happens to use.

**Resolution**: subclass `DagsterDbtTranslator` and override `get_asset_key()` to return the exact same `AssetKey` your upstream asset already produces, when `dbt_resource_props` describes that specific source. Confirm via `defs.resolve_asset_graph()` that the result is one connected chain, not two.

### One `@dbt_assets` function per independent unit of concurrency, not one for the whole project

If you're using per-asset concurrency pools (`pool="some-key"`) to serialize related work while letting unrelated work run in parallel: a single `@dbt_assets` function running `dbt build` across an entire multi-feed/multi-domain project has to sit under **one** pool for the whole function. The moment a second, unrelated feed/domain gets its own dbt models in the same function, it gets wrongly serialized against the first. Use a factory function producing one `@dbt_assets`-decorated function per independent unit (scoped via `select=f"tag:{unit}"`, with its own `pool=f"...:{unit}"`) instead of one function for everything.

### Use Dagster's own concurrency pools instead of a hand-rolled lock table

If cross-run concurrency safety is a concern (e.g. two overlapping runs of the same pipeline racing on a shared resource): Dagster already has **concurrency pools** — `pool="some-key"` on any asset, with a per-pool slot limit, backed by the instance's own storage, durable across daemon restarts. No need to build a custom Postgres-backed lock/queue table on top of an audit-log table. One caveat: pool slots aren't automatically released if a run crashes or is cancelled unless `run_monitoring.free_slots_after_run_end_seconds` is set — with a limit of 1, a single stale claim otherwise blocks that pool forever.

### A global `dbt` on `PATH` silently shadows the project's venv `dbt` inside Dagster's subprocess calls

**Symptom**: `Could not find adapter type <adapter>!` from a Dagster-launched dbt invocation, even though `dbt debug` works perfectly when run directly in the same shell.

**Cause**: `dagster_dbt`'s `DbtCliResource` invokes `dbt` by resolving it via `$PATH` in whatever process launched Dagster — if that shell never had the project's `.venv/bin` prepended to `PATH`, `which dbt` can resolve to an unrelated global install missing the needed adapter.

**Resolution**: explicitly prepend `.venv/bin` to `$PATH` before running any Dagster CLI command locally — don't assume `uv run`-style invocation alone is enough once Dagster itself starts shelling out to subprocesses. More generally: if something "works" unexpectedly (or fails unexpectedly) despite looking correct in the venv, check the actual resolved interpreter/executable path, don't just re-read the config.

### A long-running `dagster dev` process holds a stale in-memory copy of `dagster.yaml`

**Symptom**: `dagster.yaml` is edited (e.g. adding env vars to `run_launcher.config`) and confirmed correct on disk, but a freshly-launched pod still doesn't have the new config.

**Cause**: the *submission* step (`dagster job launch`) does re-read current code each time, but the actual `launch_run()` call is made by the **daemon**, a separate, already-running process that loaded its instance config once at its own startup. Editing `dagster.yaml` while `dagster dev` is running does not hot-reload the run launcher's config.

**Resolution**: restart the whole `dagster dev` process after any `dagster.yaml` change that affects the run launcher. Verify by checking the actual launched pod's spec (`kubectl get job ... -o jsonpath='{.spec.template.spec.containers[0].env}'`), not just by re-reading the config file and assuming it applied.

### `dagster dev`'s child processes (daemon, webserver) aren't killed by a `pkill` matching only the wrapper's command line

**Symptom**: after killing `dagster dev`, orphaned `dagster._daemon`/`dagster_webserver` processes remain running — reparented to pid 1, still connected to shared Postgres, still writing heartbeats. On a subsequent fresh start, the new run's legitimate daemon fights the orphaned one(s) over heartbeat ownership (visible as a churn of different daemon IDs), and launched runs may never get their Kubernetes Job created at all.

**Cause**: `dagster dev` spawns `dagster._daemon run ...` and `dagster_webserver ...` as **separate child processes with entirely different command lines** — neither contains the substring "dagster dev". `dagster dev`'s graceful-shutdown handshake (which normally cascades a clean stop to its children) only runs if the wrapper process gets a chance to react to `SIGTERM` — a `SIGKILL`, or anything that races ahead of the handshake, orphans the children instead of stopping them.

**Resolution**: match on something present in *every* process in the tree, not just the wrapper's own invocation — e.g. a path substring that appears in every child's command line via its own arguments (such as an `--instance-ref` blob referencing a shared home directory path). Check for already-orphaned processes from prior sessions (`ps aux | grep <path-substring>`) before assuming a clean slate.

### `kubectl get jobs -o jsonpath='{.items[-1]...}'` crashes on an empty list

**Symptom**: `array index out of bounds` immediately after submitting a run, when polling for the newly-created Kubernetes Job.

**Cause**: negative jsonpath indexing into `.items` throws instead of returning nothing when the list is empty — exactly the state right after a run submission returns (the daemon creates the actual Job a moment later, asynchronously).

**Resolution**: use a plain space-separated before/after set-difference instead — snapshot `.items[*].metadata.name` before submitting, poll again after, and diff the two sets — rather than indexing into a list that might still be empty.

### A workspace member's own `pyproject.toml` must declare every module it directly imports, even if another workspace member happens to already pull it in

**Symptom**: code runs fine locally under `uv run` (a shared `.venv` across the whole `uv` workspace), but the same code fails inside a Docker image built from just one workspace member's own `pyproject.toml`/lockfile-resolved dependency set — `ModuleNotFoundError` for a package that's genuinely used, and genuinely installed locally.

**Cause**: a `uv` workspace shares one `.venv` across every member, so a package declared as a dependency of member A is importable from member B's code too during local dev, even though B never declared it. A container image built from B's own dependency list alone doesn't have that accidental transitive availability — only what B itself declares.

**Resolution**: declare every module a package's own code directly imports in that package's own `pyproject.toml`, regardless of whether some other workspace member already happens to depend on it. Don't rely on "it imports fine locally" as confirmation a dependency is correctly declared in a multi-package workspace — verify against the actual built artifact (the Docker image, a fresh single-package venv) instead.

### A `kubectl wait --timeout` tuned for a normal run can be too short for a legitimate first-run workload

**Symptom**: a smoke-test/verification script's `kubectl wait --for=condition=complete ... --timeout=<N>s` times out and reports failure, even though the underlying Dagster run is healthy and completes successfully shortly after — checking `data_feed_run`/`data_model_run` (or the Dagster UI) for that run shows every stage green.

**Cause**: a fresh/reset metadata database means an incremental feed's watermark is empty, so its next run does a full historical catch-up (e.g. every available month from an external API's start, one sequential HTTP call per month) instead of a normal incremental step — genuinely, correctly slower than the steady-state case the timeout was originally tuned against.

**Resolution**: size verification timeouts for the worst *legitimate* case (a cold-start full backfill), not just the common steady-state case — a "the pipeline is broken" false negative from an under-provisioned timeout is worse than a verification step that occasionally takes a few extra minutes on a fresh environment. When a verification script also asserts specific row counts (e.g. "expect N successful rows for this run"), keep that count in sync by hand as new feeds/models get added — it's easy for the count to go stale (still asserting an old, smaller number) long after the timeout itself gets noticed and fixed.

### `pkill -f` (SIGTERM) doesn't reliably kill a process once its backing resources are already gone

**Symptom**: a `pkill` call reports success (exit code 0), but the target process is still alive 10+ seconds later, unresponsive to further signals.

**Cause**: plausibly a process stuck in a blocking call (a DB reconnect attempt, a Kubernetes API retry against an already-torn-down cluster) that never reaches its Python signal handler.

**Resolution**: send `SIGTERM`, poll for up to some bounded time (e.g. 10s), escalate to `SIGKILL` only if still alive — don't assume a `pkill` exit code of 0 means the process actually died. A responsive process still exits in under a second either way, so this doesn't slow down the common case.

### A stub asset can silently violate a documented layer contract indefinitely — nothing forces you to notice

If an early-phase placeholder ("this asset just passes its input through for now, real logic comes later") stands in for a layer with a real documented purpose (here: `raw` was supposed to be a durable, watermarked, platform-internal copy — the Roadmap said so from early on), and nothing downstream actually depends on that layer having real content (every consumer reads from the in-memory value the stub passes through, not from disk): the stub can keep working, keep passing every test, and keep shipping features for a long time with nobody noticing the original contract was never fulfilled. There's no test that fails, no error that surfaces — the gap is only visible if someone reads the original design doc side-by-side with the actual code and asks "does this really do what this says," or hits a downstream consequence of the missing behavior (here: an upstream zone accumulating data forever, because the thing that was supposed to make it safe to clear — a durable copy elsewhere — never actually existed). Worth periodically re-reading a project's own early architecture docs against current code, not just trusting that "it's been working" means every documented piece is real.

---

## dbt modeling patterns

### Avoiding a circular `ref()` in a deletion-detection intermediate model

If building a Type-2 SCD pattern where an intermediate model needs to detect "a business key that used to exist no longer does" (to synthesize a deletion): comparing against the SCD table's own current rows is tempting but creates a circular `ref()` if that intermediate model sits upstream of the SCD table in the DAG (which it will, if the SCD table's snapshot logic depends on the intermediate model's `is_deleted` flag). Resolve by comparing two genuinely *upstream* sources instead — e.g. a cumulative staging table (every key ever seen) against a fresh, non-cumulative full-load source (this run's true current state) — a key present in the former but absent from the latter is a deletion. This is also naturally idempotent without extra logic: a key already marked deleted keeps getting resynthesized identically every run, and if the downstream SCD mechanism gates new versions on an attribute-hash actually changing, an unchanged resynthesis produces zero new rows on its own.

### Iceberg tables require microsecond timestamp precision — dbt's default `current_timestamp()` renders milliseconds

If using dbt snapshots (or any model) against Iceberg tables: dbt's default `current_timestamp` macro renders `TIMESTAMP(3) WITH TIME ZONE` (millisecond precision). Iceberg's table spec only supports microsecond precision, so writes to `TIMESTAMP(3)` columns fail against Iceberg tables. Override `trino__current_timestamp()` to render `current_timestamp(6)` before any snapshot/timestamp-writing model runs against Iceberg.

### Explicit insert/update split instead of MERGE — and how to get there without an adapter that lets you invent strategy names

If avoiding `MERGE` for an incremental upsert (a defensible, common preference — MERGE's matched/unmatched query planning is comparatively newer/heavier than plain `DELETE`+`INSERT` on some engines, and a MERGE gives no built-in visibility into how many rows were inserts vs. updates): a naive first attempt at "only touch changed rows" often ends up computing a pre-filter join (an anti-join CTE excluding unchanged rows) *and then* still using `incremental_strategy='merge'`, which does its **own separate internal join** against the same target table to figure out matched/unmatched rows. That's two joins against the target where one would do — a real inefficiency, not just a stylistic MERGE-avoidance question (see the Trino OOM entry above for what this cost in practice).

**The actual fix**: compute the *single* join needed for change-detection once, in the model's own SQL, classifying each surviving row with an explicit flag column (e.g. `_change_type = 'insert' | 'update'`) — this join already tells you everything a MERGE's internal join would have told you a second time. Apply the result with a plain `DELETE` (matching only the 'update' rows' keys) followed by `INSERT` (everything from the classified set) — this achieves "update" semantics using only DELETE+INSERT, both cheap and well-supported by Iceberg/Trino, without ever invoking MERGE's own planner.

**The adapter-dispatch trick that makes this work cleanly in dbt**: you can't just invent a new `incremental_strategy` name (e.g. `'insert_update_split'`) — at least on dbt-trino, `TrinoAdapter.valid_incremental_strategies()` is a hardcoded Python list (`["append", "merge", "delete+insert", "microbatch"]`), and a project can't extend it via macros alone (that's compiled adapter code, not Jinja). The workaround: reuse an *existing*, already-whitelisted strategy name whose shipped SQL-generating macro is close in spirit — `delete+insert` is the natural fit — but override that macro's implementation at the **project level**. dbt's macro dispatch resolves a project-defined macro of the same name (`trino__get_delete_insert_merge_sql`) ahead of the adapter-shipped one, so the config stays valid (`incremental_strategy='delete+insert'`, no adapter changes needed) while the actual generated SQL is entirely custom (in this case, reading a `_change_type` column the shipped version has no concept of).

**A subtlety worth getting right**: any classification column added purely for this purpose (like `_change_type`) must never appear in the model's final `select *` on the **non-incremental** branch (a model's first run does a plain `CREATE TABLE AS SELECT`, which bakes in whatever the compiled query returns) — otherwise it becomes a real, permanently-persisted column in the target table. Gate it inside the `{% if is_incremental() %}` branch only; a first run never invokes the incremental-strategy macro at all (dbt's own incremental materialization only calls it once the target already exists), so the column never gets a chance to leak into the initial schema.

**Once the pattern repeats across several models with only column-list differences, extract the repeated join/classification logic into a macro** (here: `classify_changes(source_relation, updates_enabled)`) rather than leaving it hand-copied per model — the six models that needed this pattern had it copy-pasted with only the source CTE's name differing, which is exactly the kind of duplication that drifts the moment one copy gets a fix the other five don't.

### Generalizing a per-feed hand-written model into codegen — wait for a real second consumer, or an explicit reason not to

If a single hand-written model captures a genuinely per-feed pattern (e.g. a deletion-synthesis intermediate model, parameterized in spirit by a feed's business key and tracked columns, but only ever built for one actual feed so far): resist generalizing it into a metadata-driven codegen step *purely on spec* — with a sample size of one, it's easy to generalize the wrong axis (this project's original hand-written deletion-synthesis model carried an extra passthrough column, `updated_at`, that wasn't part of either the business key or the tracked-attribute set used for change detection — easy to miss as a distinct category if designing the generalized shape from imagination rather than from what the one real example actually needed). When there's a concrete reason to generalize anyway (an explicit ask, not just "this might get reused someday"), the safest path is a codegen script matching whatever pattern the project already uses for its other metadata-driven generation (here: the same shape as the existing serve-view generator — a build-time Python script reading Postgres, rendering one `.sql` file per row into a `generated/` directory, cleared and rebuilt every run) — and the acceptance test is that it reproduces the one known-good hand-written example byte-for-byte (logically) before trusting it for a feed that doesn't exist yet.

---

## Python tooling on macOS: `uv`, editable installs, and iCloud sync

### `uv`'s default macOS link mode can set the `UF_HIDDEN` flag on newly-written `.pth` files

**Symptom**: `ModuleNotFoundError` for a package that's genuinely installed (`uv pip show` succeeds) — `sys.path` after `uv run python -c "import sys; print(sys.path)"` is missing the paths its `.pth` files should have injected. Listing `site-packages` may show numbered duplicate `.pth` files (`foo.pth`, ` 2.pth`, ` 3.pth`, identical content/mtime) — not limited to editable workspace installs, regular third-party packages can show the same pattern.

**Root cause**: Python 3.13's `site.py` (`addpackage()`) checks the macOS `UF_HIDDEN` BSD file flag and **silently skips** any `.pth` file with it set — a security hardening added specifically to defend against hidden malicious `.pth` files ([python/cpython#113659](https://github.com/python/cpython/issues/113659); `.pth` files support an `import`-prefixed line that gets `exec()`'d directly at interpreter startup, a real code-injection vector if a malicious one is hidden from casual inspection). On at least some machines, `uv`'s default macOS `link-mode` (`clone`, using APFS `clonefile()`) sets this flag on newly-written `.pth` files as a side effect. Confirmed by testing: a from-scratch `.venv` rebuild with `UV_LINK_MODE=copy` produced zero hidden files and zero duplicates; the default `clone` mode reproduced the corruption reliably.

**Fix**: set `link-mode = "copy"` in `[tool.uv]` (`pyproject.toml`). Confirmed clean across multiple from-scratch `.venv` rebuilds. Diagnostic command, if you suspect this: `ls -lO .venv/lib/python*/site-packages/*.pth` — a hidden file shows the word `hidden` in the flags column (distinct from a dot-prefixed filename, which is a completely different, unrelated concept).

**Caveat**: this only prevents the flag being set on *new* writes — it doesn't retroactively fix a `.venv` that already has hidden files sitting in it from before the config change (an incremental `uv sync` may decide those files are already "up to date" and skip rewriting them). A full `rm -rf .venv && uv sync` is needed once, for any pre-existing venv.

### The same `UF_HIDDEN`-on-`.pth`-files symptom can recur from a completely different cause: iCloud Drive sync

**Symptom**: identical to the above (`ModuleNotFoundError`, `.pth` file has `UF_HIDDEN` set) — but recurring intermittently on already-correctly-written `.pth` files, with the file's modification time completely unchanged (proving nothing rewrote it — only the flag changed), on a roughly-cyclical schedule rather than tied to any `uv` write.

**Root cause, if your project directory lives inside `~/Documents` (or `~/Desktop`) with iCloud Drive's "Desktop & Documents Folders" sync enabled**: this is a known, documented `uv` bug — [astral-sh/uv#9902](https://github.com/astral-sh/uv/issues/9902). macOS presents an iCloud-synced path as a normal local directory (via a File Provider extension on modern macOS — **not** a plain symlink, so `readlink` won't reveal it), but it's actually backed by `~/Library/Mobile Documents/com~apple~CloudDocs/...`, and iCloud's background sync/eviction cycle touches files in that tree on its own independent schedule. `uv`'s editable-install metadata assumptions break when the underlying file gets touched by something outside `uv`'s own knowledge. Multiple independent reporters confirm the exact same shape: works repeatedly, then breaks, on a roughly 10-second cycle. Confirmed for this project specifically by a controlled test: clearing the flag, then doing nothing but *reading* the file's contents in a loop (zero writes, zero `uv`/Docker activity) reproduced the flag being reapplied — ruling out every write-path theory.

**Permanent fix**: move the project directory outside of iCloud's synced scope entirely (a plain `mv`, no symlink) — confirmed by multiple independent reports, and consistent with this project's own investigation, to resolve it completely. There's no supported way to exclude a single subfolder from Desktop & Documents Folders sync while keeping the rest of the tree synced.

**If moving isn't immediately practical** (e.g. other tools/sessions are anchored to the current path): two things measurably help without fixing the underlying cause —
- **Enabling macOS Low Power Mode** (believed to pause/throttle background sync activity) produced a fully clean run in direct testing, with zero corruption anywhere in a full rebuild-and-test cycle, after failing repeatedly beforehand.
- **A retry wrapped around any fresh-Python-process invocation**, scoped specifically to this failure signature (checking subprocess output for `ModuleNotFoundError` before deciding to retry, so an unrelated real bug still fails immediately rather than being retried pointlessly), with the retry gap tuned to iCloud's own ~10-second cycle rather than a sub-second gap (which just retries inside the same bad window). This is a legitimate retry, not a fragile workaround, *if and only if* the retried operation fails at import/module-load time, before any real work starts — nothing partially applied, so a retry is as safe as retrying a transient network blip.

**A tempting non-fix, explicitly rejected**: switching affected packages to non-editable installs (`uv sync --no-editable`) removes the `.pth` files entirely, sidestepping the bug — but editable installs are the *correct*, standard pattern for active local development across the whole Python ecosystem (pip, uv, poetry all default to editable for dev-mode installs; `--no-editable` is specifically the production/deployment pattern). Trading away correct local-dev behavior (live source-reload on save) to dodge a sync-tool interaction is worse than living with the interim mitigations above.

**Something that looked related but wasn't**: Docker Desktop's default macOS file-sharing config (VirtioFS) exports a user's entire home directory into its VM at all times, and VirtioFS has its own documented history of real metadata-corruption bugs under heavy disk I/O ([docker/for-mac#7494](https://github.com/docker/for-mac/issues/7494)). Narrowing Docker Desktop's File Sharing scope to exclude the project directory measurably reduced (but didn't eliminate) the corruption in testing — a real, secondary contributor in a Docker-heavy workflow, worth knowing about, but not the actual root cause if the iCloud sync condition above also applies.

### `uv sync` at a workspace root doesn't install member dependencies by default

**Symptom**: `uv run <something>` appears to work, but fails deeper in with a `ModuleNotFoundError` for a dependency that's clearly listed in a workspace member's `pyproject.toml` — or worse, silently falls back to an unrelated **global** install of the same tool name, masking the problem entirely.

**Cause**: a pure workspace-container root `pyproject.toml` (`package = false`, no dependencies of its own) means plain `uv sync` only syncs the root project — workspace members' own dependencies never get installed.

**Resolution**: `uv sync --all-packages` to install every workspace member. If something "works" unexpectedly despite this, check the actual resolved interpreter/executable path (`ps aux`, `which`) — a global install on the machine can silently paper over a broken project venv rather than erroring.

### `uv`'s package cache can produce genuinely corrupted installs, independent of the `.pth`/`UF_HIDDEN` issue

**Symptom**: `uv sync` reports success, `uv pip show <package>` succeeds, but `import <package>` fails with `ModuleNotFoundError` anyway. Inspecting the package's `dist-info` directory shows no `RECORD` file and no actual package directory alongside it.

**Resolution**: `uv cache clean` **and** delete `.venv` entirely, then resync from scratch. Deleting just `.venv` without clearing the cache often looks like it fixed things, but the corruption can recur with a different package shortly after — the hardlinked package cache (`~/.cache/uv`), not the venv itself, is where this class of corruption actually lives.

**Related trap**: running two `uv cache clean`/`uv sync` fix attempts concurrently (e.g. one backgrounded and forgotten about, then a second started) can deadlock both, contending for the same cache lock file — `uv cache clean` hanging indefinitely (not just slowly) is a sign of this. Check `ps aux` for an already-running fix attempt before starting another; `kill -9` all stray `uv` processes and retry with exactly one attempt at a time.

---

## Kubernetes (general)

### A `PersistentVolumeClaim` doesn't work for "many pods across many namespaces need the same directory"

A PVC binds 1:1 to exactly one PV, and PVCs are themselves namespace-scoped — one created in namespace A can't be mounted by a pod in namespace B. For genuinely shared storage across namespaces on a single-node cluster, mount a `hostPath` directly in each pod spec that needs it instead of trying to force a PVC to do cross-namespace sharing. A real single-consumer case (e.g. a database's own data directory) is still fine as a normal PVC via `volumeClaimTemplates` — this only applies to the shared-across-many-consumers case.

### Host-side tools need a NodePort + matching cluster-level port mapping, not just a Service

A plain `ClusterIP` Service is only reachable from inside the cluster. To keep a host tool (a local script, a database client running directly on the machine) able to reach an in-cluster service by `localhost:<port>`, the Service needs `type: NodePort` with a fixed `nodePort`, and (for `kind` specifically) the cluster config needs a matching `extraPortMappings` entry. This has to be set at cluster-creation time for `kind` — changing it later means recreating the cluster.

---

## Postgres / SQLAlchemy

### SQLAlchemy's bind-parameter parser mishandles a `::type` cast glued directly onto a named parameter

**Symptom**: `psycopg.errors.SyntaxError` from a raw SQL string containing `:param_name::jsonb` (a named bind parameter immediately followed by a Postgres type cast).

**Cause**: SQLAlchemy's bind-parameter regex gets confused by the second colon immediately adjacent to the parameter name, and passes something malformed straight to the driver.

**Resolution**: use `cast(:param_name as jsonb)` instead of the `::type` shorthand glued to the parameter — unambiguous, no adjacency issue.

---

## Streamlit

### `st.dataframe` renders via canvas, not the DOM

Two consequences worth knowing if you're building anything on top of `st.dataframe`:
1. **UUID columns from a database driver come back as Python `uuid.UUID` objects**, which the canvas-based grid renderer (glide-data-grid) serializes as unreadable byte-index dicts instead of readable text. Stringify UUID (and similarly awkward-typed) columns before handing the DataFrame to `st.dataframe`.
2. **Browser-automation scripts that scrape `page.inner_text()` won't see grid contents at all** — the rendered cells aren't in the DOM. Verify grid content by querying the underlying data source directly in a test, not by scraping the rendered page.
