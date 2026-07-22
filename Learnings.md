# Lessons Learned

Obscure, hard-won problems hit while building this platform, and how they were actually resolved — the stuff that isn't obvious from reading the docs or the manifests. Organized by system/component, not build order. Each entry: what broke (symptom/error), why, how it was fixed, and any residual caveat worth knowing.

If you landed here from a search engine: welcome. Every entry below was reproduced and fixed for real against a live system, not guessed from a forum post.

---

## New chat session started

Continue work on the data platform — see README.md/Roadmap.md/Progress.md/Backlog.md/Learnings.md/CLAUDE.md for context, and .claude/plans/ for pending work from previous sessions if one exists. If you find anything unexpected while working, stop and raise it before proceeding.

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

## Flink + Kafka + Iceberg (streaming/ module)

### PyFlink is a dead end for this project's arm64 (Apple Silicon) local cluster

**Symptom**: a FlinkDeployment pod built with PyFlink as the job driver sits in `ImagePullBackOff` forever, even though `imagePullPolicy: IfNotPresent` is set correctly on the pod spec and the image was already `kind load docker-image`'d onto the node. `kubectl describe pod` shows kubelet repeatedly attempting a real registry pull (`pull access denied, repository does not exist`) for a purely local, never-published image name.

**Root cause chain, confirmed at each step rather than assumed**:
1. PyFlink's `pemja` dependency (the Python↔JVM bridge) has no published `manylinux_aarch64` wheel on PyPI — only `manylinux1_x86_64` (Linux) and `macosx_*_arm64` (native macOS, not a Linux container). Confirmed directly against PyPI's release file listing, not from a GitHub issue or blog post.
2. This forces the Flink image to be built `--platform linux/amd64` even on an arm64 host, since `pip install apache-flink` needs a prebuilt `pemja` wheel and won't find one for `linux/aarch64`.
3. `docker build --platform linux/amd64` itself succeeds fine (BuildKit's QEMU-based emulation handles the build steps) — the build has no dependency on the host or target cluster's architecture.
4. The failure is specifically in `kind load docker-image` on an arm64 kind node: the image blob does get imported into containerd's content store (confirmed via `ctr -n k8s.io images list`, which showed the correct tag, digest, and `linux/amd64` platform), but the CRI image service — the layer kubelet actually queries via `crictl images` — never surfaces it as present. Kubelet concludes the image is missing and falls back to a real registry pull, which fails because the image was never published anywhere.

**What was tried and ruled out**: rebuilding without BuildKit's default provenance/attestation manifest (`--provenance=false`) fixed a *different*, real problem (a multi-manifest image `kind load` mishandled) but did not fix this one — confirmed by testing both fixes independently, not conflated as one incident.

**Resolution**: abandoned PyFlink entirely. Switched to a vendored Java driver (`org.apache.flink.examples.SqlRunner`, copied verbatim from `apache/flink-kubernetes-operator`'s own `examples/flink-sql-runner-example`) that reads a `.sql` file and executes its statements via `TableEnvironment#executeSql` — see `streaming/flink/sql-runner/`. This removes the PyFlink/`pemja` dependency entirely, so the image goes back to building natively for the host architecture (no `--platform` flag, no emulation, no cross-arch `kind load` problem) — the same category of image as every other custom-built image in this repo.

**Broader lesson, not just a PyFlink-specific one**: `kind load docker-image` cannot be trusted for a foreign-architecture image on this project's local arm64 cluster. If a future component genuinely needs a `--platform linux/amd64` image (not ruled out forever, just not needed today), budget real time for solving the CRI-visibility gap first — pushing to a real (even local/throwaway) registry and doing an actual `imagePullSecrets`-authenticated pull, rather than the `kind load` shortcut, is the more likely fix to investigate first.

**Also worth knowing for the tradeoff itself**: the actual Java involved in the SqlRunner path is small and static — one ~70-line generic class plus a boilerplate `pom.xml`, both from the operator project's own reference implementation, not hand-written pipeline logic. It needs zero modification to support a new streaming source; onboarding a new source is purely a new `.sql` file (see `streaming/flink/sql-scripts/`). PyFlink's apparent "zero Java" benefit didn't actually reduce per-source work (each new source still needs its own bespoke Python driver file) and came with the structural arm64 problem above — the SqlRunner path is both less code to maintain per source *and* architecture-portable.

### `iceberg-flink-runtime` needs `org.apache.hadoop.conf.Configuration` on the classpath even for a pure REST-catalog + S3FileIO setup that never touches HDFS

**Symptom**: `NoClassDefFoundError: org/apache/hadoop/conf/Configuration` → `ClassNotFoundException: org.apache.hadoop.conf.Configuration`, thrown from `org.apache.iceberg.flink.FlinkCatalogFactory.clusterHadoopConf()`, on the very first `CREATE CATALOG ... WITH ('type'='iceberg', 'catalog-type'='rest', ...)` statement — even though nothing about a REST catalog + `S3FileIO` setup should need Hadoop at all.

**Cause**: confirmed live, not assumed from a tutorial — `FlinkCatalogFactory.createCatalog()` unconditionally calls `clusterHadoopConf()`, which unconditionally instantiates a Hadoop `Configuration` object, regardless of which catalog type or FileIO implementation is actually configured. Iceberg's own Flink getting-started docs allude to this ("By default, Iceberg ships with Hadoop jars for Hadoop catalog") but don't call out that it's a hard classload-time dependency even for non-Hadoop catalogs.

**Resolution**: add Hadoop's classpath to the image. The commonly-cited fix in older tutorials, `flink-shaded-hadoop-2-uber`, tops out at `2.8.3-10.0` on Maven Central (last published years ago) — used the modern, actively-maintained equivalent instead: `org.apache.hadoop:hadoop-client-api` + `org.apache.hadoop:hadoop-client-runtime` (both `3.5.0` as of this writing), Hadoop's own official self-contained shaded jars for exactly this "I need Hadoop-compatible classes without a real Hadoop install" scenario. Dropped into `/opt/flink/lib/` alongside the Kafka/Iceberg connector jars.

### A Flink TaskManager builds its own separate AWS S3 client for actual data writes — catalog-level `s3.*` properties don't reach it

**Symptom**: the Iceberg REST catalog connects fine (`CREATE CATALOG`/`CREATE TABLE` succeed), but the actual `INSERT` job fails on the TaskManager with `software.amazon.awssdk.core.exception.SdkClientException: Unable to load region from any of the providers in the chain` — even though the catalog was created with `'s3.region' = 'us-east-1'` and `'s3.endpoint' = 'http://minio...'` set explicitly.

**Cause**: this is the same class of problem already documented above for Polaris's own server-side `S3FileIO` client (see "Polaris `S3` storage type against MinIO", symptom #4) — a JVM component building a *fresh* AWS SDK v2 client for actual I/O (here: `IcebergStreamWriter` opening an `S3OutputFile` to write a real data file) doesn't necessarily inherit the catalog-level properties the same client-construction code path used elsewhere did. The AWS SDK's own `DefaultAwsRegionProviderChain` only checks environment variables, system properties, a local AWS profile, or EC2 instance metadata — never arbitrary Iceberg catalog properties.

**Resolution**: same fix as the Polaris entry, applied to the Flink pods this time — set `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_ENDPOINT_URL_S3` as literal environment variables directly on the FlinkDeployment's `podTemplate` (both JobManager and TaskManager pods get it from the shared top-level `podTemplate`). Confirmed working: the very next run committed a real Iceberg snapshot (`IcebergFilesCommitter` logged `Committing append for checkpoint ... with summary: CommitSummary{dataFilesCount=1, ...}`, `Committed snapshot ... (MergeAppend)`), and the row was immediately queryable from Trino.

**Pattern worth generalizing**: any *new* JVM component added to this platform's Iceberg/S3 stack (Flink here, Polaris earlier) should be expected to need this same explicit `AWS_*` env var quartet, regardless of whatever catalog-level `s3.*` properties are also set — don't assume catalog properties alone are sufficient just because they work for the client that creates/reads metadata; the client that actually writes data files may be a structurally different code path.

### Flink SQL's `CAST(string AS TIMESTAMP(n))` fails silently on an ISO-8601 `'T'`-separated string

**Symptom**: a row produced to the Kafka source topic simply never appears in the Iceberg sink table — no exception anywhere in the JobManager or TaskManager logs, the job stays healthy and `RUNNING` throughout. The only visible trace: `IcebergFilesCommitter` logs a real commit attempt (not the usual "skip commit, no data files" no-op) with `CommitSummary{dataFilesCount=0, dataFilesRecordCount=0, ...}` — i.e. the writer had a record to flush, and produced a snapshot commit for it, but with zero actual rows/files.

**Cause**: confirmed via direct A/B test (same topic, same schema, only the timestamp string format changed) — `CAST(event_timestamp AS TIMESTAMP(6))` where `event_timestamp` is a `STRING` column expects the **SQL-standard `'yyyy-MM-dd HH:mm:ss.SSSSSS'` format (space separator between date and time)**. An ISO-8601-style string with a `'T'` separator (`'2026-07-18T14:40:00.000000'`, the default `datetime.isoformat()` output in Python) fails the cast **without raising anything catchable** — not a `json.ignore-parse-errors`-covered issue (that only governs JSON *deserialization* into the source table's declared `STRING` column, which always succeeds regardless of the string's content) and not a `ClassCastException` either — the row is just dropped somewhere between the CAST expression and the sink writer.

**Resolution**: any producer feeding this pipeline (`streaming/producer/`) must emit `event_timestamp` in the space-separated SQL format, not `datetime.isoformat()`'s default `'T'`-separated one — e.g. Python: `dt.strftime('%Y-%m-%d %H:%M:%S.%f')`, not `dt.isoformat()`.

**Broader lesson**: this class of "silent row drop with a misleadingly-normal-looking commit log line" is a real Flink SQL failure mode worth remembering — a `dataFilesCount=0` commit (as opposed to a "skip commit, no data files" no-op) is itself the tell that something was attempted and produced nothing, not that nothing happened at all. Worth checking explicitly rather than assuming "no exception in the logs" means "the row made it through."

### A generated `FlinkDeployment` name with an underscore is rejected by the Kubernetes API server

**Symptom**: `kubectl apply` on a generated `FlinkDeployment` CR fails outright — the resource name (derived straight from `streaming_source.table_name`, e.g. `sales_events-sink`) violates RFC 1123 DNS label rules, which Kubernetes enforces for every resource name (no underscores, lowercase alphanumeric + `-` only).

**Cause**: `table_name`/SQL identifiers and Kubernetes resource names are two different naming domains that happen to share the same generator input — underscores are completely normal/expected for the former (matches every other SQL identifier in this platform) and completely invalid for the latter.

**Resolution**: a small `k8s_name()` helper in `generate_streaming_ingestion.py` converts underscores to hyphens *only* for the k8s-facing name (the `FlinkDeployment`'s `metadata.name`), while every SQL/filesystem-facing name (the `.sql` script, the Iceberg table, the dbt source) keeps underscores untouched. This also broke an assumption in `streaming/flink/module.just` that the applied resource name could be derived from the generated filename — fixed by reading the real name back live after apply (`kubectl apply -f "$cr" -o jsonpath='{.metadata.name}'`) instead of assuming filename and resource name match.

**Broader lesson**: any codegen script that derives a Kubernetes resource name from a metadata-driven string (not just this one) needs the same translation step — don't assume a string that's valid as a SQL/Python identifier is automatically a valid k8s name.

### A Kubernetes `Job` spec is immutable — re-applying one with different content fails, it must be deleted and recreated

**Symptom**: re-running `kafka::start` after a second streaming source was added failed applying `kafka-create-topics` — the Job's pod template changed (two topics to create instead of one), and `kubectl apply` rejected it with a "field is immutable" error.

**Cause**: `Job.spec.template` (and most of `Job.spec`) is immutable after creation — Kubernetes Jobs are designed as run-once, not reconciled-in-place like a Deployment.

**Resolution**: `kafka::start` now deletes the existing `kafka-create-topics` Job (if present) before applying the freshly-generated one, rather than a plain `kubectl apply`. Safe because the Job is idempotent by design (topic creation uses `--if-not-exists`), so a delete-then-recreate never loses anything a plain re-apply would have preserved.

**Broader lesson**: any k8s-hosted piece of this platform that's a `Job` (not a `Deployment`/`StatefulSet`) and gets regenerated by codegen needs delete-then-recreate semantics in its own `start` recipe, not a bare `kubectl apply` — Jobs are the one workload type here where "apply is idempotent" doesn't hold once the content actually changes.

### A Flink `TaskManager`'s minimum viable memory footprint is close to 1Gi, not meaningfully compressible below it

**Symptom**: onboarding a second concurrent streaming source, `taskmanager_memory: "512m"` failed with Flink's own memory-configuration error (only ~64MB usable after ~448MB of fixed JVM/metaspace/framework overhead); bumping to `"768m"` still failed (~320MB usable, still short). Per this project's two-failed-attempts rule, both were reported together rather than guessing a third value blind.

**Cause**: Flink's TaskManager memory model reserves a largely-fixed amount for Framework Heap/Off-Heap, JVM Metaspace, and Managed/Network memory regardless of how small the total budget is — these aren't proportionally shrinkable the way, say, a JVM heap alone would be. Below roughly 900Mi–1Gi, there just isn't enough left over for the framework's own fixed costs, let alone real task execution.

**Resolution**: ~1024m (matching what `sales_events` already used, now `generate_streaming_ingestion.py`'s own fallback default whenever `streaming_source.taskmanager_memory` is left null) is the practical floor for this platform's TaskManagers. The real fix for running multiple concurrent sources on constrained local hardware wasn't shrinking each TaskManager further — it was freeing memory elsewhere (KEDA scaling `orchestration` to zero when idle, and increasing the Docker Desktop VM's own memory budget) — see this file's Kubernetes scaling entries.

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

**Recurred for real, connector library (2026-07-14)**: once schema discovery moved to extraction time (a generic `CSVConnector`, no per-feed logic), `financial_transactions`' `posted_date` started getting recorded as `string` — correctly, since that's genuinely what Polars' CSV inference sees, an ISO-8601 timestamp is just a string until something parses it. `stg_financial_transactions.sql`'s `posted_date` cast broke (`cast(x as timestamp(6) with time zone)` can't parse an ISO-8601 `...T...Z` string). The wrong first instinct was to patch the *connector* — adding a `datetime_columns` parameter to `CSVConnector` so it could parse specific columns per feed — which would have made the connector library feed-specific again, undoing the entire point of it being generic. The correct fix, and the one this entry already prescribed: leave discovery/`schema_registry` alone (it's allowed to be "just" a string, that's not a bug), and fix the cast in staging itself — `cast(from_iso8601_timestamp(posted_date) at time zone 'UTC' as timestamp(6) with time zone)`, the same "staging owns its own explicit casts, decoupled from whatever upstream infers" principle already documented above, just hit again from a different angle (a connector genuinely can't know a string is *meant* to be a timestamp, the same way it can't know an all-digit string is meant to stay a string).

---

## Metadata-driven pipeline architecture (connectors, schema_registry, codegen)

### `schema_registry` ownership: extraction writes, clean only reads, never hand-seed

**Symptom**: an ODS build failed with a live Trino `COLUMN_NOT_FOUND` error; tracing it back showed `police_crimes`' `schema_registry.primary_key_columns` was silently `[]` despite `data_feed.source_pk=["id"]` being set correctly from the start.

**Cause**: two independent violations of the same rule. `scripts/seed_metadata_db.py::seed_schema_registry()` hand-seeded a `schema_registry` row per feed at seed time — its `INSERT` never set `primary_key_columns` at all (no such parameter existed), so every hand-seeded row silently defaulted to `[]`. Separately, the REST/JSON connector kinds' generated `clean_<feed>` step bundled flatten + schema discovery + `sync_schema_registry()` together with validation, gated only on the `"validation"` pipeline step — meaning discovery silently never ran at all if a feed was ever cherry-picked to extraction-only, and `clean` wasn't read-only against the registry the way the design requires.

**Resolution**: `schema_registry` is exclusively the extraction step's concern — discovery and the registry write both complete before `clean` ever runs; `clean` only ever reads it (`PostgresMetadataResource.get_current_schema()`). `metadata_runs` had always done this correctly (no seed row, bootstraps from a real run) — that's the pattern every feed follows now, not an exception. `seed_schema_registry()` and its 4 call sites were deleted outright, not patched to also set `primary_key_columns` — there is no legitimate case for a seed script writing this table by hand at all. For REST/JSON connector kinds specifically, since flattening is what makes discovery possible in the first place, the extraction step now also performs the `clean`-layer write itself (reusing the one flattened DataFrame) rather than flattening a second time in a separate validation step — `clean_<feed>` becomes a pure pass-through for these kinds, kept only for dbt source lineage's `AssetKey` stability.

**Caveat / generalizable lesson**: a from-scratch feed or a from-scratch platform is *expected* to have a blank `schema_registry` — this is not an error state, and nothing outside a real extraction run (not a seed script, not a build-time codegen step — see the neighboring dbt-modeling-patterns entry) should assume otherwise or depend on it being populated.

### A build-time codegen script must never depend on data only the pipeline populates at runtime

**Symptom**: `scripts/generate_deletion_synthesis_views.py` (generates `int_<feed>_with_deletes.sql`, run during `just start`, before any pipeline run ever happens) hard-crashed on a genuinely fresh platform: `ValueError: No current schema_registry entry for data_feed_id=...`.

**Cause**: this script read `schema_registry.column_definitions` to render an explicit column list for its generated model's `select`. That dependency was never actually necessary — masked for a long time by `seed_schema_registry()` (see the neighboring entry) hand-seeding a row this script could always find, right up until that hand-seeding was correctly removed.

**Resolution**: read the actual downstream consumer before assuming a codegen script needs live schema state at all. Here, the hand-written model consuming this generated view (`dbt/domains/sales/snapshots/sales_dim_customer.sql`) already names its own exact columns explicitly (`select customer_id, name, email, ...`) — the same way it already does selecting straight from staging — so the generated intermediate model never needed to know column names either; it was only ever a pass-through. Fixed by switching its `select <enumerated columns>` projections to `select *`, and deriving the one thing it genuinely needed (the business-key match/anti-join predicate) from `lakehouse_models.business_key_columns`, metadata already available the instant a user defines the model — no `schema_registry` involved at all.

**Generalizable lesson**: a codegen step that runs before `dbt parse` (i.e., before any pipeline run can possibly have happened) can only ever correctly depend on metadata that exists the moment a user finishes data entry — `lakehouse_models`/`data_feed` columns, not `schema_registry` or anything else the pipeline itself populates. If a generated model seems to need to enumerate columns by name, check whether the *downstream* hand-written consumer already names them explicitly before assuming the generated layer needs to — a wildcard `select *` passthrough is often sufficient and removes the runtime dependency entirely, which also matters directly for this project's "users only write business logic, boilerplate is auto-generated the instant metadata is entered" design goal: a codegen step gated on live pipeline state can't fulfill that promise until a feed has run once.

### Duplicating a metadata write path from memory instead of copying it verbatim drops a `NOT NULL` column

**Symptom**: `streaming/testing/run.py`'s own `schema_registry` write (kept separate from `PostgresMetadataResource.update_schema_registry` since `streaming-testing` has no real dependency on the orchestration package) failed live on its first real run: `psycopg.errors.NotNullViolation: null value in column "version" of relation "schema_registry"`.

**Cause**: the duplicated `INSERT` was reconstructed from memory/the general shape of the real write path, not copied from it — it correctly handled `is_current`/`column_definitions`/`primary_key_columns`, but silently omitted `version` (`NOT NULL`, computed inline as `coalesce(max(version), 0) + 1`, scoped per `(controlling_object_id, controlling_object_type)`) and `effective_to` entirely. Nothing caught this until the real INSERT actually ran against the real schema.

**Resolution**: copied the real `INSERT` from `postgres_metadata_resource.py::update_schema_registry` verbatim (same column list, same `coalesce(max(version)...)+1` subquery) instead of re-deriving it.

**Broader lesson**: when a genuinely standalone module (no real dependency on the package that owns the canonical write) needs to duplicate a write path rather than import it, copy the actual SQL/logic verbatim and comment *why* it's duplicated, rather than reconstructing "the shape of it" from memory — a `NOT NULL` column or a computed default (like a version counter) is exactly the kind of detail that's easy to drop when rebuilding from general understanding instead of the source, and nothing catches it until the write actually runs against the real table.

### A discovery/polling loop's iteration budget must match how much data was actually produced, not an arbitrary default

**Symptom**: `streaming/testing/run.py`'s schema-discovery step (`discover_source`) took ~200 seconds per source instead of finishing in seconds — the overall `setup` step appeared to hang (no log output for minutes, though the pod was genuinely `Running`, not crashed).

**Cause**: `discover_source`'s `sample_size` parameter (target message count before it stops polling early) defaulted to 20, but the message-seeding step that runs immediately before it (`seed_source_messages`) only ever produces 5 messages by default. Since only 5 messages could ever arrive, `len(messages) >= sample_size` could never become true, so the loop ran its full budget (`sample_size * 5` attempts × up to 2.0s poll timeout each ≈ 200s) every time, for every source, even though the real data it needed showed up in the first handful of polls.

**Resolution**: matched `discover_source`'s default `sample_size` to `seed_source_messages`'s default `count` (both 5) and shortened the per-poll timeout, so the loop breaks as soon as the actually-available messages are consumed instead of waiting out a budget sized for a larger batch that was never produced.

**Broader lesson**: any "poll until N items arrive, with a bounded attempt budget" loop has two independent knobs — how much data will realistically ever arrive, and how the loop's target/timeout are sized — and they have to be reasoned about together. A default that looks reasonable in isolation (sample up to 20 messages) can silently turn into a near-worst-case wait if the actual upstream producer's own batch size is smaller, and nothing errors — it just gets slow in a way that reads as "stuck," not "misconfigured."

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

### `dagster_k8s`'s `k8s_job_executor` + `K8sRunLauncher`: two real blockers on the very first live attempt, confirmed against the installed source

If combining `K8sRunLauncher` (one pod per Dagster *run*) with `k8s_job_executor` (splits every *step* of that run into its own further Kubernetes Job/pod, for finer-grained resource isolation between steps): budget for two separate, non-obvious blockers, neither of which shows up until the very first real run using this executor.

**Blocker 1 — symptom**: the run pod crashes immediately with `kubernetes.config.config_exception.ConfigException: Invalid kube-config file. No configuration found.`, thrown from `dagster_k8s/executor.py`'s own setup code (`isinstance(init_context.instance.run_launcher, K8sRunLauncher)`).

**Blocker 1 — cause, traced to the actual line via the installed source, not assumed**: merely *accessing* `instance.run_launcher` — which the executor's own init code does just to check its type — rehydrates a fresh `K8sRunLauncher` instance from its serialized config, and `K8sRunLauncher.__init__` unconditionally calls `load_kubernetes_config(load_incluster_config=self.load_incluster_config, ...)` at construction time, every time, regardless of who's constructing it or why. `load_incluster_config: false` is the *correct* value for the local `dagster dev` process that launches the initial run pod (it needs to load a local kubeconfig file, since it runs outside the cluster) — but the exact same static config value is what's mounted into the launched pod itself (`kubectl create configmap dagster-instance --from-file=dagster.yaml=... ` copies the file byte-for-byte), where `false` is wrong: a pod running *inside* the cluster has no local kubeconfig file to load, and needs `load_incluster_config: true` (the in-cluster service-account token path) instead. One static YAML value, two contexts that need opposite settings.

**Blocker 1 — why the obvious fix doesn't work**: `dagster_k8s/job.py`'s own `Field` declaration for `load_incluster_config` is `Field(bool, ...)` — a plain boolean, not a `StringSource`/`IntSource`-style field. Dagster's `{env: VAR_NAME}` config-templating syntax (the mechanism this project already uses elsewhere, e.g. `hostname: {env: POSTGRES_HOST}`, to resolve one shared config value differently per environment) only works for `*Source`-typed fields — it silently isn't available here. Making the in-pod copy differ from the local copy requires the file *content* itself to differ between the two, not a templated value resolved differently at read time.

**Blocker 2 — a separate, additional gate, only reachable once blocker 1 is fixed**: even with `load_incluster_config: true` correctly set in-pod, the run pod's own ServiceAccount needs real RBAC to actually call the Kubernetes API and create the per-step Jobs `k8s_job_executor` launches — `create`/`get`/`list`/`watch` on `batch/v1` Jobs (and `core/v1` Pods, to poll status). By default there is none: `kubectl get role,rolebinding -n <namespace>` on a freshly-bootstrapped namespace returns nothing beyond the cluster's own default bootstrap roles, and the `default` ServiceAccount every run pod uses has zero custom grants. Without this, blocker 1's fix alone still ends in a `403 Forbidden` the moment the executor tries to create its first per-step Job.

**A separate fact worth knowing when *deciding* whether to use this executor at all, confirmed via the same source read**: `k8s_job_executor` delegates *every* step of a run to its own separate pod unconditionally — this isn't a "only when it's worth it" optimization, and there's no built-in way to get per-step pods for *some* steps of a run but not others. A run with a single step still gets a second, separate pod spawned for that one step, on top of the run's own pod — worth knowing before assuming this executor is a free way to add resource governance to an already-small job.

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

**Second occurrence, same lesson, a different angle**: `raw_police_crimes` had the mirror-image bug — not "never writes to disk" but "does real work it shouldn't." It flattened the API's nested `location`/`outcome_status` structs into the schema-registry's flat shape *inside* the `raw` stage, and its own comment even framed that as a deliberate choice ("Genuine parsing, not a passthrough"), rather than recognizing it as a raw→clean responsibility. `raw` means a verbatim, zero-transformation dump — nothing downstream flagged this either, for the same reason: `clean_police_crimes` happily accepted already-flat rows and never checked whether `raw` had done more than a source-of-truth copy. Fixed by moving the flattening into `clean_police_crimes` and making `raw_police_crimes` write the untouched nested rows to `raw/police_crimes/run_id=<id>/crimes.parquet` (parquet, not CSV/JSON, specifically to survive the round-trip with nested struct columns intact). Same root cause as the first occurrence: a layer's documented contract ("no transformation" / "durable copy") isn't self-enforcing, and violating it silently doesn't break anything nearby enough to get caught by tests.

**Third occurrence, at the infrastructure level rather than a single asset**: `Roadmap.md`'s own "Kubernetes Hosting Model" table documented Dagster's webserver+daemon as a real in-cluster `Deployment` from early on ("Deployment (webserver/daemon) + Jobs (per-run op pods)"), and the repo-structure section even lists `orchestration/k8s/` as holding that Deployment's manifests. Neither was ever actually built — `dagster dev` has run locally on the host since Phase 5 (a deliberate, explicitly-noted dev-convenience decision *at the time*), and nobody revisited it afterward. The gap sat completely undetected for many phases: `orchestration/k8s/` exists on disk (so a directory listing looks like progress was made) but is entirely empty — scaffolded once, never filled in — and `kubectl get deployments -n orchestration` returns nothing. The Streamlit frontend has the identical gap, one level worse: it never even got a placeholder `frontend/k8s/` directory, despite the same hosting-model table listing one. **The generalizable check this confirms**: a scaffolded-but-empty directory, or a table in a design doc, is not evidence a component was actually built — verify against the live cluster (`kubectl get deployments/services -n <namespace>`) directly, the same way the first two occurrences above were only caught by re-reading the original design doc side-by-side with actual runtime behavior, not by trusting repo structure at a glance.

### A dbt model tagged with two feed tags gets claimed by two competing `@dbt_assets` defs

**Symptom**: `Definitions` construction fails with `DagsterInvalidDefinitionError: Duplicate asset key` for a model's `serve` view, even though the base model itself imports and builds fine on its own.

**Cause**: this project builds one `@dbt_assets` Python function per feed (`_build_dbt_assets_for_feed(feed)`, scoped via `select=f"tag:{feed}"`). A dbt node tagged with *two* feed tags matches both selections, so it gets declared as an asset by two separate `@dbt_assets` defs in the same code location — Dagster rejects that outright, it doesn't just pick one. The base model (`fct_daily_financial_activity.sql`) was tagged with only one feed by design, but `generate_serve_views.py` independently derived its generated `_latest`/`_historical` views' tags from *every* feed in `depends_on_feeds` — reintroducing the exact problem the base model's single tag was chosen to avoid, in a different file the fix didn't touch.

**Resolution, first pass (superseded)**: a multi-feed model gets exactly one dbt tag, chosen as the alphabetically-first `depends_on_feeds` member so the base model and the derived serve views agree. This worked, but the agreement itself was still just two hand-written values (one per file) that had to happen to match a rule stated only in comments — the exact kind of thing that silently drifts the next time either file gets touched by someone who hasn't read both comments.

**Resolution, real fix**: added `lakehouse_models.owning_feed_id` (`metadata/db/init/01_platform_metadata.sql`) — a real, required (`not null`) foreign key to `data_feed`, separate from `depends_on_feeds`. `depends_on_feeds` keeps its original meaning (every feed this model genuinely depends on, for gating/`updates_enabled` sourcing); `owning_feed_id` answers a different, narrower question — which single feed's `@dbt_assets` Python function is allowed to claim this model's `AssetKey` — and is now the *one* place that decision is made. The dbt model's own `tags=[...]` and `generate_serve_views.py`'s generated tag both read this same column (`scripts/seed_metadata_db.py`'s `seed_lakehouse_model()` resolves it; `generate_serve_views.py`'s `fetch_lakehouse_models()` joins on it directly) instead of independently guessing. Considered and rejected: collapsing to one `@dbt_assets` function for the whole dbt project (removes the ownership-conflict bug class structurally, which is dagster-dbt's own recommended pattern for this scenario) — but `@dbt_assets`' `pool=` (this project's per-feed concurrency guard against two runs of the same feed racing on shared Iceberg writes) is a single value for the whole Python function, and `DagsterDbtTranslator` has no per-model override for it (confirmed via `inspect.signature`) — collapsing would force every feed's dbt work onto one global pool, a real regression, not a fix. Real Dagster-level cross-feed dependencies are still preserved via `AssetSelection.groups(feed).upstream()` on every generated per-feed job (pulls in whatever a group's assets actually depend on from other groups, a no-op for feeds with no cross-feed dependents) — unrelated to `owning_feed_id`, this was already correct from the first pass. Caught by actually building `Definitions` end to end (`just smoketest`), not by unit-testing either file in isolation.

### A `@schedule` function requesting a resource must declare it as a named parameter, not read `context.resources`

**Symptom**: `ScheduleDefinition.__call__(context)` (the direct-invocation testing path, via `build_schedule_context`) raises `TypeError: _fn() got an unexpected keyword argument 'postgres_metadata'` even though the same function has `required_resource_keys={"postgres_metadata"}` and reads `context.resources.postgres_metadata` inside its body.

**Cause**: `ScheduleDefinition.__call__` always injects required resources as keyword arguments matching the decorated function's own parameter names (confirmed by reading the actual implementation) — it doesn't just pass `context` and let the function reach into `context.resources`. Declaring a resource in `required_resource_keys` *and* accepting it as a named parameter is also rejected outright: `"Cannot specify resource requirements in both @schedule decorator and as arguments to the decorated function"`.

**Resolution**: declare the resource as a plain named parameter (`def _fn(context, postgres_metadata: PostgresMetadataResource):`) and drop `required_resource_keys=` from the `@schedule(...)` call entirely — Dagster infers the requirement from the parameter name. Worth checking this kind of API detail against the actual installed version's source directly (`inspect.getsource`) rather than assuming from general Dagster familiarity, since this behavior isn't the first thing documentation examples show.

### A GraphQL-submitted run only queues — the k8s Job appears a moment later, asynchronously

**Symptom**: `kubectl wait --for=condition=complete job/dagster-run-<id>` immediately fails with `Error from server (NotFound)`, right after `DagsterGraphQLClient.submit_job_execution(...)` successfully returned that same run id.

**Cause**: submitting a run via GraphQL (needed here since `dagster job launch`'s CLI has no `--tags` flag) only queues it — same as any other run submission, the **daemon** picks it up and calls the launcher to create the actual k8s Job a moment later (see the neighboring "sits in QUEUED forever" entry above for the general mechanism). `verify-pipeline`'s existing `dagster job launch` already accounts for this with a before/after job-name-diffing retry loop; a new recipe built around the GraphQL client needs the identical wait, not a direct `kubectl wait`.

**Resolution**: poll `kubectl get job/<name>` in a bounded retry loop until it exists, *then* `kubectl wait --for=condition=complete` on it — the same two-phase pattern, not a one-shot wait.

### A dbt-source-to-asset mapping is a hand-maintained set — forgetting to add a new feed breaks the dependency, not the import

**Symptom**: `dbt build` for a newly added feed (`metadata_runs`) failed with `TrinoUserError: Table 'iceberg.clean.metadata_runs' does not exist`, even though the upstream `clean_metadata_runs` asset had already run and reported success (`is_clean_successful=true`, real row counts) earlier in the *same* run.

**Cause**: `dbt_assets.py`'s `DataPlatformDbtTranslator.get_asset_key()` maps a dbt source node (`clean.<feed>`) onto the same `AssetKey` its Python `clean_<feed>` asset produces — but only for feed names listed in the hand-maintained `_CLEAN_SOURCE_TABLES` set. `metadata_runs` was added as a new connector-driven feed without adding it here, so Dagster never wired a real dependency between `clean_metadata_runs` and `stg_metadata_runs`' dbt build — the dbt step could (and did) start racing ahead of the write it actually depended on, timing-dependent rather than reliably ordered.

**Resolution**: add every new feed with a `stg_<feed>` model to `_CLEAN_SOURCE_TABLES`. The set's own comment already says "Add an entry here when a new feed's staging model is added" — this was simply missed once. No code change needed beyond the one-line addition; this is a process gap, not a design flaw, worth double-checking explicitly whenever a new feed is wired in.

### `path:` dbt selectors silently match zero nodes inside `@dbt_assets(select=..., exclude=...)`, even when the identical string works via the real dbt CLI

**Symptom**: `@dbt_assets(select=f"tag:{feed},path:models/serve/generated/*")` printed `The selection criterion '...' does not match any enabled nodes` at Dagster code-server startup, and the resulting asset silently did nothing (no dbt nodes ever selected, but no error either — `data_processing_runs`' stage tracking has nothing to report either way, so a genuinely broken selector and a legitimately-empty one look identical downstream). Running the *exact same selector string* via `dbt ls --select "tag:sales,path:models/serve/generated/*"` directly against the same project returned the correct 8 nodes.

**Cause**: `dagster_dbt`'s `@dbt_assets` decorator resolves `select=`/`exclude=` into AssetSpecs at Python import time via `dagster_dbt.utils._select_unique_ids_from_manifest()` — confirmed by reading the installed source directly. This function builds a **synthetic `Manifest` object in-process, directly from the parsed JSON dict**, with no real project root/config context. `path:` selection fundamentally needs to resolve a relative filesystem path, which this stripped-down context can't do, so it always silently matches nothing — regardless of whether the path string itself is correct (confirmed the hard way: an initial "fix" that changed the path string based on the manifest node's own `path` field, which turned out to be relative to `model-paths` rather than the project root, *also* failed identically here despite being validated correct via `dbt ls`). The real dbt CLI (`dbt ls`, `dbt build`) is a completely different code path with genuine project context, so it resolves the same selector correctly — making `dbt ls` an unreliable way to validate a selector that will actually be used inside `@dbt_assets(select=...)`.

**Resolution**: use `tag:` selectors instead of `path:` for anything evaluated inside `@dbt_assets`'s `select=`/`exclude=` — pure attribute matching against `node.tags`, no filesystem/project-root resolution needed, so it works identically in both the in-process and CLI code paths. If a `path:`-based selector ever needs validating again, don't trust `dbt ls --select` alone — it validates the CLI code path, not the one `@dbt_assets` actually uses.

### Warnings printed at Python import time can silently corrupt a bash `$(...)` capture

**Symptom**: `run_id=$(uv run python -m dagster_data_platform.trigger_schedule_run --feed police_crimes)` followed by `kubectl get job/dagster-run-$run_id` reported "job never appeared" — but `kubectl get jobs` showed the real job had been created and completed successfully within seconds of submission. Not a timing issue at all, confirmed by watching `kubectl get jobs` in a tight concurrent loop through the same failure.

**Cause**: `trigger_schedule_run.py` imports `pipeline_generated.py`, which (per the entry above) evaluates every feed's `@dbt_assets` `select=`/`exclude=` at import time — including feeds like `police_crimes` that genuinely own zero `lakehouse_models` rows, for which the selector legitimately (and correctly) matches nothing. dbt prints that "does not match any enabled nodes" warning straight to **stdout**, not a Python logger and not stderr, at the moment the module is imported — before `main()` even runs. A bare `run_id=$(...)` capture swallows that warning text along with the real `print(run_id)` at the end, producing multi-line garbage that no longer matches any real job name.

**Resolution**: pipe the capture through `tail -1` — `print(run_id)` is always the script's last line of output for a feed-type schedule (exactly one `RunRequest`). More generally: any script whose only contract is "print exactly one value to stdout" is at risk of this the moment it imports something with import-time side effects that also write to stdout — worth being explicit about capturing only the last line rather than assuming stdout will only ever contain what the script itself intentionally prints.

### A feed whose source is the pipeline's own run history needs an explicit "run started" step — it can't rely on a downstream stage to create that record

**Symptom**: `metadata_runs` (a feed that queries `data_processing_runs`, this platform's own run-tracking table, as its source) reliably had nothing to report on a genuinely fresh cluster's very first run — `landing_metadata_runs` legitimately saw zero rows (no run had ever completed yet to populate the table it queries), so `clean_metadata_runs` correctly skipped writing `clean.metadata_runs`, and the downstream dbt build failed outright with `TABLE_NOT_FOUND` rather than gracefully handling an empty first run.

**Cause**: every stage's `data_processing_runs` row was only ever created as an incidental side effect of the *first* stage that happened to call `log_data_feed_stage(...)` (via its internal `_ensure_run()` upsert) — there was no dedicated, guaranteed-first step whose actual job was "register that a run has started," independent of whether any particular feed's extraction logic ran yet. For a feed like `metadata_runs` that reads its own platform's run history, this mattered concretely: by the time its own `landing` stage ran, other feeds in the *same* run may or may not have already inserted their own rows, depending purely on Dagster's scheduling order — an accident of timing, not a designed guarantee.

**Resolution (original)**: added `PostgresMetadataResource.record_run_started()`, called explicitly as the very first action of a new `pipeline_init_<feed>` asset that ran before any extraction logic — every feed's run got registered up front, deterministically, not as a side effect of whichever stage happened to log first.

**Superseded 2026-07-16** by the master pipeline redesign (README.md "Master Pipeline Architecture"): `pipeline_init_<feed>` no longer exists. `record_run_started()`/`record_model_run_started()` are now called exactly once, inside `master_pipeline`'s own `run_master_pipeline` op — the single guaranteed-first step for *every* trigger path, feed- or domain-scoped alike — before it launches any child job. Every child job (`EXTRACTION_JOBS[feed]`/`MODELING_JOBS[domain]`/`SERVING_JOBS[domain]`) now does a plain `UPDATE` against the row the master already created (`PostgresMetadataResource._find_run()`, no upsert) rather than being able to create its own row — the guarantee got *stronger*, not just relocated: a child job launched without the master having run first now fails loudly (`ValueError`, "did the master pipeline run first?") instead of silently creating its own row out of order.

**Fourth occurrence, same lesson, a different trigger path**: found 2026-07-16 while implementing the redesign above, before any test caught it — `financial_transactions_sensor` (`financial_assets.py`) fired its feed's job directly (`job=FEED_JOBS[FEED_FRIENDLY_NAME]`), bypassing the master pipeline entirely. Under the new design this would crash every sensor-triggered run outright, since only `master_pipeline` creates the row `EXTRACTION_JOBS["financial_transactions"]` now expects to already exist. **This is also tech debt in the test suite, not just the code**: the sensor is `DefaultSensorStatus.STOPPED` by default, and neither `just smoketest` nor `verify-pipeline`/`verify-schedule` ever turns it on — so this exact bug class (a trigger path that skips the guaranteed-first step) can reappear again in the future and the existing test suite would not catch it. Fixed by having the sensor fire `master_pipeline` itself (`orchestration_kind="batch_group"`, resolving this feed's own `batch_group_friendly_name` live), same as every schedule. See Backlog.md for the still-open follow-up (exercising the sensor-triggered path live, not just schedules, as part of the verify-pipeline rework).

**Generalizable lesson**: if any part of a pipeline reads its own operational metadata as a data source, or if any part of a pipeline's *triggering* mechanism (schedule, sensor, manual launch) can independently start a child job, don't assume "some other path already guarantees the record exists" — that's an assumption about every other trigger path in the system agreeing to route through the same guaranteed-first step, and nothing enforces it structurally beyond code review. When a pipeline gains a new trigger path, explicitly check it starts at the same guaranteed-first step as every other one — and add it to the test suite, since an untested trigger path (like a sensor left `STOPPED` by default) is exactly the kind of gap that survives a passing `just smoketest` indefinitely.

### Two independently-named things sharing one flat `group_name` namespace can silently cross-contaminate `AssetSelection.groups()`

**Symptom**: a job meant to contain only a business domain's dbt transformation/serving steps (`AssetSelection.groups(domain)`) silently also included one of that domain's member feeds' own landing/raw/clean assets — and, symmetrically, that feed's own extraction-only job (`AssetSelection.groups(feed).upstream()`) silently also included the domain's dbt assets, plus (via `.upstream()`) other unrelated feeds' extraction chains those dbt assets happened to depend on. Neither failure raised an error; both jobs just quietly ran more/different steps than intended — discoverable only by inspecting the actual op list (`dagster job list`), not from any error message or test failure.

**Cause**: two different concepts in the same asset graph (here: "which feed" and "which business domain, a coarser grouping that legitimately spans several feeds") were both expressed via Dagster's plain `group_name` string. `AssetSelection.groups(<name>)` has no way to know which axis a given string is meant to select on — it matches *any* asset whose `group_name` equals that string, full stop. A coincidental name collision (a domain literally named the same as one of its own member feeds — not contrived: an ODS-style domain that defaults its name from its own owning feed's batch group is a legitimate, common case, not an edge case) means both axes' selectors silently pick up each other's assets.

**Resolution**: namespace the two axes explicitly rather than relying on their name strings never colliding — prefix one axis's `group_name` (e.g. `f"domain_{domain}"`) so the two string spaces can never overlap, and make every selector meant to target that axis consistently use the same prefixed form.

**Generalizable lesson**: if two different groupings in the same system share one flat namespace for their identifiers, a same-name collision between them isn't a "won't happen in practice" edge case — it silently produces wrong-but-not-erroring behavior, exactly the kind of bug that survives a passing test suite. Namespace-prefix by construction rather than trusting names not to collide, whenever two independently-chosen name spaces feed the same selection/lookup mechanism.

### An asset returning `Output(None)` can't be consumed as a typed downstream parameter — order-only dependencies need `deps=`, not a parameter

**Symptom**: `TypeError: clean_customers() missing 1 required positional argument: 'raw_customers'`, even though `clean_customers(..., raw_customers: None)` clearly declares that parameter and `raw_customers` (the upstream asset) had already run successfully in the same job.

**Cause**: `raw_customers` returns `Output(None, metadata={...})` (README.md "Master Pipeline Architecture" — raw's real durable output is the parquet file on disk, not an in-memory value; clean reads that file back itself rather than accepting a passed value, matching the "read storage from the previous layer" principle applied uniformly). Declaring the downstream asset's `raw_customers` parameter with a `None` type annotation was meant purely to establish execution ordering, not to receive a real value — but Dagster's IO manager treats an upstream `Output(None)` as "nothing to load," and doesn't pass anything at all for that parameter, rather than passing `None` itself. Python's own call then fails on a required positional argument with nothing supplied.

**Resolution**: use `@asset(deps=["raw_customers"])` and drop the parameter from the function signature entirely, for any dependency that exists purely to sequence execution, not to receive a value. This is the documented, correct way to express an order-only dependency in Dagster — confirmed via `inspect.signature(asset)` that `deps=` accepts plain asset-name strings. Caught live via the very first real `master_pipeline` end-to-end run (not by any test — `test_dbt_assets.py`/`test_schedules.py` never actually materialize these assets), across both the hand-written `extraction_assets.py`/`sales_assets.py` and every connector-generated `clean_<feed>` asset (`scripts/generate_dagster_pipeline.py`).

### Mounting a ConfigMap directly as `DAGSTER_HOME` crashes at startup — Dagster writes into it, and ConfigMap volumes are read-only

**Symptom**: `dagster-webserver`/`dagster-daemon` pods crash-loop immediately with `OSError: [Errno 30] Read-only file system: '/dagster_home/.telemetry'`, even though `dagster.yaml` itself was valid and successfully mounted.

**Cause**: Dagster's telemetry (and NUX/first-run-experience) subsystems write into `$DAGSTER_HOME` at process startup regardless of what `dagster.yaml` says — this isn't gated behind any config short of setting `telemetry: enabled: false`, which still doesn't cover every write path (NUX writes independently). A Kubernetes ConfigMap-backed volume is mounted read-only by design; there is no config flag on the ConfigMap volume itself to make it writable.

**Resolution**: mount the ConfigMap read-only at a separate path (e.g. `/dagster-config`), point `DAGSTER_HOME` at a writable `emptyDir` volume instead, and add an `initContainer` that `cp`s the ConfigMap's files (`dagster.yaml`, `workspace.yaml`) into the writable `emptyDir` before the main container starts (`orchestration/k8s/webserver-deployment.yaml`/`daemon-deployment.yaml`). `dagster-code-server` is unaffected — it never sets `DAGSTER_HOME` at all, since introspecting/serving a code location doesn't touch instance storage.

### `jobs/status` is a separate RBAC resource from `jobs` itself

**Symptom**: every `master_pipeline` run failed with `dagster_k8s.client.DagsterK8sUnrecoverableAPIError` wrapping a `403 Forbidden`: `"jobs.batch \"dagster-run-...\" is forbidden: User \"system:serviceaccount:orchestration:default\" cannot get resource \"jobs/status\" in API group \"batch\""` — this was **after** already granting `get`/`list`/`watch`/`create`/`delete` on plain `jobs` in the Role, which looked like it should have been sufficient.

**Cause**: Kubernetes RBAC treats a resource's `/status` subresource as a distinct grantable resource, not implied by permissions on the base resource — `dagster_k8s`'s `MonitoringDaemon` (which periodically calls `read_namespaced_job_status` to check run-worker health, independent of the normal run-completion path) needs its own explicit rule.

**Resolution**: add a second rule granting `get` on `resources: ["jobs/status"]` in the same `apiGroups: ["batch"]` block (`orchestration/k8s/rbac.yaml`). Generalizable: whenever a workload hits a 403 for a resource name containing a `/`, that's a subresource requiring its own RBAC rule — granting the base resource's plural name is never enough.

### Rebuilding and reloading an image into kind does not restart an already-running Deployment pod — a structural code change needs an explicit rollout restart

**Symptom**: after fixing a real bug in asset code (the `deps=` fix above) and rebuilding the image (`kind load docker-image ...`), a freshly-submitted run still failed with the *exact same* stale error — the fix appeared not to have taken effect at all.

**Cause**: `kubectl apply`'s diffing is against the Deployment *spec*, and the spec's `image: data-platform-orchestration:latest` field text never changed — only the image *content* behind that tag did. Kubernetes has no reason to recreate a pod whose spec didn't change, so `dagster-code-server` (the long-running process that reports the asset graph's structure to the webserver/daemon) kept running the old image's code indefinitely. A brand-new *run* pod (launched fresh per-run by `K8sRunLauncher`) does pull the new image content correctly — but the *execution plan* for that run is computed from whatever `dagster-code-server` currently reports, so a stale code-server can hand out a stale plan even to a run whose own pod has the fix.

**Resolution**: `kubectl rollout restart deployment/dagster-code-server` after every image rebuild, added unconditionally to `orchestration/module.just`'s `start` recipe — not worth trying to distinguish "this was a structural change" from "just asset body logic" in an automated script; the restart is cheap and the alternative is a confusing stale-behavior bug that looks exactly like the original fix didn't work.

### BSD/macOS `pgrep -f` does not support `\|` alternation the way GNU `grep`/`pgrep` does — a false "process not running" negative

**Symptom**: `pgrep -fl "dagster dev\|dagster api grpc\|dagster_webserver\|dagster._daemon" | grep -v grep || echo "no local dagster processes remaining"` printed the "remaining" fallback message — but a local `dagster dev` process (predating this session's move to in-cluster Dagster) was actually still running, over an hour old, and kept colliding with the new in-cluster daemon over heartbeat ownership in the same shared `dagster_db` (`"Another X daemon is still sending heartbeats"` — the exact symptom this project's own kill-between-phases rule exists to prevent, see the neighboring `kill` process-management entries in this section).

**Cause**: BSD `pgrep` (macOS's default, distinct from GNU `pgrep` on Linux) does not interpret `\|` as regex alternation inside a `-f` pattern the way GNU tools do — the pattern silently matched nothing instead of erroring, so the command's own `||` fallback fired and reported a false negative. A single-string `pkill -f "exact command substring"` (no alternation) run earlier in the same investigation genuinely worked and appeared to confirm the process was dead — but the confirmation check itself never actually re-verified anything.

**Resolution**: never rely on `\|` alternation in a `pgrep`/`pkill -f` pattern on macOS — either run one `pgrep -f` call per pattern in a loop (this project's `orchestration::kill` recipe already does exactly this, iterating a bash array) or use `pgrep`'s own `-x`/multiple `-f` invocations rather than a single combined regex. More generally: a verification command whose own correctness depends on regex dialect compatibility is itself worth distrusting on macOS specifically — confirm a process is dead via a plain, unambiguous `ps -p <pid>` check when in doubt, not a cleverer combined pattern.

### The full in-cluster Dagster topology: three separate Deployments, not one relocated `dagster dev`

Real production Dagster deployments (and now this project's own, see README.md "Master Pipeline Architecture") split into three independent pieces rather than `dagster dev`'s single collapsed process:
- **`dagster-code-server`**: a gRPC server (`dagster api grpc`) that imports the actual pipeline code (`dagster_data_platform.definitions`) and serves introspection/execution requests. Needs the full set of in-cluster hostnames (Postgres/Trino/Polaris/MinIO) since importing the module constructs resource config objects, even though it never connects them itself.
- **`dagster-webserver`**: serves GraphQL/UI, talks to the code-server over gRPC (`workspace.yaml`'s `grpc_server:` entry) rather than importing pipeline code directly. Only needs Postgres (for `dagster_db` itself).
- **`dagster-daemon`**: the background loop (schedules, sensors, picking up QUEUED runs) — the one piece that actually calls `K8sRunLauncher.launch_run()`, and so the only one that needs `load_incluster_config: true` plus the RBAC grants above (confirmed via the earlier `k8s_job_executor` investigation that this field can't be templated per-consumer with `{env:}`, hence two full sibling `dagster.yaml` files rather than one shared, parameterized one).

This split was forced, not a style choice: `master_pipeline`'s own op calls back into the webserver's GraphQL API to launch its child jobs from *inside* its own pod, and there was no cluster-addressable webserver for it to reach before this existed — see the neighboring "A feed whose source is the pipeline's own run history..." entry's fourth occurrence and Progress.md's Phase 14 section for the full arc from `dagster dev` on the host to this topology.

### Per-job container image overrides via the `dagster-k8s/config` tag — no separate code locations needed

**Problem this solves**: metadata-driven per-domain Docker images (README.md "Master Pipeline Architecture") need each domain's `MODELING_JOBS[domain]`/`SERVING_JOBS[domain]` to launch from its own narrower image (`data-platform-domain-<domain>`), while every other job (`EXTRACTION_JOBS`, `master_pipeline`) keeps using the one shared `data-platform-orchestration` image, all from the *same* single code location/`Definitions` object.

**Confirmed against the installed `dagster_k8s` source, not assumed**: `K8sRunLauncher.launch_run()` resolves the image for a launched pod via `container_context.get_k8s_job_config(job_image=repository_origin.container_image, run_launcher=self)`, and inside `construct_dagster_k8s_job()`, a per-job `dagster-k8s/config` tag's `container_config.image` is popped out and takes priority over both the launcher's static `job_image` config and the code location's own `container_image`: `job_image = container_config.pop("image", job_config.job_image)`. So a plain job-level tag — `tags={"dagster-k8s/config": {"container_config": {"image": "data-platform-domain-sales:latest"}}}` on `define_asset_job(...)` — is enough to override the image for just that one job's launched runs, with zero changes to `K8sRunLauncher`'s config or `workspace.yaml`. No need for one gRPC code-server per domain (which would also require partitioning the asset graph itself across multiple code locations — a much bigger change).

**The real remaining problem, and how it was solved**: even with the image override working, every job's run pod re-imports the *whole* `dagster_data_platform.definitions` module fresh to resolve its own execution plan — and `pipeline_generated.py` used to eagerly construct a `DbtProject`/dbt-assets pair for *every* domain at that import time, meaning even a job tagged to run from a narrower single-domain image would still crash trying to construct every *other* domain's `DbtProject` against a manifest that image doesn't have. Fixed by gating the per-domain construction loop on `target/manifest.json` actually existing on disk (not just `dbt_project.yml`), and having every downstream dict (`TRANSFORMATION_ASSETS`, `SERVING_ASSETS`, `MODELING_JOBS`, `SERVING_JOBS`) iterate only the domains present in the previous dict, rather than the full `DOMAIN_FEEDS` set baked in from Postgres. A domain absent from one particular image's `DBT_PROJECTS` is simply absent from that image's job/asset dicts too — which is fine, since that pod was only ever launched to run one specific domain's job. Verified live, not just by inspection: `kubectl get pods -o json` on a real run confirmed every `*_modeling_job`/`*_serving_job` pod's `spec.containers[0].image` was its own domain's image, while every `extraction_*_job`/`master_pipeline` pod stayed on the shared image.

**Docker-side counterpart**: same `Dockerfile`, one different build arg (`--build-arg DOMAIN=<name>`) — the `dbt parse` loop only runs for that one domain when `DOMAIN` is set, leaving every other domain's directory present in the image (unchanged `COPY` layer, so Docker's build cache is shared across every domain's build) but with no `target/manifest.json`, which is exactly what the Python-side gating above expects.

### Programmatically starting/stopping a sensor: no client-library shortcut, and its cursor outlives the test that set it

**Problem**: verifying `financial_transactions_sensor`'s real end-to-end path (not just its evaluation logic, which `build_sensor_context` already covers with no daemon involved) needs to actually start the sensor, wait for a live daemon tick, then stop it again — `DagsterGraphQLClient` has no `start_sensor`/`stop_sensor`/`list_runs` convenience methods (confirmed via `dir()`), unlike `submit_job_execution`/`get_run_status`.

**Resolution**: its own internal `_execute(query, variables)` (the same private method every one of its public methods calls) works fine for hand-written GraphQL directly — confirmed the exact mutation/query shapes against the installed `dagster_graphql` schema source rather than guessing: `startSensor(sensorSelector: SensorSelector!)`, `stopSensor(id: String!)` (takes the sensor **state id**, resolved via a separate `sensorOrError(sensorSelector) { sensorState { id } }` query first — a different shape from `startSensor`, not symmetric), and `pipelineRunsOrError(filter: {pipelineName, createdAfter})` to find the specific run a sensor tick launched without knowing its run id ahead of time.

**Two real bugs found live while building this, both in the test script itself, not the platform**:
1. A dropped test CSV written as a genuinely empty (0-byte) file crashed `CSVConnector.fetch()`'s `pl.read_csv()` with `NoDataError: empty CSV` — the sensor's own file-presence check doesn't care about content, but the real extraction step downstream does. Fixed by writing a header-only CSV (a valid, zero-row file) instead of truly empty.
2. **The sensor's cursor is real, persisted state in `dagster_db`, independent of any single test run** — a second test run reusing the same hardcoded test filename compared equal to the cursor already left behind by the first run and was correctly (from the sensor's own logic's perspective) skipped as "not new," never firing at all. Worse: an even earlier attempt used an artificial far-future filename (`transactions_99991231_...`) to guarantee "newness," which succeeded once but left the cursor permanently parked past every real date — silently breaking the *real* sensor's ability to ever detect a genuine future file again, since any normal timestamp sorts before `9999`. Fixed two ways: (a) generate a genuinely unique, realistic filename per invocation (the real convention already embeds a timestamp, `transactions_<YYYYMMDD_HHMMSS>.csv` — reusing it guarantees freshness without resorting to an artificial value), and (b) manually cleared the already-poisoned cursor via a direct `setSensorCursor(sensorSelector, cursor: null)` call before retesting. **Generalizable lesson**: any test that toggles a sensor/schedule's live state (cursor, running status) is mutating something that outlives the test process — verify it's back in a real, sane state afterward, not just that the test itself reported success, and never use an artificial "obviously in the future" placeholder value for state a real process will keep comparing against indefinitely.

### Confirmed live: `dagster-webserver`/`dagster-daemon` never need their own restart, only `dagster-code-server` does

**Question this settles**: `orchestration::start`'s rollout-restart only targets `dagster-code-server`, not `dagster-webserver`/`dagster-daemon`, even though all three run from the same image. Was that a real gap (could a webserver/daemon-only code change silently never take effect) or genuinely unnecessary?

**Confirmed live, not just by reading `workspace.yaml`'s `grpc_server` config**: added a one-line, uniquely-tagged `context.log.info(...)` probe to `wake_sleep_sensor.py`'s `_sleep_if_no_other_runs_in_flight()`, rebuilt the image, restarted *only* `dagster-code-server` (`dagster-daemon`'s pod was never touched — same pod, same age, throughout), then triggered a real `master_pipeline` run via `verify-sensor` while streaming `dagster-code-server`'s own logs to a file (necessary because KEDA scales that Deployment back to 0 shortly after use, taking its `kubectl logs` history with it — capture the stream live, don't try to fetch it after the fact). The probe line appeared in `dagster-code-server`'s log output, correctly attributed to `master_pipeline_sleep_on_success`, and the daemon correctly received and acted on the result via gRPC — despite never running the updated code itself.

**Why this is architecturally guaranteed, not incidental**: `workspace.yaml` configures a `grpc_server` code location (not `python_file`), so every definition lookup and sensor/schedule *function body* evaluation is a gRPC call to `dagster-code-server`, executed inside *its* process. `dagster.yaml`/`dagster-incluster.yaml` (webserver/daemon's own instance config) reference exactly one custom class, `dagster_k8s.launcher.K8sRunLauncher` — a third-party class, not anything from this repo. And actual op/asset execution never happens in the long-lived webserver/daemon pods at all — `K8sRunLauncher` launches a brand-new pod per run, pulling whatever image is current at that moment. Net result: webserver and daemon never import or execute this repo's own Python in their own process, for anything — `orchestration::start`'s code-server-only restart is correct as written, not a gap.

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

### A local-path dbt package dependency for shared macros works on a project's first build, then silently breaks

**Symptom**: splitting one dbt project into several compile-isolated projects that need to share common macros (`row_hash`, `classify_changes`, `generate_schema_name`, adapter-dispatch overrides), installed as a local-path package dependency (`dependencies.yml`: `packages: - local: ../../_shared`), looked correct at every static check — `dbt deps` installed it cleanly, `dbt parse` succeeded with zero macro-resolution errors, and even the *first* real `dbt build`/`dbt compile` against a fresh, never-before-built target table succeeded, correctly calling every shared macro. A *second* build of the *same* already-existing table then failed non-deterministically (varying which macro/node hit it first) with `'<macro>' is undefined` — even with `--full-refresh` and a fully wiped `target/`/`dbt_packages/` dir, which rules out partial-parse or install-cache staleness as the cause.

**Cause, confirmed via the installed dbt-core source, not assumed**: one specific macro family — `generate_schema_name`/`generate_alias_name`/`generate_database_name` — goes through a genuinely different, special-cased resolver (`dbt/parser/base.py::RelationUpdate`, `dbt/contracts/graph/manifest.py::find_generate_macro_by_name`) that explicitly filters to `Locality != Imported` when resolving a project's own nodes. An installed package's own definition of one of these three macros only ever applies to *that package's own* model nodes — never borrowed by the root project that installed it as a dependency, confirmed directly from source, not inferred from behavior. Plain macros (`row_hash`) and adapter-dispatch macros (`trino__current_timestamp`) are *not* part of that special-cased family and go through the ordinary `MacroResolver` path, which does search installed packages — yet they exhibited the exact same "works once, breaks on rebuild" symptom in direct testing, both locally and inside the actual Docker image used for real pipeline runs. The plain-macro case's root cause was not further isolated (not worth the cost for a project this size once the practical fix was confirmed) — the two failure modes may or may not share a single underlying mechanism.

**The fix that worked, confirmed empirically against a live k8s cluster, not just locally**: don't install shared macros as a package dependency at all. Physically copy the macro files into each consuming project's own `macros/` directory (giving them root-project locality), from one canonical source directory a human edits once. Do this as a build-time codegen step that re-copies unconditionally on every run (not a one-time scaffold-and-forget) so every consumer's copy stays current — this removes the need for `dbt deps`/`dbt_packages/` for these macros entirely.

**Generalizable lesson**: a dbt local-path package dependency passing `dbt deps` + `dbt parse` + even one successful `dbt build` is not sufficient evidence that macro sharing actually works — the failure here only manifested on a *second* build of an *already-existing* table. If sharing macros across multiple dbt projects, test a rebuild of an already-materialized model before trusting the approach, not just a fresh-table first build.

### Generalizing a per-feed hand-written model into codegen — wait for a real second consumer, or an explicit reason not to

If a single hand-written model captures a genuinely per-feed pattern (e.g. a deletion-synthesis intermediate model, parameterized in spirit by a feed's business key and tracked columns, but only ever built for one actual feed so far): resist generalizing it into a metadata-driven codegen step *purely on spec* — with a sample size of one, it's easy to generalize the wrong axis (this project's original hand-written deletion-synthesis model carried an extra passthrough column, `updated_at`, that wasn't part of either the business key or the tracked-attribute set used for change detection — easy to miss as a distinct category if designing the generalized shape from imagination rather than from what the one real example actually needed). When there's a concrete reason to generalize anyway (an explicit ask, not just "this might get reused someday"), the safest path is a codegen script matching whatever pattern the project already uses for its other metadata-driven generation (here: the same shape as the existing serve-view generator — a build-time Python script reading Postgres, rendering one `.sql` file per row into a `generated/` directory, cleared and rebuilt every run) — and the acceptance test is that it reproduces the one known-good hand-written example byte-for-byte (logically) before trusting it for a feed that doesn't exist yet.

### A model tagged into the wrong dbt selector silently joins a build graph it has no business being part of

**Symptom**: `sales_modeling_job` — the ordinary batch transformation step, with no logical connection to streaming — failed with `TrinoUserError: TABLE_NOT_FOUND` trying to build `serve.sales_events`/`serve.inventory_events`, whose source Iceberg tables only exist once Flink has done its first checkpoint.

**Cause**: `_build_transformation_assets_for_domain` (`dbt_assets.py`) selects its build set by `exclude=tag:serving_layer` alone — everything in a domain's manifest *except* the generated `_latest`/`_historical` views. Streaming serve views were tagged `['sales', 'streaming']`/`['streaming']`, neither of which matched that exclusion, so they were swept into the regular batch build by default rather than by intent. `dbt`'s tag-based node selection is opt-out by default for any selector built as an exclusion list — a new tag on a model doesn't automatically keep it out of an existing job unless something excludes that specific tag.

**Resolution**: gave streaming serve views their own dedicated exclusion tag (`tag:streaming`, `dbt_assets.py`'s `_STREAMING_TAG`) and added it to the transformation job's exclude selector (`exclude=f"{_SERVING_LAYER_TAG} {_STREAMING_TAG}"` — dbt selector strings union space-separated terms). Also stamped two more granular tags per streaming view (`streaming_<model_schema>`, `streaming_<table_name>`) for future per-domain/per-object selection — prefixed, not the bare model_schema/table_name, for the same reason `domain_group_name()` already prefixes with `domain_` (`dbt_assets.py`): a bare model_schema tag would collide with a same-named feed's own per-feed tag (the `sales` domain vs. the `sales` feed both already exist in this project).

**Broader lesson**: whenever a new model category gets added to a shared dbt project/manifest, check what it inherited by default from every *existing* selector in the build graph (both `select=` and `exclude=` sides), not just what selector the new category itself needs — an `exclude=`-based job silently absorbs anything untagged for exclusion, which is easy to miss since nothing errors until the excluded dependency (here, a not-yet-existent table) actually fails.

---

## Python tooling on macOS: `uv`, editable installs, and iCloud sync side effects

### Hidden `.pth` files (`ModuleNotFoundError`) caused by iCloud sync — fix immediately with a full `.venv` rebuild, don't diagnose

**Symptom**: `ModuleNotFoundError` for a package that's genuinely installed and genuinely used (`No module named 'connectors'`, `'raw_to_clean'`, `'polaris_client'`, `'dagster_data_platform'`, etc.) — `uv pip show` succeeds, but the import still fails.

**Cause**: this repo lives under `~/Documents`, which has iCloud Desktop & Documents Folders sync enabled. iCloud's background sync intermittently re-applies the macOS `UF_HIDDEN` BSD file flag to `.venv/lib/.../*.pth` files, and Python 3.13's `site.py` silently skips any hidden `.pth` file (security hardening against hidden malicious `.pth` files, [python/cpython#113659](https://github.com/python/cpython/issues/113659)) — breaking the import even though the package is genuinely installed. This is a known, documented `uv` interaction ([astral-sh/uv#9902](https://github.com/astral-sh/uv/issues/9902)): macOS presents an iCloud-synced path as a normal local directory (a File Provider extension, not a symlink — `readlink` won't reveal it), but iCloud's own sync/eviction cycle touches files in that tree on a schedule outside `uv`'s knowledge. `link-mode = "copy"` is already set in `[tool.uv]` (`pyproject.toml`) as a partial mitigation — it stops the flag being set on *new* writes — but doesn't retroactively fix files already sitting in an existing `.venv`, and doesn't stop iCloud from re-hiding a file again later regardless of when it was written.

**Fix — the instant this symptom shows up, no diagnosis, no theorizing, no retry loop**: kill any running `dagster dev`/`streamlit` process (it holds the `uv` cache lock), then:
```
rm -rf .venv && uv cache clean && uv sync --all-packages
```
Retry. Do this proactively before any real verification run, not just reactively after a failure — the built-in `.just` recipes' 3x sweep-and-retry is not a substitute for this and has been observed to fail 3/3 while a full rebuild alone fixes it.

**A tempting non-fix, explicitly rejected**: `uv sync --no-editable` removes `.pth` files entirely, sidestepping the bug — but in direct testing it left the package completely unimportable (`ModuleNotFoundError` even though `uv pip show` still reported it installed), a strictly worse state than the bug it was meant to dodge, and editable installs are the correct standard pattern for active local dev regardless. Recovering from it still required the same full rebuild above.

### Duplicate `<name> 2.<ext>` files (iCloud "conflicted copy") break dbt/codegen — find, confirm, delete, don't diagnose

**Symptom**: dbt compilation fails with an error like "found two `schema.yml` entries for the same resource," pointing at a stale duplicate file (e.g. `schema 2.yml`) sitting alongside the real, current one. The duplicate has owner-only permissions (`-rw-------`, versus the real file's normal permissions) and an older modification time.

**Cause**: the same iCloud Desktop & Documents Folders sync condition as the `.pth`-hiding entry above, a different manifestation of it — when a generated file is rewritten rapidly and repeatedly in a short window (e.g. several back-to-back full codegen regenerations), iCloud's background sync can lose the race against the local rewrite and leave behind a numbered "conflicted copy" (`<name> 2.<ext>`) instead of cleanly resolving to the latest version.

**Fix — the instant this symptom shows up, no diagnosis, no theorizing**:
```
find <repo> -iname "* 2.<ext>"
```
For every match, confirm a real, current, non-`2.`-suffixed original exists alongside it (never delete one that doesn't — that would mean it isn't actually a duplicate), then delete the `2.`-suffixed file outright. These are always stale, superseded by the freshly-regenerated original, never real work worth preserving.

### `uv sync` (with or without `--reinstall`) can report false success on a broken or empty venv

Compounding the workspace-root gotcha below: `uv sync`, `uv sync --reinstall`, and even `uv sync --reinstall-package <name> -v` all reported clean success (`Audited in 0.00ms` or similar) against a `.venv` that had only 1–2 entries in `site-packages` — none of the `--reinstall` variants forced a real re-resolution of workspace members that plain `uv sync` (without `--all-packages`) never installed in the first place. The reinstall flags aren't a substitute for `--all-packages` at a workspace root; check actual `site-packages` content (or just try the real `import`) rather than trusting a fast, silent "success" after any of these.

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

### Kubernetes scaling options compared (KEDA, Knative, HPA, VPA, Cluster Autoscaler) — real maturity levels, not general impressions

**Context**: surfaced while onboarding a second streaming source — `orchestration`'s three Deployments (webserver, daemon, code-server) ran 24/7 with no resource requests/limits set at all, consuming real memory whether or not anyone was using Dagster, which was part of why a second Flink `TaskManager` couldn't schedule. This became a genuine "which of these tools actually solves 'idle workloads shouldn't hold resources'" investigation, since the honest answer differs a lot by mechanism.

**The mechanisms are not interchangeable — each solves a different problem**:

- **Horizontal Pod Autoscaler (HPA)** — core Kubernetes API (`autoscaling/v2`), extremely mature, near-universal in production. Scales *replica count* based on CPU/memory or custom metrics (needs `metrics-server`, or Prometheus + `prometheus-adapter` for custom metrics). **Cannot scale to zero — minimum is always 1 replica.** Solves "handle more load," not "don't run when idle."

- **Vertical Pod Autoscaler (VPA)** — Kubernetes SIG project. Right-sizes a pod's CPU/memory *requests* from observed usage, not replica count. Often run in "recommendation only" mode in serious production environments specifically because Auto mode's resize has historically meant evicting and recreating the pod — disruptive for anything stateful. **Real, confirmed-live update to this**: in-place Pod resource resizing (changing a running container's requests/limits without recreating the pod) is stable exactly as of Kubernetes **v1.36** — and this project's own kind cluster already runs **v1.36.1**, confirmed via the live `kube-apiserver` image tag, not assumed. This makes non-disruptive VPA genuinely closer to viable than it would have been a year or two ago. **The catch, also confirmed live**: only CPU resizes in-place by default — memory's default `resizePolicy` is still `RestartContainer`, i.e. a restart is still the default behavior for the exact dimension (memory) that actually constrains this platform. Whether VPA's own controller has been updated to exploit the in-place primitive for memory, or still defaults to evict-and-recreate regardless, wasn't verified — a real follow-up before adopting this for something stateful like Postgres, not an assumption either way.

- **Cluster Autoscaler** — mature, standard on every major managed Kubernetes (EKS/GKE/AKS first-class support). Adds/removes actual *nodes* based on aggregate pending-pod demand. **Not applicable to a local `kind` cluster** — `kind` is a single, fixed-size Docker container acting as one node; there's no real node pool for a cluster autoscaler to grow. In real enterprise deployments this runs *alongside* HPA/KEDA (node-level elasticity + pod-level elasticity are complementary, not competing) — locally, the closest analog is bumping the Docker Desktop VM's own memory allocation, since the single node's capacity *is* that VM's capacity.

- **KEDA (Kubernetes Event-Driven Autoscaling), core** — CNCF **Graduated** project, the same maturity tier as Kubernetes itself, Prometheus, and Envoy. First-class managed add-on in Azure Kubernetes Service. Extends the same underlying HPA mechanism to allow `minReplicaCount: 0` (true scale-to-zero) and adds pluggable "scalers" beyond CPU/memory — Kafka consumer lag, queue depth, Prometheus metrics, and **Cron** (scale up/down on a defined time window). The Cron scaler specifically is simple, stable, and was the actual mechanism adopted here — no beta components involved. **Real limitation**: it's time-based, not demand-based — it doesn't know a real request is imminent, it just follows a schedule. A legitimate, common enterprise pattern for predictable-demand internal tools (and this project's actual Dagster schedules are, in fact, already cron expressions — so a Cron-scaler window keyed to the *real* configured schedule times isn't really an approximation of demand, it's an accurate match to a demand source that already is a schedule).

- **KEDA HTTP Add-on** — a separate KEDA sub-project, **beta status, not yet v1.0** ("minor breaking changes... may still occur"), confirmed live via its own repo, not assumed. This is what would let a Deployment scale from zero the instant a real HTTP request arrives (by intercepting traffic, holding it, triggering scale-up, then forwarding) — the piece that would make Streamlit's or Dagster's webserver's scale-to-zero feel instant rather than schedule-bound. **WebSocket / long-lived-connection support is not documented** — a real, live risk specifically for Streamlit, whose UI depends on WebSocket for its reactive updates. Deliberately not adopted this session given that gap.

- **Knative Serving** — CNCF **Incubating** project (more mature than the KEDA HTTP Add-on specifically for this use case — it's what Google Cloud Run is built on internally), purpose-built for scale-to-zero-and-back on real HTTP traffic, with its own request-queueing/cold-start machinery. **Drawback**: needs its own networking layer in front of it (Istio, Kourier, or Contour) — a meaningfully bigger operational commitment than adding KEDA, not a drop-in.

**Resolution / what was actually adopted**: KEDA core + the Cron scaler only, scoped to `orchestration`'s webserver + code-server (the daemon stays always-on, since it's what evaluates schedules/sensors — if it were asleep, nothing would wake it on a real trigger, the same "what wakes the thing that wakes other things" problem one level down). Cron windows keyed to this project's actual configured schedules, not a generic business-hours guess.

**Update (2026-07-20)**: Streamlit now has its own cooperative-wake mechanism too (`frontend/dagster_wake.py`), not just the verification tooling — see this file's "A KEDA-paused wake needs a matching guard on the automatic sleep" entry below for the full build, including a real race found and fixed along the way.

### A plain `kubectl scale` doesn't stick against a Deployment a KEDA `ScaledObject` targets

**Symptom**: `_wake-orchestration` ran `kubectl scale deployment/dagster-webserver --replicas=1`, and `kubectl rollout status` reported success — but the very next real request (a `dagster_graphql.DagsterGraphQLClient` call from `verify-pipeline`) failed with `ConnectionResetError`/`TransportConnectionFailed`. Looked at first like a weak-readinessProbe race (the Deployment's `tcpSocket` readinessProbe only checks the port is listening, not that the app finished initializing), but tightening the probe wouldn't have fixed it.

**Cause**: once a `ScaledObject` targets a Deployment, KEDA installs an HPA (`keda-hpa-<scaledobject-name>`) that continuously owns that Deployment's replica count and reconciles it back to the trigger's computed value on every polling tick. Outside the configured Cron window, that computed value is `0` — so a manual `kubectl scale --replicas=1` gets silently reverted by KEDA (confirmed live: reverted to 0 within ~15s of the manual scale, sometimes before a client even finished connecting). `kubectl scale`/`rollout status` succeeding only proves the pod briefly existed and became Ready, not that it survives.

**Resolution**: use KEDA's own supported override, the `autoscaling.keda.sh/paused-replicas` annotation on the `ScaledObject` (not the Deployment) — setting it forces the target to that exact replica count *and* makes KEDA stop reconciling it (confirmed live: the `keda-hpa-*` HPA object itself disappears while paused). `_wake-orchestration` sets `autoscaling.keda.sh/paused-replicas=1` on both `dagster-webserver-scaler`/`dagster-code-server-scaler`; a new `_sleep-orchestration` recipe removes the annotation afterward, handing control back to KEDA (confirmed live: the HPA reappears and reconciles back to 0 immediately, since we're outside any Cron window). `verify-pipeline`/`verify-schedule`/`verify-sensor` each register `_sleep-orchestration` via `trap ... EXIT` right after waking, so it always runs even on failure — the same "kill every process spun up for a phase's work" convention (CLAUDE.md), applied to a KEDA-paused replica count instead of a local process.

**Caveat**: this is a stable, documented KEDA feature (not a workaround). **Update (2026-07-20)**: the same pause-then-scale pattern is now also built into Streamlit's own trigger page (`frontend/dagster_wake.py`), not just this recipe — see the next entry for the automatic-sleep counterpart and a real race it took to get right.

### A KEDA-paused "wake" needs a matching guard on the automatic "sleep," not just a run-in-flight check

**Symptom**: after building a Dagster run-status sensor to automatically remove a KEDA `paused-replicas` annotation once a triggering `master_pipeline` run reached a terminal status, a freshly-woken `dagster-webserver` pod was killed roughly 17 seconds after starting — well before its own readiness probe could ever pass. Reproduced twice in a row, both times immediately after a fresh wake, not randomly.

**Cause**: the sleep sensor's only safety check was "does Dagster's run storage show any other `master_pipeline` run still non-terminal?" (`RunsFilter(job_name="master_pipeline", statuses=<non-terminal>)`). That check has a real blind spot: a sensor tick evaluating an *older*, unrelated `master_pipeline` run's completion event can fire in the exact window after a fresh wake but *before* the new trigger's own run has actually been created — from the check's point of view, "no other runs in flight" is true, so it unpauses, even though a wake is actively in progress for a run that doesn't exist in Dagster's storage yet. Confirmed to affect not just the new Streamlit feature but this project's own pre-existing `verify-pipeline` recipe too — the identical race hit the gap between its own sequential `master_pipeline` invocations, killing the webserver mid-poll with a raw `ConnectionResetError`.

**Resolution**: every wake path stamps a second annotation, `data-platform.internal/last-woken-at` (current UTC timestamp), on the same `patch`/`annotate` call that sets `autoscaling.keda.sh/paused-replicas` — both `frontend/dagster_wake.py` (Python, via the `kubernetes` client) and `orchestration/module.just`'s `_wake-orchestration` recipe (shell, via `kubectl annotate`). The sleep sensor (`orchestration/dagster_data_platform/dagster_data_platform/wake_sleep_sensor.py`, `_recently_woken()`) reads this timestamp before unpausing and skips (leaves it paused) if it's less than 60 seconds old, regardless of what the run-storage check says. Self-healing by construction, not just a delay tactic: if a wake is never followed by a real run (e.g. a failed submission), the grace period simply expires and the next unrelated `master_pipeline` completion event re-evaluates normally — no permanent "stuck awake" leak requiring separate cleanup.

**Caveat**: the grace period (60s) is sized against a normal wake-then-submit round trip (typically single-digit seconds), not tuned to the common case — generous on purpose. Any *new* caller of the wake mechanism must also stamp this timestamp annotation, or it inherits the same blind spot; the sensor has no way to distinguish "a wake with no timestamp" from "no wake happened at all."

### `kubectl delete -f --ignore-not-found` doesn't cover a CRD that was never installed

**Symptom**: `orchestration::kill` and `streaming/flink::kill` (both run under `set -euo pipefail`) aborted with `error: Recipe kill failed with exit code 1` on the very first `just smoketest` run against a genuinely fresh cluster (Docker itself wasn't even running yet) — `kubectl delete -f k8s/keda-scaledobjects.yaml --ignore-not-found` (orchestration) and `kubectl delete -f generated/ --ignore-not-found` (flink, against a generated `FlinkDeployment` manifest) both printed `no matches for kind "ScaledObject"`/`"FlinkDeployment"` ... `ensure CRDs are installed first`, and the script died right there — before ever reaching the `helm uninstall ... || true` line immediately below it in both recipes.

**Cause**: `--ignore-not-found` only suppresses "no object with this name exists" — it does *not* suppress "this API kind isn't registered at all," a different, harder kubectl error that happens when the CRD providing that kind (installed by `helm install keda`/`helm install flink-kubernetes-operator`, a separate step from `kill`) was never installed in this cluster. Both recipes already anticipated the *same* "operator/CRD never installed" scenario for their `helm uninstall` line (`|| true` / helm's own `--ignore-not-found` flag) but missed the identical case one line above it for the `kubectl delete -f <manifest-referencing-the-CRD-kind>` line.

**Resolution**: added `|| true` to both `kubectl delete -f ... --ignore-not-found` lines, matching the guard already present on the very next line in each recipe. Verified live against the exact failure condition (a freshly created `kind` cluster, before KEDA or the Flink Operator were ever installed): the top-level `just kill` now runs the full module chain to completion (`EXIT_CODE:0`) instead of aborting on the first CRD-based module it reaches.

**Caveat/generalizable lesson**: this only affects a `kill` recipe that `kubectl delete -f`s a manifest referencing a CRD-provided kind (KEDA's `ScaledObject`, Flink Operator's `FlinkDeployment`) — every other module's `kill` recipe in this repo only references built-in kinds (Deployment/Service/Secret/StatefulSet/PVC/ConfigMap/Job), which are always registered regardless of what's installed, so `--ignore-not-found` alone is sufficient there. Checked all of them (2026-07-21) — exactly these two were affected, not a repo-wide pattern. Any *new* module that introduces its own CRD-based custom resource needs the same `|| true` on its `kill` recipe's delete-by-manifest line, or it inherits this exact gap.

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

### Cross-field reactivity is impossible inside `st.form()` — confirmed against the installed package, not memory

**Symptom**: a second widget's options/default can't be filtered by a first widget's current selection while both live inside the same `st.form(...)` block — the second widget only ever sees the first's value as of the *last* rerun (page load, or the previous submission), never the user's current, not-yet-submitted click.

**Cause**: this is by design, not a bug or a version quirk. `st.form`'s own docstring (confirmed on the installed 1.59.0, not assumed from general familiarity) states it directly: *"Within a form, the only widget that can have a callback function is `st.form_submit_button`."* Streamlit's actual execution model: there's no fine-grained per-widget update — every interaction with any widget reruns the *entire* script top to bottom, and each `st.xxx(...)` call, when reached, returns whatever that widget's current stored value is. Forms exist specifically to suppress that per-widget rerun for everything inside them, batching all field values together until the submit button is pressed — which is exactly what breaks "widget B's options depend on widget A's live value," since both live in the same batch that only resolves at submission.

**Resolution**: this project moved all three CRUD pages (`frontend/pages/*.py`) off `st.form()` entirely, onto plain reactive widgets with a manual submit `st.button()` gating the actual DB write (`if submitted: ...`, same as before — no safety lost, since nothing writes until *that* button is the one just clicked on *that* rerun). This unlocks real cross-field filtering (e.g. `3_Lakehouse_Models.py`'s "Owning feed" dropdown narrowing live to whatever's checked in "Depends on feeds") and incidentally fixed a second, previously-unnoticed instance of the same bug (`2_Data_Feeds.py`'s "New batch friendly name" field not appearing/disappearing until submit). Lost `st.form`'s free `clear_on_submit=True`; replaced with a `st.session_state` generation counter bumped after a successful insert — changing every "Add new" widget's `key=` forces Streamlit to treat the next render as brand-new widget instances with fresh defaults, rather than remembering the just-submitted values.

A related, sharper edge worth remembering if a dependent widget's *options* can shrink (not just its default) as another live widget changes: don't pass a static `index=`/`value=` alongside a `key=` in that case — Streamlit raises if a keyed widget's already-stored value isn't present in the `options` list passed on a given render. Pre-correct `st.session_state[key]` directly, immediately before instantiating the widget, whenever the current stored value has fallen outside the live-computed options.

### `curl`-ing a Streamlit page URL, or re-typing its backend logic in isolation, is not testing the page

**Symptom**: `frontend/pages/5_Trigger_Pipeline.py` crashed immediately on load in a real browser (`pandas.errors.DatabaseError: column "created_at" does not exist`, from `fetch_table(engine, "data_feed")` silently relying on `fetch_table`'s `order_by="created_at"` default — `data_feed` has no such column; every other page passes `order_by="friendly_name"` explicitly), despite having been "verified" beforehand two different ways that both reported success.

**Cause, both false-positives explained**: (1) `curl http://localhost:8501/Trigger_Pipeline` returned `200` — but a Streamlit multi-page app serves a static HTML/JS shell over plain HTTP; the actual page script only executes over the browser's WebSocket connection once the client JS connects, so a bare HTTP GET never runs the page's Python at all, buggy or not. (2) The trigger *function* itself was verified by `kubectl exec`-ing a hand-copied snippet of just that function into the running pod — real, and it did prove the GraphQL logic works, but it never touched the page's top-level code (the `fetch_table` calls that populate the dropdowns, which run unconditionally on every page load, before any button is clicked) — the actual bug was in code that was never executed by either check.

**Resolution**: `streamlit.testing.v1.AppTest` (stable since Streamlit ~1.28, confirmed available on this project's installed 1.59.0) runs a page's real script through Streamlit's actual script-running machinery, headlessly, with no browser needed — `AppTest.from_file(path).run()`, then assert `not at.exception`. This is what would have caught the bug immediately (confirmed live: reverting to the buggy `order_by` default and re-running the new test in `frontend/tests/test_trigger_pipeline_page.py` fails exactly the way the browser did). It also supports simulating real interaction (`at.radio[0].set_value(...).run()`, `at.button[0].click().run()`), so the same harness verifies widget interactions, not just the initial render.

**Broader lesson**: for any Streamlit page, "the backend function I extracted works" and "the HTTP endpoint returns 200" are both real signals but neither one exercises the page module's own top-level code — which is exactly where the widgets, their data-fetching calls, and their wiring together live. `AppTest` is the only check in this project that actually does.

### `kubectl apply` on an unchanged Deployment spec never restarts a `:latest`-tagged pod, even after a real image rebuild

**Symptom**: a real bug fix (the entry above) was rebuilt (`docker build`), reloaded into the cluster (`kind load docker-image`), and "redeployed" (`kubectl apply -f deployment.yaml`, reporting success, `kubectl rollout status` returning immediately) — yet `kubectl exec`-ing into the "redeployed" pod and reading the file directly showed the *old*, still-buggy content. The fix genuinely worked (confirmed separately via `AppTest` running the file directly, which bypasses the cluster entirely) — it just was never actually running anywhere a browser could reach it.

**Cause**: every image in this project is tagged `:latest`, and every Deployment uses `imagePullPolicy: IfNotPresent`. `kind load docker-image` correctly updates what `:latest` points to in containerd's content store — but `kubectl apply` only recreates a pod when the Deployment's **pod template spec** changes, and `image: data-platform-frontend:latest` is the same string before and after a rebuild. No diff, no new pod, no re-evaluation of what `:latest` currently points to — the already-running container just keeps running its own already-pulled filesystem, indefinitely, regardless of what's freshly loaded into the node.

**Resolution**: `orchestration/module.just` had already solved this for `dagster-code-server` specifically (`kubectl rollout restart deployment/dagster-code-server`, with its own detailed comment) — but the same fix was missing from `frontend/module.just` and `streaming/producer/module.just`, both of which have the exact same `docker build` → `kind load docker-image` → `kubectl apply` → `kubectl rollout status` shape. Added `kubectl rollout restart deployment/<name>` to both, immediately after `kubectl apply` and before waiting on rollout status. Confirmed live both ways: without the restart, a fresh `kubectl exec` still showed old file content after a full `start`; with it, `kubectl apply` reports `deployment.apps/frontend restarted` and a genuinely new pod (fresh name, age resets to seconds) serves the new content.

**Not yet applied to `dagster-webserver`/`dagster-daemon`** in `orchestration/module.just` — only `dagster-code-server` gets the forced restart there, on the specific reasoning that code-server is what reports the asset graph's *structure* to the other two. That reasoning may not cover every kind of code change to the webserver/daemon themselves; flagged as a related, not-yet-verified gap, not fixed as part of this entry.

**Broader lesson**: any `module.just` recipe with the `docker build` → `kind load docker-image` → `kubectl apply -f deployment.yaml` shape needs an explicit `kubectl rollout restart` to actually deploy a code change under a stable `:latest` tag — `kubectl apply` alone silently does nothing useful for an unchanged spec, and neither `kind load docker-image`'s own success nor `kubectl rollout status`'s "successfully rolled out" message are evidence otherwise (`rollout status` reports success just as fast for "nothing to roll" as for a real rollout — the message text doesn't distinguish the two). The only way to know a redeploy actually happened is checking the pod's own age/name changed, or (more directly) reading the file the pod is actually serving.

### A Dagster `LaunchRunSuccess` mutation result proves the submission was *accepted*, not that the run itself succeeds

**Symptom**: `frontend/pages/5_Trigger_Pipeline.py`'s `batch_group` picker populated its dropdown from `data_feed.batch_group` — a `uuid` column — instead of the separate `data_feed.batch_group_friendly_name` text column. Submitting a `batch_group` trigger returned `LaunchRunSuccess` with a real `runId` every time — no error, no exception, the exact response a correct submission would give — but every launched run then failed *inside* Dagster with `Failure: No active feeds resolved for orchestration_kind='batch_group' orchestration_value='<uuid>'`, because `PostgresMetadataResource.get_batch_group_feeds()` matches `WHERE batch_group_friendly_name = %s`, and a uuid string matches no row.

**Cause, and why it went undetected for a while**: the GraphQL mutation and the pipeline run it launches are two genuinely separate things, checked at two different times — `launchPipelineExecution`'s `LaunchRunSuccess` only means Dagster *accepted the submission*, before `run_master_pipeline`'s own op body ever executes. A test (or a manual check) that stops at "the button click didn't raise, and a success message appeared" — or, worse, a `SELECT ... ORDER BY job_started_timestamp DESC LIMIT N` sanity check run shortly after a *different*, unrelated real pipeline run (a full `just smoketest` in this case) — can look like confirmation while actually observing a completely different run's leftover rows, not the one just triggered.

**Resolution**: the dropdown now sources `batch_group_friendly_name`. More importantly, `frontend/tests/test_trigger_pipeline_page.py` no longer treats `at.success` (the mutation accepted) as sufficient proof for its two most important tests — `test_trigger_button_batch_group_resolves_real_feeds`/`..._model_schema_resolves_real_feeds` extract the actual `run_id` from the success message and poll `data_processing_runs` for a real `job_successful = true` row keyed to *that specific* `master_dagster_run_id`, not "something recent."

**Broader lesson**: for any "submit and poll/observe" trigger path (not just this one), verifying the submission call succeeded is a materially weaker claim than verifying the thing it submitted actually completed successfully — and a "recent rows" check without pinning the exact identifier being verified is vulnerable to being satisfied by unrelated concurrent or recent activity, which reads as confirmation while proving nothing about the specific case under test.
