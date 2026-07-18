from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

import psycopg
from connectors import SchemaSyncResult, compute_schema_sync
from dagster import ConfigurableResource
from domain_naming import slugify_domain

_DATA_FEED_STAGES = ("raw", "clean")
_DATA_MODEL_STAGES = ("staging", "model", "serve")

# Which of the three pipeline-step run-id columns a given schema-level
# stage belongs to -- see the `pipeline_steps` lookup table's own
# definitions (0=extraction spans raw+clean -- raw and clean are tightly
# coupled, raw exists specifically to feed clean, so they share one
# job/run, not two; 1=transformation spans staging+model; 2=serving is
# serve only). No "landing" stage -- that was never a real pipeline
# concept, just a historical mislabeling of the fetch sub-step within
# extraction; its outcome/watermark tracking is now just part of "raw"'s
# own columns. Each of the three independent stage-jobs (EXTRACTION_JOBS/
# TRANSFORMATION_JOBS/SERVING_JOBS, see scripts/generate_dagster_pipeline.py)
# writes its own dagster_run_id into exactly one of these columns --
# possibly from more than one schema-stage call within the same run (e.g.
# EXTRACTION_JOBS logs both "raw" and "clean" from the one extraction run,
# writing the same run id both times). The five schema-level stage columns
# each of these maps to (is_raw_successful/rows_read/etc.) stay fully
# independent per schema stage regardless -- only which Dagster run
# produced them is consolidated here, preserving the granularity a future
# rerun-just-raw-to-clean feature would need.
_STAGE_TO_RUN_ID_COLUMN = {
    "raw": "extraction_dagster_run_id",
    "clean": "extraction_dagster_run_id",
    "staging": "transformation_dagster_run_id",
    "model": "transformation_dagster_run_id",
    "serve": "serving_dagster_run_id",
}


class IngestionStepLog:
    """Handle for one stage's column group on a data_processing_runs row,
    yielded by PostgresMetadataResource.log_data_feed_stage()/
    log_data_model_stage(). Call set_counts(...) any time before the `with`
    block exits; that stage's columns are finalized automatically on exit —
    successful if the block completed, failed (with the exception message)
    if it raised. The exception always re-raises after being logged; this
    only observes failures, it doesn't swallow them."""

    def __init__(self, run_id: str, storage_watermark: Optional[str] = None):
        self.run_id = run_id
        # Only ever set for a feed-run row's raw/clean stages (see
        # record_run_started()) -- None for staging/model/serve stages,
        # which never touch raw and have no use for it.
        self.storage_watermark = storage_watermark
        self._counts: dict[str, Any] = {}

    def set_counts(
        self,
        *,
        rows_read: Optional[int] = None,
        rows_inserted: Optional[int] = None,
        rows_updated: Optional[int] = None,
        rows_deleted: Optional[int] = None,
        output_path: Optional[str] = None,
        watermark_value_start: Optional[str] = None,
        watermark_value_end: Optional[str] = None,
    ) -> None:
        for key, value in (
            ("rows_read", rows_read),
            ("rows_inserted", rows_inserted),
            ("rows_updated", rows_updated),
            ("rows_deleted", rows_deleted),
            ("output_path", output_path),
            ("watermark_value_start", watermark_value_start),
            ("watermark_value_end", watermark_value_end),
        ):
            if value is not None:
                self._counts[key] = value


class PostgresMetadataResource(ConfigurableResource):
    """Connection to the platform_metadata Postgres database — reading
    data_feed/lakehouse_models config, and writing data_processing_runs
    rows, per metadata/DataModel.md."""

    host: str
    port: int
    user: str
    password: str
    dbname: str

    @contextmanager
    def _connect(self):
        conn = psycopg.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
        )
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def get_data_feed(self, friendly_name: str) -> dict[str, Any]:
        with self._connect() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT * FROM data_feed WHERE friendly_name = %s", (friendly_name,))
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No data_feed with friendly_name={friendly_name!r}")
            return row

    def resolve_model_domain_and_feeds(self, model_friendly_name: str) -> tuple[str, list[str]]:
        """Resolves a lakehouse_models row's domain (its model_schema value,
        slugified the same way dbt/domains/<domain>/ itself is named -- see
        Roadmap.md "multi-project dbt split") plus the union of
        depends_on_feeds across every active lakehouse_models row sharing
        that same model_schema -- one level, not recursive (confirmed
        design: a model_schema's own dependent feeds only, not a transitive
        closure). Backs pipeline_generated.py's resolve_run_plan_for_model()
        -- "trigger by lakehouse model" launches these feed jobs, then the
        resolved domain's job."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT model_schema FROM lakehouse_models WHERE friendly_name = %s AND is_active = true",
                (model_friendly_name,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No active lakehouse_models row with friendly_name={model_friendly_name!r}")
            model_schema = row[0]

            cur.execute(
                """
                SELECT DISTINCT df.friendly_name
                FROM lakehouse_models lm
                JOIN data_feed df ON df.id::text = ANY(string_to_array(lm.depends_on_feeds, ','))
                WHERE lm.model_schema = %s AND lm.is_active = true AND df.is_active = true
                ORDER BY df.friendly_name
                """,
                (model_schema,),
            )
            feeds = [r[0] for r in cur.fetchall()]
        return slugify_domain(model_schema), feeds

    def get_domain_feeds_for_model_schema(self, model_schema: str) -> tuple[str, list[str]]:
        """Same union-of-depends_on_feeds query as resolve_model_domain_and_feeds()
        above, but taking a `model_schema` value directly rather than first
        looking it up from one specific lakehouse_models row's friendly_name
        -- this is what the master pipeline job uses when it's triggered
        with `orchestration_kind='model_schema'` (Roadmap.md "Master
        pipeline orchestration"), since the caller already has the domain,
        not a specific model to resolve it from. Returns (slugified_domain,
        feeds) for symmetry with resolve_model_domain_and_feeds()."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT df.friendly_name
                FROM lakehouse_models lm
                JOIN data_feed df ON df.id::text = ANY(string_to_array(lm.depends_on_feeds, ','))
                WHERE lm.model_schema = %s AND lm.is_active = true AND df.is_active = true
                ORDER BY df.friendly_name
                """,
                (model_schema,),
            )
            feeds = [r[0] for r in cur.fetchall()]
        return slugify_domain(model_schema), feeds

    def get_batch_group_ods_domain(self, batch_group_friendly_name: str) -> Optional[str]:
        """The (expected-1:1, not yet enforced anywhere -- see Backlog.md)
        ODS domain a batch group's feeds map to via their own
        `data_feed.batch_ods_name`. Used by the master pipeline job when
        triggered with `orchestration_kind='batch_group'`: extraction always
        happens for every feed in the batch, but the modeling+serving
        outcome is *always* ODS for a batch-triggered run, never a
        hand-modeled domain (a hand-modeled domain is only ever reached via
        `orchestration_kind='model_schema'`). Returns None if no feed in
        this batch is ODS-enabled (an extraction-only batch, which is
        valid) -- if more than one distinct `batch_ods_name` value is
        present (the un-enforced 1:1 rule being violated), the first one
        found (alphabetically) is used, not an error."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT batch_ods_name FROM data_feed
                WHERE batch_group_friendly_name = %s AND is_active = true
                  AND ods_enabled = true AND batch_ods_name IS NOT NULL
                ORDER BY batch_ods_name
                """,
                (batch_group_friendly_name,),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return slugify_domain(rows[0][0])

    def get_batch_group_feeds(self, batch_group_friendly_name: str) -> list[str]:
        """Every active feed sharing this batch_group -- backs
        pipeline_generated.py's resolve_run_plan_for_batch_group()
        ("trigger by batch group"): feed jobs only, no domain job implied.
        batch_group and model_schema stay two structurally separate axes
        (see data_processing_runs.tracking_group_type's existing design) --
        a batch's feeds may span multiple domains, so there is no single
        domain job to launch here."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT friendly_name FROM data_feed
                WHERE batch_group_friendly_name = %s AND is_active = true
                ORDER BY friendly_name
                """,
                (batch_group_friendly_name,),
            )
            return [r[0] for r in cur.fetchall()]

    def update_watermark(self, *, data_feed_id: str, watermark_value: str, run_id: str) -> None:
        """Advances data_feed.last_watermark_value (Phase 9, Roadmap.md
        "Incremental Loading & Watermarks") -- call this only after `clean`
        has actually succeeded for the run, never before or on failure. A
        failed run must not advance the watermark, or the next run would
        silently skip whatever failed to load; this is the correctness
        property safe re-run depends on. `run_id` is accepted for call-site
        symmetry with the run this watermark advance belongs to, but
        data_feed no longer stores last_run_id (see metadata/DataModel.md
        -- derivable from data_processing_runs instead)."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE data_feed
                SET last_watermark_value = %(watermark_value)s
                WHERE id = %(data_feed_id)s
                """,
                {"watermark_value": watermark_value, "data_feed_id": data_feed_id},
            )

    def update_schema_registry(
        self, *, controlling_object_id: str, controlling_object_type: str = "feed",
        column_definitions: list[dict[str, Any]],
        primary_key_columns: list[str], created_by: str,
    ) -> None:
        """Writes a new current schema_registry version for a feed or
        streaming_source (controlling_object_type -- see
        metadata/DataModel.md, polymorphic like ingestion_triggers) --
        called by connectors.schema_registry_sync.sync_schema_registry()
        for a feed when discovery finds a new column, a changed column
        type, or a different resolved primary key (see Roadmap.md
        "Metadata Schema"), or directly from the frontend's "Discover
        Schema" action for a streaming_source. Both writes happen in one
        transaction: flip the existing is_current row to false first,
        then insert the new one -- required by uq_schema_registry_current
        (a partial unique index allowing only one is_current=true row per
        (controlling_object_id, controlling_object_type)), so the old row
        must stop being current before the new one can become current."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE schema_registry
                SET is_current = false,
                    effective_to = now()
                WHERE controlling_object_id = %(controlling_object_id)s
                  AND controlling_object_type = %(controlling_object_type)s
                  AND is_current
                """,
                {"controlling_object_id": controlling_object_id, "controlling_object_type": controlling_object_type},
            )
            cur.execute(
                """
                INSERT INTO schema_registry (controlling_object_id, controlling_object_type, version, column_definitions, primary_key_columns, is_current, effective_from, created_by)
                VALUES (
                    %(controlling_object_id)s,
                    %(controlling_object_type)s,
                    coalesce((SELECT max(version) FROM schema_registry WHERE controlling_object_id = %(controlling_object_id)s AND controlling_object_type = %(controlling_object_type)s), 0) + 1,
                    %(column_definitions)s,
                    %(primary_key_columns)s,
                    true,
                    now(),
                    %(created_by)s
                )
                """,
                {
                    "controlling_object_id": controlling_object_id,
                    "controlling_object_type": controlling_object_type,
                    "column_definitions": psycopg.types.json.Json(column_definitions),
                    "primary_key_columns": psycopg.types.json.Json(primary_key_columns),
                    "created_by": created_by,
                },
            )

    def get_updates_enabled_map(self, feed_friendly_name: str) -> dict[str, bool]:
        """Maps every dbt model built under this feed's tag to whether
        updates are enabled for it, per the staging update-tracking rule in
        metadata/DataModel.md:

        - stg_<feed_friendly_name>: the logical OR of updates_enabled across
          every lakehouse_models row whose depends_on_feeds includes this
          feed's id, defaulting to true if no such row exists (the safe
          default -- assume changes matter until a dependent model
          explicitly says otherwise).
        - <lakehouse_models.friendly_name>: that row's own updates_enabled,
          verbatim, for every model depending on this feed.

        Consumed via `dbt build --vars` (dbt_assets.py) since Trino has no
        catalog federating into platform_metadata -- a dbt model can't look
        this up live, it has to be resolved before the dbt CLI invocation
        and passed in. When false for a given model, that model's own SQL
        skips attribute-hash change detection entirely and treats its
        source as insert-only (see stg_customers.sql)."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH feed AS (
                    SELECT id FROM data_feed WHERE friendly_name = %(feed_friendly_name)s
                ),
                dependents AS (
                    SELECT lakehouse_models.friendly_name, lakehouse_models.updates_enabled
                    FROM lakehouse_models, feed
                    WHERE feed.id::text = ANY(string_to_array(lakehouse_models.depends_on_feeds, ','))
                )
                SELECT friendly_name AS model_name, updates_enabled FROM dependents

                UNION ALL

                SELECT 'stg_' || %(feed_friendly_name)s AS model_name,
                       coalesce(bool_or(updates_enabled), true) AS updates_enabled
                FROM dependents
                """,
                {"feed_friendly_name": feed_friendly_name},
            )
            return {model_name: updates_enabled for model_name, updates_enabled in cur.fetchall()}

    def get_current_schema(self, controlling_object_id: str, controlling_object_type: str = "feed") -> list[dict[str, Any]]:
        """The current schema_registry.column_definitions for a feed (the
        default) or a streaming_source (controlling_object_type=
        "streaming_source") -- raw_to_clean.validate_schema()'s contract
        for a feed, see Roadmap.md "Metadata Schema". controlling_object_type
        defaults to "feed" so every pre-existing single-positional-arg call
        site (the overwhelmingly common case) keeps working unchanged."""
        with self._connect() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT column_definitions FROM schema_registry WHERE controlling_object_id = %s AND controlling_object_type = %s AND is_current",
                (controlling_object_id, controlling_object_type),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(
                    f"No current schema_registry entry for controlling_object_id={controlling_object_id!r}, "
                    f"controlling_object_type={controlling_object_type!r}"
                )
            return row["column_definitions"]

    def get_current_primary_key_columns(self, controlling_object_id: str, controlling_object_type: str = "feed") -> list[str]:
        """The current schema_registry.primary_key_columns for a feed --
        the ODS layer's (scripts/generate_ods_models.py) only consumer
        today, deciding upsert-by-key vs. insert-only. Mirrors
        get_current_schema()'s shape/error handling exactly."""
        with self._connect() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT primary_key_columns FROM schema_registry WHERE controlling_object_id = %s AND controlling_object_type = %s AND is_current",
                (controlling_object_id, controlling_object_type),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(
                    f"No current schema_registry entry for controlling_object_id={controlling_object_id!r}, "
                    f"controlling_object_type={controlling_object_type!r}"
                )
            return row["primary_key_columns"]

    def sync_schema_registry(
        self, *, data_feed_id: str, discovered_column_definitions: list[dict[str, Any]],
        metadata_source_pk: list[str], discovered_primary_key_columns: Optional[list[str]],
        created_by: str,
    ) -> SchemaSyncResult:
        """Feed-specific -- this is the one function in this class that
        stays batch-only (its primary-key resolution logic below is
        meaningless for a streaming_source, which has no primary-key
        concept), so its own signature keeps the feed-specific
        `data_feed_id` name rather than the generalized
        `controlling_object_id`/`controlling_object_type` pair
        get_current_schema()/get_current_primary_key_columns()/
        update_schema_registry() now take -- internally, this always
        passes controlling_object_type="feed" to those. A
        streaming_source's schema (see metadata/DataModel.md,
        streaming_source) is written directly via update_schema_registry()
        from the frontend's "Discover Schema" action instead, since there's
        no equivalent primary-key precedence to resolve there.

        Establishes/updates a feed's schema_registry contract from a
        freshly discovered schema (connector.discover_schema() /
        connectors.infer_column_definitions()) -- the extraction-time
        schema *discovery* step, run once before raw_to_clean.
        reconcile_schema()/validate_schema() ever check a batch against
        the contract. Diffing itself is connectors.compute_schema_sync()
        (pure, no I/O); this method owns the Postgres read/write around
        it. Call with the result's .column_definitions as the now-current
        contract for raw_to_clean.reconcile_schema()/validate_schema()/
        write_clean_snapshot() -- the latter self-determines whether the
        physical Iceberg table's schema needs to change, so .changed here
        is informational only, not something callers need to thread
        through.

        Primary key precedence, resolved here (not in the pure
        compute_schema_sync()) since it needs data_feed.source_pk, which
        only this I/O-performing method's caller has on hand:
        metadata_source_pk (a feed's manually-entered data_feed.source_pk)
        wins if non-empty; else discovered_primary_key_columns (real
        catalog introspection -- only PostgresConnector implements this,
        see connectors.postgres.discover_primary_key(); every other
        connector kind's caller always passes None here, since there's no
        live catalog to introspect); else empty, meaning no key is known
        at all -- see the ODS design, Roadmap.md, for what that implies
        downstream."""
        try:
            current_columns = self.get_current_schema(data_feed_id)
        except ValueError:
            current_columns = None
        try:
            current_pk = self.get_current_primary_key_columns(data_feed_id)
        except ValueError:
            current_pk = None

        resolved_pk = metadata_source_pk if metadata_source_pk else (discovered_primary_key_columns or [])
        result = compute_schema_sync(discovered_column_definitions, current_columns, resolved_pk, current_pk)
        if result.changed:
            self.update_schema_registry(
                controlling_object_id=data_feed_id,
                controlling_object_type="feed",
                column_definitions=result.column_definitions,
                primary_key_columns=result.primary_key_columns,
                created_by=created_by,
            )
        return result

    def is_trigger_active(self, trigger_id: str) -> bool:
        """Live re-check at schedule-tick or sensor-evaluation time -- lets
        an ingestion_triggers row be disabled via the metadata DB and take
        effect immediately, without waiting for the next
        `generate_dagster_pipeline.py` regen to remove the generated
        ScheduleDefinition/SensorDefinition object entirely. A missing/
        deleted row is treated as inactive, not an error -- a trigger row
        disappearing out from under a still-running generated definition
        (stale until the next codegen regen) should skip, not crash. One
        method for both trigger kinds -- schedules and sensors share the
        same ingestion_triggers row shape, just a different trigger_type."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT is_active FROM ingestion_triggers WHERE id = %s", (trigger_id,))
            row = cur.fetchone()
            return bool(row and row[0])

    def record_run_started(self, *, data_feed_id: str, master_dagster_run_id: str, tracking_group: str) -> None:
        """The feed-scoped master pipeline's own first action: creates the
        data_processing_runs row for this feed, keyed by the MASTER's own
        dagster_run_id -- not the child stage's. `EXTRACTION_JOBS[feed]`
        (spanning raw+clean as one job -- see Roadmap.md "Master
        pipeline orchestration") is a genuinely separate Dagster run from
        the master; it looks this row up by (data_feed_id,
        master_dagster_run_id) -- passed to it as a launch-time run tag --
        and records its own separate dagster_run_id into its own column
        (log_data_feed_stage() below) rather than creating a new row itself.

        Matters concretely for metadata_runs, which queries
        data_processing_runs as its own source: on a fresh cluster's very
        first run, every feed (including metadata_runs itself) is running
        for the first time simultaneously, so without an explicit,
        guaranteed-first write, metadata_runs' own extraction has nothing
        to report and never creates `clean.metadata_runs`, and the
        downstream dbt build fails outright with TABLE_NOT_FOUND rather
        than building on an empty/thin first run. Idempotent -- calling
        this more than once for the same (data_feed_id,
        master_dagster_run_id) is always safe, a no-op after the first --
        including the storage_watermark generated below: `_ensure_run`'s
        ON CONFLICT branch never overwrites it, so a retried call reuses
        the same watermark rather than minting a new one."""
        storage_watermark = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H/%M/%S")
        self._ensure_run(
            insert_columns={
                "data_feed_id": data_feed_id,
                "tracking_group": tracking_group,
                "tracking_group_type": "batch_group",
                "storage_watermark": storage_watermark,
            },
            conflict_columns=("data_feed_id", "master_dagster_run_id"),
            conflict_where="data_feed_id IS NOT NULL",
            master_dagster_run_id=master_dagster_run_id,
        )

    def record_model_run_started(self, *, model_key: str, uses_feeds: str, master_dagster_run_id: str, tracking_group: str) -> None:
        """The domain-scoped master pipeline's own first action -- same
        role as record_run_started() above, but for a model-run row.
        `MODELING_JOBS[domain]` and `SERVING_JOBS[domain]` each look this
        row up by (model_key, master_dagster_run_id) and record their own
        dagster_run_id into their own column (log_data_model_stage()
        below)."""
        self._ensure_run(
            insert_columns={
                "model_key": model_key,
                "uses_feeds": uses_feeds,
                "tracking_group": tracking_group,
                "tracking_group_type": "model_schema",
            },
            conflict_columns=("model_key", "master_dagster_run_id"),
            conflict_where="model_key IS NOT NULL",
            master_dagster_run_id=master_dagster_run_id,
        )

    # --- public API: one context manager per row kind, same table ----------
    # data_feed_run and data_model_run were merged into one wide
    # data_processing_runs table (metadata schema redesign, see
    # metadata/DataModel.md) -- a feed-run row and a model-run row are
    # distinguished by which of data_feed_id/model_key is set, enforced by
    # chk_data_processing_runs_one_target.

    @contextmanager
    def log_data_feed_stage(
        self, *, data_feed_id: str, stage: str, master_dagster_run_id: str, dagster_run_id: str
    ) -> Iterator[IngestionStepLog]:
        """Logs one stage (raw/clean) of a data_processing_runs
        feed-run row. The row itself is guaranteed to already exist --
        created by the feed-scoped master pipeline's record_run_started()
        before any child stage job ever runs -- so this only finds it (by
        data_feed_id + master_dagster_run_id, the master's own run id,
        threaded to this child job as a launch-time run tag) and updates
        that stage's column group, this stage's own dagster_run_id column
        (see _STAGE_TO_RUN_ID_COLUMN), and the job-level roll-up
        (job_ended_timestamp/job_successful) — see extraction_assets.py/
        sales_assets.py for usage."""
        with self._log_stage(
            identity_column="data_feed_id",
            identity_value=data_feed_id,
            master_dagster_run_id=master_dagster_run_id,
            dagster_run_id=dagster_run_id,
            valid_stages=_DATA_FEED_STAGES,
            stage=stage,
        ) as log:
            yield log

    @contextmanager
    def log_data_model_stage(
        self, *, model_key: str, stage: str, master_dagster_run_id: str, dagster_run_id: str
    ) -> Iterator[IngestionStepLog]:
        """Logs one stage (staging/model/serve) of a data_processing_runs
        model-run row — the warehouse-building concern, kept separate from
        the feed-run rows' extraction-and-validation concern (see
        metadata/DataModel.md). The row itself is guaranteed to already
        exist -- created by the domain-scoped master pipeline's
        record_model_run_started() -- so this only finds it (by model_key +
        master_dagster_run_id) and updates that stage's column group, this
        stage's own dagster_run_id column, and the job-level roll-up."""
        with self._log_stage(
            identity_column="model_key",
            identity_value=model_key,
            master_dagster_run_id=master_dagster_run_id,
            dagster_run_id=dagster_run_id,
            valid_stages=_DATA_MODEL_STAGES,
            stage=stage,
        ) as log:
            yield log

    # --- shared implementation, generic over the two row kinds --------------
    # Both public methods above delegate to the same private helpers instead
    # of each having their own copy, since a feed-run row and a model-run
    # row are structurally identical (a wide row with a job-level roll-up
    # plus one repeated column group per stage) apart from which column
    # identifies the row and which stages are valid.

    @contextmanager
    def _log_stage(
        self,
        *,
        identity_column: str,
        identity_value: str,
        master_dagster_run_id: str,
        dagster_run_id: str,
        valid_stages: tuple[str, ...],
        stage: str,
    ) -> Iterator[IngestionStepLog]:
        if stage not in valid_stages:
            raise ValueError(f"Unknown stage {stage!r}, expected one of {valid_stages}")
        run_id, storage_watermark = self._find_run(
            identity_column=identity_column,
            identity_value=identity_value,
            master_dagster_run_id=master_dagster_run_id,
            stage_run_id_column=_STAGE_TO_RUN_ID_COLUMN[stage],
            dagster_run_id=dagster_run_id,
        )
        log = IngestionStepLog(run_id, storage_watermark)
        try:
            yield log
        except Exception as e:
            self._finish_stage(run_id, stage, successful=False, error_message=str(e), **log._counts)
            raise
        else:
            self._finish_stage(run_id, stage, successful=True, **log._counts)

    def _find_run(
        self,
        *,
        identity_column: str,
        identity_value: str,
        master_dagster_run_id: str,
        stage_run_id_column: str,
        dagster_run_id: str,
    ) -> tuple[str, Optional[str]]:
        # identity_column/stage_run_id_column are always internal literals
        # passed by log_data_feed_stage/log_data_model_stage above, never
        # caller-supplied free text -- same safety property as the f-string
        # table/column composition in frontend/metadata_db.py.
        # A plain UPDATE, not an upsert: the row is guaranteed to already
        # exist by this point (created by the master pipeline's
        # record_run_started()/record_model_run_started()) -- a child stage
        # job never creates its own row. Zero rows matched means the master
        # never ran (or the wrong master_dagster_run_id was threaded
        # through), which is a real bug worth a clear error, not a silent
        # row creation that would violate the "one master, several stage
        # run ids" invariant. Also returns storage_watermark -- set on the
        # row at creation time by record_run_started(), never by this
        # UPDATE -- so the raw/clean steps can read/write the right raw
        # path without a second round trip.
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE data_processing_runs
                SET {stage_run_id_column} = %(dagster_run_id)s
                WHERE {identity_column} = %(identity_value)s
                  AND master_dagster_run_id = %(master_dagster_run_id)s
                RETURNING run_id, storage_watermark
                """,
                {
                    "dagster_run_id": dagster_run_id,
                    "identity_value": identity_value,
                    "master_dagster_run_id": master_dagster_run_id,
                },
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(
                    f"No data_processing_runs row for {identity_column}={identity_value!r}, "
                    f"master_dagster_run_id={master_dagster_run_id!r} -- did the master pipeline run first?"
                )
            return str(row[0]), row[1]

    def _ensure_run(
        self,
        *,
        insert_columns: dict[str, str],
        conflict_columns: tuple[str, ...],
        conflict_where: str,
        master_dagster_run_id: str,
    ) -> str:
        # insert_columns keys/conflict_columns/conflict_where are always
        # internal literals passed by record_run_started/
        # record_model_run_started above, never caller-supplied free text.
        # Upsert-and-return-id: calling this more than once for the same
        # (identity..., master_dagster_run_id) is safe -- only the first
        # call actually inserts, later calls hit the ON CONFLICT branch and
        # get the existing run_id back. A single round-trip
        # INSERT ... ON CONFLICT is used rather than a SELECT-then-INSERT-
        # if-missing specifically to avoid the TOCTOU race the latter would
        # introduce. The ON CONFLICT target includes the same WHERE
        # predicate as the matching partial unique index
        # (uq_data_processing_runs_feed/_model) -- required for Postgres to
        # resolve which partial index this insert is targeting.
        columns = (*insert_columns.keys(), "master_dagster_run_id")
        values = (*insert_columns.values(), master_dagster_run_id)
        column_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(values))
        conflict_target = ", ".join(conflict_columns)
        noop_column = conflict_columns[0]
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO data_processing_runs ({column_list})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target}) WHERE {conflict_where} DO UPDATE
                    SET {noop_column} = excluded.{noop_column}
                RETURNING run_id
                """,
                values,
            )
            return str(cur.fetchone()[0])

    def _finish_stage(
        self,
        run_id: str,
        stage: str,
        *,
        successful: bool,
        error_message: Optional[str] = None,
        rows_read: Optional[int] = None,
        rows_inserted: Optional[int] = None,
        rows_updated: Optional[int] = None,
        rows_deleted: Optional[int] = None,
        output_path: Optional[str] = None,
        watermark_value_start: Optional[str] = None,
        watermark_value_end: Optional[str] = None,
    ) -> None:
        # `stage` is already validated against the caller's valid_stages
        # tuple in _log_stage() before this is ever reached, so the
        # f-string column-prefix interpolation below is safe -- never
        # caller-supplied free text by the time it gets here.
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE data_processing_runs
                SET is_{stage}_successful = %(successful)s,
                    {stage}_end_timestamp = now(),
                    {stage}_error_message = %(error_message)s,
                    {stage}_rows_read = %(rows_read)s,
                    {stage}_rows_inserted = %(rows_inserted)s,
                    {stage}_rows_updated = %(rows_updated)s,
                    {stage}_rows_deleted = %(rows_deleted)s,
                    {stage}_output_path = %(output_path)s,
                    {stage}_watermark_value_start = %(watermark_value_start)s,
                    {stage}_watermark_value_end = %(watermark_value_end)s,
                    job_ended_timestamp = now(),
                    job_successful = coalesce(job_successful, true) AND %(successful)s
                WHERE run_id = %(run_id)s
                """,
                {
                    "successful": successful,
                    "error_message": error_message,
                    "rows_read": rows_read,
                    "rows_inserted": rows_inserted,
                    "rows_updated": rows_updated,
                    "rows_deleted": rows_deleted,
                    "output_path": output_path,
                    "watermark_value_start": watermark_value_start,
                    "watermark_value_end": watermark_value_end,
                    "run_id": run_id,
                },
            )
