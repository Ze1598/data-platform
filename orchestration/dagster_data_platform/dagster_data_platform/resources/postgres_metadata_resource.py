from contextlib import contextmanager
from typing import Any, Iterator, Optional

import psycopg
from connectors import SchemaSyncResult, compute_schema_sync
from dagster import ConfigurableResource

_DATA_FEED_STAGES = ("landing", "raw", "clean")
_DATA_MODEL_STAGES = ("staging", "model", "serve")


class IngestionStepLog:
    """Handle for one stage's column group on a data_processing_runs row,
    yielded by PostgresMetadataResource.log_data_feed_stage()/
    log_data_model_stage(). Call set_counts(...) any time before the `with`
    block exits; that stage's columns are finalized automatically on exit —
    successful if the block completed, failed (with the exception message)
    if it raised. The exception always re-raises after being logged; this
    only observes failures, it doesn't swallow them."""

    def __init__(self, run_id: str):
        self.run_id = run_id
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

    def update_schema_registry(self, *, data_feed_id: str, column_definitions: list[dict[str, Any]], created_by: str) -> None:
        """Writes a new current schema_registry version for a feed --
        called by raw_to_clean's schema reconciliation (schema_evolution.py)
        when incoming data adds a column or changes an existing column's
        type (see Roadmap.md "Metadata Schema"). Both writes happen in one
        transaction: flip the existing is_current row to false first, then
        insert the new one -- required by uq_schema_registry_current (a
        partial unique index allowing only one is_current=true row per
        data_feed_id), so the old row must stop being current before the
        new one can become current."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE schema_registry
                SET is_current = false,
                    effective_to = now()
                WHERE data_feed_id = %(data_feed_id)s AND is_current
                """,
                {"data_feed_id": data_feed_id},
            )
            cur.execute(
                """
                INSERT INTO schema_registry (data_feed_id, version, column_definitions, is_current, effective_from, created_by)
                VALUES (
                    %(data_feed_id)s,
                    coalesce((SELECT max(version) FROM schema_registry WHERE data_feed_id = %(data_feed_id)s), 0) + 1,
                    %(column_definitions)s,
                    true,
                    now(),
                    %(created_by)s
                )
                """,
                {
                    "data_feed_id": data_feed_id,
                    "column_definitions": psycopg.types.json.Json(column_definitions),
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

    def get_current_schema(self, data_feed_id: str) -> list[dict[str, Any]]:
        """The current schema_registry.column_definitions for a feed —
        raw_to_clean.validate_schema()'s contract, see Roadmap.md "Metadata
        Schema"."""
        with self._connect() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT column_definitions FROM schema_registry WHERE data_feed_id = %s AND is_current",
                (data_feed_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No current schema_registry entry for data_feed_id={data_feed_id!r}")
            return row["column_definitions"]

    def sync_schema_registry(
        self, *, data_feed_id: str, discovered_column_definitions: list[dict[str, Any]], created_by: str
    ) -> SchemaSyncResult:
        """Establishes/updates a feed's schema_registry contract from a
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
        through."""
        try:
            current = self.get_current_schema(data_feed_id)
        except ValueError:
            current = None
        result = compute_schema_sync(discovered_column_definitions, current)
        if result.changed:
            self.update_schema_registry(
                data_feed_id=data_feed_id, column_definitions=result.column_definitions, created_by=created_by
            )
        return result

    def is_schedule_active(self, schedule_id: str) -> bool:
        """Live re-check at schedule-tick time -- lets a schedule be
        disabled via the metadata DB and take effect immediately, without
        waiting for the next `generate_dagster_pipeline.py` regen to remove
        the ScheduleDefinition object entirely. A missing/deleted row is
        treated as inactive, not an error -- a schedule row disappearing
        out from under a still-running generated ScheduleDefinition
        (stale until the next codegen regen) should skip, not crash."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT is_active FROM schedule WHERE id = %s", (schedule_id,))
            row = cur.fetchone()
            return bool(row and row[0])

    def record_run_started(self, *, data_feed_id: str, dagster_run_id: str, tracking_group: str) -> None:
        """Explicit master-pipeline extraction-start step: guarantees a
        data_processing_runs row exists for (data_feed_id, dagster_run_id)
        before extraction's connector.fetch() ever runs, not just as an
        incidental side effect of log_data_feed_stage()'s own bookkeeping.
        Matters concretely for metadata_runs, which queries
        data_processing_runs as its own source: on a fresh cluster's very
        first run, every feed (including metadata_runs itself) is running
        for the first time simultaneously, so without an explicit,
        guaranteed-first write, metadata_runs' own extraction has nothing
        to report and never creates `clean.metadata_runs`, and the
        downstream dbt build fails outright with TABLE_NOT_FOUND rather
        than building on an empty/thin first run. Idempotent against
        log_data_feed_stage()'s own row-ensure (ON CONFLICT DO UPDATE) --
        calling both for the same (data_feed_id, dagster_run_id) is always
        safe, the second call is a no-op insert-wise."""
        self._ensure_run(
            insert_columns={
                "data_feed_id": data_feed_id,
                "tracking_group": tracking_group,
                "tracking_group_type": "batch_group",
            },
            conflict_columns=("data_feed_id", "dagster_run_id"),
            conflict_where="data_feed_id IS NOT NULL",
            dagster_run_id=dagster_run_id,
        )

    # --- public API: one context manager per row kind, same table ----------
    # data_feed_run and data_model_run were merged into one wide
    # data_processing_runs table (metadata schema redesign, see
    # metadata/DataModel.md) -- a feed-run row and a model-run row are
    # distinguished by which of data_feed_id/model_key is set, enforced by
    # chk_data_processing_runs_one_target.

    @contextmanager
    def log_data_feed_stage(
        self, *, data_feed_id: str, stage: str, dagster_run_id: str, tracking_group: str
    ) -> Iterator[IngestionStepLog]:
        """Logs one stage (landing/raw/clean) of a data_processing_runs
        feed-run row, creating the row on the first stage of a given
        (data_feed_id, dagster_run_id) and updating just that stage's
        column group plus the job-level roll-up (job_ended_timestamp/
        job_successful) on every call — see extraction_assets.py/
        sales_assets.py for usage. `tracking_group` is the feed's
        data_feed.batch_group_friendly_name -- every feed has one (see
        metadata/DataModel.md), the platform tracks runs by batch or model
        schema, never by bare feed."""
        with self._log_stage(
            insert_columns={
                "data_feed_id": data_feed_id,
                "tracking_group": tracking_group,
                "tracking_group_type": "batch_group",
            },
            conflict_columns=("data_feed_id", "dagster_run_id"),
            conflict_where="data_feed_id IS NOT NULL",
            valid_stages=_DATA_FEED_STAGES,
            stage=stage,
            dagster_run_id=dagster_run_id,
        ) as log:
            yield log

    @contextmanager
    def log_data_model_stage(
        self, *, model_key: str, uses_feeds: str, stage: str, dagster_run_id: str, tracking_group: str
    ) -> Iterator[IngestionStepLog]:
        """Logs one stage (staging/model/serve) of a data_processing_runs
        model-run row — the warehouse-building concern, kept separate from
        the feed-run rows' extraction-and-validation concern (see
        metadata/DataModel.md). `model_key` names the model unit being
        built (today just the feed friendly_name, since staging is still
        1:1 with a data_feed); `uses_feeds` is a comma-separated list of
        the data_feed friendly_names this model unit draws from.
        `tracking_group` is a lakehouse_models.model_schema value (e.g.
        'model') -- the schema this build's output primarily lands in."""
        with self._log_stage(
            insert_columns={
                "model_key": model_key,
                "uses_feeds": uses_feeds,
                "tracking_group": tracking_group,
                "tracking_group_type": "model_schema",
            },
            conflict_columns=("model_key", "dagster_run_id"),
            conflict_where="model_key IS NOT NULL",
            valid_stages=_DATA_MODEL_STAGES,
            stage=stage,
            dagster_run_id=dagster_run_id,
        ) as log:
            yield log

    # --- shared implementation, generic over the two row kinds --------------
    # Both public methods above delegate to the same three private helpers
    # instead of each having their own copy, since a feed-run row and a
    # model-run row are structurally identical (a wide row with a job-level
    # roll-up plus one repeated column group per stage) apart from which
    # columns identify the row and which stages are valid.

    @contextmanager
    def _log_stage(
        self,
        *,
        insert_columns: dict[str, str],
        conflict_columns: tuple[str, ...],
        conflict_where: str,
        valid_stages: tuple[str, ...],
        stage: str,
        dagster_run_id: str,
    ) -> Iterator[IngestionStepLog]:
        if stage not in valid_stages:
            raise ValueError(f"Unknown stage {stage!r}, expected one of {valid_stages}")
        run_id = self._ensure_run(
            insert_columns=insert_columns,
            conflict_columns=conflict_columns,
            conflict_where=conflict_where,
            dagster_run_id=dagster_run_id,
        )
        log = IngestionStepLog(run_id)
        try:
            yield log
        except Exception as e:
            self._finish_stage(run_id, stage, successful=False, error_message=str(e), **log._counts)
            raise
        else:
            self._finish_stage(run_id, stage, successful=True, **log._counts)

    def _ensure_run(
        self,
        *,
        insert_columns: dict[str, str],
        conflict_columns: tuple[str, ...],
        conflict_where: str,
        dagster_run_id: str,
    ) -> str:
        # insert_columns keys/conflict_columns/conflict_where are always
        # internal literals passed by log_data_feed_stage/
        # log_data_model_stage above, never caller-supplied free text --
        # same safety property as the f-string table/column composition in
        # frontend/metadata_db.py.
        # Upsert-and-return-id: only the *first* stage of a given
        # (identity..., dagster_run_id) actually inserts a new row; later
        # stages hit the ON CONFLICT branch and get the existing run_id
        # back. A single round-trip INSERT ... ON CONFLICT is used rather
        # than a SELECT-then-INSERT-if-missing specifically to avoid the
        # TOCTOU race the latter would introduce between concurrent stages.
        # The ON CONFLICT target includes the same WHERE predicate as the
        # matching partial unique index (uq_data_processing_runs_feed/
        # _model) -- required for Postgres to resolve which partial index
        # this insert is targeting.
        columns = (*insert_columns.keys(), "dagster_run_id")
        values = (*insert_columns.values(), dagster_run_id)
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
