from contextlib import contextmanager
from typing import Any, Iterator, Optional

import psycopg
from dagster import ConfigurableResource

_DATA_FEED_STAGES = ("landing", "raw", "clean")
_DATA_MODEL_STAGES = ("staging", "model", "serve")


class IngestionStepLog:
    """Handle for one stage's column group on a data_feed_run/data_model_run
    row, yielded by PostgresMetadataResource.log_data_feed_stage()/
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
    data_feed/model_feed config, and writing data_feed_run/data_model_run
    rows, per Roadmap.md's "Metadata Schema"."""

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

    def get_data_feed(self, code: str) -> dict[str, Any]:
        with self._connect() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT * FROM data_feed WHERE code = %s", (code,))
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No data_feed with code={code!r}")
            return row

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

    # --- public API: one context manager per run table ---------------------

    @contextmanager
    def log_data_feed_stage(
        self, *, data_feed_id: str, stage: str, dagster_run_id: str
    ) -> Iterator[IngestionStepLog]:
        """Logs one stage (landing/raw/clean) of a data_feed_run row,
        creating the row on the first stage of a given (data_feed_id,
        dagster_run_id) and updating just that stage's column group plus
        the job-level roll-up (job_ended_timestamp/job_successful) on
        every call — see extraction_assets.py/sales_assets.py for usage."""
        with self._log_stage(
            table="data_feed_run",
            insert_columns={"data_feed_id": data_feed_id},
            conflict_columns=("data_feed_id", "dagster_run_id"),
            valid_stages=_DATA_FEED_STAGES,
            stage=stage,
            dagster_run_id=dagster_run_id,
        ) as log:
            yield log

    @contextmanager
    def log_data_model_stage(
        self, *, model_key: str, uses_feeds: str, stage: str, dagster_run_id: str
    ) -> Iterator[IngestionStepLog]:
        """Logs one stage (staging/model/serve) of a data_model_run row —
        the warehouse-building concern, kept separate from data_feed_run's
        extraction-and-validation concern (see Roadmap.md "Metadata
        Schema"). `model_key` names the model unit being built (today just
        the feed code, since staging is still 1:1 with a data_feed);
        `uses_feeds` is a comma-separated list of the data_feed codes this
        model unit draws from. Deliberately not the same thing as
        model_feed/model_feed_source — see the comment on model_feed_source
        in metadata/db/init/01_platform_metadata.sql for why those don't
        fit a staging-only build unit."""
        with self._log_stage(
            table="data_model_run",
            insert_columns={"model_key": model_key, "uses_feeds": uses_feeds},
            conflict_columns=("model_key", "dagster_run_id"),
            valid_stages=_DATA_MODEL_STAGES,
            stage=stage,
            dagster_run_id=dagster_run_id,
        ) as log:
            yield log

    # --- shared implementation, generic over the two run tables ------------
    # data_feed_run and data_model_run are structurally identical (a wide
    # row with a job-level roll-up plus one repeated column group per
    # stage) apart from which columns identify the row and which stages are
    # valid, so both public methods above delegate to the same three
    # private helpers instead of each having their own copy.

    @contextmanager
    def _log_stage(
        self,
        *,
        table: str,
        insert_columns: dict[str, str],
        conflict_columns: tuple[str, ...],
        valid_stages: tuple[str, ...],
        stage: str,
        dagster_run_id: str,
    ) -> Iterator[IngestionStepLog]:
        if stage not in valid_stages:
            raise ValueError(f"Unknown stage {stage!r} for {table}, expected one of {valid_stages}")
        run_id = self._ensure_run(
            table=table,
            insert_columns=insert_columns,
            conflict_columns=conflict_columns,
            dagster_run_id=dagster_run_id,
        )
        log = IngestionStepLog(run_id)
        try:
            yield log
        except Exception as e:
            self._finish_stage(table, run_id, stage, successful=False, error_message=str(e), **log._counts)
            raise
        else:
            self._finish_stage(table, run_id, stage, successful=True, **log._counts)

    def _ensure_run(
        self,
        *,
        table: str,
        insert_columns: dict[str, str],
        conflict_columns: tuple[str, ...],
        dagster_run_id: str,
    ) -> str:
        # table/insert_columns keys/conflict_columns are always internal
        # literals passed by log_data_feed_stage/log_data_model_stage above,
        # never caller-supplied free text -- same safety property as the
        # f-string table/column composition in frontend/metadata_db.py.
        # Upsert-and-return-id: only the *first* stage of a given
        # (identity..., dagster_run_id) actually inserts a new row; later
        # stages hit the ON CONFLICT branch and get the existing run_id
        # back. A single round-trip INSERT ... ON CONFLICT is used rather
        # than a SELECT-then-INSERT-if-missing specifically to avoid the
        # TOCTOU race the latter would introduce between concurrent stages.
        columns = (*insert_columns.keys(), "dagster_run_id")
        values = (*insert_columns.values(), dagster_run_id)
        column_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(values))
        conflict_target = ", ".join(conflict_columns)
        noop_column = conflict_columns[0]
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table} ({column_list})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target}) DO UPDATE
                    SET {noop_column} = excluded.{noop_column}
                RETURNING run_id
                """,
                values,
            )
            return str(cur.fetchone()[0])

    def _finish_stage(
        self,
        table: str,
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
                UPDATE {table}
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
