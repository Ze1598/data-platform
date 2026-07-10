from contextlib import contextmanager
from typing import Any, Iterator, Optional

import psycopg
from dagster import ConfigurableResource


class IngestionStepLog:
    """Handle for one run_audit_log row, yielded by
    PostgresMetadataResource.log_ingestion_step(). Call set_counts(...) any
    time before the `with` block exits; the row is finalized automatically
    on exit — 'success' if the block completed, 'failed' (with the
    exception message) if it raised. The exception always re-raises after
    being logged; this only observes failures, it doesn't swallow them."""

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
    ) -> None:
        for key, value in (
            ("rows_read", rows_read),
            ("rows_inserted", rows_inserted),
            ("rows_updated", rows_updated),
            ("rows_deleted", rows_deleted),
            ("output_path", output_path),
        ):
            if value is not None:
                self._counts[key] = value


class PostgresMetadataResource(ConfigurableResource):
    """Connection to the platform_metadata Postgres database — reading
    data_feed/model_feed config and writing run_audit_log rows, per
    Roadmap.md's "Metadata Schema"."""

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

    def start_run(
        self,
        *,
        layer: str,
        feed_type: str,
        data_feed_id: Optional[str] = None,
        model_feed_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        dagster_run_id: Optional[str] = None,
    ) -> str:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO run_audit_log
                    (layer, feed_type, data_feed_id, model_feed_id, parent_run_id, dagster_run_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'running')
                RETURNING run_id
                """,
                (layer, feed_type, data_feed_id, model_feed_id, parent_run_id, dagster_run_id),
            )
            return str(cur.fetchone()[0])

    @contextmanager
    def log_ingestion_step(
        self,
        *,
        layer: str,
        feed_type: str,
        data_feed_id: Optional[str] = None,
        model_feed_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        dagster_run_id: Optional[str] = None,
    ) -> Iterator[IngestionStepLog]:
        """One call to log a run_audit_log row around a pipeline step,
        instead of hand-paired start_run/finish_run calls at every call
        site (see extraction_assets.py / dbt_assets.py for usage)."""
        run_id = self.start_run(
            layer=layer,
            feed_type=feed_type,
            data_feed_id=data_feed_id,
            model_feed_id=model_feed_id,
            parent_run_id=parent_run_id,
            dagster_run_id=dagster_run_id,
        )
        log = IngestionStepLog(run_id)
        try:
            yield log
        except Exception as e:
            self.finish_run(run_id, status="failed", error_message=str(e), **log._counts)
            raise
        else:
            self.finish_run(run_id, status="success", **log._counts)

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        rows_read: Optional[int] = None,
        rows_inserted: Optional[int] = None,
        rows_updated: Optional[int] = None,
        rows_deleted: Optional[int] = None,
        output_path: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE run_audit_log
                SET status = %s, ended_at = now(), rows_read = %s, rows_inserted = %s,
                    rows_updated = %s, rows_deleted = %s, output_path = %s, error_message = %s
                WHERE run_id = %s
                """,
                (
                    status,
                    rows_read,
                    rows_inserted,
                    rows_updated,
                    rows_deleted,
                    output_path,
                    error_message,
                    run_id,
                ),
            )
