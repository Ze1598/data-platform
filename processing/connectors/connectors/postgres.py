"""Postgres source connector. discover_schema() is scoped to
sample-inference (the shared TabularConnector default) rather than true
information_schema/pg_catalog lookup -- the one real feed this connector
serves today (metadata_runs) queries multiple joined tables via a custom
SQL string, not a single real table, so a catalog lookup wouldn't apply
to it anyway. True catalog-based discovery for a single-table source is a
documented future enhancement, not built now for a feed that wouldn't use
it -- see the connector library plan.

discover_primary_key() is real catalog introspection, unlike
discover_schema() above -- it only needs a table name (no column
shape/type inference required), so it doesn't hit the "query is often a
multi-table JOIN, not a single real table" problem the same way column
discovery does; a caller simply doesn't supply table_name for a
non-single-table query (see the ODS design, Roadmap.md).

Credential handling is deliberately minimal: source_system.connection_secret
is documented as a reference, not a real credential (see
metadata/DataModel.md) -- resolving a real vaulted secret for a genuinely
external Postgres source is out of scope here. The one real feed today
(metadata_runs) queries this platform's own metadata Postgres instance,
so its connection details come from the same environment variables every
other resource in this codebase already uses.
"""

import polars as pl
import psycopg

from connectors.base import TabularConnector


class PostgresConnector(TabularConnector):
    def __init__(
        self, *, host: str, port: int, dbname: str, user: str, password: str, query: str,
        table_name: str | None = None,
    ):
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._password = password
        self._query = query
        self._table_name = table_name

    def fetch(self) -> pl.DataFrame:
        with psycopg.connect(
            host=self._host, port=self._port, dbname=self._dbname, user=self._user, password=self._password
        ) as conn, conn.cursor() as cur:
            cur.execute(self._query)
            columns = [d.name for d in cur.description]
            rows = cur.fetchall()
        return pl.DataFrame(rows, schema=columns, orient="row") if rows else pl.DataFrame()

    def discover_primary_key(self) -> list[str] | None:
        """Returns the real primary key column list (correctly ordered for
        a composite key) for `table_name`, or [] if that table genuinely
        has no primary key. Returns None -- and never opens a connection at
        all -- when table_name wasn't provided, which is the expected case
        for a query spanning more than one table (e.g. metadata_runs' own
        3-table LEFT JOIN): there's no single table whose catalog entry
        would even answer the question, so this deliberately doesn't try
        to parse the query to guess one.
        """
        if self._table_name is None:
            return None
        with psycopg.connect(
            host=self._host, port=self._port, dbname=self._dbname, user=self._user, password=self._password
        ) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum)
                """,
                (self._table_name,),
            )
            return [row[0] for row in cur.fetchall()]
