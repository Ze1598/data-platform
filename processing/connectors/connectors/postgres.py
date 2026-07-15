"""Postgres source connector. discover_schema() is scoped to
sample-inference (the shared TabularConnector default) rather than true
information_schema/pg_catalog lookup -- the one real feed this connector
serves today (metadata_runs) queries multiple joined tables via a custom
SQL string, not a single real table, so a catalog lookup wouldn't apply
to it anyway. True catalog-based discovery for a single-table source is a
documented future enhancement, not built now for a feed that wouldn't use
it -- see the connector library plan.

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
    def __init__(self, *, host: str, port: int, dbname: str, user: str, password: str, query: str):
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._password = password
        self._query = query

    def fetch(self) -> pl.DataFrame:
        with psycopg.connect(
            host=self._host, port=self._port, dbname=self._dbname, user=self._user, password=self._password
        ) as conn, conn.cursor() as cur:
            cur.execute(self._query)
            columns = [d.name for d in cur.description]
            rows = cur.fetchall()
        return pl.DataFrame(rows, schema=columns, orient="row") if rows else pl.DataFrame()
