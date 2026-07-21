"""Postgres source connector. `query` is optional -- a plain single-table
feed (the default case: data_feed.extraction_config is empty, `{}`) gets a
generated `SELECT * FROM <table_name>` (table_name defaulting to
data_feed.source_object_name, that source's own real table identifier),
matching a database source's real default: fetch the whole table, no
per-feed query logic. `extraction_config.query` is a deliberate escape
hatch for a feed that genuinely needs one -- a multi-table join, or any
other custom SQL a user wants this connector to run verbatim -- not the
common case, and not something a database-type feed needs by default.
Incremental filtering never needs a custom query either way: it's handled
generically, after fetch(), via data_feed.watermark_column/
last_watermark_value, the same for every connector kind.

discover_schema() is real pg_catalog introspection (pg_attribute/pg_type)
when table_name is known -- a single real table's column names, types, and
NOT NULL constraints are all authoritative there, no sampling involved.
Falls back to the shared TabularConnector sample-inference default only
for a custom multi-table query (table_name is None) -- there's no single
table whose catalog entry would answer "what are this query's columns,"
same reasoning discover_primary_key() below already documents for itself.

discover_primary_key() is real catalog introspection too -- it only needs
a table name (no column shape/type inference required), so it doesn't hit
the "query may span more than one table" problem the same way column
discovery would; a caller simply doesn't supply table_name for a genuinely
multi-table custom query (see the ODS design, Roadmap.md).

Credential handling is deliberately minimal: source_system.connection_secret
is documented as a reference, not a real credential (see
metadata/DataModel.md) -- resolving a real vaulted secret for a genuinely
external Postgres source is out of scope here. A feed querying this
platform's own metadata Postgres instance (e.g. metadata_runs) gets its
connection details from the same environment variables every other
resource in this codebase already uses.
"""

import uuid
from typing import Any

import polars as pl
import psycopg

from connectors.base import TabularConnector

# pg_type.typname -> schema_registry data_type vocabulary (see
# raw_to_clean.schema_validation.TYPE_MAP). Postgres-internal type names,
# not information_schema's SQL-standard ones -- matches what the
# pg_attribute/pg_type query below actually returns.
_POSTGRES_TYPE_MAP: dict[str, str] = {
    "int2": "long",
    "int4": "long",
    "int8": "long",
    "numeric": "double",
    "float4": "double",
    "float8": "double",
    "bool": "boolean",
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "date": "timestamp",
    "varchar": "string",
    "bpchar": "string",
    "text": "string",
    "uuid": "string",
    "json": "string",
    "jsonb": "string",
}


def _resolve_postgres_data_type(column_name: str, typname: str) -> str:
    data_type = _POSTGRES_TYPE_MAP.get(typname)
    if data_type is None:
        raise ValueError(
            f"column '{column_name}': unsupported Postgres catalog type {typname!r}, "
            "no schema_registry data_type mapping"
        )
    return data_type


class PostgresConnector(TabularConnector):
    def __init__(
        self, *, host: str, port: int, dbname: str, user: str, password: str,
        table_name: str | None = None, query: str | None = None,
    ):
        if query is None and table_name is None:
            raise ValueError("PostgresConnector requires at least one of query, table_name")
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._password = password
        self._table_name = table_name
        self._query = query or f"SELECT * FROM {table_name}"

    def fetch(self) -> pl.DataFrame:
        with psycopg.connect(
            host=self._host, port=self._port, dbname=self._dbname, user=self._user, password=self._password
        ) as conn, conn.cursor() as cur:
            cur.execute(self._query)
            columns = [d.name for d in cur.description]
            rows = cur.fetchall()
        if not rows:
            return pl.DataFrame()
        # psycopg returns a native uuid column as a real uuid.UUID object,
        # not a string -- Polars can't infer a concrete dtype for a column
        # of those (falls back to Object, which schema_registry has no
        # data_type mapping for -- confirmed live: a plain `SELECT *` from
        # data_processing_runs crashed schema inference on `run_id` this
        # way). Stringified here, generically, for every row/column, not
        # just the columns a specific feed happens to need -- any future
        # Postgres feed with a uuid column hits the exact same problem
        # under a plain select, not something worth re-solving per feed.
        rows = [tuple(str(v) if isinstance(v, uuid.UUID) else v for v in row) for row in rows]
        # infer_schema_length=None: every row here was already fetched
        # over the wire above, so there's no cost to having Polars look at
        # all of them for dtype inference rather than its own default
        # first-100-rows window, which could pick a type that's wrong for
        # rows 101+ despite every row being in memory already.
        return pl.DataFrame(rows, schema=columns, orient="row", infer_schema_length=None)

    def discover_schema(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        if self._table_name is None:
            return super().discover_schema(df)
        with psycopg.connect(
            host=self._host, port=self._port, dbname=self._dbname, user=self._user, password=self._password
        ) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname, t.typname, NOT a.attnotnull AS is_nullable, a.attnum
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                (self._table_name,),
            )
            rows = cur.fetchall()
        return [
            {
                "name": name,
                "data_type": _resolve_postgres_data_type(name, typname),
                "nullable": is_nullable,
                "ordinal": attnum - 1,
            }
            for name, typname, is_nullable, attnum in rows
        ]

    def discover_primary_key(self) -> list[str] | None:
        """Returns the real primary key column list (correctly ordered for
        a composite key) for `table_name`, or [] if that table genuinely
        has no primary key. Returns None -- and never opens a connection at
        all -- when table_name wasn't provided, which is the expected case
        for a custom query spanning more than one table: there's no single
        table whose catalog entry would even answer the question, so this
        deliberately doesn't try to parse the query to guess one.
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
