"""Postgres connection and generic CRUD helpers for platform metadata tables.

Framework-agnostic on purpose (no Streamlit import) so it can be reused by the
orchestrator later. Table/column names passed in by callers are always
hardcoded literals from this codebase, never user input, so building SQL by
string composition for identifiers is safe here — only values go through
parameterized binds.
"""

import os
import uuid

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine, create_engine


def get_engine() -> Engine:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "platform")
    password = os.environ.get("POSTGRES_PASSWORD", "platform")
    database = os.environ.get("POSTGRES_DB", "platform_metadata")
    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, pool_pre_ping=True)


def fetch_table(engine: Engine, table: str, order_by: str = "created_at") -> pd.DataFrame:
    df = pd.read_sql(text(f"select * from {table} order by {order_by}"), engine)
    # psycopg returns uuid columns as uuid.UUID objects, which Streamlit's dataframe
    # renderer serializes as byte dicts instead of readable text — stringify them.
    for col in df.columns:
        if df[col].map(lambda v: isinstance(v, uuid.UUID)).any():
            df[col] = df[col].map(lambda v: str(v) if isinstance(v, uuid.UUID) else v)
    return df


def fetch_lookup(engine: Engine, table: str, code_col: str = "code", id_col: str = "id") -> dict:
    """Return {code: id} for building selectboxes against a foreign table."""
    df = pd.read_sql(text(f"select {id_col}, {code_col} from {table} order by {code_col}"), engine)
    return dict(zip(df[code_col], df[id_col]))


def insert_row(engine: Engine, table: str, values: dict, json_columns: set[str] | None = None) -> None:
    # Note: cast(:param as jsonb) rather than :param::jsonb — SQLAlchemy's text()
    # bind-parameter parser mishandles a "::" cast stuck directly onto a named param.
    json_columns = json_columns or set()
    columns = ", ".join(values.keys())
    placeholders = ", ".join(
        f"cast(:{k} as jsonb)" if k in json_columns else f":{k}" for k in values.keys()
    )
    stmt = text(f"insert into {table} ({columns}) values ({placeholders})")
    with engine.begin() as conn:
        conn.execute(stmt, values)


def update_row(
    engine: Engine,
    table: str,
    id_col: str,
    id_value,
    values: dict,
    json_columns: set[str] | None = None,
) -> None:
    json_columns = json_columns or set()
    set_clause = ", ".join(
        f"{k} = cast(:{k} as jsonb)" if k in json_columns else f"{k} = :{k}" for k in values.keys()
    )
    stmt = text(f"update {table} set {set_clause} where {id_col} = :__id")
    with engine.begin() as conn:
        conn.execute(stmt, {**values, "__id": id_value})


def delete_row(engine: Engine, table: str, id_col: str, id_value) -> None:
    stmt = text(f"delete from {table} where {id_col} = :__id")
    with engine.begin() as conn:
        conn.execute(stmt, {"__id": id_value})


def write_schema_registry_version(
    engine: Engine,
    controlling_object_id: str,
    controlling_object_type: str,
    column_definitions: list[dict],
    primary_key_columns: list[str],
    created_by: str,
) -> None:
    """Writes a new current schema_registry version -- the frontend-side
    equivalent of orchestration's PostgresMetadataResource.
    update_schema_registry() (same two-statement transaction: flip the
    existing is_current row false, then insert the new one, required by
    uq_schema_registry_current's partial unique index). Used by
    4_Streaming_Sources.py's "Discover Schema" action -- a streaming_source
    has no equivalent to sync_schema_registry()'s per-run diff-then-write
    (discovery here is a deliberate, manual, one-time action, not an
    automated per-run check), so this always writes a new version rather
    than first comparing against the current one."""
    import json

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE schema_registry
                SET is_current = false, effective_to = now()
                WHERE controlling_object_id = :controlling_object_id
                  AND controlling_object_type = :controlling_object_type
                  AND is_current
                """
            ),
            {"controlling_object_id": controlling_object_id, "controlling_object_type": controlling_object_type},
        )
        conn.execute(
            text(
                """
                INSERT INTO schema_registry
                    (controlling_object_id, controlling_object_type, version, column_definitions, primary_key_columns, is_current, effective_from, created_by)
                VALUES (
                    :controlling_object_id,
                    :controlling_object_type,
                    coalesce((SELECT max(version) FROM schema_registry
                              WHERE controlling_object_id = :controlling_object_id
                                AND controlling_object_type = :controlling_object_type), 0) + 1,
                    cast(:column_definitions as jsonb),
                    cast(:primary_key_columns as jsonb),
                    true,
                    now(),
                    :created_by
                )
                """
            ),
            {
                "controlling_object_id": controlling_object_id,
                "controlling_object_type": controlling_object_type,
                "column_definitions": json.dumps(column_definitions),
                "primary_key_columns": json.dumps(primary_key_columns),
                "created_by": created_by,
            },
        )


def fetch_current_schema(engine: Engine, controlling_object_id: str, controlling_object_type: str) -> list[dict] | None:
    """The current schema_registry.column_definitions for a
    controlling_object, or None if discovery hasn't run yet. Frontend-side
    read counterpart to write_schema_registry_version() above."""
    df = pd.read_sql(
        text(
            "SELECT column_definitions FROM schema_registry "
            "WHERE controlling_object_id = :id AND controlling_object_type = :type AND is_current"
        ),
        engine,
        params={"id": controlling_object_id, "type": controlling_object_type},
    )
    if df.empty:
        return None
    return df.iloc[0]["column_definitions"]


def safe_str(value) -> str:
    """NaN/None-safe string coercion for prefilling form fields from a DataFrame row."""
    return "" if pd.isna(value) else str(value)


def to_json_text(value, default: str = "{}") -> str:
    """Render a jsonb column's Python value (dict/list/None/str) back to editable JSON text."""
    import json

    if value is None or (not isinstance(value, (dict, list)) and pd.isna(value)):
        return default
    if isinstance(value, str):
        return value
    return json.dumps(value)


def to_csv_text(value, default: str = "") -> str:
    """Render a jsonb list column's Python value (list/None/str) back to
    editable comma-separated text -- the list-column counterpart to
    to_json_text, for columns re-worded as plain CSV in the CRUD forms."""
    import json

    if value is None or (not isinstance(value, list) and pd.isna(value)):
        return default
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return ", ".join(str(v) for v in value)


def parse_csv_text(text_value: str) -> list[str]:
    """Inverse of to_csv_text -- split on commas, strip whitespace, drop empty entries."""
    return [part.strip() for part in text_value.split(",") if part.strip()]
