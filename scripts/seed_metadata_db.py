"""Idempotently seeds source_system/data_feed/schema_registry rows for this
project's feeds. These are business-configuration rows, not schema — DDL
migrations (metadata/db/init/*.sql) create the tables, this populates them.

Existed only as ad hoc psql commands run by hand through Phase 4-6 until
now — not reproducible from a fresh or restarted cluster, which matters
now that the cluster gets stopped between phases (Learnings.md). Safe to
re-run: every insert is ON CONFLICT DO NOTHING against each table's real
unique constraint.
"""

import os

import psycopg

CONN_KWARGS = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    user=os.environ.get("POSTGRES_USER", "platform"),
    password=os.environ.get("POSTGRES_PASSWORD", "platform"),
    dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
)

CUSTOMERS_SCHEMA = [
    {"name": "customer_id", "data_type": "long", "nullable": False, "ordinal": 1, "description": "Business key"},
    {"name": "name", "data_type": "string", "nullable": False, "ordinal": 2, "description": "Customer name"},
    {"name": "email", "data_type": "string", "nullable": False, "ordinal": 3, "description": "Customer email"},
    {"name": "updated_at", "data_type": "timestamp", "nullable": False, "ordinal": 4, "description": "Last update, UTC"},
]

SALES_SCHEMA = [
    {"name": "invoice_id", "data_type": "string", "nullable": False, "ordinal": 1, "description": "Unique transaction identifier"},
    {"name": "branch", "data_type": "string", "nullable": False, "ordinal": 2, "description": "Branch code (A, B, C)"},
    {"name": "city", "data_type": "string", "nullable": False, "ordinal": 3, "description": "Branch city"},
    {"name": "customer_type", "data_type": "string", "nullable": False, "ordinal": 4, "description": "Member or Normal"},
    {"name": "gender", "data_type": "string", "nullable": False, "ordinal": 5, "description": "Customer gender"},
    {"name": "product_line", "data_type": "string", "nullable": False, "ordinal": 6, "description": "Product category"},
    {"name": "unit_price", "data_type": "double", "nullable": False, "ordinal": 7, "description": "Price per unit"},
    {"name": "quantity", "data_type": "long", "nullable": False, "ordinal": 8, "description": "Units purchased"},
    {"name": "tax_amount", "data_type": "double", "nullable": False, "ordinal": 9, "description": "Tax amount (5%)"},
    {"name": "total", "data_type": "double", "nullable": False, "ordinal": 10, "description": "Total incl. tax"},
    {"name": "payment_method", "data_type": "string", "nullable": False, "ordinal": 11, "description": "Cash, Credit card, or Ewallet"},
    {"name": "cogs", "data_type": "double", "nullable": False, "ordinal": 12, "description": "Cost of goods sold"},
    {"name": "gross_income", "data_type": "double", "nullable": False, "ordinal": 13, "description": "Gross income for the line"},
    {"name": "rating", "data_type": "double", "nullable": True, "ordinal": 14, "description": "Customer satisfaction rating, 1-10"},
    {"name": "sale_timestamp", "data_type": "timestamp", "nullable": False, "ordinal": 15, "description": "Transaction timestamp, UTC"},
]


def seed_source_system(cur, *, code: str, name: str, description: str, system_type: str) -> None:
    cur.execute(
        """
        INSERT INTO source_system (code, name, description, system_type)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (code) DO NOTHING
        """,
        (code, name, description, system_type),
    )


def seed_data_feed(
    cur,
    *,
    source_system_code: str,
    code: str,
    name: str,
    object_name: str,
    extraction_type: str,
    business_key_columns: list[str],
    staging_table_name: str,
    processing_engine: str,
) -> None:
    cur.execute(
        """
        INSERT INTO data_feed (
            source_system_id, code, name, object_name, extraction_type,
            business_key_columns, staging_table_name, processing_engine
        )
        VALUES (
            (SELECT id FROM source_system WHERE code = %s),
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (code) DO NOTHING
        """,
        (source_system_code, code, name, object_name, extraction_type, psycopg.types.json.Json(business_key_columns), staging_table_name, processing_engine),
    )


def seed_schema_registry(cur, *, data_feed_code: str, version: int, column_definitions: list[dict], created_by: str) -> None:
    cur.execute(
        """
        INSERT INTO schema_registry (data_feed_id, version, column_definitions, is_current, created_by)
        VALUES ((SELECT id FROM data_feed WHERE code = %s), %s, %s, true, %s)
        ON CONFLICT (data_feed_id, version) DO NOTHING
        """,
        (data_feed_code, version, psycopg.types.json.Json(column_definitions), created_by),
    )


def seed_model_feed(
    cur,
    *,
    code: str,
    model_type: str,
    staging_source_data_feed_code: str,
    business_key_columns: list[str],
    tracked_columns: list[str],
    scd_type: int,
    deletions_enabled: bool,
) -> None:
    cur.execute(
        """
        INSERT INTO model_feed (
            code, model_type, staging_source_data_feed_id,
            business_key_columns, tracked_columns, scd_type, deletions_enabled
        )
        VALUES (
            %s, %s, (SELECT id FROM data_feed WHERE code = %s),
            %s, %s, %s, %s
        )
        ON CONFLICT (code) DO NOTHING
        """,
        (
            code,
            model_type,
            staging_source_data_feed_code,
            psycopg.types.json.Json(business_key_columns),
            psycopg.types.json.Json(tracked_columns),
            scd_type,
            deletions_enabled,
        ),
    )


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        seed_source_system(
            cur,
            code="phase3_manual",
            name="Phase 3/4 manual test source",
            description="Phase 3/4 manual test source",
            system_type="database",
        )
        seed_source_system(
            cur,
            code="supermarket_pos",
            name="Supermarket POS",
            description="Point-of-sale system for supermarket branch transactions",
            system_type="database",
        )

        seed_data_feed(
            cur,
            source_system_code="phase3_manual",
            code="customers",
            name="Customers",
            object_name="customers",
            extraction_type="full",
            business_key_columns=["customer_id"],
            staging_table_name="customers",
            processing_engine="polars",
        )
        seed_data_feed(
            cur,
            source_system_code="supermarket_pos",
            code="sales",
            name="Supermarket Sales",
            object_name="sales",
            extraction_type="full",
            business_key_columns=["invoice_id"],
            staging_table_name="sales",
            processing_engine="polars",
        )

        seed_schema_registry(cur, data_feed_code="customers", version=1, column_definitions=CUSTOMERS_SCHEMA, created_by="seed_metadata_db")
        seed_schema_registry(cur, data_feed_code="sales", version=1, column_definitions=SALES_SCHEMA, created_by="seed_metadata_db")

        # Model layer (Phase 7): dim_customer stands alone (no real FK from
        # sales to customers in this dataset -- see Learnings.md); dim_branch
        # is conformed out of sales' own branch/city columns, and fct_sales
        # joins to it. See Roadmap.md "Model Layer: SCD Design".
        seed_model_feed(
            cur,
            code="dim_customer",
            model_type="dimension",
            staging_source_data_feed_code="customers",
            business_key_columns=["customer_id"],
            tracked_columns=["name", "email"],
            scd_type=2,
            deletions_enabled=True,
        )
        seed_model_feed(
            cur,
            code="dim_branch",
            model_type="dimension",
            staging_source_data_feed_code="sales",
            business_key_columns=["branch"],
            tracked_columns=["city"],
            scd_type=1,
            deletions_enabled=False,
        )
        seed_model_feed(
            cur,
            code="fct_sales",
            model_type="fact",
            staging_source_data_feed_code="sales",
            business_key_columns=["invoice_id"],
            tracked_columns=["unit_price", "quantity", "tax_amount", "total", "cogs", "gross_income", "rating"],
            scd_type=1,
            deletions_enabled=False,
        )

        conn.commit()
    print("Seed complete.")


if __name__ == "__main__":
    main()
