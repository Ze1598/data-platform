"""Idempotently seeds source_system/data_feed/schema_registry/lakehouse_models
rows for this project's feeds. These are business-configuration rows, not
schema — DDL migrations (metadata/db/init/*.sql) create the tables, this
populates them.

Existed only as ad hoc psql commands run by hand through Phase 4-6 until
now — not reproducible from a fresh or restarted cluster, which matters
now that the cluster gets stopped between phases (Learnings.md). Safe to
re-run: every insert is ON CONFLICT DO NOTHING against each table's real
unique constraint.

Idempotency key is friendly_name throughout (data_feed.code / model_feed.code
were removed in the metadata schema redesign — see metadata/DataModel.md).
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

# Phase 9: financial_transactions (CSV file-drop source, incremental on
# posted_date) -- see scripts/generate_financial_reports.py for the
# generator this feed's landing asset reads the output of.
FINANCIAL_TRANSACTIONS_SCHEMA = [
    {"name": "transaction_id", "data_type": "string", "nullable": False, "ordinal": 1, "description": "Business key, unique per journal-entry line"},
    {"name": "posted_date", "data_type": "timestamp", "nullable": False, "ordinal": 2, "description": "When the transaction was posted, UTC -- the incremental watermark column"},
    {"name": "account_code", "data_type": "string", "nullable": False, "ordinal": 3, "description": "Chart-of-accounts code"},
    {"name": "account_name", "data_type": "string", "nullable": False, "ordinal": 4, "description": "Chart-of-accounts name"},
    {"name": "description", "data_type": "string", "nullable": False, "ordinal": 5, "description": "Free-text transaction description"},
    {"name": "debit_amount", "data_type": "double", "nullable": False, "ordinal": 6, "description": "Debit amount; 0 if this line is a credit"},
    {"name": "credit_amount", "data_type": "double", "nullable": False, "ordinal": 7, "description": "Credit amount; 0 if this line is a debit"},
    {"name": "currency", "data_type": "string", "nullable": False, "ordinal": 8, "description": "ISO currency code"},
    {"name": "cost_center", "data_type": "string", "nullable": False, "ordinal": 9, "description": "Owning cost center"},
]

# Phase 9: police_crimes (UK Police API, https://data.police.uk/docs/,
# incremental on month -- one calendar month of street-level crime data per
# run, for a fixed point in central London to keep volume bounded).
# Flattened from the API's nested location/outcome_status JSON shape,
# confirmed against a live call before designing this.
POLICE_CRIMES_SCHEMA = [
    {"name": "id", "data_type": "long", "nullable": False, "ordinal": 1, "description": "Business key, the crime's own numeric ID"},
    {"name": "persistent_id", "data_type": "string", "nullable": True, "ordinal": 2, "description": "Stable cross-request ID; often empty per the API"},
    {"name": "category", "data_type": "string", "nullable": False, "ordinal": 3, "description": "Crime category"},
    {"name": "location_type", "data_type": "string", "nullable": True, "ordinal": 4, "description": "'Force' or 'BTP' (British Transport Police)"},
    {"name": "location_subtype", "data_type": "string", "nullable": True, "ordinal": 5, "description": "Further location classification, often empty"},
    {"name": "street_id", "data_type": "long", "nullable": True, "ordinal": 6, "description": "Anonymised street ID"},
    {"name": "street_name", "data_type": "string", "nullable": True, "ordinal": 7, "description": "Anonymised street name ('On or near ...')"},
    {"name": "latitude", "data_type": "double", "nullable": True, "ordinal": 8, "description": "Approximate latitude"},
    {"name": "longitude", "data_type": "double", "nullable": True, "ordinal": 9, "description": "Approximate longitude"},
    {"name": "context", "data_type": "string", "nullable": True, "ordinal": 10, "description": "Extra context, often empty"},
    {"name": "month", "data_type": "string", "nullable": False, "ordinal": 11, "description": "YYYY-MM this record belongs to -- the incremental watermark column"},
    {"name": "outcome_category", "data_type": "string", "nullable": True, "ordinal": 12, "description": "Latest known outcome category, null if none yet"},
    {"name": "outcome_date", "data_type": "string", "nullable": True, "ordinal": 13, "description": "YYYY-MM of the latest outcome, null if none yet"},
]


# Connector library plan: metadata_runs (Postgres source, this platform's
# own metadata DB) -- moved verbatim from the now-deleted
# metadata_runs_assets.py, which existed only via
# Walkthrough_Metadata_Source_Feed.md's manual setup. Stored as
# data_feed.extraction_config, read live by the generated PostgresConnector
# at landing time -- never baked into generated code.
METADATA_RUNS_QUERY = """
    select
        r.run_id::text as run_id, r.data_feed_id::text as data_feed_id, r.model_key,
        r.tracking_group, r.tracking_group_type, r.dagster_run_id,
        r.job_started_timestamp, r.job_ended_timestamp, r.job_successful,
        r.landing_rows_read, r.raw_rows_read, r.clean_rows_inserted,
        r.staging_rows_updated, r.model_rows_updated, r.serve_rows_read,
        df.friendly_name as feed_friendly_name,
        df.batch_group_friendly_name as feed_batch_group_friendly_name,
        df.extraction_type as feed_extraction_type,
        df.processing_engine as feed_processing_engine,
        df.is_active as feed_is_active,
        lm.friendly_name as model_friendly_name,
        lm.model_schema as model_model_schema,
        lm.table_type as model_table_type,
        lm.scd_type as model_scd_type,
        lm.updates_enabled as model_updates_enabled,
        lm.deletes_enabled as model_deletes_enabled
    from data_processing_runs r
    left join data_feed df on df.id = r.data_feed_id
    left join lakehouse_models lm on lm.friendly_name = r.model_key
"""


def seed_source_system(
    cur,
    *,
    code: str,
    name: str,
    description: str,
    system_type: str,
    connector_kind: str | None = None,
    base_location: str | None = None,
) -> None:
    # connector_kind=None means this system's feeds keep a fully
    # hand-written asset file (customers/sales' synthetic stub generators)
    # -- see processing/connectors/ and scripts/generate_dagster_pipeline.py.
    cur.execute(
        """
        INSERT INTO source_system (code, name, description, system_type, connector_kind, base_location)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (code) DO NOTHING
        """,
        (code, name, description, system_type, connector_kind, base_location),
    )


def seed_data_feed(
    cur,
    *,
    source_system_code: str,
    friendly_name: str,
    source_object_name: str,
    extraction_type: str,
    source_pk: list[str],
    processing_engine: str,
    watermark_column: str | None = None,
    batch_group_friendly_name: str | None = None,
    extraction_config: dict | None = None,
    pipeline_steps: str = "0,1,2,3",
) -> None:
    # Every feed must belong to a batch (see metadata/DataModel.md and
    # 01_platform_metadata.sql's batch_group not-null comment) -- none of
    # today's feeds have a real multi-feed batch relationship yet, so each
    # defaults to being its own singleton batch (batch_group_friendly_name
    # = its own friendly_name) unless a real one is passed in.
    batch_group_friendly_name = batch_group_friendly_name or friendly_name
    cur.execute(
        """
        INSERT INTO data_feed (
            source_system_id, friendly_name, source_object_name, extraction_type,
            source_pk, processing_engine, watermark_column,
            batch_group, batch_group_friendly_name, extraction_config, pipeline_steps
        )
        VALUES (
            (SELECT id FROM source_system WHERE code = %s),
            %s, %s, %s, %s, %s, %s,
            gen_random_uuid(), %s, %s, %s
        )
        ON CONFLICT (friendly_name) DO NOTHING
        """,
        (
            source_system_code, friendly_name, source_object_name, extraction_type,
            psycopg.types.json.Json(source_pk), processing_engine, watermark_column,
            batch_group_friendly_name,
            psycopg.types.json.Json(extraction_config) if extraction_config is not None else None,
            pipeline_steps,
        ),
    )


def seed_schema_registry(cur, *, data_feed_friendly_name: str, version: int, column_definitions: list[dict], created_by: str) -> None:
    cur.execute(
        """
        INSERT INTO schema_registry (data_feed_id, version, column_definitions, is_current, created_by)
        VALUES ((SELECT id FROM data_feed WHERE friendly_name = %s), %s, %s, true, %s)
        ON CONFLICT (data_feed_id, version) DO NOTHING
        """,
        (data_feed_friendly_name, version, psycopg.types.json.Json(column_definitions), created_by),
    )


def seed_lakehouse_model(
    cur,
    *,
    friendly_name: str,
    table_type: str,
    depends_on_feed_friendly_names: list[str],
    owning_feed_friendly_name: str,
    business_key_columns: list[str],
    tracked_columns: list[str],
    scd_type: int,
    deletes_enabled: bool,
    model_schema: str = "model",
    load_type: int = 0,
    updates_enabled: bool = True,
    pipeline_steps: str = "2,3",
) -> None:
    # owning_feed_friendly_name is required, not defaulted from
    # depends_on_feed_friendly_names[0] -- the whole point of this field is
    # that "which feed owns this model" is never implicit (see
    # 01_platform_metadata.sql's owning_feed_id comment).
    assert owning_feed_friendly_name in depends_on_feed_friendly_names, (
        f"owning_feed_friendly_name={owning_feed_friendly_name!r} must be one of "
        f"depends_on_feed_friendly_names={depends_on_feed_friendly_names!r}"
    )
    cur.execute(
        """
        INSERT INTO lakehouse_models (
            friendly_name, model_schema, table_type, business_key_columns,
            tracked_columns, scd_type, updates_enabled, deletes_enabled,
            load_type, depends_on_feeds, owning_feed_id, pipeline_steps
        )
        VALUES (
            %(friendly_name)s, %(model_schema)s, %(table_type)s, %(business_key_columns)s,
            %(tracked_columns)s, %(scd_type)s, %(updates_enabled)s, %(deletes_enabled)s,
            %(load_type)s,
            (SELECT string_agg(id::text, ',') FROM data_feed WHERE friendly_name = ANY(%(depends_on)s)),
            (SELECT id FROM data_feed WHERE friendly_name = %(owning_feed)s),
            %(pipeline_steps)s
        )
        ON CONFLICT (friendly_name) DO NOTHING
        """,
        {
            "friendly_name": friendly_name,
            "model_schema": model_schema,
            "table_type": table_type,
            "business_key_columns": psycopg.types.json.Json(business_key_columns),
            "tracked_columns": psycopg.types.json.Json(tracked_columns),
            "scd_type": scd_type,
            "updates_enabled": updates_enabled,
            "deletes_enabled": deletes_enabled,
            "load_type": load_type,
            "depends_on": depends_on_feed_friendly_names,
            "owning_feed": owning_feed_friendly_name,
            "pipeline_steps": pipeline_steps,
        },
    )


def seed_schedule(
    cur,
    *,
    cron: str,
    controlling_object_type: str,
    controlling_object_friendly_name: str,
) -> None:
    table = "data_feed" if controlling_object_type == "feed" else "lakehouse_models"
    # table is an internal literal (one of exactly two values above), not
    # caller-supplied free text -- same safety pattern as postgres_metadata_
    # resource.py's _ensure_run table/column composition.
    cur.execute(
        f"""
        INSERT INTO schedule (cron, controlling_object_id, controlling_object_type)
        SELECT %(cron)s, id, %(controlling_object_type)s
        FROM {table}
        WHERE friendly_name = %(friendly_name)s
        ON CONFLICT (controlling_object_type, controlling_object_id) DO NOTHING
        """,
        {
            "cron": cron,
            "controlling_object_type": controlling_object_type,
            "friendly_name": controlling_object_friendly_name,
        },
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
        seed_source_system(
            cur,
            code="erp_export",
            name="ERP financial export",
            description="Periodic CSV export of general-ledger transactions, dropped into data-lake/landing/financial_transactions/",
            system_type="file_drop",
            connector_kind="csv",
        )
        seed_source_system(
            cur,
            code="uk_police_api",
            name="UK Police API",
            description="https://data.police.uk/docs/ -- street-level crime data",
            system_type="api",
            connector_kind="rest",
            base_location="https://data.police.uk/api",
        )
        # Connector library plan: metadata_runs queries this platform's own
        # data_processing_runs table (a real Postgres source, previously
        # only reachable by hand-following Walkthrough_Metadata_Source_Feed.md's
        # manual SQL -- not reproducible from a fresh cluster until now).
        seed_source_system(
            cur,
            code="platform_metadata_db",
            name="Platform metadata database",
            description="This platform's own platform_metadata Postgres instance, queried as a source",
            system_type="database",
            connector_kind="postgres",
        )

        seed_data_feed(
            cur,
            source_system_code="phase3_manual",
            friendly_name="customers",
            source_object_name="customers",
            extraction_type="full",
            source_pk=["customer_id"],
            processing_engine="polars",
        )
        seed_data_feed(
            cur,
            source_system_code="supermarket_pos",
            friendly_name="sales",
            source_object_name="sales",
            extraction_type="full",
            source_pk=["invoice_id"],
            processing_engine="polars",
        )
        seed_data_feed(
            cur,
            source_system_code="erp_export",
            friendly_name="financial_transactions",
            source_object_name="financial_transactions",
            extraction_type="incremental",
            source_pk=["transaction_id"],
            processing_engine="polars",
            watermark_column="posted_date",
        )
        seed_data_feed(
            cur,
            source_system_code="uk_police_api",
            friendly_name="police_crimes",
            source_object_name="crimes-street/all-crime",
            extraction_type="incremental",
            source_pk=["id"],
            processing_engine="polars",
            watermark_column="month",
        )
        # No schema_registry seed row for metadata_runs -- deliberately, to
        # prove the connector library's actual point: schema discovery
        # bootstraps schema_registry on its own on the first real run, no
        # hand-written baseline needed (see connectors/schema_registry_sync.py).
        seed_data_feed(
            cur,
            source_system_code="platform_metadata_db",
            friendly_name="metadata_runs",
            source_object_name="data_processing_runs",
            extraction_type="full",
            source_pk=["run_id"],
            processing_engine="polars",
            extraction_config={"query": METADATA_RUNS_QUERY},
        )

        seed_schema_registry(cur, data_feed_friendly_name="customers", version=1, column_definitions=CUSTOMERS_SCHEMA, created_by="seed_metadata_db")
        seed_schema_registry(cur, data_feed_friendly_name="sales", version=1, column_definitions=SALES_SCHEMA, created_by="seed_metadata_db")
        seed_schema_registry(cur, data_feed_friendly_name="financial_transactions", version=1, column_definitions=FINANCIAL_TRANSACTIONS_SCHEMA, created_by="seed_metadata_db")
        seed_schema_registry(cur, data_feed_friendly_name="police_crimes", version=1, column_definitions=POLICE_CRIMES_SCHEMA, created_by="seed_metadata_db")

        # Model layer (Phase 7): dim_customer stands alone (no real FK from
        # sales to customers in this dataset -- see Learnings.md); dim_branch
        # is conformed out of sales' own branch/city columns, and fct_sales
        # joins to it. See Roadmap.md "Model Layer: SCD Design".
        #
        # updates_enabled=False on dim_branch/fct_sales carries forward the
        # already-established "sales is immutable" reasoning (a posted
        # invoice line isn't edited in place, only refunded/voided) -- this
        # used to live on data_feed.updates_enabled, which the metadata
        # redesign removed in favor of the "OR across depends_on_feeds"
        # staging rule (see metadata/DataModel.md, "Staging update-tracking
        # rule"). Setting it false on both dependents is what's required to
        # keep that rule's outcome unchanged for the sales feed.
        seed_lakehouse_model(
            cur,
            friendly_name="dim_customer_snapshot",
            table_type="dimension",
            depends_on_feed_friendly_names=["customers"],
            owning_feed_friendly_name="customers",
            business_key_columns=["customer_id"],
            tracked_columns=["name", "email"],
            scd_type=2,
            deletes_enabled=True,
            updates_enabled=True,
        )
        seed_lakehouse_model(
            cur,
            friendly_name="dim_branch",
            table_type="dimension",
            depends_on_feed_friendly_names=["sales"],
            owning_feed_friendly_name="sales",
            business_key_columns=["branch"],
            tracked_columns=["city"],
            scd_type=1,
            deletes_enabled=False,
            updates_enabled=False,
        )
        seed_lakehouse_model(
            cur,
            friendly_name="fct_sales",
            table_type="fact",
            depends_on_feed_friendly_names=["sales"],
            owning_feed_friendly_name="sales",
            business_key_columns=["invoice_id"],
            tracked_columns=["unit_price", "quantity", "tax_amount", "total", "cogs", "gross_income", "rating"],
            scd_type=1,
            deletes_enabled=False,
            updates_enabled=False,
        )
        # First lakehouse_models row to depend on financial_transactions --
        # flips stg_financial_transactions from the "zero dependents,
        # defaults to updates_enabled=true" case to a real false, matching
        # that staging model's own already-stated insert-only assumption
        # (a posted GL entry isn't edited in place). A correction, not a
        # regression -- see Progress.md.
        seed_lakehouse_model(
            cur,
            friendly_name="fct_daily_financial_activity",
            table_type="fact",
            depends_on_feed_friendly_names=["sales", "financial_transactions"],
            owning_feed_friendly_name="financial_transactions",
            business_key_columns=["source_feed", "source_id"],
            tracked_columns=["activity_date", "category", "amount"],
            scd_type=1,
            deletes_enabled=False,
            updates_enabled=False,
        )

        # Migrates police_crimes' previously-hardcoded _SCHEDULE_CRON into
        # real metadata; fct_daily_financial_activity is the first
        # model-type schedule (expands into one generated Dagster schedule
        # per dependent feed -- see scripts/generate_dagster_pipeline.py).
        seed_schedule(cur, cron="0 6 * * *", controlling_object_type="feed", controlling_object_friendly_name="police_crimes")
        seed_schedule(cur, cron="0 7 * * *", controlling_object_type="model", controlling_object_friendly_name="fct_daily_financial_activity")

        conn.commit()
    print("Seed complete.")


if __name__ == "__main__":
    main()
