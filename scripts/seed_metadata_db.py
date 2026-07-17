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

# Connector library plan: metadata_runs (Postgres source, this platform's
# own metadata DB) -- moved verbatim from the now-deleted
# metadata_runs_assets.py, which existed only via
# Walkthrough_Metadata_Source_Feed.md's manual setup. Stored as
# data_feed.extraction_config, read live by the generated PostgresConnector
# at landing time -- never baked into generated code.
METADATA_RUNS_QUERY = """
    select
        r.run_id::text as run_id, r.data_feed_id::text as data_feed_id, r.model_key,
        r.tracking_group, r.tracking_group_type, r.master_dagster_run_id,
        r.extraction_dagster_run_id,
        r.transformation_dagster_run_id, r.serving_dagster_run_id,
        r.job_started_timestamp, r.job_ended_timestamp, r.job_successful,
        r.raw_rows_read, r.clean_rows_inserted,
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
    pipeline_steps: str = "0,1,2",
    ods_enabled: bool = False,
    batch_ods_name: str | None = None,
) -> None:
    # Every feed must belong to a batch (see metadata/DataModel.md and
    # 01_platform_metadata.sql's batch_group not-null comment) -- none of
    # today's feeds have a real multi-feed batch relationship yet, so each
    # defaults to being its own singleton batch (batch_group_friendly_name
    # = its own friendly_name) unless a real one is passed in.
    batch_group_friendly_name = batch_group_friendly_name or friendly_name
    # ods_enabled/batch_ods_name default to off/null for every feed that
    # doesn't pass them -- matches the DDL's own defaults
    # (data_feed.ods_enabled default false, batch_ods_name nullable), so
    # every existing feed's behavior is unchanged unless explicitly opted
    # in (see Roadmap.md "ODS layer" / "multi-project dbt split").
    cur.execute(
        """
        INSERT INTO data_feed (
            source_system_id, friendly_name, source_object_name, extraction_type,
            source_pk, processing_engine, watermark_column,
            batch_group, batch_group_friendly_name, extraction_config, pipeline_steps,
            ods_enabled, batch_ods_name
        )
        VALUES (
            (SELECT id FROM source_system WHERE code = %s),
            %s, %s, %s, %s, %s, %s,
            gen_random_uuid(), %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (friendly_name) DO NOTHING
        """,
        (
            source_system_code, friendly_name, source_object_name, extraction_type,
            psycopg.types.json.Json(source_pk), processing_engine, watermark_column,
            batch_group_friendly_name,
            psycopg.types.json.Json(extraction_config) if extraction_config is not None else None,
            pipeline_steps,
            ods_enabled, batch_ods_name,
        ),
    )


def seed_lakehouse_model(
    cur,
    *,
    friendly_name: str,
    table_name: str,
    model_schema: str,
    table_type: str,
    depends_on_feed_friendly_names: list[str],
    owning_feed_friendly_name: str,
    business_key_columns: list[str],
    tracked_columns: list[str],
    scd_type: int,
    deletes_enabled: bool,
    load_type: int = 0,
    updates_enabled: bool = True,
    pipeline_steps: str = "1,2",
) -> None:
    # owning_feed_friendly_name is required, not defaulted from
    # depends_on_feed_friendly_names[0] -- the whole point of this field is
    # that "which feed owns this model" is never implicit (see
    # 01_platform_metadata.sql's owning_feed_id comment).
    assert owning_feed_friendly_name in depends_on_feed_friendly_names, (
        f"owning_feed_friendly_name={owning_feed_friendly_name!r} must be one of "
        f"depends_on_feed_friendly_names={depends_on_feed_friendly_names!r}"
    )
    # table_name/model_schema have no default (unlike model_schema's old
    # 'model' default, pre multi-project-dbt-split) -- every caller must be
    # explicit about which domain (dbt/domains/<model_schema>/) a model
    # belongs to and what its technical identifier is, see
    # metadata/DataModel.md, lakehouse_models.table_name/model_schema.
    cur.execute(
        """
        INSERT INTO lakehouse_models (
            friendly_name, table_name, model_schema, table_type, business_key_columns,
            tracked_columns, scd_type, updates_enabled, deletes_enabled,
            load_type, depends_on_feeds, owning_feed_id, pipeline_steps
        )
        VALUES (
            %(friendly_name)s, %(table_name)s, %(model_schema)s, %(table_type)s, %(business_key_columns)s,
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
            "table_name": table_name,
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


def seed_ingestion_trigger(
    cur,
    *,
    trigger_type: str,
    controlling_object_type: str,
    controlling_object_friendly_name: str,
    cron: str | None = None,
) -> None:
    table = "data_feed" if controlling_object_type == "feed" else "lakehouse_models"
    # table is an internal literal (one of exactly two values above), not
    # caller-supplied free text -- same safety pattern as postgres_metadata_
    # resource.py's _ensure_run table/column composition.
    cur.execute(
        f"""
        INSERT INTO ingestion_triggers (trigger_type, cron, controlling_object_id, controlling_object_type)
        SELECT %(trigger_type)s, %(cron)s, id, %(controlling_object_type)s
        FROM {table}
        WHERE friendly_name = %(friendly_name)s
        ON CONFLICT (controlling_object_type, controlling_object_id) DO NOTHING
        """,
        {
            "trigger_type": trigger_type,
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
            # No hand-modeled dimension/fact owns this feed -- ODS delivers
            # an automatic Type 1 model.police_crimes table instead (keyed,
            # since source_pk is set above), superseding the old
            # hand-written stg_police_crimes.sql (deleted, see Roadmap.md
            # "multi-project dbt split" -- a feed with no lakehouse_models
            # row and no ODS domain has nowhere to build under the
            # domain-based topology). batch_ods_name defaults to this
            # feed's own batch_group_friendly_name (itself defaulting to
            # "police_crimes", its own singleton batch).
            ods_enabled=True,
            batch_ods_name="police_crimes",
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

        # schema_registry is never hand-seeded -- extraction's own schema
        # discovery (connectors.schema_registry_sync.sync_schema_registry())
        # populates it for every feed, uniformly, from each feed's first
        # real run. metadata_runs above already followed this correctly;
        # customers/sales/financial_transactions/police_crimes used to have
        # a seed_schema_registry() call here that bypassed discovery
        # entirely -- removed, see .claude/plans/
        # fix-schema-registry-extraction-ownership.md.

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
            table_name="sales_dim_customer",
            model_schema="sales",
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
            table_name="sales_dim_branch",
            model_schema="sales",
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
            table_name="sales_fct_sales",
            model_schema="sales",
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
            table_name="sales_fct_daily_financial_activity",
            model_schema="sales",
            table_type="fact",
            depends_on_feed_friendly_names=["sales", "financial_transactions"],
            owning_feed_friendly_name="financial_transactions",
            business_key_columns=["source_feed", "source_id"],
            tracked_columns=["activity_date", "category", "amount"],
            scd_type=1,
            deletes_enabled=False,
            updates_enabled=False,
        )

        # metadata domain: Walkthrough_Metadata_Source_Feed.md's worked
        # example, hand-built directly against dbt/data_platform/ (its own
        # lakehouse_models rows inserted by hand while following that
        # walkthrough, never added here) -- backfilled here so the whole
        # walkthrough scenario becomes reproducible from a fresh cluster,
        # same "not reproducible until now" fix already applied to
        # metadata_runs the FEED itself. See the multi-project dbt split
        # addendum for why this needed resolving now: these three model
        # files exist on disk with no seeding call, discovered while
        # migrating dbt/data_platform/ into dbt/domains/.
        seed_lakehouse_model(
            cur,
            friendly_name="dim_metadata_feed",
            table_name="metadata_dim_feed",
            model_schema="metadata",
            table_type="dimension",
            depends_on_feed_friendly_names=["metadata_runs"],
            owning_feed_friendly_name="metadata_runs",
            business_key_columns=["feed_friendly_name"],
            tracked_columns=[
                "feed_batch_group_friendly_name", "feed_extraction_type",
                "feed_processing_engine", "feed_is_active",
            ],
            scd_type=1,
            deletes_enabled=False,
            updates_enabled=True,
        )
        seed_lakehouse_model(
            cur,
            friendly_name="dim_metadata_model",
            table_name="metadata_dim_model",
            model_schema="metadata",
            table_type="dimension",
            depends_on_feed_friendly_names=["metadata_runs"],
            owning_feed_friendly_name="metadata_runs",
            business_key_columns=["model_friendly_name"],
            tracked_columns=[
                "model_model_schema", "model_table_type", "model_scd_type",
                "model_updates_enabled", "model_deletes_enabled",
            ],
            scd_type=1,
            deletes_enabled=False,
            updates_enabled=True,
        )
        seed_lakehouse_model(
            cur,
            friendly_name="fct_metadata_runs",
            table_name="metadata_fct_runs",
            model_schema="metadata",
            table_type="fact",
            depends_on_feed_friendly_names=["metadata_runs"],
            owning_feed_friendly_name="metadata_runs",
            business_key_columns=["run_id"],
            tracked_columns=[
                "job_successful", "job_ended_timestamp", "raw_rows_read",
                "clean_rows_inserted", "staging_rows_updated", "model_rows_updated", "serve_rows_read",
            ],
            scd_type=1,
            deletes_enabled=False,
            updates_enabled=True,
        )

        # Migrates police_crimes' previously-hardcoded _SCHEDULE_CRON into
        # real metadata; fct_daily_financial_activity is the first
        # model-type schedule (expands into one generated Dagster schedule
        # per dependent feed -- see scripts/generate_dagster_pipeline.py).
        seed_ingestion_trigger(
            cur, trigger_type="schedule", cron="0 6 * * *",
            controlling_object_type="feed", controlling_object_friendly_name="police_crimes",
        )
        seed_ingestion_trigger(
            cur, trigger_type="schedule", cron="0 7 * * *",
            controlling_object_type="model", controlling_object_friendly_name="fct_daily_financial_activity",
        )
        # Migrates the original hand-wired financial_transactions_sensor
        # into a real, generated, metadata-driven sensor (Item 2+3's
        # ingestion_triggers generalization) -- financial_transactions is
        # csv-kind (has a landing directory), so it's sensor-eligible.
        seed_ingestion_trigger(
            cur, trigger_type="sensor",
            controlling_object_type="feed", controlling_object_friendly_name="financial_transactions",
        )

        conn.commit()
    print("Seed complete.")


if __name__ == "__main__":
    main()
