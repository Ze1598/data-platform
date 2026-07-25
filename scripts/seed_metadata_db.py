"""Idempotently seeds source_system/data_feed/schema_registry/lakehouse_models/
streaming_source rows for this project's feeds. These are business-configuration rows, not
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
    batch_feed_hierarchy: int = 0,
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
    # batch_feed_hierarchy defaults to 0 (the DDL's own default) -- meaningless
    # for a singleton batch, real once a batch group actually spans multiple
    # tiers (see run_master_pipeline's tiered extraction, Roadmap.md
    # "Hierarchy-tiered, dependency-driven master_pipeline execution").
    cur.execute(
        """
        INSERT INTO data_feed (
            source_system_id, friendly_name, source_object_name, extraction_type,
            source_pk, processing_engine, watermark_column,
            batch_group, batch_group_friendly_name, batch_feed_hierarchy, extraction_config, pipeline_steps,
            ods_enabled, batch_ods_name
        )
        VALUES (
            (SELECT id FROM source_system WHERE code = %s),
            %s, %s, %s, %s, %s, %s,
            gen_random_uuid(), %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (friendly_name) DO NOTHING
        """,
        (
            source_system_code, friendly_name, source_object_name, extraction_type,
            psycopg.types.json.Json(source_pk), processing_engine, watermark_column,
            batch_group_friendly_name, batch_feed_hierarchy,
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


def seed_lakehouse_model_columns(cur, *, model_friendly_name: str, columns: list[dict]) -> None:
    """Idempotently seeds lakehouse_model_columns rows for one model --
    the same data frontend/pages/6_Model_Table_Columns.py's editor grid
    submits, here as a reproducible seed for a model that predates any
    real user filling in that page. Each column dict: column_name,
    source_feed_friendly_name, data_type, is_nullable, is_business_key,
    is_tracked. `ordinal_position` is the list's own order, not passed
    explicitly -- same convention as the frontend page (entry order in
    the editor grid)."""
    for position, col in enumerate(columns):
        cur.execute(
            """
            INSERT INTO lakehouse_model_columns (
                model_id, source_feed_id, column_name, data_type,
                is_nullable, is_business_key, is_tracked, ordinal_position
            )
            VALUES (
                (SELECT id FROM lakehouse_models WHERE friendly_name = %(model_friendly_name)s),
                (SELECT id FROM data_feed WHERE friendly_name = %(source_feed_friendly_name)s),
                %(column_name)s, %(data_type)s, %(is_nullable)s, %(is_business_key)s, %(is_tracked)s, %(ordinal_position)s
            )
            ON CONFLICT (model_id, column_name) DO NOTHING
            """,
            {
                "model_friendly_name": model_friendly_name,
                "source_feed_friendly_name": col["source_feed_friendly_name"],
                "column_name": col["column_name"],
                "data_type": col["data_type"],
                "is_nullable": col["is_nullable"],
                "is_business_key": col["is_business_key"],
                "is_tracked": col["is_tracked"],
                "ordinal_position": position,
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


def seed_streaming_source(
    cur,
    *,
    friendly_name: str,
    topic_name: str,
    table_name: str,
    model_schema: str,
) -> None:
    # Roadmap Phase 11 generalization -- streaming_source is a real
    # metadata row (seeded here, same as data_feed/lakehouse_models rows
    # above), but its schema_registry entry is never hand-seeded, same
    # "discovery bootstraps it, no hand-written baseline needed" rule as
    # a data_feed's schema_registry entry -- see 4_Streaming_Sources.py's
    # "Discover Schema" action. event_timestamp_column is likewise left
    # null here, set by hand via the frontend once discovery has run.
    cur.execute(
        """
        INSERT INTO streaming_source (friendly_name, topic_name, table_name, model_schema)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (friendly_name) DO NOTHING
        """,
        (friendly_name, topic_name, table_name, model_schema),
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
        # iot_telemetry_landing backs the iot_telemetry batch group below --
        # synthetic IoT device telemetry, dropped as JSON files into
        # data-lake/landing/<feed>/ rather than a real external system.
        # Exists to prove batch_feed_hierarchy tiered extraction against a
        # real multi-feed batch (Backlog.md's "batch_group/batch_feed_hierarchy
        # are metadata-only" item, Roadmap.md "Hierarchy-tiered,
        # dependency-driven master_pipeline execution").
        seed_source_system(
            cur,
            code="iot_telemetry_landing",
            name="IoT Telemetry Landing",
            description="Synthetic IoT device telemetry, dropped as JSON files into data-lake/landing/<feed>/ -- not a real external system, exists to test tiered batch extraction",
            system_type="file_drop",
            connector_kind="json_file",
        )
        # metadata_runs queries this platform's own data_processing_runs
        # table, a real Postgres source -- see Walkthrough_Metadata_Ingestion.md
        # for the full reproducible-from-a-fresh-cluster setup.
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
            # since source_pk is set above). A feed with no lakehouse_models
            # row and no ODS domain has nowhere to build under the
            # domain-based topology (Roadmap.md "multi-project dbt split").
            # batch_ods_name defaults to this feed's own
            # batch_group_friendly_name (itself defaulting to
            # "police_crimes", its own singleton batch).
            ods_enabled=True,
            batch_ods_name="police_crimes",
        )
        # No schema_registry seed row for metadata_runs -- deliberately, to
        # prove the connector library's actual point: schema discovery
        # bootstraps schema_registry on its own on the first real run, no
        # hand-written baseline needed (see connectors/schema_registry_sync.py).
        # extraction_config left unset (the default): PostgresConnector
        # builds a plain SELECT * FROM data_processing_runs from
        # source_object_name alone -- no custom query needed for a
        # single-table source (see metadata/DataModel.md,
        # data_feed.extraction_config).
        seed_data_feed(
            cur,
            source_system_code="platform_metadata_db",
            friendly_name="metadata_runs",
            source_object_name="data_processing_runs",
            extraction_type="full",
            source_pk=["run_id"],
            processing_engine="polars",
        )

        # schema_registry is never hand-seeded -- extraction's own schema
        # discovery (connectors.schema_registry_sync.sync_schema_registry())
        # populates it for every feed, uniformly, from each feed's first
        # real run.

        # iot_telemetry batch group: a real multi-feed, multi-tier batch
        # (unlike every other feed above, still its own singleton batch) --
        # 2 feeds at tier 1, 3 at tier 2, 1 at tier 3, all sharing
        # batch_group_friendly_name="iot_telemetry". No ODS domain for any
        # of these (ods_enabled left at its default false) since this batch
        # group exists to prove feed-tier parallel extraction, not
        # model-tiering (out of scope for this test, see Roadmap.md).
        # pipeline_steps="0" (extraction only) for every feed here EXCEPT
        # device_heartbeats, which gained a real lakehouse_models consumer
        # (dim_iot_device, seeded below) once this became the live test for
        # the lakehouse_model_columns frontend/codegen feature -- "0,1,2"
        # there so its tag's dbt nodes aren't excluded from the domain's
        # transformation/serving build (dbt_assets.py's per-feed
        # cherry-picking would otherwise skip the whole build as a no-op:
        # confirmed live, a first attempt with "0" here produced a
        # trivially-successful 41ms dbt step that built nothing). Every
        # other feed keeps pipeline_steps="0" -- extraction only, no model
        # consumer. extraction_type="full"/no watermark_column: each run
        # just reads whatever JSON files are currently in landing, no
        # incremental filtering needed for this test.
        seed_data_feed(
            cur,
            source_system_code="iot_telemetry_landing",
            friendly_name="device_heartbeats",
            source_object_name="device_heartbeats",
            extraction_type="full",
            source_pk=["device_id", "ts"],
            processing_engine="polars",
            batch_group_friendly_name="iot_telemetry",
            batch_feed_hierarchy=1,
            pipeline_steps="0,1,2",
        )
        for friendly_name, source_pk in [
            ("device_errors", ["device_id", "ts"]),
        ]:
            seed_data_feed(
                cur,
                source_system_code="iot_telemetry_landing",
                friendly_name=friendly_name,
                source_object_name=friendly_name,
                extraction_type="full",
                source_pk=source_pk,
                processing_engine="polars",
                batch_group_friendly_name="iot_telemetry",
                batch_feed_hierarchy=1,
                pipeline_steps="0",
            )
        for friendly_name, source_pk in [
            ("session_events", ["session_id", "ts"]),
            ("location_pings", ["device_id", "ts"]),
            ("network_usage", ["device_id", "ts"]),
        ]:
            seed_data_feed(
                cur,
                source_system_code="iot_telemetry_landing",
                friendly_name=friendly_name,
                source_object_name=friendly_name,
                extraction_type="full",
                source_pk=source_pk,
                processing_engine="polars",
                batch_group_friendly_name="iot_telemetry",
                batch_feed_hierarchy=2,
                pipeline_steps="0",
            )
        seed_data_feed(
            cur,
            source_system_code="iot_telemetry_landing",
            friendly_name="device_health_snapshots",
            source_object_name="device_health_snapshots",
            extraction_type="full",
            source_pk=["device_id", "ts"],
            processing_engine="polars",
            batch_group_friendly_name="iot_telemetry",
            batch_feed_hierarchy=3,
            pipeline_steps="0",
        )

        # Model layer (Phase 7): dim_customer stands alone (no real FK from
        # sales to customers in this dataset -- see Learnings.md); dim_branch
        # is conformed out of sales' own branch/city columns, and fct_sales
        # joins to it. See Roadmap.md "Model Layer: SCD Design".
        #
        # updates_enabled=False on dim_branch/fct_sales reflects sales being
        # immutable (a posted invoice line isn't edited in place, only
        # refunded/voided) -- see metadata/DataModel.md, "Staging
        # update-tracking rule" for how this flag propagates (staging only
        # tracks updates for a feed if at least one dependent model has
        # updates_enabled=true; setting it false on both of sales' models
        # keeps that feed's staging insert-only).
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

        # metadata domain (the metadata_runs feed's own fact) -- see
        # Walkthrough_Metadata_Ingestion.md for the full reproducible setup.
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

        # iot_telemetry domain -- a live test of the frontend's column-
        # definition page (frontend/pages/6_Model_Table_Columns.py) and
        # generate_model_scaffolds.py's dedicated-staging codegen (Backlog.md's
        # "Frontend page for defining model tables"), kept seeded rather than
        # a one-off manual test so every nuke-and-rebuild continues to prove
        # it, same reasoning as inventory_events below. Single-feed
        # (device_heartbeats, from the iot_telemetry batch group) -- proves
        # the primary, single-feed path; the multi-feed CTE-per-feed/TODO-
        # combine path is implemented but not exercised by this seed, since
        # no real model spans 2+ feeds today.
        seed_lakehouse_model(
            cur,
            friendly_name="dim_iot_device",
            table_name="iot_telemetry_dim_device",
            model_schema="iot_telemetry",
            table_type="dimension",
            depends_on_feed_friendly_names=["device_heartbeats"],
            owning_feed_friendly_name="device_heartbeats",
            business_key_columns=[],
            tracked_columns=[],
            scd_type=1,
            deletes_enabled=False,
        )
        seed_lakehouse_model_columns(
            cur,
            model_friendly_name="dim_iot_device",
            columns=[
                {
                    "column_name": "device_id", "source_feed_friendly_name": "device_heartbeats",
                    "data_type": "string", "is_nullable": False, "is_business_key": True, "is_tracked": False,
                },
                {
                    "column_name": "battery_level", "source_feed_friendly_name": "device_heartbeats",
                    "data_type": "long", "is_nullable": False, "is_business_key": False, "is_tracked": True,
                },
                {
                    "column_name": "signal_strength", "source_feed_friendly_name": "device_heartbeats",
                    "data_type": "long", "is_nullable": False, "is_business_key": False, "is_tracked": True,
                },
                {
                    "column_name": "firmware_version", "source_feed_friendly_name": "device_heartbeats",
                    "data_type": "string", "is_nullable": False, "is_business_key": False, "is_tracked": True,
                },
                {
                    # Deliberately neither business key nor tracked -- proves
                    # the third state (a passthrough column excluded from
                    # both _key_hash and _attr_hash).
                    "column_name": "ts", "source_feed_friendly_name": "device_heartbeats",
                    "data_type": "timestamp", "is_nullable": False, "is_business_key": False, "is_tracked": False,
                },
            ],
        )

        # police_crimes' cron schedule; fct_daily_financial_activity is a
        # model-type schedule (see scripts/generate_dagster_pipeline.py).
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

        # streaming_source (Roadmap Phase 11 generalization) -- migrates
        # the original hand-built sales_events stream into the new
        # metadata-driven onboarding flow (streaming/'s first hand-written
        # slice, see Progress.md's Phase 11 section). model_schema='sales'
        # since its serve view joins to sales_dim_branch, seeded above.
        seed_streaming_source(
            cur,
            friendly_name="sales_events",
            topic_name="sales-events",
            table_name="sales_events",
            model_schema="sales",
        )
        # Second concurrent source -- deliberately kept seeded, not just a
        # one-off manual test row, so every nuke-and-rebuild (this
        # project's own regression-testing methodology) continues to prove
        # two independent Flink TaskManagers/Iceberg sinks run side by
        # side, not just one. Its serve view is still a bare
        # generate_streaming_serve_scaffolds.py TODO scaffold (see
        # dbt/domains/sales/models/serve/streaming/inventory_events.sql) --
        # no real dimension to join yet, same as when this was proven live.
        seed_streaming_source(
            cur,
            friendly_name="inventory_events",
            topic_name="inventory-events",
            table_name="inventory_events",
            model_schema="sales",
        )

        conn.commit()
    print("Seed complete.")


if __name__ == "__main__":
    main()
