import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_data_platform.assets.dbt_assets import dbt_project
from dagster_data_platform.assets.metadata_runs_assets import landing_metadata_runs,raw_metadata_runs, clean_metadata_runs
from dagster_data_platform.assets.extraction_assets import clean_customers, landing_customers, raw_customers
from dagster_data_platform.assets.financial_assets import (
    archive_financial_transactions,
    clean_financial_transactions,
    financial_transactions_sensor,
    landing_financial_transactions,
    raw_financial_transactions,
)
from dagster_data_platform.assets.police_assets import clean_police_crimes, landing_police_crimes, raw_police_crimes
from dagster_data_platform.assets.sales_assets import clean_sales, landing_sales, raw_sales
from dagster_data_platform.pipeline_generated import ALL_DBT_ASSETS, ALL_FEED_JOBS, ALL_SCHEDULES
from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource

defs = Definitions(
    assets=[
        landing_metadata_runs,
        raw_metadata_runs,
        clean_metadata_runs,
        landing_customers,
        raw_customers,
        clean_customers,
        landing_sales,
        raw_sales,
        clean_sales,
        landing_financial_transactions,
        raw_financial_transactions,
        clean_financial_transactions,
        archive_financial_transactions,
        landing_police_crimes,
        raw_police_crimes,
        clean_police_crimes,
        *ALL_DBT_ASSETS,
    ],
    jobs=ALL_FEED_JOBS,
    sensors=[financial_transactions_sensor],
    schedules=ALL_SCHEDULES,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project.project_dir, profiles_dir=dbt_project.profiles_dir),
        "postgres_metadata": PostgresMetadataResource(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ.get("POSTGRES_USER", "platform"),
            password=os.environ.get("POSTGRES_PASSWORD", "platform"),
            dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
        ),
        "iceberg_catalog": IcebergCatalogResource(
            polaris_host=os.environ.get("POLARIS_HOST", "localhost"),
            polaris_port=int(os.environ.get("POLARIS_PORT", "8181")),
            minio_host=os.environ.get("MINIO_HOST", "localhost"),
            minio_port=int(os.environ.get("MINIO_PORT", "9000")),
        ),
    },
)
