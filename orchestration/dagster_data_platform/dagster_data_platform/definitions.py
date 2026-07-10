import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_data_platform.assets.dbt_assets import dbt_customers_assets, dbt_sales_assets, dbt_project
from dagster_data_platform.assets.extraction_assets import clean_customers, landing_customers, raw_customers
from dagster_data_platform.assets.sales_assets import clean_sales, landing_sales, raw_sales
from dagster_data_platform.resources.iceberg_resource import IcebergCatalogResource
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource

defs = Definitions(
    assets=[
        landing_customers,
        raw_customers,
        clean_customers,
        dbt_customers_assets,
        landing_sales,
        raw_sales,
        clean_sales,
        dbt_sales_assets,
    ],
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
