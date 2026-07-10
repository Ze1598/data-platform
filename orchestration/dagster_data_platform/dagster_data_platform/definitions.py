import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_data_platform.assets.dbt_assets import data_platform_dbt_assets, dbt_project
from dagster_data_platform.assets.extraction_assets import clean_customers, landing_customers, raw_customers
from dagster_data_platform.resources.postgres_metadata_resource import PostgresMetadataResource
from dagster_data_platform.resources.trino_resource import TrinoResource

defs = Definitions(
    assets=[landing_customers, raw_customers, clean_customers, data_platform_dbt_assets],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project.project_dir, profiles_dir=dbt_project.profiles_dir),
        "postgres_metadata": PostgresMetadataResource(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ.get("POSTGRES_USER", "platform"),
            password=os.environ.get("POSTGRES_PASSWORD", "platform"),
            dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
        ),
        "trino": TrinoResource(
            host=os.environ.get("TRINO_HOST", "localhost"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
        ),
    },
)
