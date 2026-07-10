from dagster import ConfigurableResource
from pyiceberg.catalog import Catalog
from raw_to_clean import load_iceberg_catalog


class IcebergCatalogResource(ConfigurableResource):
    """Hands out a PyIceberg catalog client for the raw->clean write path
    (Phase 6) — everything downstream of `clean` still goes through dbt's
    own Trino connection (profiles.yml), not this. See
    raw_to_clean.catalog.load_iceberg_catalog for why the connection is
    configured the way it is."""

    polaris_host: str
    polaris_port: int
    minio_host: str
    minio_port: int

    def get_catalog(self) -> Catalog:
        return load_iceberg_catalog(
            polaris_host=self.polaris_host,
            polaris_port=self.polaris_port,
            minio_host=self.minio_host,
            minio_port=self.minio_port,
        )
