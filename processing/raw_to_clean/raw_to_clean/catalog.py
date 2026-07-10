from pyiceberg.catalog import Catalog, load_catalog


def load_iceberg_catalog(
    *,
    polaris_host: str,
    polaris_port: int,
    minio_host: str,
    minio_port: int,
    warehouse: str = "data_platform",
    client_id: str = "root",
    client_secret: str = "s3cr3t",
    minio_access_key: str = "admin",
    minio_secret_key: str = "password",
) -> Catalog:
    """A PyIceberg REST catalog client pointed at Polaris + MinIO.

    Two things here aren't optional, both proven necessary against a live
    Polaris + MinIO catalog before this was trusted (see Learnings.md,
    Phase 6):

    - `header.X-Iceberg-Access-Delegation: ""` — PyIceberg defaults to
      requesting server-vended S3 credentials, which Polaris can't satisfy
      against this catalog's `stsUnavailable` storage config (same root
      cause as Trino's `vended-credentials-enabled=false`, Phase 3).
      Without this, every write fails with "Credential vending was
      requested ..., but no credentials are available."
    - explicit `oauth2-server-uri` — Polaris's token endpoint isn't at the
      path PyIceberg would otherwise guess from `uri` alone.
    """
    return load_catalog(
        warehouse,
        **{
            "type": "rest",
            "uri": f"http://{polaris_host}:{polaris_port}/api/catalog",
            "warehouse": warehouse,
            "credential": f"{client_id}:{client_secret}",
            "oauth2-server-uri": f"http://{polaris_host}:{polaris_port}/api/catalog/v1/oauth/tokens",
            "scope": "PRINCIPAL_ROLE:ALL",
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": f"http://{minio_host}:{minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )
