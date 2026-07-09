"""Provisions (or verifies) the "data_platform" Iceberg catalog in Polaris
against MinIO's "lakehouse" bucket, and grants catalog_admin the
TABLE_WRITE_DATA privilege real table purges / staged-write delegation
actually need. Idempotent: safe to re-run against an already-provisioned
realm, and safe to run on a realm that's missing either the catalog or the
privilege grant.

The Python equivalent of what register-catalog.sh used to do via the
Polaris CLI directly — kept as the reference implementation of the specific
Polaris operations this platform's tooling performs, reusable by anything
else that needs them (see query-engine/polaris_client/README or
Learnings.md for why this module exists).
"""

from polaris_client.client import PolarisClient
from polaris_client.port_forward import kubectl_port_forward

POLARIS_PORT = 8181
CLIENT_ID = "root"
CLIENT_SECRET = "s3cr3t"
CATALOG_NAME = "data_platform"


def main() -> None:
    with kubectl_port_forward(
        service="polaris",
        local_port=POLARIS_PORT,
        remote_port=8181,
        namespace="query-engine",
    ):
        client = PolarisClient(
            host="localhost",
            port=POLARIS_PORT,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        if client.catalog_exists(CATALOG_NAME):
            print(f"Catalog '{CATALOG_NAME}' already exists — ensuring properties are current.")
            client.set_catalog_property(
                CATALOG_NAME, "polaris.config.drop-with-purge.enabled", "true"
            )
        else:
            print(f"Creating catalog '{CATALOG_NAME}'...")
            # storage_config_info (role_arn/path_style_access/sts_unavailable)
            # can only be set here — no supported update path, see
            # Learnings.md. If storage config ever needs to change: delete
            # and recreate the catalog.
            client.create_s3_catalog(
                CATALOG_NAME,
                default_base_location="s3://lakehouse",
                allowed_locations=["s3://lakehouse/*"],
                role_arn="arn:aws:iam::000000000000:role/minio-polaris-role",
                path_style_access=True,
                sts_unavailable=True,
                properties={"polaris.config.drop-with-purge.enabled": "true"},
            )

        # TABLE_WRITE_DATA is required for real table purges (DROP TABLE
        # ... purge) and staged-write-delegated table creation — see
        # Learnings.md ("The RBAC privilege gap..."). catalog_admin only
        # has metadata-management privileges by default; those don't imply
        # TABLE_WRITE_DATA. Granting an already-held privilege is a no-op.
        print(f"Ensuring catalog_admin has TABLE_WRITE_DATA on '{CATALOG_NAME}'...")
        client.grant_catalog_privilege(CATALOG_NAME, "catalog_admin", "TABLE_WRITE_DATA")

        print(f"Catalog '{CATALOG_NAME}' ready.")


if __name__ == "__main__":
    main()
