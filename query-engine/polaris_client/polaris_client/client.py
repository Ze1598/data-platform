import json
import os
from typing import Optional

from apache_polaris.sdk.management import (
    AddGrantRequest,
    ApiClient,
    AwsStorageConfigInfo,
    Catalog,
    CatalogGrant,
    CatalogPrivilege,
    CatalogProperties,
    Configuration,
    CreateCatalogRequest,
    GrantResources,
    PolarisCatalog,
    PolarisDefaultApi,
    RevokeGrantRequest,
    UpdateCatalogRequest,
)
from apache_polaris.sdk.management.exceptions import NotFoundException


class PolarisClient:
    """Wraps apache_polaris.sdk.management.PolarisDefaultApi with the
    specific catalog/privilege operations this platform's tooling actually
    performs — not the full Management API surface.

    Deliberately mirrors the request-construction patterns the Polaris CLI
    itself uses (apache_polaris.cli.command.catalogs/privileges), not a
    from-scratch implementation: catalog property updates always re-fetch
    the current catalog and merge into its existing properties before
    PUT-ing, because a partial hand-rolled update body has been proven to
    silently no-op against this server rather than error (see Learnings.md,
    "DROP_WITH_PURGE_ENABLED..." and "The RBAC privilege gap...").
    """

    def __init__(self, host: str, port: int, client_id: str, client_secret: str):
        self._base_url = f"http://{host}:{port}"
        self._client_id = client_id
        self._client_secret = client_secret
        self._api = PolarisDefaultApi(self._build_api_client())

    @classmethod
    def from_env(cls) -> "PolarisClient":
        return cls(
            host=os.environ.get("POLARIS_HOST", "localhost"),
            port=int(os.environ.get("POLARIS_PORT", "8181")),
            client_id=os.environ["POLARIS_CLIENT_ID"],
            client_secret=os.environ["POLARIS_CLIENT_SECRET"],
        )

    def _build_api_client(self) -> ApiClient:
        config = Configuration(host=f"{self._base_url}/api/management/v1")
        config.access_token = self._fetch_token()
        return ApiClient(config)

    def _fetch_token(self) -> str:
        conf = Configuration(host=f"{self._base_url}/api/management/v1")
        response = ApiClient(conf).call_api(
            "POST",
            f"{self._base_url}/api/catalog/v1/oauth/tokens",
            header_params={"Content-Type": "application/x-www-form-urlencoded"},
            post_params={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
        ).response.data
        data = json.loads(response)
        if "access_token" not in data:
            raise RuntimeError(f"Failed to get Polaris access token: {data}")
        return data["access_token"]

    # --- catalogs -------------------------------------------------------

    def get_catalog(self, catalog_name: str) -> Catalog:
        return self._api.get_catalog(catalog_name)

    def catalog_exists(self, catalog_name: str) -> bool:
        try:
            self._api.get_catalog(catalog_name)
            return True
        except NotFoundException:
            return False

    def create_s3_catalog(
        self,
        catalog_name: str,
        *,
        default_base_location: str,
        allowed_locations: list[str],
        role_arn: str,
        path_style_access: bool = True,
        sts_unavailable: bool = True,
        properties: Optional[dict[str, str]] = None,
    ) -> Catalog:
        storage_config = AwsStorageConfigInfo(
            storage_type="S3",
            allowed_locations=allowed_locations,
            role_arn=role_arn,
            path_style_access=path_style_access,
            sts_unavailable=sts_unavailable,
        )
        request = CreateCatalogRequest(
            catalog=PolarisCatalog(
                type="INTERNAL",
                name=catalog_name,
                storage_config_info=storage_config,
                properties=CatalogProperties(
                    default_base_location=default_base_location,
                    additional_properties=properties or {},
                ),
            )
        )
        return self._api.create_catalog(request)

    def set_catalog_property(self, catalog_name: str, key: str, value: str) -> Catalog:
        """Merge one property into an existing catalog. storage_config_info
        (role_arn/path_style_access/sts_unavailable) has no update path —
        only properties can change after creation. See Learnings.md."""
        catalog = self._api.get_catalog(catalog_name)
        merged_properties = dict(catalog.properties.additional_properties or {})
        merged_properties[key] = value
        request = UpdateCatalogRequest(
            current_entity_version=catalog.entity_version,
            properties=CatalogProperties(
                default_base_location=catalog.properties.default_base_location,
                additional_properties=merged_properties,
            ).to_dict(),
        )
        return self._api.update_catalog(catalog_name, request)

    def delete_catalog(self, catalog_name: str) -> None:
        self._api.delete_catalog(catalog_name)

    # --- catalog-role privileges -----------------------------------------

    def list_catalog_role_privileges(
        self, catalog_name: str, catalog_role: str
    ) -> GrantResources:
        return self._api.list_grants_for_catalog_role(catalog_name, catalog_role)

    def grant_catalog_privilege(
        self, catalog_name: str, catalog_role: str, privilege: str
    ) -> None:
        grant = CatalogGrant(type="catalog", privilege=CatalogPrivilege(privilege))
        self._api.add_grant_to_catalog_role(
            catalog_name, catalog_role, AddGrantRequest(grant=grant)
        )

    def revoke_catalog_privilege(
        self,
        catalog_name: str,
        catalog_role: str,
        privilege: str,
        cascade: bool = False,
    ) -> None:
        grant = CatalogGrant(type="catalog", privilege=CatalogPrivilege(privilege))
        self._api.revoke_grant_from_catalog_role(
            catalog_name, catalog_role, cascade, RevokeGrantRequest(grant=grant)
        )
