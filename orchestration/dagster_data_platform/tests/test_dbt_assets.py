from dagster import AssetKey

from dagster_data_platform.assets.dbt_assets import DataPlatformDbtTranslator


def test_clean_source_tables_map_to_stub_asset_keys():
    translator = DataPlatformDbtTranslator()

    for table in ("customers", "sales"):
        props = {"resource_type": "source", "source_name": "clean", "name": table}
        assert translator.get_asset_key(props) == AssetKey(f"clean_{table}")
