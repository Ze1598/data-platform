import polars as pl

from dagster_data_platform.connectors.police_crimes_connector import Connector


def _connector() -> Connector:
    return Connector(base_url="https://data.police.uk/api")


def test_flatten_extracts_nested_location_and_outcome_fields():
    raw = pl.DataFrame(
        [
            {
                "id": 1,
                "persistent_id": "abc",
                "category": "burglary",
                "location_type": "Force",
                "location_subtype": None,
                "location": {
                    "street": {"id": 100, "name": "On or near Test Street"},
                    "latitude": "51.5",
                    "longitude": "-0.1",
                },
                "context": None,
                "month": "2026-01",
                "outcome_status": {"category": "Under investigation", "date": "2026-02"},
            }
        ]
    )
    flat = _connector().flatten(raw)
    row = flat.to_dicts()[0]
    assert row["street_id"] == 100
    assert row["street_name"] == "On or near Test Street"
    assert row["latitude"] == 51.5
    assert row["outcome_category"] == "Under investigation"
    assert row["persistent_id"] == "abc"
    assert row["context"] == ""  # null filled to empty string, not None


def test_flatten_handles_all_rows_missing_outcome():
    # outcome_status infers as Null (not Struct) when every row lacks it --
    # the specific failure mode _outcome_field() guards against.
    raw = pl.DataFrame(
        [
            {
                "id": 1,
                "persistent_id": None,
                "category": "burglary",
                "location_type": None,
                "location_subtype": None,
                "location": {"street": {"id": 100, "name": "Test"}, "latitude": "51.5", "longitude": "-0.1"},
                "context": None,
                "month": "2026-01",
                "outcome_status": None,
            }
        ]
    )
    assert raw.schema["outcome_status"] == pl.Null
    flat = _connector().flatten(raw)
    row = flat.to_dicts()[0]
    assert row["outcome_category"] is None
    assert row["outcome_date"] is None
