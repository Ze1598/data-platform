import polars as pl
import pytest

from raw_to_clean import MissingColumnsError, reconcile_schema

_COLUMNS = [
    {"name": "id", "data_type": "long", "nullable": False, "ordinal": 1},
    {"name": "name", "data_type": "string", "nullable": True, "ordinal": 2},
]


def test_matching_dataframe_passes_through_unchanged():
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    result = reconcile_schema(df, _COLUMNS)
    assert result.to_dicts() == df.to_dicts()


def test_all_null_column_is_cast_to_registered_type():
    df = pl.DataFrame({"id": [1, 2], "name": [None, None]})
    assert df.schema["name"] == pl.Null
    result = reconcile_schema(df, _COLUMNS)
    assert result.schema["name"] == pl.Utf8


def test_missing_registered_column_raises():
    df = pl.DataFrame({"id": [1, 2]})
    with pytest.raises(MissingColumnsError, match="name"):
        reconcile_schema(df, _COLUMNS)


def test_column_not_yet_in_registry_raises_caller_bug_error():
    # Schema discovery (connectors.schema_registry_sync) should have added
    # any genuinely new column to the registry before this ever runs --
    # reconcile_schema() itself no longer evolves the registry.
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "new_col": ["x", "y"]})
    with pytest.raises(ValueError, match="new_col"):
        reconcile_schema(df, _COLUMNS)


def test_type_mismatch_is_left_for_validate_schema_to_report():
    # reconcile_schema() no longer auto-heals a type change (that's
    # discovery's job, run before this) -- a genuine mismatch passes
    # through untouched, for validate_schema() to catch afterward.
    df = pl.DataFrame({"id": ["not-a-long"], "name": ["a"]})
    result = reconcile_schema(df, _COLUMNS)
    assert result.schema["id"] == pl.Utf8
