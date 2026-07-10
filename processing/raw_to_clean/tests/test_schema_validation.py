import polars as pl
import pytest

from raw_to_clean import SchemaValidationError, validate_schema

_COLUMNS = [
    {"name": "id", "data_type": "long", "nullable": False, "ordinal": 1},
    {"name": "name", "data_type": "string", "nullable": True, "ordinal": 2},
]


def test_valid_dataframe_passes():
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    validate_schema(df, _COLUMNS)


def test_missing_column_raises():
    df = pl.DataFrame({"id": [1, 2]})
    with pytest.raises(SchemaValidationError, match="missing required columns"):
        validate_schema(df, _COLUMNS)


def test_unexpected_column_raises():
    df = pl.DataFrame({"id": [1], "name": ["a"], "extra": ["x"]})
    with pytest.raises(SchemaValidationError, match="unexpected columns"):
        validate_schema(df, _COLUMNS)


def test_wrong_dtype_raises():
    df = pl.DataFrame({"id": ["not-a-long"], "name": ["a"]})
    with pytest.raises(SchemaValidationError, match="expected long"):
        validate_schema(df, _COLUMNS)


def test_null_in_non_nullable_column_raises():
    df = pl.DataFrame({"id": [1, None], "name": ["a", "b"]})
    with pytest.raises(SchemaValidationError, match="non-nullable"):
        validate_schema(df, _COLUMNS)


def test_null_in_nullable_column_is_fine():
    df = pl.DataFrame({"id": [1, 2], "name": ["a", None]})
    validate_schema(df, _COLUMNS)
