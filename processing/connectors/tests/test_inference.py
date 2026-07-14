import polars as pl

from connectors import infer_column_definitions


def test_infers_basic_types_and_ordinals():
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "active": [True, False]})
    result = infer_column_definitions(df)
    assert result == [
        {"name": "id", "data_type": "long", "nullable": True, "ordinal": 0, "description": None},
        {"name": "name", "data_type": "string", "nullable": True, "ordinal": 1, "description": None},
        {"name": "active", "data_type": "boolean", "nullable": True, "ordinal": 2, "description": None},
    ]


def test_all_null_column_defaults_to_string():
    df = pl.DataFrame({"id": [1], "mystery": [None]})
    result = infer_column_definitions(df)
    mystery = next(c for c in result if c["name"] == "mystery")
    assert mystery["data_type"] == "string"
