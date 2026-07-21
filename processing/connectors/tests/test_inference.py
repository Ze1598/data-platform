import polars as pl

from connectors import infer_column_definitions


def test_infers_basic_types_and_ordinals():
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "active": [True, False]})
    result = infer_column_definitions(df)
    assert result == [
        {"name": "id", "data_type": "long", "nullable": False, "ordinal": 0},
        {"name": "name", "data_type": "string", "nullable": False, "ordinal": 1},
        {"name": "active", "data_type": "boolean", "nullable": False, "ordinal": 2},
    ]


def test_all_null_column_gets_data_type_none_sentinel():
    df = pl.DataFrame({"id": [1], "mystery": [None]})
    result = infer_column_definitions(df)
    mystery = next(c for c in result if c["name"] == "mystery")
    assert mystery["data_type"] is None
    assert mystery["nullable"] is True


def test_nullable_true_when_any_row_has_a_null():
    df = pl.DataFrame({"id": [1, 2], "maybe": [1, None]})
    result = infer_column_definitions(df)
    maybe = next(c for c in result if c["name"] == "maybe")
    assert maybe["nullable"] is True


def test_no_description_field():
    df = pl.DataFrame({"id": [1]})
    result = infer_column_definitions(df)
    assert "description" not in result[0]
