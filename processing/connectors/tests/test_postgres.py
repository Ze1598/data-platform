import pytest

from connectors.postgres import _resolve_postgres_data_type


@pytest.mark.parametrize(
    "typname,expected",
    [
        ("int4", "long"),
        ("int8", "long"),
        ("numeric", "double"),
        ("float8", "double"),
        ("bool", "boolean"),
        ("timestamp", "timestamp"),
        ("timestamptz", "timestamp"),
        ("date", "timestamp"),
        ("varchar", "string"),
        ("text", "string"),
        ("uuid", "string"),
        ("jsonb", "string"),
    ],
)
def test_resolves_known_postgres_types(typname, expected):
    assert _resolve_postgres_data_type("col", typname) == expected


def test_unmapped_postgres_type_raises():
    with pytest.raises(ValueError, match="unsupported Postgres catalog type"):
        _resolve_postgres_data_type("col", "point")
