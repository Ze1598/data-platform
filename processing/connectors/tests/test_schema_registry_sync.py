from connectors import compute_schema_sync

_DISCOVERED = [
    {"name": "id", "data_type": "long", "nullable": True, "ordinal": 0, "description": None},
    {"name": "name", "data_type": "string", "nullable": True, "ordinal": 1, "description": None},
]


def test_bootstrap_when_no_current_registry():
    result = compute_schema_sync(_DISCOVERED, None)
    assert result.changed is True
    assert result.column_definitions == _DISCOVERED


def test_no_op_when_discovery_matches_current():
    result = compute_schema_sync(_DISCOVERED, _DISCOVERED)
    assert result.changed is False
    assert result.column_definitions == _DISCOVERED


def test_new_column_appended_and_marked_changed():
    current = [_DISCOVERED[0]]
    discovered = _DISCOVERED
    result = compute_schema_sync(discovered, current)
    assert result.changed is True
    names = [c["name"] for c in result.column_definitions]
    assert names == ["id", "name"]
    assert result.column_definitions[1]["ordinal"] == 1


def test_type_change_updates_in_place_and_marks_changed():
    current = _DISCOVERED
    discovered = [
        {"name": "id", "data_type": "string", "nullable": True, "ordinal": 0, "description": None},
        _DISCOVERED[1],
    ]
    result = compute_schema_sync(discovered, current)
    assert result.changed is True
    id_col = next(c for c in result.column_definitions if c["name"] == "id")
    assert id_col["data_type"] == "string"


def test_column_missing_from_discovery_is_left_untouched():
    # A column vanishing from discovery isn't this function's concern --
    # that's raw_to_clean.MissingColumnsError's job, at validation time.
    current = _DISCOVERED
    discovered = [_DISCOVERED[0]]
    result = compute_schema_sync(discovered, current)
    assert result.changed is False
    names = [c["name"] for c in result.column_definitions]
    assert names == ["id", "name"]
