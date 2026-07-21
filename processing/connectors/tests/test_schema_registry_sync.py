from connectors import compute_schema_sync

_DISCOVERED = [
    {"name": "id", "data_type": "long", "nullable": True, "ordinal": 0},
    {"name": "name", "data_type": "string", "nullable": True, "ordinal": 1},
]


def test_bootstrap_when_no_current_registry():
    result = compute_schema_sync(_DISCOVERED, None, ["id"], None)
    assert result.changed is True
    assert result.column_definitions == _DISCOVERED
    assert result.primary_key_columns == ["id"]


def test_no_op_when_discovery_matches_current():
    result = compute_schema_sync(_DISCOVERED, _DISCOVERED, ["id"], ["id"])
    assert result.changed is False
    assert result.column_definitions == _DISCOVERED


def test_new_column_appended_and_marked_changed():
    current = [_DISCOVERED[0]]
    discovered = _DISCOVERED
    result = compute_schema_sync(discovered, current, [], [])
    assert result.changed is True
    names = [c["name"] for c in result.column_definitions]
    assert names == ["id", "name"]
    assert result.column_definitions[1]["ordinal"] == 1


def test_type_change_updates_in_place_and_marks_changed():
    current = _DISCOVERED
    discovered = [
        {"name": "id", "data_type": "string", "nullable": True, "ordinal": 0},
        _DISCOVERED[1],
    ]
    result = compute_schema_sync(discovered, current, [], [])
    assert result.changed is True
    id_col = next(c for c in result.column_definitions if c["name"] == "id")
    assert id_col["data_type"] == "string"


def test_column_missing_from_discovery_is_left_untouched():
    # A column vanishing from discovery isn't this function's concern --
    # that's raw_to_clean.MissingColumnsError's job, at validation time.
    current = _DISCOVERED
    discovered = [_DISCOVERED[0]]
    result = compute_schema_sync(discovered, current, [], [])
    assert result.changed is False
    names = [c["name"] for c in result.column_definitions]
    assert names == ["id", "name"]


def test_primary_key_change_alone_marks_changed():
    # No column changes at all -- only the resolved PK differs from
    # what's currently persisted. Still needs a new version, since
    # ODS reads primary_key_columns to decide upsert-by-key vs.
    # insert-only.
    result = compute_schema_sync(_DISCOVERED, _DISCOVERED, ["id"], [])
    assert result.changed is True
    assert result.primary_key_columns == ["id"]


def test_primary_key_unchanged_and_columns_unchanged_is_no_op():
    result = compute_schema_sync(_DISCOVERED, _DISCOVERED, ["id"], ["id"])
    assert result.changed is False
    assert result.primary_key_columns == ["id"]


def test_current_primary_key_none_treated_as_empty():
    # First time a PK is ever resolved for a feed whose existing registry
    # row predates primary_key_columns entirely (current_primary_key_columns
    # is None, not []) -- a real, non-empty resolved PK should still count
    # as a change.
    result = compute_schema_sync(_DISCOVERED, _DISCOVERED, ["id"], None)
    assert result.changed is True


def test_new_column_discovered_null_defaults_to_string():
    # A brand-new column whose data is entirely null this run
    # (data_type=None sentinel from infer_column_definitions) has no prior
    # recorded type to fall back on -- defaults to "string".
    current = [_DISCOVERED[0]]
    discovered = [_DISCOVERED[0], {"name": "name", "data_type": None, "nullable": True, "ordinal": 1}]
    result = compute_schema_sync(discovered, current, [], [])
    assert result.changed is True
    name_col = next(c for c in result.column_definitions if c["name"] == "name")
    assert name_col["data_type"] == "string"


def test_existing_column_discovered_null_keeps_recorded_type():
    # This run's data doesn't answer the question for an existing column
    # (all null) -- the last real recorded type wins, not overwritten with
    # a default, and this alone doesn't mark the sync as changed.
    current = _DISCOVERED
    discovered = [_DISCOVERED[0], {"name": "name", "data_type": None, "nullable": False, "ordinal": 1}]
    result = compute_schema_sync(discovered, current, [], [])
    name_col = next(c for c in result.column_definitions if c["name"] == "name")
    assert name_col["data_type"] == "string"
    assert result.changed is False


def test_nullable_flips_false_to_true_and_marks_changed():
    current = [{"name": "id", "data_type": "long", "nullable": False, "ordinal": 0}]
    discovered = [{"name": "id", "data_type": "long", "nullable": True, "ordinal": 0}]
    result = compute_schema_sync(discovered, current, [], [])
    assert result.changed is True
    assert result.column_definitions[0]["nullable"] is True


def test_nullable_does_not_flip_true_to_false():
    current = [{"name": "id", "data_type": "long", "nullable": True, "ordinal": 0}]
    discovered = [{"name": "id", "data_type": "long", "nullable": False, "ordinal": 0}]
    result = compute_schema_sync(discovered, current, [], [])
    assert result.changed is False
    assert result.column_definitions[0]["nullable"] is True
