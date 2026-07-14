import pytest

from dagster_data_platform.pipeline_steps import parse_selected_steps


def test_parses_all_four():
    assert parse_selected_steps("0,1,2,3") == {"extraction", "validation", "transformation", "serving"}


def test_parses_a_subset():
    assert parse_selected_steps("0,1") == {"extraction", "validation"}


def test_parses_a_single_value():
    assert parse_selected_steps("2") == {"transformation"}


def test_unknown_id_raises():
    with pytest.raises(KeyError):
        parse_selected_steps("9")
