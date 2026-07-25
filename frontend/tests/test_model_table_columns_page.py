"""Exercises frontend/pages/6_Model_Table_Columns.py through Streamlit's own
headless script-running harness (AppTest) -- same philosophy as
test_trigger_pipeline_page.py: not a hand-copied snippet of its backend
logic, and not just "no exception raised."

Needs the live cluster reachable (same as test_metadata_db.py).

Coverage limitation, stated plainly rather than faked: this Streamlit
version's `AppTest` (streamlit==1.59.0) has no typed accessor for
`st.data_editor` at all -- only `at.dataframe` exists (confirmed:
`[m for m in dir(AppTest) if 'edit' in m.lower()]` returns nothing). Its
internal widget state is an edit-diff structure keyed against the original
value, not a plain settable dataframe, so the *save* path (editing the grid,
clicking "Save column definitions") cannot be driven through AppTest the
way test_trigger_pipeline_page.py drives a button click. What's tested here
instead: the page loads without exception against real lakehouse_models
rows, correctly resolves a model's `depends_on_feeds`, and correctly
renders whatever `lakehouse_model_columns` rows already exist for it --
proving the fetch/render integration against live data. The write path
(`replace_lakehouse_model_columns`) has its own direct round-trip test in
test_metadata_db.py; there is currently no way to test it end-to-end
through this specific page's UI.
"""

from pathlib import Path

from streamlit.testing.v1 import AppTest

_PAGE_PATH = Path(__file__).resolve().parents[1] / "pages" / "6_Model_Table_Columns.py"


def _run():
    at = AppTest.from_file(str(_PAGE_PATH))
    at.run()
    return at


def test_page_loads_and_lists_active_models():
    at = _run()
    assert not at.exception, f"Page load raised: {[str(e.value) for e in at.exception]}"
    assert at.selectbox, "Expected a model picker selectbox"
    assert "dim_iot_device" in at.selectbox[0].options


def test_selecting_a_model_shows_its_depends_on_feeds_and_existing_columns():
    """dim_iot_device is a real, live-seeded model (Backlog.md's "Frontend
    page for defining model tables") with real lakehouse_model_columns rows
    -- this checks the page actually reads and renders them correctly, not
    just that selecting a model doesn't crash."""
    at = _run()
    at.selectbox[0].select("dim_iot_device").run()

    assert not at.exception, f"Selecting a model raised: {[str(e.value) for e in at.exception]}"
    assert any("device_heartbeats" in c.value for c in at.caption), (
        "Expected the depends-on-feeds caption to mention device_heartbeats"
    )

    assert at.dataframe, "Expected the column editor grid to render"
    rows = {r["column_name"]: r for r in at.dataframe[0].value.to_dict("records")}
    assert set(rows) == {"device_id", "battery_level", "signal_strength", "firmware_version", "ts"}
    assert rows["device_id"]["is_business_key"] is True
    assert rows["device_id"]["is_tracked"] is False
    assert rows["battery_level"]["is_tracked"] is True
    # ts is neither business key nor tracked -- the third state this
    # feature explicitly supports (a passthrough column, staging-only).
    assert rows["ts"]["is_business_key"] is False
    assert rows["ts"]["is_tracked"] is False
