"""Metadata-driven, codegen-to-dbt UX alternative to hand-writing a model's
staging/model SQL column list by hand -- define a lakehouse model's columns
here (name, data type, nullability, business-key/tracked status) instead.
Optional: a model with no rows here keeps the original, fully hand-written
business_key_columns/tracked_columns-scaffolded flow (3_Lakehouse_Models.py's
existing text fields, scripts/generate_model_scaffolds.py's pre-existing
behavior) -- unaffected either way. See Backlog.md's "Frontend page for
defining model tables" and metadata/DataModel.md's `lakehouse_model_columns`
section for the full design.

Saving here only writes lakehouse_model_columns rows -- it does not
generate any file or touch dbt/Trino. The dedicated per-model staging file
(stg_<table_name>.sql) and the model's own base CTE get generated the next
time scripts/generate_model_scaffolds.py runs (`just orchestration
generate-model-scaffolds`, or as part of `just orchestration start`), same
as every other codegen script in this platform -- never live, at Python
import time or from a button click here.
"""

import pandas as pd
import streamlit as st
from metadata_db import (
    fetch_lakehouse_model_columns,
    fetch_lookup,
    fetch_table,
    get_engine,
    replace_lakehouse_model_columns,
)

st.set_page_config(page_title="Model Table Columns", page_icon="📐", layout="wide")
st.title("Model Table Columns")

DATA_TYPES = ["string", "long", "double", "boolean", "timestamp"]
_EDITOR_COLUMNS = ["column_name", "source_feed", "data_type", "is_nullable", "is_business_key", "is_tracked"]

engine = get_engine()
models_df = fetch_table(engine, "lakehouse_models", order_by="friendly_name")
if models_df.empty:
    st.info("Create a lakehouse model first on the **Lakehouse Models** page.")
    st.stop()

data_feed_lookup = fetch_lookup(engine, "data_feed", code_col="friendly_name")  # name -> id
data_feed_name_by_id = {str(v): k for k, v in data_feed_lookup.items()}

model_friendly_name = st.selectbox("Model", models_df["friendly_name"].tolist())
model_row = models_df[models_df["friendly_name"] == model_friendly_name].iloc[0]
model_id = str(model_row["id"])
depends_on_feed_ids = [fid for fid in str(model_row["depends_on_feeds"] or "").split(",") if fid]
depends_on_feed_names = [data_feed_name_by_id[fid] for fid in depends_on_feed_ids if fid in data_feed_name_by_id]

if not depends_on_feed_names:
    st.warning("This model has no **Depends on feeds** set -- add at least one on the Lakehouse Models page first.")
    st.stop()

st.caption(f"Depends on feeds: {', '.join(depends_on_feed_names)}")
if len(depends_on_feed_names) > 1:
    st.caption(
        "This model depends on more than one feed -- the generated staging model will get one CTE "
        "per feed, but combining them (join/union) is left as a hand-written TODO. There's no "
        "metadata describing how two feeds' rows relate."
    )

existing_df = fetch_lakehouse_model_columns(engine, model_id)
if not existing_df.empty:
    existing_df = existing_df.copy()
    existing_df["source_feed"] = existing_df["source_feed_id"].map(lambda fid: data_feed_name_by_id.get(str(fid), ""))
    editor_df = existing_df[_EDITOR_COLUMNS]
else:
    editor_df = pd.DataFrame(columns=_EDITOR_COLUMNS)

edited_df = st.data_editor(
    editor_df,
    num_rows="dynamic",
    use_container_width=True,
    column_config={
        "column_name": st.column_config.TextColumn("Column name", required=True),
        "source_feed": st.column_config.SelectboxColumn("Source feed", options=depends_on_feed_names, required=True),
        "data_type": st.column_config.SelectboxColumn("Data type", options=DATA_TYPES, required=True),
        "is_nullable": st.column_config.CheckboxColumn("Nullable", default=True),
        "is_business_key": st.column_config.CheckboxColumn("Business key", default=False),
        "is_tracked": st.column_config.CheckboxColumn("Tracked (row hash)", default=True),
    },
    key=f"columns_editor_{model_id}",
)
st.caption(
    "A business key column is always excluded from row-hash tracking, regardless of the Tracked "
    "checkbox -- a column is either the identity (business key) or a change-tracked attribute, never both."
)

if st.button("Save column definitions", key=f"save_{model_id}"):
    errors = []
    seen_names = set()
    clean_rows = []
    for i, row in enumerate(edited_df.to_dict("records"), start=1):
        name = (row.get("column_name") or "").strip()
        source_feed = row.get("source_feed")
        data_type = row.get("data_type")
        if not name or not source_feed or not data_type:
            errors.append(f"Row {i}: column name, source feed, and data type are all required.")
            continue
        if name in seen_names:
            errors.append(f"Row {i}: duplicate column name {name!r}.")
            continue
        seen_names.add(name)
        is_business_key = bool(row.get("is_business_key"))
        clean_rows.append(
            {
                "column_name": name,
                "source_feed_id": data_feed_lookup[source_feed],
                "data_type": data_type,
                "is_nullable": bool(row.get("is_nullable", True)),
                "is_business_key": is_business_key,
                "is_tracked": bool(row.get("is_tracked", True)) and not is_business_key,
                "ordinal_position": len(clean_rows),
            }
        )

    if not clean_rows:
        errors.append("At least one column is required.")
    elif not any(c["is_business_key"] for c in clean_rows):
        errors.append("At least one column must be marked as the business key.")

    if errors:
        for e in errors:
            st.error(e)
    else:
        replace_lakehouse_model_columns(engine, model_id, clean_rows)
        st.success(f"Saved {len(clean_rows)} column definition(s) for {model_friendly_name}.")
        st.rerun()
