import json
import uuid

import pandas as pd
import streamlit as st
from metadata_db import (
    delete_row,
    fetch_lookup,
    fetch_table,
    get_engine,
    insert_row,
    safe_str,
    to_json_text,
    update_row,
)
from sqlalchemy import text

st.set_page_config(page_title="Data Feeds", page_icon="🛰️", layout="wide")
st.title("Data Feeds")

EXTRACTION_TYPES = ["full", "incremental"]
PROCESSING_ENGINES = ["polars", "spark"]
NEW_BATCH_OPTION = "<New batch>"

engine = get_engine()
df = fetch_table(engine, "data_feed", order_by="friendly_name")
source_systems = fetch_lookup(engine, "source_system")

st.dataframe(df, use_container_width=True, hide_index=True)
st.divider()

if not source_systems:
    st.info("Create a source system first on the **Source Systems** page.")
    st.stop()

# Every feed must belong to a batch (data_feed.batch_group/
# batch_group_friendly_name are not null -- see metadata/DataModel.md, "the
# platform tracks and schedules runs by batch or model schema, never by a
# bare individual feed"). Existing batches are offered as a pick list;
# picking one joins that batch (same batch_group id, so runs group
# together), or a new singleton batch can be created inline.
existing_batches = pd.read_sql(
    text("select distinct batch_group, batch_group_friendly_name from data_feed order by batch_group_friendly_name"),
    engine,
)
batch_group_by_name = dict(zip(existing_batches["batch_group_friendly_name"], existing_batches["batch_group"]))

mode = st.radio("Action", ["Add new", "Edit existing", "Delete existing"], horizontal=True)


def render_batch_picker(default_batch_name: str | None):
    options = [NEW_BATCH_OPTION] + list(batch_group_by_name.keys())
    default_index = options.index(default_batch_name) if default_batch_name in batch_group_by_name else 0
    choice = st.selectbox("Batch group", options, index=default_index)
    if choice == NEW_BATCH_OPTION:
        new_name = st.text_input("New batch friendly name", value=default_batch_name or "")
        return new_name, None  # batch_group id generated at submit time
    return choice, batch_group_by_name[choice]


def render_form(defaults: dict, submit_label: str):
    source_codes = list(source_systems.keys())
    source_code = st.selectbox(
        "Source system", source_codes, index=source_codes.index(defaults["source_code"])
    )
    friendly_name = st.text_input(
        "Friendly name", value=defaults["friendly_name"], disabled=defaults["friendly_name_locked"]
    )
    source_object_name = st.text_input(
        "Source object name (source table/endpoint)", value=defaults["source_object_name"]
    )
    batch_group_friendly_name, batch_group = render_batch_picker(defaults["batch_group_friendly_name"])
    batch_feed_hierarchy = st.number_input(
        "Batch feed hierarchy", value=defaults["batch_feed_hierarchy"], min_value=0, step=1,
        help="Feeds sharing the same tier can extract in parallel; lower tiers complete before higher tiers within the same batch",
    )
    extraction_type = st.selectbox(
        "Extraction type", EXTRACTION_TYPES, index=EXTRACTION_TYPES.index(defaults["extraction_type"])
    )
    watermark_column = st.text_input("Watermark column", value=defaults["watermark_column"])
    extraction_config = st.text_area("Extraction config (JSON)", value=defaults["extraction_config"])
    source_pk = st.text_area(
        "Source PK columns (JSON array)", value=defaults["source_pk"],
        help="Column names identifying a row in the source",
    )
    processing_engine = st.selectbox(
        "Processing engine", PROCESSING_ENGINES, index=PROCESSING_ENGINES.index(defaults["processing_engine"])
    )
    is_active = st.checkbox("Active", value=defaults["is_active"])
    submitted = st.form_submit_button(submit_label)
    return submitted, {
        "source_code": source_code,
        "friendly_name": friendly_name,
        "source_object_name": source_object_name,
        "batch_group_friendly_name": batch_group_friendly_name,
        "batch_group": batch_group,
        "batch_feed_hierarchy": int(batch_feed_hierarchy),
        "extraction_type": extraction_type,
        "watermark_column": watermark_column,
        "extraction_config": extraction_config,
        "source_pk": source_pk,
        "processing_engine": processing_engine,
        "is_active": is_active,
    }


def build_values(form_values: dict) -> dict | None:
    try:
        extraction_config = json.loads(form_values["extraction_config"] or "{}")
        source_pk = json.loads(form_values["source_pk"] or "[]")
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON: {e}")
        return None

    if not form_values["friendly_name"] or not form_values["source_object_name"]:
        st.error("Friendly name and source object name are required.")
        return None

    if not form_values["batch_group_friendly_name"]:
        st.error("Batch group friendly name is required.")
        return None

    if form_values["extraction_type"] == "incremental" and not form_values["watermark_column"]:
        st.error("Watermark column is required when extraction type is 'incremental'.")
        return None

    return {
        "source_system_id": source_systems[form_values["source_code"]],
        "friendly_name": form_values["friendly_name"],
        "source_object_name": form_values["source_object_name"],
        "batch_group": form_values["batch_group"] or str(uuid.uuid4()),
        "batch_group_friendly_name": form_values["batch_group_friendly_name"],
        "batch_feed_hierarchy": form_values["batch_feed_hierarchy"],
        "extraction_type": form_values["extraction_type"],
        "watermark_column": form_values["watermark_column"] or None,
        "extraction_config": json.dumps(extraction_config),
        "source_pk": json.dumps(source_pk),
        "processing_engine": form_values["processing_engine"],
        "is_active": form_values["is_active"],
    }


JSON_COLUMNS = {"extraction_config", "source_pk"}

if mode == "Add new":
    with st.form("add_data_feed", clear_on_submit=True):
        submitted, form_values = render_form(
            {
                "source_code": list(source_systems.keys())[0],
                "friendly_name": "",
                "friendly_name_locked": False,
                "source_object_name": "",
                "batch_group_friendly_name": None,
                "batch_feed_hierarchy": 0,
                "extraction_type": "full",
                "watermark_column": "",
                "extraction_config": "{}",
                "source_pk": "[]",
                "processing_engine": "polars",
                "is_active": True,
            },
            "Create",
        )

    if submitted:
        values = build_values(form_values)
        if values is not None:
            try:
                insert_row(engine, "data_feed", values, json_columns=JSON_COLUMNS)
                st.success(f"Created data feed '{values['friendly_name']}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to create: {e}")

elif mode == "Edit existing":
    if df.empty:
        st.info("No data feeds yet.")
    else:
        selected_name = st.selectbox("Select data feed", df["friendly_name"])
        row = df[df["friendly_name"] == selected_name].iloc[0]
        source_code_lookup = {v: k for k, v in source_systems.items()}
        with st.form("edit_data_feed"):
            submitted, form_values = render_form(
                {
                    "source_code": source_code_lookup.get(row["source_system_id"], list(source_systems.keys())[0]),
                    "friendly_name": selected_name,
                    "friendly_name_locked": True,
                    "source_object_name": safe_str(row["source_object_name"]),
                    "batch_group_friendly_name": safe_str(row["batch_group_friendly_name"]),
                    "batch_feed_hierarchy": int(row["batch_feed_hierarchy"]),
                    "extraction_type": row["extraction_type"],
                    "watermark_column": safe_str(row["watermark_column"]),
                    "extraction_config": to_json_text(row["extraction_config"]),
                    "source_pk": to_json_text(row["source_pk"], default="[]"),
                    "processing_engine": row["processing_engine"],
                    "is_active": bool(row["is_active"]),
                },
                "Save changes",
            )

        if submitted:
            values = build_values(form_values)
            if values is not None:
                values.pop("friendly_name")  # friendly_name is immutable once created
                try:
                    update_row(engine, "data_feed", "id", row["id"], values, json_columns=JSON_COLUMNS)
                    st.success(f"Updated '{selected_name}'")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update: {e}")

else:  # Delete existing
    if df.empty:
        st.info("No data feeds yet.")
    else:
        selected_name = st.selectbox("Select data feed to delete", df["friendly_name"])
        row = df[df["friendly_name"] == selected_name].iloc[0]
        st.warning(
            f"This will permanently delete '{selected_name}'. "
            "Lakehouse models referencing it (via depends_on_feeds) must be updated first."
        )
        if st.button("Delete", type="primary"):
            try:
                delete_row(engine, "data_feed", "id", row["id"])
                st.success(f"Deleted '{selected_name}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete: {e}")
