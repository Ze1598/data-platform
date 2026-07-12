import json

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

st.set_page_config(page_title="Lakehouse Models", page_icon="🏗️", layout="wide")
st.title("Lakehouse Models")

TABLE_TYPES = ["fact", "dimension"]
SCD_TYPES = [1, 2]

engine = get_engine()
df = fetch_table(engine, "lakehouse_models", order_by="friendly_name")
data_feeds_df = fetch_table(engine, "data_feed", order_by="friendly_name")
data_feed_lookup = fetch_lookup(engine, "data_feed", code_col="friendly_name")
data_feed_name_by_id = {str(v): k for k, v in data_feed_lookup.items()}
data_feed_extraction_type = dict(zip(data_feeds_df["friendly_name"], data_feeds_df["extraction_type"]))
load_types_df = pd.read_sql(text("select id, label from load_type order by id"), engine)
load_type_label_by_id = dict(zip(load_types_df["id"], load_types_df["label"]))
load_type_id_by_label = {v: k for k, v in load_type_label_by_id.items()}

st.dataframe(df, use_container_width=True, hide_index=True)
st.divider()

if not data_feed_lookup:
    st.info("Create a data feed first on the **Data Feeds** page.")
    st.stop()


def _depends_on_ids_to_names(depends_on_feeds: str | None) -> list[str]:
    if not depends_on_feeds or pd.isna(depends_on_feeds):
        return []
    return [data_feed_name_by_id[fid] for fid in str(depends_on_feeds).split(",") if fid in data_feed_name_by_id]


mode = st.radio("Action", ["Add new", "Edit existing", "Delete existing"], horizontal=True)


def render_form(defaults: dict, submit_label: str):
    feed_names = list(data_feed_lookup.keys())
    friendly_name = st.text_input(
        "Friendly name", value=defaults["friendly_name"], disabled=defaults["friendly_name_locked"]
    )
    model_schema = st.text_input(
        "Model schema", value=defaults["model_schema"], help="Which Trino/Iceberg schema this table lands in"
    )
    batch_hierarchy = st.number_input("Batch hierarchy", value=defaults["batch_hierarchy"], min_value=0, step=1)
    table_type = st.selectbox("Table type", TABLE_TYPES, index=TABLE_TYPES.index(defaults["table_type"]))
    depends_on_feed_names = st.multiselect(
        "Depends on feeds", feed_names, default=defaults["depends_on_feed_names"],
        help="Every data feed this model must have successfully ingested before it builds",
    )
    business_key_columns = st.text_area(
        "Business key columns (JSON array)", value=defaults["business_key_columns"]
    )
    tracked_columns = st.text_area("Tracked columns (JSON array)", value=defaults["tracked_columns"])
    scd_type = st.selectbox(
        "SCD type", SCD_TYPES, index=SCD_TYPES.index(defaults["scd_type"]),
        help="1 = overwrite in place, 2 = new version per change",
    )
    updates_enabled = st.checkbox(
        "Updates enabled", value=defaults["updates_enabled"],
        help="Also drives whether this model's upstream staging source(s) merge on attribute change "
        "-- see metadata/DataModel.md, 'Staging update-tracking rule'.",
    )
    deletes_enabled = st.checkbox(
        "Deletes enabled", value=defaults["deletes_enabled"],
        help="Only valid when every dependent data feed uses a full extraction -- deletion is "
        "detected as a business key missing from the current full load.",
    )
    watermark_column = st.text_input("Watermark column", value=defaults["watermark_column"])
    load_type_label = st.selectbox(
        "Load type", list(load_type_id_by_label.keys()),
        index=list(load_type_id_by_label.keys()).index(defaults["load_type_label"]),
    )
    is_active = st.checkbox("Active", value=defaults["is_active"])
    submitted = st.form_submit_button(submit_label)
    return submitted, {
        "friendly_name": friendly_name,
        "model_schema": model_schema,
        "batch_hierarchy": int(batch_hierarchy),
        "table_type": table_type,
        "depends_on_feed_names": depends_on_feed_names,
        "business_key_columns": business_key_columns,
        "tracked_columns": tracked_columns,
        "scd_type": scd_type,
        "updates_enabled": updates_enabled,
        "deletes_enabled": deletes_enabled,
        "watermark_column": watermark_column,
        "load_type_label": load_type_label,
        "is_active": is_active,
    }


def build_values(form_values: dict) -> dict | None:
    try:
        business_key_columns = json.loads(form_values["business_key_columns"] or "[]")
        tracked_columns = json.loads(form_values["tracked_columns"] or "[]")
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON: {e}")
        return None

    if not form_values["friendly_name"] or not form_values["model_schema"]:
        st.error("Friendly name and model schema are required.")
        return None

    if not form_values["depends_on_feed_names"]:
        st.error("At least one dependent feed is required.")
        return None

    if form_values["deletes_enabled"]:
        non_full = [
            name for name in form_values["depends_on_feed_names"]
            if data_feed_extraction_type.get(name) != "full"
        ]
        if non_full:
            st.error(
                "Deletes enabled requires every dependent feed's extraction type to be 'full' "
                f"(not true for: {', '.join(non_full)})."
            )
            return None

    depends_on_feeds = ",".join(str(data_feed_lookup[name]) for name in form_values["depends_on_feed_names"])

    return {
        "friendly_name": form_values["friendly_name"],
        "model_schema": form_values["model_schema"],
        "batch_hierarchy": form_values["batch_hierarchy"],
        "table_type": form_values["table_type"],
        "depends_on_feeds": depends_on_feeds,
        "business_key_columns": json.dumps(business_key_columns),
        "tracked_columns": json.dumps(tracked_columns),
        "scd_type": form_values["scd_type"],
        "updates_enabled": form_values["updates_enabled"],
        "deletes_enabled": form_values["deletes_enabled"],
        "watermark_column": form_values["watermark_column"] or None,
        "load_type": load_type_id_by_label[form_values["load_type_label"]],
        "is_active": form_values["is_active"],
    }


JSON_COLUMNS = {"business_key_columns", "tracked_columns"}

if mode == "Add new":
    with st.form("add_lakehouse_model", clear_on_submit=True):
        submitted, form_values = render_form(
            {
                "friendly_name": "",
                "friendly_name_locked": False,
                "model_schema": "model",
                "batch_hierarchy": 0,
                "table_type": "dimension",
                "depends_on_feed_names": [],
                "business_key_columns": "[]",
                "tracked_columns": "[]",
                "scd_type": 2,
                "updates_enabled": True,
                "deletes_enabled": False,
                "watermark_column": "",
                "load_type_label": "full",
                "is_active": True,
            },
            "Create",
        )

    if submitted:
        values = build_values(form_values)
        if values is not None:
            try:
                insert_row(engine, "lakehouse_models", values, json_columns=JSON_COLUMNS)
                st.success(f"Created lakehouse model '{values['friendly_name']}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to create: {e}")

elif mode == "Edit existing":
    if df.empty:
        st.info("No lakehouse models yet.")
    else:
        selected_name = st.selectbox("Select lakehouse model", df["friendly_name"])
        row = df[df["friendly_name"] == selected_name].iloc[0]
        with st.form("edit_lakehouse_model"):
            submitted, form_values = render_form(
                {
                    "friendly_name": selected_name,
                    "friendly_name_locked": True,
                    "model_schema": safe_str(row["model_schema"]),
                    "batch_hierarchy": int(row["batch_hierarchy"]),
                    "table_type": row["table_type"],
                    "depends_on_feed_names": _depends_on_ids_to_names(row["depends_on_feeds"]),
                    "business_key_columns": to_json_text(row["business_key_columns"], default="[]"),
                    "tracked_columns": to_json_text(row["tracked_columns"], default="[]"),
                    "scd_type": int(row["scd_type"]),
                    "updates_enabled": bool(row["updates_enabled"]),
                    "deletes_enabled": bool(row["deletes_enabled"]),
                    "watermark_column": safe_str(row["watermark_column"]),
                    "load_type_label": load_type_label_by_id[int(row["load_type"])],
                    "is_active": bool(row["is_active"]),
                },
                "Save changes",
            )

        if submitted:
            values = build_values(form_values)
            if values is not None:
                values.pop("friendly_name")  # friendly_name is immutable once created
                try:
                    update_row(engine, "lakehouse_models", "id", row["id"], values, json_columns=JSON_COLUMNS)
                    st.success(f"Updated '{selected_name}'")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update: {e}")

else:  # Delete existing
    if df.empty:
        st.info("No lakehouse models yet.")
    else:
        selected_name = st.selectbox("Select lakehouse model to delete", df["friendly_name"])
        row = df[df["friendly_name"] == selected_name].iloc[0]
        st.warning(f"This will permanently delete '{selected_name}'.")
        if st.button("Delete", type="primary"):
            try:
                delete_row(engine, "lakehouse_models", "id", row["id"])
                st.success(f"Deleted '{selected_name}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete: {e}")
