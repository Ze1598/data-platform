import json

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

st.set_page_config(page_title="Model Feeds", page_icon="🏗️", layout="wide")
st.title("Model Feeds")

MODEL_TYPES = ["fact", "dimension"]
SCD_TYPES = [1, 2]

engine = get_engine()
df = fetch_table(engine, "model_feed", order_by="code")
data_feeds_df = fetch_table(engine, "data_feed", order_by="code")
data_feed_lookup = fetch_lookup(engine, "data_feed")
data_feed_extraction_type = dict(zip(data_feeds_df["code"], data_feeds_df["extraction_type"]))

st.dataframe(df, use_container_width=True, hide_index=True)
st.divider()

if not data_feed_lookup:
    st.info("Create a data feed first on the **Data Feeds** page.")
    st.stop()

mode = st.radio("Action", ["Add new", "Edit existing", "Delete existing"], horizontal=True)


def render_form(defaults: dict, submit_label: str):
    feed_codes = list(data_feed_lookup.keys())
    code = st.text_input("Code", value=defaults["code"], disabled=defaults["code_locked"])
    model_type = st.selectbox("Model type", MODEL_TYPES, index=MODEL_TYPES.index(defaults["model_type"]))
    staging_source_code = st.selectbox(
        "Staging source data feed", feed_codes, index=feed_codes.index(defaults["staging_source_code"])
    )
    business_key_columns = st.text_area(
        "Business key columns (JSON array)", value=defaults["business_key_columns"]
    )
    tracked_columns = st.text_area("Tracked columns (JSON array)", value=defaults["tracked_columns"])
    surrogate_key_column = st.text_input("Surrogate key column", value=defaults["surrogate_key_column"])
    scd_type = st.selectbox(
        "SCD type", SCD_TYPES, index=SCD_TYPES.index(defaults["scd_type"]),
        help="1 = overwrite in place, 2 = new version per change",
    )
    updates_enabled = st.checkbox("Updates enabled", value=defaults["updates_enabled"])
    deletions_enabled = st.checkbox(
        "Deletions enabled", value=defaults["deletions_enabled"],
        help="Only valid when the source data feed uses a full extraction — deletion is detected "
        "as a business key missing from the current full load.",
    )
    watermark_column = st.text_input("Watermark column", value=defaults["watermark_column"])
    is_active = st.checkbox("Active", value=defaults["is_active"])
    submitted = st.form_submit_button(submit_label)
    return submitted, {
        "code": code,
        "model_type": model_type,
        "staging_source_code": staging_source_code,
        "business_key_columns": business_key_columns,
        "tracked_columns": tracked_columns,
        "surrogate_key_column": surrogate_key_column,
        "scd_type": scd_type,
        "updates_enabled": updates_enabled,
        "deletions_enabled": deletions_enabled,
        "watermark_column": watermark_column,
        "is_active": is_active,
    }


def build_values(form_values: dict) -> dict | None:
    try:
        business_key_columns = json.loads(form_values["business_key_columns"] or "[]")
        tracked_columns = json.loads(form_values["tracked_columns"] or "[]")
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON: {e}")
        return None

    if not form_values["code"] or not form_values["surrogate_key_column"]:
        st.error("Code and surrogate key column are required.")
        return None

    if form_values["deletions_enabled"]:
        source_extraction_type = data_feed_extraction_type.get(form_values["staging_source_code"])
        if source_extraction_type != "full":
            st.error(
                "Deletions enabled requires the source data feed's extraction type to be 'full' "
                f"(it's currently '{source_extraction_type}')."
            )
            return None

    return {
        "code": form_values["code"],
        "model_type": form_values["model_type"],
        "staging_source_data_feed_id": data_feed_lookup[form_values["staging_source_code"]],
        "business_key_columns": json.dumps(business_key_columns),
        "tracked_columns": json.dumps(tracked_columns),
        "surrogate_key_column": form_values["surrogate_key_column"],
        "scd_type": form_values["scd_type"],
        "updates_enabled": form_values["updates_enabled"],
        "deletions_enabled": form_values["deletions_enabled"],
        "watermark_column": form_values["watermark_column"] or None,
        "is_active": form_values["is_active"],
    }


JSON_COLUMNS = {"business_key_columns", "tracked_columns"}

if mode == "Add new":
    with st.form("add_model_feed", clear_on_submit=True):
        submitted, form_values = render_form(
            {
                "code": "",
                "code_locked": False,
                "model_type": "dimension",
                "staging_source_code": list(data_feed_lookup.keys())[0],
                "business_key_columns": "[]",
                "tracked_columns": "[]",
                "surrogate_key_column": "_scd_id",
                "scd_type": 2,
                "updates_enabled": True,
                "deletions_enabled": False,
                "watermark_column": "",
                "is_active": True,
            },
            "Create",
        )

    if submitted:
        values = build_values(form_values)
        if values is not None:
            try:
                insert_row(engine, "model_feed", values, json_columns=JSON_COLUMNS)
                st.success(f"Created model feed '{values['code']}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to create: {e}")

elif mode == "Edit existing":
    if df.empty:
        st.info("No model feeds yet.")
    else:
        selected_code = st.selectbox("Select model feed", df["code"])
        row = df[df["code"] == selected_code].iloc[0]
        feed_code_lookup = {v: k for k, v in data_feed_lookup.items()}
        with st.form("edit_model_feed"):
            submitted, form_values = render_form(
                {
                    "code": selected_code,
                    "code_locked": True,
                    "model_type": row["model_type"],
                    "staging_source_code": feed_code_lookup.get(
                        row["staging_source_data_feed_id"], list(data_feed_lookup.keys())[0]
                    ),
                    "business_key_columns": to_json_text(row["business_key_columns"], default="[]"),
                    "tracked_columns": to_json_text(row["tracked_columns"], default="[]"),
                    "surrogate_key_column": safe_str(row["surrogate_key_column"]),
                    "scd_type": int(row["scd_type"]),
                    "updates_enabled": bool(row["updates_enabled"]),
                    "deletions_enabled": bool(row["deletions_enabled"]),
                    "watermark_column": safe_str(row["watermark_column"]),
                    "is_active": bool(row["is_active"]),
                },
                "Save changes",
            )

        if submitted:
            values = build_values(form_values)
            if values is not None:
                values.pop("code")  # code is immutable once created
                try:
                    update_row(engine, "model_feed", "id", row["id"], values, json_columns=JSON_COLUMNS)
                    st.success(f"Updated '{selected_code}'")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update: {e}")

else:  # Delete existing
    if df.empty:
        st.info("No model feeds yet.")
    else:
        selected_code = st.selectbox("Select model feed to delete", df["code"])
        row = df[df["code"] == selected_code].iloc[0]
        st.warning(f"This will permanently delete '{selected_code}'.")
        if st.button("Delete", type="primary"):
            try:
                delete_row(engine, "model_feed", "id", row["id"])
                st.success(f"Deleted '{selected_code}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete: {e}")
