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

st.set_page_config(page_title="Data Feeds", page_icon="🛰️", layout="wide")
st.title("Data Feeds")

EXTRACTION_TYPES = ["full", "incremental"]

engine = get_engine()
df = fetch_table(engine, "data_feed", order_by="code")
source_systems = fetch_lookup(engine, "source_system")

st.dataframe(df, use_container_width=True, hide_index=True)
st.divider()

if not source_systems:
    st.info("Create a source system first on the **Source Systems** page.")
    st.stop()

mode = st.radio("Action", ["Add new", "Edit existing", "Delete existing"], horizontal=True)


def render_form(defaults: dict, submit_label: str):
    source_codes = list(source_systems.keys())
    source_code = st.selectbox(
        "Source system", source_codes, index=source_codes.index(defaults["source_code"])
    )
    code = st.text_input("Code", value=defaults["code"], disabled=defaults["code_locked"])
    name = st.text_input("Name", value=defaults["name"])
    object_name = st.text_input("Object name (source table/endpoint)", value=defaults["object_name"])
    extraction_type = st.selectbox(
        "Extraction type", EXTRACTION_TYPES, index=EXTRACTION_TYPES.index(defaults["extraction_type"])
    )
    incremental_column = st.text_input("Incremental column", value=defaults["incremental_column"])
    incremental_column_type = st.text_input(
        "Incremental column type", value=defaults["incremental_column_type"]
    )
    extraction_config = st.text_area("Extraction config (JSON)", value=defaults["extraction_config"])
    landing_path_template = st.text_input(
        "Landing path template", value=defaults["landing_path_template"]
    )
    raw_path_template = st.text_input("Raw path template", value=defaults["raw_path_template"])
    business_key_columns = st.text_area(
        "Business key columns (JSON array)", value=defaults["business_key_columns"]
    )
    staging_table_name = st.text_input("Staging table name", value=defaults["staging_table_name"])
    schedule_cron = st.text_input("Schedule (cron)", value=defaults["schedule_cron"])
    is_active = st.checkbox("Active", value=defaults["is_active"])
    submitted = st.form_submit_button(submit_label)
    return submitted, {
        "source_code": source_code,
        "code": code,
        "name": name,
        "object_name": object_name,
        "extraction_type": extraction_type,
        "incremental_column": incremental_column,
        "incremental_column_type": incremental_column_type,
        "extraction_config": extraction_config,
        "landing_path_template": landing_path_template,
        "raw_path_template": raw_path_template,
        "business_key_columns": business_key_columns,
        "staging_table_name": staging_table_name,
        "schedule_cron": schedule_cron,
        "is_active": is_active,
    }


def build_values(form_values: dict) -> dict | None:
    try:
        extraction_config = json.loads(form_values["extraction_config"] or "{}")
        business_key_columns = json.loads(form_values["business_key_columns"] or "[]")
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON: {e}")
        return None

    if not form_values["code"] or not form_values["name"] or not form_values["object_name"]:
        st.error("Code, name, and object name are required.")
        return None

    if form_values["extraction_type"] == "incremental" and not form_values["incremental_column"]:
        st.error("Incremental column is required when extraction type is 'incremental'.")
        return None

    return {
        "source_system_id": source_systems[form_values["source_code"]],
        "code": form_values["code"],
        "name": form_values["name"],
        "object_name": form_values["object_name"],
        "extraction_type": form_values["extraction_type"],
        "incremental_column": form_values["incremental_column"] or None,
        "incremental_column_type": form_values["incremental_column_type"] or None,
        "extraction_config": json.dumps(extraction_config),
        "landing_path_template": form_values["landing_path_template"] or None,
        "raw_path_template": form_values["raw_path_template"] or None,
        "business_key_columns": json.dumps(business_key_columns),
        "staging_table_name": form_values["staging_table_name"] or None,
        "schedule_cron": form_values["schedule_cron"] or None,
        "is_active": form_values["is_active"],
    }


JSON_COLUMNS = {"extraction_config", "business_key_columns"}

if mode == "Add new":
    with st.form("add_data_feed", clear_on_submit=True):
        submitted, form_values = render_form(
            {
                "source_code": list(source_systems.keys())[0],
                "code": "",
                "code_locked": False,
                "name": "",
                "object_name": "",
                "extraction_type": "full",
                "incremental_column": "",
                "incremental_column_type": "",
                "extraction_config": "{}",
                "landing_path_template": "",
                "raw_path_template": "",
                "business_key_columns": "[]",
                "staging_table_name": "",
                "schedule_cron": "",
                "is_active": True,
            },
            "Create",
        )

    if submitted:
        values = build_values(form_values)
        if values is not None:
            try:
                insert_row(engine, "data_feed", values, json_columns=JSON_COLUMNS)
                st.success(f"Created data feed '{values['code']}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to create: {e}")

elif mode == "Edit existing":
    if df.empty:
        st.info("No data feeds yet.")
    else:
        selected_code = st.selectbox("Select data feed", df["code"])
        row = df[df["code"] == selected_code].iloc[0]
        source_code_lookup = {v: k for k, v in source_systems.items()}
        with st.form("edit_data_feed"):
            submitted, form_values = render_form(
                {
                    "source_code": source_code_lookup.get(row["source_system_id"], list(source_systems.keys())[0]),
                    "code": selected_code,
                    "code_locked": True,
                    "name": safe_str(row["name"]),
                    "object_name": safe_str(row["object_name"]),
                    "extraction_type": row["extraction_type"],
                    "incremental_column": safe_str(row["incremental_column"]),
                    "incremental_column_type": safe_str(row["incremental_column_type"]),
                    "extraction_config": to_json_text(row["extraction_config"]),
                    "landing_path_template": safe_str(row["landing_path_template"]),
                    "raw_path_template": safe_str(row["raw_path_template"]),
                    "business_key_columns": to_json_text(row["business_key_columns"], default="[]"),
                    "staging_table_name": safe_str(row["staging_table_name"]),
                    "schedule_cron": safe_str(row["schedule_cron"]),
                    "is_active": bool(row["is_active"]),
                },
                "Save changes",
            )

        if submitted:
            values = build_values(form_values)
            if values is not None:
                values.pop("code")  # code is immutable once created
                try:
                    update_row(engine, "data_feed", "id", row["id"], values, json_columns=JSON_COLUMNS)
                    st.success(f"Updated '{selected_code}'")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update: {e}")

else:  # Delete existing
    if df.empty:
        st.info("No data feeds yet.")
    else:
        selected_code = st.selectbox("Select data feed to delete", df["code"])
        row = df[df["code"] == selected_code].iloc[0]
        st.warning(
            f"This will permanently delete '{selected_code}'. "
            "Model feeds referencing it must be deleted first."
        )
        if st.button("Delete", type="primary"):
            try:
                delete_row(engine, "data_feed", "id", row["id"])
                st.success(f"Deleted '{selected_code}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete: {e}")
