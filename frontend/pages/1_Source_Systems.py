import json

import streamlit as st
from metadata_db import (
    delete_row,
    fetch_table,
    get_engine,
    insert_row,
    safe_str,
    to_json_text,
    update_row,
)

st.set_page_config(page_title="Source Systems", page_icon="🔌", layout="wide")
st.title("Source Systems")

SYSTEM_TYPES = ["database", "api", "file_drop", "saas"]

engine = get_engine()
df = fetch_table(engine, "source_system", order_by="code")

st.dataframe(df, use_container_width=True, hide_index=True)
st.divider()

mode = st.radio("Action", ["Add new", "Edit existing", "Delete existing"], horizontal=True)

if mode == "Add new":
    with st.form("add_source_system", clear_on_submit=True):
        code = st.text_input("Code")
        name = st.text_input("Name")
        description = st.text_area("Description")
        system_type = st.selectbox("System type", SYSTEM_TYPES)
        connection_config = st.text_area("Connection config (JSON)", value="{}")
        is_active = st.checkbox("Active", value=True)
        submitted = st.form_submit_button("Create")

    if submitted:
        try:
            cfg = json.loads(connection_config or "{}")
        except json.JSONDecodeError as e:
            st.error(f"Connection config is not valid JSON: {e}")
        else:
            if not code or not name:
                st.error("Code and name are required.")
            else:
                try:
                    insert_row(
                        engine,
                        "source_system",
                        {
                            "code": code,
                            "name": name,
                            "description": description or None,
                            "system_type": system_type,
                            "connection_config": json.dumps(cfg),
                            "is_active": is_active,
                        },
                        json_columns={"connection_config"},
                    )
                    st.success(f"Created source system '{code}'")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to create: {e}")

elif mode == "Edit existing":
    if df.empty:
        st.info("No source systems yet.")
    else:
        selected_code = st.selectbox("Select source system", df["code"])
        row = df[df["code"] == selected_code].iloc[0]
        with st.form("edit_source_system"):
            name = st.text_input("Name", value=safe_str(row["name"]))
            description = st.text_area("Description", value=safe_str(row["description"]))
            system_type = st.selectbox(
                "System type", SYSTEM_TYPES, index=SYSTEM_TYPES.index(row["system_type"])
            )
            connection_config = st.text_area(
                "Connection config (JSON)", value=to_json_text(row["connection_config"])
            )
            is_active = st.checkbox("Active", value=bool(row["is_active"]))
            submitted = st.form_submit_button("Save changes")

        if submitted:
            try:
                cfg = json.loads(connection_config or "{}")
            except json.JSONDecodeError as e:
                st.error(f"Connection config is not valid JSON: {e}")
            else:
                try:
                    update_row(
                        engine,
                        "source_system",
                        "id",
                        row["id"],
                        {
                            "name": name,
                            "description": description or None,
                            "system_type": system_type,
                            "connection_config": json.dumps(cfg),
                            "is_active": is_active,
                        },
                        json_columns={"connection_config"},
                    )
                    st.success(f"Updated '{selected_code}'")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update: {e}")

else:  # Delete existing
    if df.empty:
        st.info("No source systems yet.")
    else:
        selected_code = st.selectbox("Select source system to delete", df["code"])
        row = df[df["code"] == selected_code].iloc[0]
        st.warning(
            f"This will permanently delete '{selected_code}'. "
            "Data feeds referencing it must be deleted first."
        )
        if st.button("Delete", type="primary"):
            try:
                delete_row(engine, "source_system", "id", row["id"])
                st.success(f"Deleted '{selected_code}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete: {e}")
