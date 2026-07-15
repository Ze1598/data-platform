import json
import re

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
NEW_SCHEMA_OPTION = "<New schema>"
# Same shape as scripts/generate_domain_projects.py::slugify_domain() expects
# to receive -- validated HERE (not shared cross-package, frontend/ and
# scripts/ are separate uv workspace members) so a value reaching that
# script's live SELECT DISTINCT is already a valid dbt project/directory
# name, not just whatever free text a user typed (see Roadmap.md
# "multi-project dbt split").
_DOMAIN_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]*$")

engine = get_engine()
df = fetch_table(engine, "lakehouse_models", order_by="friendly_name")
existing_model_schemas = sorted(
    df["model_schema"].dropna().unique().tolist()
) if not df.empty else []
data_feeds_df = fetch_table(engine, "data_feed", order_by="friendly_name")
data_feed_lookup = fetch_lookup(engine, "data_feed", code_col="friendly_name")
data_feed_name_by_id = {str(v): k for k, v in data_feed_lookup.items()}
data_feed_extraction_type = dict(zip(data_feeds_df["friendly_name"], data_feeds_df["extraction_type"]))
load_types_df = pd.read_sql(text("select id, label from load_type order by id"), engine)
load_type_label_by_id = dict(zip(load_types_df["id"], load_types_df["label"]))
load_type_id_by_label = {v: k for k, v in load_type_label_by_id.items()}
pipeline_steps_df = pd.read_sql(text("select id, label from pipeline_steps order by id"), engine)
pipeline_step_label_by_id = dict(zip(pipeline_steps_df["id"], pipeline_steps_df["label"]))
pipeline_step_id_by_label = {v: k for k, v in pipeline_step_label_by_id.items()}


def _pipeline_step_ids_to_labels(pipeline_steps: str | None) -> list[str]:
    if not pipeline_steps or pd.isna(pipeline_steps):
        return []
    return [pipeline_step_label_by_id[int(s)] for s in str(pipeline_steps).split(",") if s.strip()]

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

# No st.form() on this page -- see Learnings.md, "Cross-field reactivity is
# impossible inside st.form()". This is what makes "Owning feed" below
# genuinely possible: it's live-filtered to whatever's currently checked in
# "Depends on feeds", in the same rerun, instead of needing a separate
# validation error after the fact. "Add new" replaces st.form's free
# clear_on_submit with a session_state generation counter (see
# 1_Source_Systems.py's identical pattern/comment).


def render_model_schema_picker(default_schema: str | None, key_prefix: str):
    # Mirrors 2_Data_Feeds.py::render_batch_picker's exact pattern --
    # simpler here since model_schema has no backing UUID, just a bare
    # string (a domain/business grouping of related lakehouse model
    # tables, see metadata/DataModel.md -- NOT a physical Trino/Iceberg
    # schema anymore, that meaning moved to a fixed 'model' literal once
    # the multi-project dbt split landed).
    options = [NEW_SCHEMA_OPTION] + existing_model_schemas
    default_index = options.index(default_schema) if default_schema in existing_model_schemas else 0
    choice = st.selectbox(
        "Model schema (domain)", options, index=default_index, key=f"{key_prefix}_model_schema_choice",
        help="Which domain/dbt project (dbt/domains/<domain>/) this model belongs to -- a business "
        "grouping, not tied to any single source. Pick an existing domain or create a new one.",
    )
    if choice == NEW_SCHEMA_OPTION:
        return st.text_input(
            "New domain name", value=default_schema or "", key=f"{key_prefix}_new_model_schema",
            help="lowercase letters, digits, underscores, starting with a letter -- becomes the dbt "
            "project directory name verbatim.",
        )
    return choice


def render_form(defaults: dict, submit_label: str, key_prefix: str):
    feed_names = list(data_feed_lookup.keys())
    friendly_name = st.text_input(
        "Friendly name", value=defaults["friendly_name"], disabled=defaults["friendly_name_locked"],
        help="Pure display label -- not a technical identifier. See 'Table name' below for that.",
        key=f"{key_prefix}_friendly_name",
    )
    table_name = st.text_input(
        "Table name", value=defaults["table_name"], disabled=defaults["table_name_locked"],
        help="The technical identifier -- drives both the physical table alias and the dbt model's own "
        "filename. Enter the complete name following the '<model_schema>_<fct|dim>_<name>' convention "
        "(e.g. sales_dim_customer). Locked after creation: renaming here would orphan the already-"
        "scaffolded file rather than rename it (see scripts/generate_model_scaffolds.py).",
        key=f"{key_prefix}_table_name",
    )
    model_schema = render_model_schema_picker(defaults["model_schema"], key_prefix)
    batch_hierarchy = st.number_input(
        "Batch hierarchy", value=defaults["batch_hierarchy"], min_value=0, step=1, key=f"{key_prefix}_batch_hierarchy"
    )
    table_type = st.selectbox(
        "Table type", TABLE_TYPES, index=TABLE_TYPES.index(defaults["table_type"]), key=f"{key_prefix}_table_type"
    )
    depends_on_feed_names = st.multiselect(
        "Depends on feeds", feed_names, default=defaults["depends_on_feed_names"],
        help="Every data feed this model must have successfully ingested before it builds",
        key=f"{key_prefix}_depends_on",
    )

    # Owning feed is live-filtered to whatever's currently checked above --
    # depends_on_feed_names reflects the user's current, not-yet-submitted
    # multiselect state by the time this line runs, in the same rerun (no
    # form gating it). Falls back to every feed when nothing's checked yet,
    # purely so the selectbox has something to show -- the "at least one
    # dependent feed" check in build_values() still blocks submission in
    # that case. Pre-correcting st.session_state directly (rather than
    # passing index=) is required here: options can shrink between reruns
    # (the user unchecking a feed that was the current owning feed), and
    # Streamlit raises if a keyed widget's stored value isn't in the
    # options list passed that render.
    owning_options = depends_on_feed_names if depends_on_feed_names else feed_names
    owning_key = f"{key_prefix}_owning_feed"
    if owning_key not in st.session_state or st.session_state[owning_key] not in owning_options:
        st.session_state[owning_key] = (
            defaults["owning_feed_name"] if defaults["owning_feed_name"] in owning_options else owning_options[0]
        )
    owning_feed_name = st.selectbox(
        "Owning feed", owning_options, key=owning_key,
        help="Which single feed's Dagster job/dbt build actually builds this model -- automatically "
        "narrowed to whatever's checked above. Only matters for models depending on more than one "
        "feed; required either way so the meaning is never implicit.",
    )

    business_key_columns = st.text_area(
        "Business key columns (JSON array)", value=defaults["business_key_columns"],
        key=f"{key_prefix}_business_key_columns",
    )
    tracked_columns = st.text_area(
        "Tracked columns (JSON array)", value=defaults["tracked_columns"], key=f"{key_prefix}_tracked_columns"
    )
    scd_type = st.selectbox(
        "SCD type", SCD_TYPES, index=SCD_TYPES.index(defaults["scd_type"]),
        help="1 = overwrite in place, 2 = new version per change", key=f"{key_prefix}_scd_type",
    )
    updates_enabled = st.checkbox(
        "Updates enabled", value=defaults["updates_enabled"],
        help="Also drives whether this model's upstream staging source(s) merge on attribute change "
        "-- see metadata/DataModel.md, 'Staging update-tracking rule'.",
        key=f"{key_prefix}_updates_enabled",
    )
    deletes_enabled = st.checkbox(
        "Deletes enabled", value=defaults["deletes_enabled"],
        help="Only valid when every dependent data feed uses a full extraction -- deletion is "
        "detected as a business key missing from the current full load.",
        key=f"{key_prefix}_deletes_enabled",
    )
    watermark_column = st.text_input(
        "Watermark column", value=defaults["watermark_column"], key=f"{key_prefix}_watermark_column"
    )
    load_type_label = st.selectbox(
        "Load type", list(load_type_id_by_label.keys()),
        index=list(load_type_id_by_label.keys()).index(defaults["load_type_label"]), key=f"{key_prefix}_load_type",
    )
    pipeline_step_labels = st.multiselect(
        "Pipeline steps", list(pipeline_step_id_by_label.keys()), default=defaults["pipeline_step_labels"],
        help="A model has no extraction/validation of its own -- in practice this only ever "
        "meaningfully gates 'serving' (whether this model's _latest/_historical views get generated). "
        "See metadata/DataModel.md, 'pipeline_steps'.",
        key=f"{key_prefix}_pipeline_steps",
    )
    is_active = st.checkbox("Active", value=defaults["is_active"], key=f"{key_prefix}_is_active")
    submitted = st.button(submit_label, key=f"{key_prefix}_submit")
    return submitted, {
        "friendly_name": friendly_name,
        "table_name": table_name,
        "model_schema": model_schema,
        "batch_hierarchy": int(batch_hierarchy),
        "table_type": table_type,
        "depends_on_feed_names": depends_on_feed_names,
        "owning_feed_name": owning_feed_name,
        "business_key_columns": business_key_columns,
        "tracked_columns": tracked_columns,
        "scd_type": scd_type,
        "updates_enabled": updates_enabled,
        "deletes_enabled": deletes_enabled,
        "watermark_column": watermark_column,
        "load_type_label": load_type_label,
        "pipeline_step_labels": pipeline_step_labels,
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

    if not form_values["table_name"]:
        st.error("Table name is required.")
        return None

    if not _DOMAIN_SLUG_RE.match(form_values["model_schema"]):
        st.error(
            f"Model schema {form_values['model_schema']!r} must be lowercase letters, digits, and "
            "underscores, starting with a letter -- it becomes a dbt project directory name verbatim."
        )
        return None

    if not form_values["depends_on_feed_names"]:
        st.error("At least one dependent feed is required.")
        return None

    if not form_values["pipeline_step_labels"]:
        st.error("At least one pipeline step is required.")
        return None

    # Should be unreachable via the UI now -- "Owning feed" is live-filtered
    # to depends_on_feed_names above, so it can never actually hold a value
    # outside that list. Kept as a defensive safety net, same reasoning as
    # the table/column literal-safety checks in postgres_metadata_resource.py.
    if form_values["owning_feed_name"] not in form_values["depends_on_feed_names"]:
        st.error(
            f"Owning feed ({form_values['owning_feed_name']}) must be one of the selected "
            f"depends-on feeds ({', '.join(form_values['depends_on_feed_names'])})."
        )
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
        "table_name": form_values["table_name"],
        "model_schema": form_values["model_schema"],
        "batch_hierarchy": form_values["batch_hierarchy"],
        "table_type": form_values["table_type"],
        "depends_on_feeds": depends_on_feeds,
        "owning_feed_id": data_feed_lookup[form_values["owning_feed_name"]],
        "business_key_columns": json.dumps(business_key_columns),
        "tracked_columns": json.dumps(tracked_columns),
        "scd_type": form_values["scd_type"],
        "updates_enabled": form_values["updates_enabled"],
        "deletes_enabled": form_values["deletes_enabled"],
        "watermark_column": form_values["watermark_column"] or None,
        "load_type": load_type_id_by_label[form_values["load_type_label"]],
        "pipeline_steps": ",".join(str(pipeline_step_id_by_label[label]) for label in form_values["pipeline_step_labels"]),
        "is_active": form_values["is_active"],
    }


JSON_COLUMNS = {"business_key_columns", "tracked_columns"}

if mode == "Add new":
    if "add_lakehouse_model_gen" not in st.session_state:
        st.session_state["add_lakehouse_model_gen"] = 0
    key_prefix = f"add_lm_{st.session_state['add_lakehouse_model_gen']}"

    submitted, form_values = render_form(
        {
            "friendly_name": "",
            "friendly_name_locked": False,
            "table_name": "",
            "table_name_locked": False,
            "model_schema": "",
            "batch_hierarchy": 0,
            "table_type": "dimension",
            "depends_on_feed_names": [],
            "owning_feed_name": list(data_feed_lookup.keys())[0],
            "business_key_columns": "[]",
            "tracked_columns": "[]",
            "scd_type": 2,
            "updates_enabled": True,
            "deletes_enabled": False,
            "watermark_column": "",
            "load_type_label": "full",
            "pipeline_step_labels": ["transformation", "serving"],
            "is_active": True,
        },
        "Create",
        key_prefix,
    )

    if submitted:
        values = build_values(form_values)
        if values is not None:
            try:
                insert_row(engine, "lakehouse_models", values, json_columns=JSON_COLUMNS)
                st.success(f"Created lakehouse model '{values['friendly_name']}'")
                st.session_state["add_lakehouse_model_gen"] += 1
                st.rerun()
            except Exception as e:
                st.error(f"Failed to create: {e}")

elif mode == "Edit existing":
    if df.empty:
        st.info("No lakehouse models yet.")
    else:
        selected_name = st.selectbox("Select lakehouse model", df["friendly_name"])
        row = df[df["friendly_name"] == selected_name].iloc[0]
        key_prefix = f"edit_lm_{row['id']}"

        submitted, form_values = render_form(
            {
                "friendly_name": selected_name,
                "friendly_name_locked": True,
                "table_name": safe_str(row["table_name"]),
                "table_name_locked": True,
                "model_schema": safe_str(row["model_schema"]),
                "batch_hierarchy": int(row["batch_hierarchy"]),
                "table_type": row["table_type"],
                "depends_on_feed_names": _depends_on_ids_to_names(row["depends_on_feeds"]),
                "owning_feed_name": data_feed_name_by_id.get(
                    str(row["owning_feed_id"]), list(data_feed_lookup.keys())[0]
                ),
                "business_key_columns": to_json_text(row["business_key_columns"], default="[]"),
                "tracked_columns": to_json_text(row["tracked_columns"], default="[]"),
                "scd_type": int(row["scd_type"]),
                "updates_enabled": bool(row["updates_enabled"]),
                "deletes_enabled": bool(row["deletes_enabled"]),
                "watermark_column": safe_str(row["watermark_column"]),
                "load_type_label": load_type_label_by_id[int(row["load_type"])],
                "pipeline_step_labels": _pipeline_step_ids_to_labels(row["pipeline_steps"]),
                "is_active": bool(row["is_active"]),
            },
            "Save changes",
            key_prefix,
        )

        if submitted:
            values = build_values(form_values)
            if values is not None:
                values.pop("friendly_name")  # friendly_name is immutable once created
                values.pop("table_name")  # table_name is immutable once created -- see the field's own help text
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
