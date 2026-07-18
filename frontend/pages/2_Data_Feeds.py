import json
import re
import uuid

import pandas as pd
import streamlit as st
from metadata_db import (
    delete_row,
    fetch_lookup,
    fetch_table,
    get_engine,
    insert_row,
    parse_csv_text,
    safe_str,
    to_csv_text,
    to_json_text,
    update_row,
)
from sqlalchemy import text

st.set_page_config(page_title="Data Feeds", page_icon="🛰️", layout="wide")
st.title("Data Feeds")

EXTRACTION_TYPES = ["full", "incremental"]
PROCESSING_ENGINES = ["polars", "spark"]
NEW_BATCH_OPTION = "<New batch>"
# Same shape scripts/generate_domain_projects.py::slugify_domain() expects
# to receive -- validated HERE (not shared cross-package) so a value
# reaching that script's live SELECT DISTINCT is already a valid dbt
# project/directory name. See 3_Lakehouse_Models.py's identical constant.
_DOMAIN_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]*$")

engine = get_engine()
df = fetch_table(engine, "data_feed", order_by="friendly_name")
source_systems = fetch_lookup(engine, "source_system")
pipeline_steps_df = pd.read_sql(text("select id, label from pipeline_steps order by id"), engine)
pipeline_step_label_by_id = dict(zip(pipeline_steps_df["id"], pipeline_steps_df["label"]))
pipeline_step_id_by_label = {v: k for k, v in pipeline_step_label_by_id.items()}


def _pipeline_step_ids_to_labels(pipeline_steps: str | None) -> list[str]:
    if not pipeline_steps or pd.isna(pipeline_steps):
        return []
    return [pipeline_step_label_by_id[int(s)] for s in str(pipeline_steps).split(",") if s.strip()]

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

# No st.form() on this page -- see Learnings.md, "Cross-field reactivity is
# impossible inside st.form()". render_batch_picker()'s "New batch friendly
# name" field now correctly appears/disappears live as the selectbox above
# it changes, instead of only updating on submit (a real, previously-latent
# bug). "Add new" replaces st.form's free clear_on_submit with a
# session_state generation counter (see 1_Source_Systems.py's identical
# pattern/comment).


def render_batch_picker(default_batch_name: str | None, key_prefix: str):
    options = [NEW_BATCH_OPTION] + list(batch_group_by_name.keys())
    default_index = options.index(default_batch_name) if default_batch_name in batch_group_by_name else 0
    choice = st.selectbox("Batch group", options, index=default_index, key=f"{key_prefix}_batch_choice")
    if choice == NEW_BATCH_OPTION:
        new_name = st.text_input(
            "New batch friendly name", value=default_batch_name or "", key=f"{key_prefix}_new_batch_name"
        )
        return new_name, None  # batch_group id generated at submit time
    return choice, batch_group_by_name[choice]


def render_form(defaults: dict, submit_label: str, key_prefix: str):
    source_codes = list(source_systems.keys())
    source_code = st.selectbox(
        "Source system", source_codes, index=source_codes.index(defaults["source_code"]), key=f"{key_prefix}_source_code"
    )
    friendly_name = st.text_input(
        "Friendly name", value=defaults["friendly_name"], disabled=defaults["friendly_name_locked"],
        key=f"{key_prefix}_friendly_name",
    )
    source_object_name = st.text_input(
        "Source object name (source table/endpoint)", value=defaults["source_object_name"],
        key=f"{key_prefix}_source_object_name",
    )
    batch_group_friendly_name, batch_group = render_batch_picker(defaults["batch_group_friendly_name"], key_prefix)
    batch_feed_hierarchy = st.number_input(
        "Batch feed hierarchy", value=defaults["batch_feed_hierarchy"], min_value=0, step=1,
        help="Feeds sharing the same tier can extract in parallel; lower tiers complete before higher tiers within the same batch",
        key=f"{key_prefix}_batch_feed_hierarchy",
    )
    extraction_type = st.selectbox(
        "Extraction type", EXTRACTION_TYPES, index=EXTRACTION_TYPES.index(defaults["extraction_type"]),
        key=f"{key_prefix}_extraction_type",
    )
    watermark_column = st.text_input(
        "Watermark column", value=defaults["watermark_column"], key=f"{key_prefix}_watermark_column"
    )
    extraction_config = st.text_area(
        "Extraction config (JSON)", value=defaults["extraction_config"], key=f"{key_prefix}_extraction_config"
    )
    source_pk = st.text_input(
        "Source PK columns (comma-separated)", value=defaults["source_pk"],
        help="Column names identifying a row in the source", key=f"{key_prefix}_source_pk",
    )
    processing_engine = st.selectbox(
        "Processing engine", PROCESSING_ENGINES, index=PROCESSING_ENGINES.index(defaults["processing_engine"]),
        key=f"{key_prefix}_processing_engine",
    )
    pipeline_step_labels = st.multiselect(
        "Pipeline steps", list(pipeline_step_id_by_label.keys()), default=defaults["pipeline_step_labels"],
        help="Which of the three pipeline steps this feed's master pipeline actually runs -- decided "
        "live, per run, by the master pipeline itself. See metadata/DataModel.md, 'pipeline_steps'.",
        key=f"{key_prefix}_pipeline_steps",
    )
    ods_enabled = st.checkbox(
        "ODS enabled", value=defaults["ods_enabled"],
        help="Deliver an automatic ODS (Operational Data Store) table for this feed -- clean data pushed "
        "as-is (no casts) through an auto-generated staging + Type 1 model layer, driven purely by "
        "schema_registry. Silently ignored if this feed already owns any Lakehouse Models rows -- those "
        "always take precedence. See Roadmap.md, 'ODS layer'.",
        key=f"{key_prefix}_ods_enabled",
    )
    # Only shown once ODS is enabled -- no st.form() means this appears/
    # disappears live in the same rerun, same reactive pattern as
    # render_batch_picker's "New batch friendly name" field. Defaults to
    # this feed's own batch group name the FIRST time ODS is enabled (a
    # sensible starting point -- the feed's batch is already a natural
    # grouping), but is a real, independently-stored column thereafter --
    # defaults["batch_ods_name"] (the stored value) always wins over the
    # batch-group-derived default when present, so editing this field never
    # gets silently overwritten by a later batch_group rename. See
    # data_feed.batch_ods_name, Roadmap.md "multi-project dbt split".
    if ods_enabled:
        batch_ods_name = st.text_input(
            "ODS domain name", value=defaults["batch_ods_name"] or batch_group_friendly_name or "",
            help="Which domain/dbt project (dbt/domains/<domain>/) this feed's ODS table lands in -- "
            "each batch group producing ODS output is its own legitimate individual ODS lakehouse "
            "model. Defaults to this feed's batch group name but is stored and editable independently.",
            key=f"{key_prefix}_batch_ods_name",
        )
    else:
        batch_ods_name = None
    is_active = st.checkbox("Active", value=defaults["is_active"], key=f"{key_prefix}_is_active")
    submitted = st.button(submit_label, key=f"{key_prefix}_submit")
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
        "pipeline_step_labels": pipeline_step_labels,
        "ods_enabled": ods_enabled,
        "batch_ods_name": batch_ods_name,
        "is_active": is_active,
    }


def build_values(form_values: dict, editing_id: str | None = None) -> dict | None:
    try:
        extraction_config = json.loads(form_values["extraction_config"] or "{}")
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON: {e}")
        return None
    source_pk = parse_csv_text(form_values["source_pk"])

    # Resolved once, here, and reused for both the ODS conflict check below
    # and the final return value -- computing it twice would mint two
    # different uuid.uuid4() values for a new batch, one used to check for
    # conflicts and a different one actually stored. df's "batch_group"
    # column is already stringified by fetch_table, but
    # form_values["batch_group"] can still be a raw uuid.UUID (picked from
    # existing_batches, a separate pd.read_sql call that doesn't go through
    # fetch_table) -- coerce to str, or comparisons against df silently
    # never match.
    resolved_batch_group = str(form_values["batch_group"]) if form_values["batch_group"] else str(uuid.uuid4())

    if not form_values["friendly_name"] or not form_values["source_object_name"]:
        st.error("Friendly name and source object name are required.")
        return None

    if not form_values["batch_group_friendly_name"]:
        st.error("Batch group friendly name is required.")
        return None

    if form_values["extraction_type"] == "incremental" and not form_values["watermark_column"]:
        st.error("Watermark column is required when extraction type is 'incremental'.")
        return None

    if not form_values["pipeline_step_labels"]:
        st.error("At least one pipeline step is required.")
        return None

    if form_values["ods_enabled"]:
        if not form_values["batch_ods_name"]:
            st.error("ODS domain name is required when ODS is enabled.")
            return None
        if not _DOMAIN_SLUG_RE.match(form_values["batch_ods_name"]):
            st.error(
                f"ODS domain name {form_values['batch_ods_name']!r} must be lowercase letters, digits, "
                "and underscores, starting with a letter -- it becomes a dbt project directory name "
                "verbatim."
            )
            return None

        # A batch_group is expected to map to exactly one ODS domain (1:1) --
        # see Roadmap.md "Master pipeline orchestration" / Backlog.md. Only a
        # feed with ODS enabled can conflict (one with it off contributes no
        # batch_ods_name).
        conflicting = df[
            (df["batch_group"] == resolved_batch_group)
            & (df["id"] != editing_id)
            & (df["ods_enabled"] == True)  # noqa: E712 (pandas boolean-column comparison, not identity)
            & (df["batch_ods_name"] != form_values["batch_ods_name"])
        ]
        if not conflicting.empty:
            conflict_row = conflicting.iloc[0]
            st.error(
                f"Batch group {form_values['batch_group_friendly_name']!r} already has ODS domain "
                f"{conflict_row['batch_ods_name']!r} (from feed {conflict_row['friendly_name']!r}) -- "
                "every feed sharing a batch_group must resolve to the same ODS domain name."
            )
            return None

    return {
        "source_system_id": source_systems[form_values["source_code"]],
        "friendly_name": form_values["friendly_name"],
        "source_object_name": form_values["source_object_name"],
        "batch_group": resolved_batch_group,
        "batch_group_friendly_name": form_values["batch_group_friendly_name"],
        "batch_feed_hierarchy": form_values["batch_feed_hierarchy"],
        "extraction_type": form_values["extraction_type"],
        "watermark_column": form_values["watermark_column"] or None,
        "extraction_config": json.dumps(extraction_config),
        "source_pk": json.dumps(source_pk),
        "processing_engine": form_values["processing_engine"],
        "pipeline_steps": ",".join(str(pipeline_step_id_by_label[label]) for label in form_values["pipeline_step_labels"]),
        "ods_enabled": form_values["ods_enabled"],
        "batch_ods_name": form_values["batch_ods_name"] if form_values["ods_enabled"] else None,
        "is_active": form_values["is_active"],
    }


JSON_COLUMNS = {"extraction_config", "source_pk"}

if mode == "Add new":
    if "add_data_feed_gen" not in st.session_state:
        st.session_state["add_data_feed_gen"] = 0
    key_prefix = f"add_df_{st.session_state['add_data_feed_gen']}"

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
            "source_pk": "",
            "processing_engine": "polars",
            "pipeline_step_labels": ["extraction", "transformation", "serving"],
            "ods_enabled": False,
            "batch_ods_name": None,
            "is_active": True,
        },
        "Create",
        key_prefix,
    )

    if submitted:
        values = build_values(form_values)
        if values is not None:
            try:
                insert_row(engine, "data_feed", values, json_columns=JSON_COLUMNS)
                st.success(f"Created data feed '{values['friendly_name']}'")
                st.session_state["add_data_feed_gen"] += 1
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
        key_prefix = f"edit_df_{row['id']}"

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
                "source_pk": to_csv_text(row["source_pk"]),
                "processing_engine": row["processing_engine"],
                "pipeline_step_labels": _pipeline_step_ids_to_labels(row["pipeline_steps"]),
                "ods_enabled": bool(row["ods_enabled"]),
                "batch_ods_name": safe_str(row["batch_ods_name"]),
                "is_active": bool(row["is_active"]),
            },
            "Save changes",
            key_prefix,
        )

        if submitted:
            values = build_values(form_values, editing_id=row["id"])
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
