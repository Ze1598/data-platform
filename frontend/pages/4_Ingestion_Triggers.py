import pandas as pd
import streamlit as st
from metadata_db import (
    delete_row,
    fetch_lookup,
    fetch_table,
    get_engine,
    insert_row,
    safe_str,
    update_row,
)
from sqlalchemy import text

st.set_page_config(page_title="Ingestion Triggers", page_icon="⏱️", layout="wide")
st.title("Ingestion Triggers")

CONTROLLING_OBJECT_TYPES = ["feed", "model"]
TRIGGER_TYPES = ["schedule", "sensor"]
# Only these connector_kinds have a landing directory a sensor could
# plausibly watch (processing/connectors/connectors/csv.py, json_file.py) --
# see chk_ingestion_triggers_sensor_feed_only (metadata/db/init/01_platform_metadata.sql),
# which enforces the feed-only half of this in the DB; the connector_kind
# half can only ever be an application-layer check, since it reaches
# source_system through two joins.
SENSOR_ELIGIBLE_CONNECTOR_KINDS = ("csv", "json_file")

engine = get_engine()
# order_by="id", not the fetch_table default "created_at" -- this table has
# no created_at column (unlike every other table this page family reads).
df = fetch_table(engine, "ingestion_triggers", order_by="id")
feed_lookup = fetch_lookup(engine, "data_feed", code_col="friendly_name")
model_lookup = fetch_lookup(engine, "lakehouse_models", code_col="friendly_name")
feed_name_by_id = {str(v): k for k, v in feed_lookup.items()}
model_name_by_id = {str(v): k for k, v in model_lookup.items()}

feed_connector_kind_df = pd.read_sql(
    text(
        "select df.friendly_name, ss.connector_kind from data_feed df "
        "join source_system ss on ss.id = df.source_system_id"
    ),
    engine,
)
feed_connector_kind = dict(zip(feed_connector_kind_df["friendly_name"], feed_connector_kind_df["connector_kind"]))


def _target_name(row) -> str:
    lookup = feed_name_by_id if row["controlling_object_type"] == "feed" else model_name_by_id
    return lookup.get(row["controlling_object_id"], row["controlling_object_id"])


if not df.empty:
    df["target"] = df.apply(_target_name, axis=1)
    df["label"] = df.apply(lambda r: f"{r['target']} [{r['controlling_object_type']}] -- {r['trigger_type']}", axis=1)

st.dataframe(df, use_container_width=True, hide_index=True)
st.divider()

if not feed_lookup and not model_lookup:
    st.info("Create a data feed or lakehouse model first.")
    st.stop()

mode = st.radio("Action", ["Add new", "Edit existing", "Delete existing"], horizontal=True)

# No st.form() on this page -- see Learnings.md, "Cross-field reactivity is
# impossible inside st.form()", same reasoning as every other CRUD page
# here. cron's visibility (schedule-type only) and the sensor-eligibility
# warning both need to react live to trigger_type/target changes in the
# same rerun, the same pattern as Data Feeds' batch_ods_name field.


def render_form(defaults: dict, submit_label: str, key_prefix: str):
    controlling_object_type = st.radio(
        "Controls a", CONTROLLING_OBJECT_TYPES,
        index=CONTROLLING_OBJECT_TYPES.index(defaults["controlling_object_type"]),
        horizontal=True, key=f"{key_prefix}_controlling_object_type",
    )
    target_options = list(feed_lookup.keys()) if controlling_object_type == "feed" else list(model_lookup.keys())

    # Same "pre-correct session_state before the widget renders" pattern as
    # Lakehouse Models' owning-feed selectbox -- options change shape
    # entirely (feed names vs model names) when controlling_object_type
    # flips, so a stored selection from the other type would be invalid.
    target_key = f"{key_prefix}_target_name"
    if target_key not in st.session_state or st.session_state[target_key] not in target_options:
        st.session_state[target_key] = (
            defaults["target_name"] if defaults["target_name"] in target_options
            else (target_options[0] if target_options else None)
        )
    if target_options:
        target_name = st.selectbox("Target", target_options, key=target_key)
    else:
        target_name = None
        st.info(f"No {controlling_object_type}s exist yet.")

    trigger_type = st.radio(
        "Trigger type", TRIGGER_TYPES, index=TRIGGER_TYPES.index(defaults["trigger_type"]),
        horizontal=True, key=f"{key_prefix}_trigger_type",
        help="A feed/model picks one trigger mechanism -- schedule (cron) or sensor (new-file arrival). "
        "Sensor is only valid for a feed whose source is a landing-style file drop.",
    )
    if trigger_type == "schedule":
        cron = st.text_input(
            "Cron schedule", value=defaults["cron"] or "",
            help="Standard 5-field cron syntax, e.g. '0 6 * * *'", key=f"{key_prefix}_cron",
        )
    else:
        cron = None
        eligible = (
            controlling_object_type == "feed"
            and target_name is not None
            and feed_connector_kind.get(target_name) in SENSOR_ELIGIBLE_CONNECTOR_KINDS
        )
        if not eligible:
            st.warning(
                "A sensor watches a feed's own landing directory -- only valid for a feed whose source "
                "system's connector kind is 'csv' or 'json_file'. This selection isn't eligible yet."
            )

    is_active = st.checkbox("Active", value=defaults["is_active"], key=f"{key_prefix}_is_active")
    submitted = st.button(submit_label, key=f"{key_prefix}_submit")
    return submitted, {
        "controlling_object_type": controlling_object_type,
        "target_name": target_name,
        "trigger_type": trigger_type,
        "cron": cron,
        "is_active": is_active,
    }


def build_values(form_values: dict) -> dict | None:
    if not form_values["target_name"]:
        st.error("A target feed/model is required.")
        return None

    if form_values["trigger_type"] == "schedule":
        if not form_values["cron"]:
            st.error("Cron schedule is required when trigger type is 'schedule'.")
            return None
    else:
        if form_values["controlling_object_type"] != "feed":
            st.error("Sensor-type triggers are only valid for a feed, not a model.")
            return None
        if feed_connector_kind.get(form_values["target_name"]) not in SENSOR_ELIGIBLE_CONNECTOR_KINDS:
            st.error(
                f"Feed {form_values['target_name']!r} isn't sensor-eligible -- its source system's connector "
                "kind must be 'csv' or 'json_file' (a sensor watches a landing directory)."
            )
            return None

    lookup = feed_lookup if form_values["controlling_object_type"] == "feed" else model_lookup
    return {
        "trigger_type": form_values["trigger_type"],
        "cron": form_values["cron"] if form_values["trigger_type"] == "schedule" else None,
        "controlling_object_id": lookup[form_values["target_name"]],
        "controlling_object_type": form_values["controlling_object_type"],
        "is_active": form_values["is_active"],
    }


if mode == "Add new":
    if "add_ingestion_trigger_gen" not in st.session_state:
        st.session_state["add_ingestion_trigger_gen"] = 0
    key_prefix = f"add_it_{st.session_state['add_ingestion_trigger_gen']}"

    submitted, form_values = render_form(
        {
            "controlling_object_type": "feed",
            "target_name": list(feed_lookup.keys())[0] if feed_lookup else None,
            "trigger_type": "schedule",
            "cron": "",
            "is_active": True,
        },
        "Create",
        key_prefix,
    )

    if submitted:
        values = build_values(form_values)
        if values is not None:
            try:
                insert_row(engine, "ingestion_triggers", values)
                st.success(f"Created {values['trigger_type']} trigger for {form_values['target_name']!r}")
                st.session_state["add_ingestion_trigger_gen"] += 1
                st.rerun()
            except Exception as e:
                st.error(f"Failed to create: {e}")

elif mode == "Edit existing":
    if df.empty:
        st.info("No ingestion triggers yet.")
    else:
        selected_label = st.selectbox("Select ingestion trigger", df["label"])
        row = df[df["label"] == selected_label].iloc[0]
        key_prefix = f"edit_it_{row['id']}"

        name_lookup = feed_name_by_id if row["controlling_object_type"] == "feed" else model_name_by_id
        submitted, form_values = render_form(
            {
                "controlling_object_type": row["controlling_object_type"],
                "target_name": name_lookup.get(row["controlling_object_id"]),
                "trigger_type": row["trigger_type"],
                "cron": safe_str(row["cron"]),
                "is_active": bool(row["is_active"]),
            },
            "Save changes",
            key_prefix,
        )

        if submitted:
            values = build_values(form_values)
            if values is not None:
                try:
                    update_row(engine, "ingestion_triggers", "id", row["id"], values)
                    st.success(f"Updated trigger for {form_values['target_name']!r}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update: {e}")

else:  # Delete existing
    if df.empty:
        st.info("No ingestion triggers yet.")
    else:
        selected_label = st.selectbox("Select ingestion trigger to delete", df["label"])
        row = df[df["label"] == selected_label].iloc[0]
        st.warning(f"This will permanently delete the trigger for '{selected_label}'.")
        if st.button("Delete", type="primary"):
            try:
                delete_row(engine, "ingestion_triggers", "id", row["id"])
                st.success(f"Deleted trigger for '{selected_label}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete: {e}")
