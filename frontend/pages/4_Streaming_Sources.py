import re

import pandas as pd
import streamlit as st
from metadata_db import (
    delete_row,
    fetch_current_schema,
    fetch_table,
    get_engine,
    insert_row,
    safe_str,
    update_row,
    write_schema_registry_version,
)

st.set_page_config(page_title="Streaming Sources", page_icon="📡", layout="wide")
st.title("Streaming Sources")
st.caption(
    "Real-time Kafka -> Flink -> Iceberg ingestion pipelines (Roadmap Phase 11). "
    "A new, standalone concept -- not a Data Feed -- see metadata/DataModel.md, 'streaming_source'."
)

NEW_SCHEMA_OPTION = "<New domain>"
# Same shape scripts/generate_domain_projects.py::slugify_domain() expects --
# validated HERE (not shared cross-package), same convention as
# 2_Data_Feeds.py/3_Lakehouse_Models.py's identical constant.
_DOMAIN_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]*$")

engine = get_engine()
df = fetch_table(engine, "streaming_source", order_by="friendly_name")
existing_model_schemas = sorted(df["model_schema"].dropna().unique().tolist()) if not df.empty else []

st.dataframe(df, use_container_width=True, hide_index=True)
st.divider()

mode = st.radio("Action", ["Add new", "Edit existing", "Discover schema", "Delete existing"], horizontal=True)

# No st.form() on this page -- see Learnings.md, "Cross-field reactivity is
# impossible inside st.form()" -- same reactive-widget pattern as every
# other CRUD page in this app.


def render_model_schema_picker(default_schema: str | None, key_prefix: str):
    options = [NEW_SCHEMA_OPTION] + existing_model_schemas
    default_index = options.index(default_schema) if default_schema in existing_model_schemas else 0
    choice = st.selectbox(
        "Model schema (domain)", options, index=default_index, key=f"{key_prefix}_model_schema_choice",
        help="Which dbt domain (dbt/domains/<domain>/) the generated serve scaffold lands in.",
    )
    if choice == NEW_SCHEMA_OPTION:
        return st.text_input(
            "New domain name", value=default_schema or "", key=f"{key_prefix}_new_model_schema",
            help="lowercase letters, digits, underscores, starting with a letter -- becomes the dbt "
            "project directory name verbatim.",
        )
    return choice


def render_form(defaults: dict, submit_label: str, key_prefix: str):
    friendly_name = st.text_input(
        "Friendly name", value=defaults["friendly_name"], disabled=defaults["friendly_name_locked"],
        key=f"{key_prefix}_friendly_name",
    )
    topic_name = st.text_input(
        "Kafka topic name", value=defaults["topic_name"],
        help="Must already exist with real messages flowing before schema discovery can run -- this "
        "platform has exactly one shared Kafka broker (kafka.streaming.svc.cluster.local:9092), not "
        "per-row metadata.",
        key=f"{key_prefix}_topic_name",
    )
    table_name = st.text_input(
        "Table name", value=defaults["table_name"], disabled=defaults["table_name_locked"],
        help="The technical identifier -- target Iceberg table is streaming.<table_name>. Locked after "
        "creation: renaming here would orphan the already-scaffolded serve view file rather than rename "
        "it (see scripts/generate_streaming_serve_scaffolds.py).",
        key=f"{key_prefix}_table_name",
    )
    model_schema = render_model_schema_picker(defaults["model_schema"], key_prefix)

    discovered_columns = defaults.get("discovered_columns") or []
    if discovered_columns:
        column_names = [c["name"] for c in discovered_columns]
        default_ts_index = (
            column_names.index(defaults["event_timestamp_column"])
            if defaults["event_timestamp_column"] in column_names
            else 0
        )
        event_timestamp_column = st.selectbox(
            "Event timestamp column", column_names, index=default_ts_index,
            help="Which discovered column represents event time -- CAST to TIMESTAMP(6) in the "
            "generated Flink sink SQL.",
            key=f"{key_prefix}_event_timestamp_column",
        )
    else:
        st.info("Run schema discovery (**Discover schema** tab) before an event timestamp column can be picked.")
        event_timestamp_column = defaults["event_timestamp_column"] or None

    st.caption(
        "Optional Flink resource sizing -- blank falls back to the platform default. Real production "
        "Kubernetes clusters are multi-node with autoscaling, so a JobManager+TaskManager pair per "
        "source is trivial overhead there; a genuinely high-throughput source can be sized deliberately "
        "here rather than only ever getting the demo-sized default."
    )
    col1, col2 = st.columns(2)
    with col1:
        jobmanager_memory = st.text_input(
            "JobManager memory (e.g. 1024m)", value=defaults["jobmanager_memory"], key=f"{key_prefix}_jm_mem",
        )
        taskmanager_memory = st.text_input(
            "TaskManager memory (e.g. 1024m)", value=defaults["taskmanager_memory"], key=f"{key_prefix}_tm_mem",
        )
    with col2:
        taskmanager_cpu = st.text_input(
            "TaskManager CPU (e.g. 0.5)", value=defaults["taskmanager_cpu"], key=f"{key_prefix}_tm_cpu",
        )
        parallelism = st.text_input(
            "Parallelism", value=defaults["parallelism"], key=f"{key_prefix}_parallelism",
        )
    autoscaler_enabled = st.checkbox(
        "Autoscaler enabled", value=defaults["autoscaler_enabled"],
        help="The Flink Kubernetes Operator's own built-in autoscaler (job.autoscaler.enabled) -- "
        "adjusts per-job-vertex parallelism from observed load. Flagged experimental in the operator's "
        "own current docs -- opt-in only.",
        key=f"{key_prefix}_autoscaler_enabled",
    )
    schema_discovery_enabled = st.checkbox(
        "Schema discovery enabled", value=defaults["schema_discovery_enabled"],
        help="When off, the 'Discover schema' action and the streaming smoketest harness both refuse to "
        "run discovery for this source -- turn off once its schema is deemed stable.",
        key=f"{key_prefix}_schema_discovery_enabled",
    )
    is_active = st.checkbox("Active", value=defaults["is_active"], key=f"{key_prefix}_is_active")
    submitted = st.button(submit_label, key=f"{key_prefix}_submit")
    return submitted, {
        "friendly_name": friendly_name,
        "topic_name": topic_name,
        "table_name": table_name,
        "model_schema": model_schema,
        "event_timestamp_column": event_timestamp_column,
        "jobmanager_memory": jobmanager_memory,
        "taskmanager_memory": taskmanager_memory,
        "taskmanager_cpu": taskmanager_cpu,
        "parallelism": parallelism,
        "autoscaler_enabled": autoscaler_enabled,
        "schema_discovery_enabled": schema_discovery_enabled,
        "is_active": is_active,
    }


def build_values(form_values: dict) -> dict | None:
    if not form_values["friendly_name"] or not form_values["topic_name"] or not form_values["table_name"]:
        st.error("Friendly name, topic name, and table name are required.")
        return None
    if not form_values["model_schema"]:
        st.error("Model schema (domain) is required.")
        return None
    if not _DOMAIN_SLUG_RE.match(form_values["model_schema"]):
        st.error(
            f"Model schema {form_values['model_schema']!r} must be lowercase letters, digits, and "
            "underscores, starting with a letter -- it becomes a dbt project directory name verbatim."
        )
        return None

    def _blank_to_none(v):
        return v if v not in ("", None) else None

    taskmanager_cpu = _blank_to_none(form_values["taskmanager_cpu"])
    if taskmanager_cpu is not None:
        try:
            taskmanager_cpu = float(taskmanager_cpu)
        except ValueError:
            st.error(f"TaskManager CPU {taskmanager_cpu!r} must be a number.")
            return None

    parallelism = _blank_to_none(form_values["parallelism"])
    if parallelism is not None:
        try:
            parallelism = int(parallelism)
        except ValueError:
            st.error(f"Parallelism {parallelism!r} must be an integer.")
            return None

    return {
        "friendly_name": form_values["friendly_name"],
        "topic_name": form_values["topic_name"],
        "table_name": form_values["table_name"],
        "model_schema": form_values["model_schema"],
        "event_timestamp_column": _blank_to_none(form_values["event_timestamp_column"]),
        "jobmanager_memory": _blank_to_none(form_values["jobmanager_memory"]),
        "taskmanager_memory": _blank_to_none(form_values["taskmanager_memory"]),
        "taskmanager_cpu": taskmanager_cpu,
        "parallelism": parallelism,
        "autoscaler_enabled": form_values["autoscaler_enabled"],
        "schema_discovery_enabled": form_values["schema_discovery_enabled"],
        "is_active": form_values["is_active"],
    }


if mode == "Add new":
    if "add_streaming_source_gen" not in st.session_state:
        st.session_state["add_streaming_source_gen"] = 0
    key_prefix = f"add_ss_{st.session_state['add_streaming_source_gen']}"

    submitted, form_values = render_form(
        {
            "friendly_name": "",
            "friendly_name_locked": False,
            "topic_name": "",
            "table_name": "",
            "table_name_locked": False,
            "model_schema": "",
            "event_timestamp_column": None,
            "discovered_columns": None,
            "jobmanager_memory": "",
            "taskmanager_memory": "",
            "taskmanager_cpu": "",
            "parallelism": "",
            "autoscaler_enabled": False,
            "schema_discovery_enabled": True,
            "is_active": True,
        },
        "Create",
        key_prefix,
    )

    if submitted:
        values = build_values(form_values)
        if values is not None:
            try:
                insert_row(engine, "streaming_source", values)
                st.success(f"Created streaming source '{values['friendly_name']}'")
                st.session_state["add_streaming_source_gen"] += 1
                st.rerun()
            except Exception as e:
                st.error(f"Failed to create: {e}")

elif mode == "Edit existing":
    if df.empty:
        st.info("No streaming sources yet.")
    else:
        selected_name = st.selectbox("Select streaming source", df["friendly_name"])
        row = df[df["friendly_name"] == selected_name].iloc[0]
        key_prefix = f"edit_ss_{row['id']}"
        discovered_columns = fetch_current_schema(engine, row["id"], "streaming_source")

        submitted, form_values = render_form(
            {
                "friendly_name": selected_name,
                "friendly_name_locked": True,
                "topic_name": safe_str(row["topic_name"]),
                "table_name": safe_str(row["table_name"]),
                "table_name_locked": True,
                "model_schema": safe_str(row["model_schema"]),
                "event_timestamp_column": safe_str(row["event_timestamp_column"]),
                "discovered_columns": discovered_columns,
                "jobmanager_memory": safe_str(row["jobmanager_memory"]),
                "taskmanager_memory": safe_str(row["taskmanager_memory"]),
                "taskmanager_cpu": safe_str(row["taskmanager_cpu"]),
                "parallelism": safe_str(row["parallelism"]),
                "autoscaler_enabled": bool(row["autoscaler_enabled"]),
                "schema_discovery_enabled": bool(row["schema_discovery_enabled"]),
                "is_active": bool(row["is_active"]),
            },
            "Save changes",
            key_prefix,
        )

        if submitted:
            values = build_values(form_values)
            if values is not None:
                values.pop("friendly_name")  # friendly_name is immutable once created
                values.pop("table_name")  # table_name is immutable once created
                try:
                    update_row(engine, "streaming_source", "id", row["id"], values)
                    st.success(f"Updated '{selected_name}'")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update: {e}")

elif mode == "Discover schema":
    if df.empty:
        st.info("No streaming sources yet -- create one first.")
    else:
        selected_name = st.selectbox("Select streaming source", df["friendly_name"], key="discover_select")
        row = df[df["friendly_name"] == selected_name].iloc[0]
        current = fetch_current_schema(engine, row["id"], "streaming_source")

        if current:
            st.write("Current discovered schema:")
            st.dataframe(pd.DataFrame(current), use_container_width=True, hide_index=True)
        else:
            st.info("No schema discovered yet for this source.")

        if not row["schema_discovery_enabled"]:
            st.info(
                f"Schema discovery is disabled for '{selected_name}' -- its schema is deemed stable. "
                "Re-enable it under 'Edit existing' to run discovery again."
            )
        else:
            sample_size = st.number_input("Sample messages to consume", min_value=1, max_value=1000, value=20)
            st.caption(
                f"Connects to topic '{row['topic_name']}' and consumes up to {int(sample_size)} sample "
                "messages to infer column types. The topic must already have real messages flowing -- "
                "there's no live stream to sample from before one exists."
            )

            if st.button("Discover schema now", type="primary"):
                import json

                import polars as pl
                from confluent_kafka import Consumer
                from connectors.inference import infer_column_definitions

                KAFKA_BOOTSTRAP_SERVERS = "kafka.streaming.svc.cluster.local:9092"
                consumer = Consumer(
                    {
                        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                        "group.id": f"schema-discovery-{row['id']}",
                        "auto.offset.reset": "earliest",
                    }
                )
                consumer.subscribe([row["topic_name"]])
                messages = []
                try:
                    with st.spinner(f"Consuming sample messages from '{row['topic_name']}'..."):
                        for _ in range(int(sample_size) * 5):  # bounded polling attempts, not just message count
                            if len(messages) >= sample_size:
                                break
                            msg = consumer.poll(timeout=2.0)
                            if msg is None or msg.error():
                                continue
                            try:
                                messages.append(json.loads(msg.value()))
                            except json.JSONDecodeError:
                                continue
                finally:
                    consumer.close()

                if not messages:
                    st.error(
                        f"No messages consumed from topic '{row['topic_name']}' -- discovery requires the "
                        "topic to already have real data flowing. Confirm a producer is writing to it."
                    )
                else:
                    sample_df = pl.DataFrame(messages)
                    column_definitions = infer_column_definitions(sample_df)
                    write_schema_registry_version(
                        engine,
                        controlling_object_id=row["id"],
                        controlling_object_type="streaming_source",
                        column_definitions=column_definitions,
                        primary_key_columns=[],
                        created_by="4_Streaming_Sources_discover_schema",
                    )
                    st.success(f"Discovered {len(column_definitions)} column(s) from {len(messages)} sample message(s).")
                    st.rerun()

else:  # Delete existing
    if df.empty:
        st.info("No streaming sources yet.")
    else:
        selected_name = st.selectbox("Select streaming source to delete", df["friendly_name"])
        row = df[df["friendly_name"] == selected_name].iloc[0]
        st.warning(f"This will permanently delete '{selected_name}'.")
        if st.button("Delete", type="primary"):
            try:
                delete_row(engine, "streaming_source", "id", row["id"])
                st.success(f"Deleted '{selected_name}'")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete: {e}")
