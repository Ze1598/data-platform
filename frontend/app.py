import streamlit as st

st.set_page_config(page_title="Data Platform Control", page_icon="🗂️", layout="wide")

st.title("Data Platform — Metadata Control")
st.markdown(
    """
    Use the pages in the sidebar to manage platform configuration:

    - **Source Systems** — connection details for source systems (databases, APIs, file drops)
    - **Data Feeds** — individual objects/endpoints to extract from each source system
    - **Lakehouse Models** — fact/dimension configuration for the model layer (SCD type, updates, deletions)
    - **Ingestion Triggers** — how a feed/model's pipeline run actually gets kicked off (a cron schedule, or a sensor watching a feed's landing directory for a new file)
    """
)
