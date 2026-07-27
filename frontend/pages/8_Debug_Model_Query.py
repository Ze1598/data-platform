import os

import pandas as pd
import streamlit as st
import trino.dbapi

st.set_page_config(page_title="Debug: Model Query", page_icon="🐛", layout="wide")
st.title("Debug: Model Query")
st.caption(
    "Ad hoc inspection only, no write path -- pick a table in the model schema (iceberg.model.<table>) "
    "and see up to its first 500 rows."
)

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))


@st.cache_resource
def get_trino_conn():
    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="frontend_debug", catalog="iceberg")


@st.cache_data(ttl=60)
def list_model_tables() -> list[str]:
    cur = get_trino_conn().cursor()
    cur.execute("SHOW TABLES FROM iceberg.model")
    return sorted(row[0] for row in cur.fetchall())


tables = list_model_tables()
if not tables:
    st.info("No tables found in the model schema.")
    st.stop()

selected_table = st.selectbox("Table", tables)

# selected_table only ever comes from SHOW TABLES above (a controlled,
# platform-generated list, never free-typed user input), so interpolating
# it directly into the FROM clause is safe here -- same principle
# metadata_db.py's own docstring already states for table/column names.
cur = get_trino_conn().cursor()
cur.execute(f"SELECT * FROM iceberg.model.{selected_table} LIMIT 500")
columns = [d[0] for d in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=columns)

st.caption(f"{len(df)} row(s) shown (limited to 500)")
st.dataframe(df, use_container_width=True, hide_index=True)
