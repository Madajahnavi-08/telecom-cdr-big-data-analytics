import json
from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Telecom CDR Big Data Analytics", page_icon="📡", layout="wide")
st.title("📡 Telecom CDR Big Data Analytics")
st.caption("Multi-source CDR integration • Streaming-ready architecture • Kafka + Spark Structured Streaming")

REQUIRED = ["timestamp", "source", "caller_id", "receiver_id", "duration_sec", "data_mb", "cell_id", "call_status"]

@st.cache_data
def load_demo():
    path = Path(__file__).parent / "sample_data" / "cdr_sample.csv"
    return pd.read_csv(path)

def normalize(df, source_name="uploaded"):
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    aliases = {
        "event_time": "timestamp", "call_time": "timestamp", "datetime": "timestamp",
        "from": "caller_id", "caller": "caller_id", "to": "receiver_id", "receiver": "receiver_id",
        "duration": "duration_sec", "duration_seconds": "duration_sec", "data_usage_mb": "data_mb",
        "cell_tower": "cell_id", "status": "call_status"
    }
    df = df.rename(columns={c: aliases[c] for c in df.columns if c in aliases})
    for col in REQUIRED:
        if col not in df.columns:
            if col == "source":
                df[col] = source_name
            elif col == "duration_sec" or col == "data_mb":
                df[col] = 0
            elif col == "call_status":
                df[col] = "UNKNOWN"
            else:
                df[col] = "UNKNOWN"
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce").fillna(0)
    df["data_mb"] = pd.to_numeric(df["data_mb"], errors="coerce").fillna(0)
    df["source"] = df["source"].fillna(source_name).astype(str)
    df["call_status"] = df["call_status"].fillna("UNKNOWN").astype(str).str.upper()
    df = df.dropna(subset=["timestamp"])
    return df[REQUIRED].drop_duplicates()

with st.sidebar:
    st.header("Data sources")
    uploaded = st.file_uploader("Upload CDR CSV (optional)", type=["csv"])
    st.write("No upload? The built-in sample dataset will be used.")
    st.divider()
    st.subheader("Filters")
    st.caption("Filters apply to the current dataset.")

try:
    if uploaded is not None:
        raw = pd.read_csv(uploaded)
        df = normalize(raw)
        st.sidebar.success(f"Loaded {len(df):,} rows from uploaded CSV")
    else:
        df = normalize(load_demo(), "demo")
except Exception as e:
    st.error(f"Could not load the data: {e}")
    st.stop()

if df.empty:
    st.warning("No valid records found. Check that the CSV contains a valid timestamp column.")
    st.stop()

sources = sorted(df["source"].dropna().astype(str).unique().tolist())
chosen_sources = st.sidebar.multiselect("Source system", sources, default=sources)
statuses = sorted(df["call_status"].dropna().astype(str).unique().tolist())
chosen_statuses = st.sidebar.multiselect("Call status", statuses, default=statuses)
filtered = df[df["source"].astype(str).isin(chosen_sources) & df["call_status"].astype(str).isin(chosen_statuses)].copy()

if filtered.empty:
    st.warning("No records match the selected filters.")
    st.stop()

total_calls = len(filtered)
success_rate = (filtered["call_status"].eq("COMPLETED").mean() * 100) if total_calls else 0
total_minutes = filtered["duration_sec"].sum() / 60
total_data = filtered["data_mb"].sum()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total CDR events", f"{total_calls:,}")
c2.metric("Completed calls", f"{(filtered['call_status'] == 'COMPLETED').sum():,}")
c3.metric("Completion rate", f"{success_rate:.1f}%")
c4.metric("Data usage", f"{total_data:,.1f} MB")

c5, c6 = st.columns(2)
c5.metric("Total call duration", f"{total_minutes:,.1f} min")
c6.metric("Unique cell IDs", f"{filtered['cell_id'].nunique():,}")

st.subheader("Traffic over time")
time_data = (
    filtered.dropna(subset=["timestamp"])
    .set_index("timestamp")
    .resample("1h")
    .size()
    .reset_index(name="events")
)
if not time_data.empty:
    st.plotly_chart(px.line(time_data, x="timestamp", y="events", markers=True, title="CDR events per hour"), use_container_width=True)

left, right = st.columns(2)
with left:
    st.subheader("Events by source")
    src = filtered.groupby("source", as_index=False).size().rename(columns={"size": "events"})
    st.plotly_chart(px.bar(src, x="source", y="events", color="source", title="Multi-source contribution"), use_container_width=True)
with right:
    st.subheader("Call status distribution")
    stat = filtered.groupby("call_status", as_index=False).size().rename(columns={"size": "events"})
    st.plotly_chart(px.pie(stat, names="call_status", values="events", title="Call outcomes"), use_container_width=True)

left, right = st.columns(2)
with left:
    st.subheader("Top cell towers")
    towers = filtered.groupby("cell_id", as_index=False).agg(events=("cell_id", "size"), data_mb=("data_mb", "sum")).sort_values("events", ascending=False).head(10)
    st.plotly_chart(px.bar(towers, x="cell_id", y="events", hover_data=["data_mb"], title="Top 10 cells by events"), use_container_width=True)
with right:
    st.subheader("Top callers")
    callers = filtered.groupby("caller_id", as_index=False).agg(events=("caller_id", "size"), duration_sec=("duration_sec", "sum")).sort_values("events", ascending=False).head(10)
    st.plotly_chart(px.bar(callers, x="caller_id", y="events", hover_data=["duration_sec"], title="Top 10 callers"), use_container_width=True)

st.subheader("Integrated CDR records")
st.dataframe(filtered.sort_values("timestamp", ascending=False), use_container_width=True, hide_index=True)
st.download_button("Download filtered records as CSV", filtered.to_csv(index=False).encode("utf-8"), "filtered_cdr_records.csv", "text/csv")

with st.expander("Architecture notes"):
    st.markdown("""
    **Demo mode:** Streamlit reads a bundled sample CSV and supports CSV upload from multiple systems.

    **Streaming mode:** `kafka_producer.py` publishes CDR JSON events to Kafka topic `telecom-cdr`.
    `spark_streaming_consumer.py` reads that topic with Spark Structured Streaming and writes parsed events to `output/cdr_stream/` in JSON format.

    This dashboard is a batch/interactive analytics layer. To display live Kafka/Spark results, point it at an exported CSV or extend the app to read the Spark output directory.
    """)
