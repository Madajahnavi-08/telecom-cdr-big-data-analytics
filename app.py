
import random
import time
from datetime import datetime

import streamlit as st

st.set_page_config(
    page_title="Live Telecom CDR Stream",
    page_icon="📡",
    layout="wide",
)

st.title("📡 Telecom CDR — Live Stream")
st.caption("Continuous telecom data simulation | Auto-updating values")

# Keep values between automatic reruns
if "total_calls" not in st.session_state:
    st.session_state.total_calls = 0
    st.session_state.total_revenue = 0.0
    st.session_state.dropped_calls = 0
    st.session_state.latest_records = []
    st.session_state.last_batch = []

# Generate one new synthetic CDR record
def generate_cdr():
    call_type = random.choice(["VOICE", "VIDEO", "DATA"])
    duration = random.randint(5, 900)

    rates = {
        "VOICE": 0.35,
        "VIDEO": 1.20,
        "DATA": 0.08,
    }

    return {
        "CDR ID": f"CDR-{random.randint(100000, 999999)}",
        "Time": datetime.now().strftime("%H:%M:%S"),
        "Caller": f"SUB{random.randint(1000, 9999)}",
        "Call Type": call_type,
        "Network": random.choice(["4G", "5G", "VoLTE"]),
        "Cell Tower": random.choice(
            ["ELR-01", "VJA-01", "GNT-01", "RJY-01"]
        ),
        "Duration (sec)": duration,
        "Revenue (₹)": round(duration / 60 * rates[call_type], 2),
        "Dropped": random.random() < 0.035,
    }

# Live display areas
status = st.empty()
metrics = st.empty()
latest = st.empty()

# Start / stop control
if "stream_running" not in st.session_state:
    st.session_state.stream_running = False

col1, col2 = st.columns(2)

with col1:
    if st.button("▶ Start Live Stream", use_container_width=True):
        st.session_state.stream_running = True

with col2:
    if st.button("⏹ Stop Live Stream", use_container_width=True):
        st.session_state.stream_running = False

if st.session_state.stream_running:
    # Add new records on each rerun
    new_records = [generate_cdr() for _ in range(random.randint(1, 3))]
    st.session_state.last_batch = new_records

    for record in new_records:
        st.session_state.total_calls += 1
        st.session_state.total_revenue += record["Revenue (₹)"]
        if record["Dropped"]:
            st.session_state.dropped_calls += 1

    st.session_state.latest_records = (
        new_records + st.session_state.latest_records
    )[:20]

status.success(
    "🟢 LIVE STREAM RUNNING"
    if st.session_state.stream_running
    else "⚪ STREAM STOPPED"
)

with metrics.container():
    a, b, c, d = st.columns(4)
    a.metric("Total Calls", st.session_state.total_calls)
    b.metric("Total Revenue", f"₹{st.session_state.total_revenue:.2f}")
    c.metric("Dropped Calls", st.session_state.dropped_calls)
    d.metric(
        "Latest Batch",
        len(st.session_state.last_batch),
    )

with latest.container():
    st.subheader("Incoming CDR Records")
    if st.session_state.latest_records:
        st.dataframe(
            st.session_state.latest_records,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Click Start Live Stream to begin generating records.")

st.caption(
    "Last page update: " + datetime.now().strftime("%H:%M:%S")
)

# Automatically rerun every second while streaming
if st.session_state.stream_running:
    time.sleep(1)
    st.rerun()
