import streamlit as st
import pandas as pd
import sqlalchemy
import time 
import psutil

engine = sqlalchemy.create_engine(
    "mysql+mysqlconnector://root:password@localhost:3306/cpu_monitoring"
)

st.title("CPU Monitoring Dashboard")

chart = st.empty()

N = st.sidebar.slider("Number of data points", 10, 500, 100)

poll_interval = st.sidebar.slider("Poll interval (seconds)", 1, 30, 5)
col1, col2 = st.columns(2)
real_cpu_placeholder = col1.empty()
hist_chart = col2.empty()

while True:
    current_cpu = psutil.cpu_percent(interval=None)

    real_cpu_placeholder.metric(
        "💻 Real-Time Local CPU Usage (%)",
        f"{current_cpu:.2f}"
    )

    df = pd.read_sql(
        f"""
        SELECT * FROM cpu_metrics
        ORDER BY timestamp DESC
        LIMIT {N}
        """,
        engine
    )

    if not df.empty:
        df = df.sort_values(by='timestamp')
        hist_chart.line_chart(
            df.set_index('timestamp')[['cpu_percent']]
        )
    else:
        hist_chart.info("No historical data found yet!")

    time.sleep(poll_interval)
    st.rerun()