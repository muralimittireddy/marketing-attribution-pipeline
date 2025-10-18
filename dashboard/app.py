import streamlit as st
import pandas as pd
import plotly.express as px
from bq_utils import fetch_attribution_data
from google.cloud import bigquery
from streamlit_autorefresh import st_autorefresh
import os
from dotenv import load_dotenv
from bq_utils import load_live_panel_data

dotenv_path = "/path/to/env_file/.env"

load_dotenv(dotenv_path)

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")
MART_TABLE = os.getenv("MART_TABLE")
STREAM_TABLE = os.getenv("STREAM_TABLE")



st.set_page_config(
    page_title="First vs Last Click Attribution",
    layout="wide",
)


st_autorefresh(interval=5000, limit=None, key="live_refresh")


st.title("📊 First vs Last Click Attribution Dashboard")

client = bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=60)
def load_mart_data():
    return fetch_attribution_data(PROJECT_ID, DATASET, MART_TABLE,client)

mart_data = load_mart_data()

if mart_data.empty:
    st.warning("No historical data found.")
    st.stop()

st.subheader("Total Revenue Metrics (Historical 14-Day)")

col1, col2 = st.columns(2)
with col1:
    first_rev = mart_data.groupby("first_click_channel")["purchase_value"].sum().sum()
    st.metric("Total Revenue (First Click)", f"${first_rev:,.2f}")

with col2:
    last_rev = mart_data.groupby("last_click_channel")["purchase_value"].sum().sum()
    st.metric("Total Revenue (Last Click)", f"${last_rev:,.2f}")


st.subheader("📅 14-Day Revenue Trend")

first_click_daily = mart_data.groupby(["conversion_date", "first_click_channel"])["purchase_value"].sum().reset_index()
last_click_daily = mart_data.groupby(["conversion_date", "last_click_channel"])["purchase_value"].sum().reset_index()

tab1, tab2 = st.tabs(["First Click", "Last Click"])

with tab1:
    fig1 = px.line(
        first_click_daily,
        x="conversion_date",
        y="purchase_value",
        color="first_click_channel",
        title="Revenue by First Click Channel (14 days)"
    )
    st.plotly_chart(fig1, use_container_width=True)

with tab2:
    fig2 = px.line(
        last_click_daily,
        x="conversion_date",
        y="purchase_value",
        color="last_click_channel",
        title="Revenue by Last Click Channel (14 days)"
    )
    st.plotly_chart(fig2, use_container_width=True)


st.subheader("📈 Channel Breakdown")

col3, col4 = st.columns(2)

with col3:
    first_breakdown = mart_data.groupby("first_click_channel")["purchase_value"].sum().reset_index()
    fig3 = px.bar(first_breakdown, x="first_click_channel", y="purchase_value",
                  title="First Click Channel Revenue", text_auto=True)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    last_breakdown = mart_data.groupby("last_click_channel")["purchase_value"].sum().reset_index()
    fig4 = px.bar(last_breakdown, x="last_click_channel", y="purchase_value",
                  title="Last Click Channel Revenue", text_auto=True)
    st.plotly_chart(fig4, use_container_width=True)


st.subheader("🔴 Live Streaming Events")

@st.cache_data(ttl=1)
def load_live_data():
    return load_live_panel_data(PROJECT_ID, DATASET, STREAM_TABLE,client)

live_data = load_live_data()

if live_data.empty:
    st.info("No live events yet. Run the streaming script.")
else:
    st.dataframe(
        live_data,
        use_container_width=True
    )
