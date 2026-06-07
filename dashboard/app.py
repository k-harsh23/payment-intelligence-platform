import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# --------------------------------------------------
# Page Config
# --------------------------------------------------

st.set_page_config(
    page_title="Payment Intelligence Platform",
    layout="wide"
)

# --------------------------------------------------
# Helper Functions
# --------------------------------------------------

def format_currency(value):
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.2f}K"
    return f"${value:.2f}"

# --------------------------------------------------
# PostgreSQL Connection
# --------------------------------------------------

engine = create_engine(
    "postgresql+psycopg2://admin:admin123@payment-postgres:5432/payment_db"
)

# --------------------------------------------------
# Latest Payment Snapshot
# --------------------------------------------------

payment_df = pd.read_sql(
    """
    SELECT *
    FROM payment_metrics_history
    WHERE snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM payment_metrics_history
    )
    """,
    engine
)

# --------------------------------------------------
# Previous Payment Snapshot
# --------------------------------------------------

previous_payment_df = pd.read_sql(
    """
    SELECT *
    FROM payment_metrics_history
    WHERE snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM payment_metrics_history
        WHERE snapshot_time <
        (
            SELECT MAX(snapshot_time)
            FROM payment_metrics_history
        )
    )
    """,
    engine
)

# --------------------------------------------------
# Latest Risk Snapshot
# --------------------------------------------------

risk_df = pd.read_sql(
    """
    SELECT *
    FROM risk_metrics_history
    WHERE snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM risk_metrics_history
    )
    """,
    engine
)

# --------------------------------------------------
# Previous Risk Snapshot
# --------------------------------------------------

previous_risk_df = pd.read_sql(
    """
    SELECT *
    FROM risk_metrics_history
    WHERE snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM risk_metrics_history
        WHERE snapshot_time <
        (
            SELECT MAX(snapshot_time)
            FROM risk_metrics_history
        )
    )
    """,
    engine
)

# --------------------------------------------------
# Latest City Snapshot
# --------------------------------------------------

city_df = pd.read_sql(
    """
    SELECT *
    FROM city_metrics_history
    WHERE snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM city_metrics_history
    )
    """,
    engine
)

# --------------------------------------------------
# Historical Revenue Trend
# --------------------------------------------------

revenue_trend_df = pd.read_sql(
    """
    SELECT
        snapshot_time,
        SUM(total_amount) AS revenue
    FROM payment_metrics_history
    GROUP BY snapshot_time
    ORDER BY snapshot_time
    """,
    engine
)

# --------------------------------------------------
# Historical Transaction Trend
# --------------------------------------------------

transaction_trend_df = pd.read_sql(
    """
    SELECT
        snapshot_time,
        SUM(transaction_count) AS transactions
    FROM payment_metrics_history
    GROUP BY snapshot_time
    ORDER BY snapshot_time
    """,
    engine
)

# --------------------------------------------------
# Historical High Risk Trend
# --------------------------------------------------

high_risk_trend_df = pd.read_sql(
    """
    SELECT
        snapshot_time,
        transaction_count
    FROM risk_metrics_history
    WHERE risk_level = 'HIGH'
    ORDER BY snapshot_time
    """,
    engine
)

# --------------------------------------------------
# Header
# --------------------------------------------------

st.title("💳 Payment Intelligence Platform")
st.markdown("Real-Time Payment Analytics Dashboard")

latest_update = payment_df["snapshot_time"].max()

st.caption(
    f"Last Updated: {latest_update}"
)

# --------------------------------------------------
# KPI Calculations
# --------------------------------------------------

total_revenue = payment_df["total_amount"].sum()

previous_revenue = (
    previous_payment_df["total_amount"].sum()
    if not previous_payment_df.empty
    else 0
)

revenue_delta = total_revenue - previous_revenue

total_transactions = (
    payment_df["transaction_count"].sum()
)

previous_transactions = (
    previous_payment_df["transaction_count"].sum()
    if not previous_payment_df.empty
    else 0
)

transaction_delta = (
    total_transactions -
    previous_transactions
)

high_risk_transactions = risk_df.loc[
    risk_df["risk_level"] == "HIGH",
    "transaction_count"
].sum()

previous_high_risk = previous_risk_df.loc[
    previous_risk_df["risk_level"] == "HIGH",
    "transaction_count"
].sum()

high_risk_delta = (
    high_risk_transactions -
    previous_high_risk
)

cities_covered = city_df["city"].nunique()

# --------------------------------------------------
# KPI Cards
# --------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Revenue",
    format_currency(total_revenue),
    format_currency(revenue_delta)
)

col2.metric(
    "Transactions",
    f"{int(total_transactions):,}",
    f"{int(transaction_delta):,}"
)

col3.metric(
    "High Risk Txns",
    f"{int(high_risk_transactions):,}",
    f"{int(high_risk_delta):,}"
)

col4.metric(
    "Top Cities",
    f"{cities_covered}"
)

st.divider()

# --------------------------------------------------
# Trend Charts
# --------------------------------------------------

col1, col2 = st.columns(2)

with col1:

    st.subheader("Revenue Trend")

    revenue_fig = px.line(
        revenue_trend_df,
        x="snapshot_time",
        y="revenue"
    )

    st.plotly_chart(
        revenue_fig,
        use_container_width=True
    )

with col2:

    st.subheader("Transaction Trend")

    transaction_fig = px.line(
        transaction_trend_df,
        x="snapshot_time",
        y="transactions"
    )

    st.plotly_chart(
        transaction_fig,
        use_container_width=True
    )

# --------------------------------------------------
# Second Row
# --------------------------------------------------

col1, col2 = st.columns(2)

with col1:

    st.subheader("High Risk Trend")

    risk_trend_fig = px.line(
        high_risk_trend_df,
        x="snapshot_time",
        y="transaction_count"
    )

    st.plotly_chart(
        risk_trend_fig,
        use_container_width=True
    )

with col2:

    st.subheader("Payment Distribution")

    payment_fig = px.bar(
        payment_df,
        x="payment_type",
        y="transaction_count"
    )

    st.plotly_chart(
        payment_fig,
        use_container_width=True
    )

# --------------------------------------------------
# Third Row
# --------------------------------------------------

col1, col2 = st.columns(2)

with col1:

    st.subheader("Risk Distribution")

    risk_fig = px.pie(
        risk_df,
        names="risk_level",
        values="transaction_count"
    )

    st.plotly_chart(
        risk_fig,
        use_container_width=True
    )

with col2:

    st.subheader("Top Cities")

    top_city_df = city_df.sort_values(
        "transaction_count",
        ascending=False
    ).head(10)

    city_fig = px.bar(
        top_city_df,
        x="city",
        y="transaction_count"
    )

    st.plotly_chart(
        city_fig,
        use_container_width=True
    )

# --------------------------------------------------
# Dashboard Health
# --------------------------------------------------

st.divider()

st.subheader("Platform Health")

health_col1, health_col2, health_col3 = st.columns(3)

health_col1.metric(
    "Payment Types",
    payment_df["payment_type"].nunique()
)

health_col2.metric(
    "Risk Levels",
    risk_df["risk_level"].nunique()
)

health_col3.metric(
    "Cities Tracked",
    city_df["city"].nunique()
)

# --------------------------------------------------
# Footer
# --------------------------------------------------

st.divider()

st.caption(
    "Payment Intelligence Platform v1.0 | Data Source: PostgreSQL | Refresh via Dashboard Reload"
)