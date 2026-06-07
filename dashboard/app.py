import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

st.set_page_config(
    page_title="Payment Intelligence Platform",
    layout="wide"
)

st.title("💳 Payment Intelligence Platform")
st.markdown("Real-Time Payment Analytics Dashboard")

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
# KPI Cards
# --------------------------------------------------

total_revenue = payment_df["total_amount"].sum()
total_transactions = payment_df["transaction_count"].sum()

high_risk_transactions = risk_df.loc[
    risk_df["risk_level"] == "HIGH",
    "transaction_count"
].sum()

cities_covered = city_df["city"].nunique()

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total Revenue",
    f"${total_revenue:,.2f}"
)

col2.metric(
    "Transactions",
    f"{int(total_transactions):,}"
)

col3.metric(
    "High Risk Txns",
    f"{int(high_risk_transactions):,}"
)

col4.metric(
    "Top Cities",
    f"{cities_covered}"
)

st.divider()

# --------------------------------------------------
# Payment Distribution
# --------------------------------------------------

st.subheader("Payment Type Distribution")

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
# Risk Distribution
# --------------------------------------------------

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

# --------------------------------------------------
# Top Cities
# --------------------------------------------------

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