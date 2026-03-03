import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
from db.db_connection import get_connection

st.set_page_config(page_title="Smart Logistics Dashboard", layout="wide")

st.title("Smart Logistics Analytics Dashboard")

# =====================================================
# FETCH DATA FROM DATABASE
# =====================================================

def load_data():
    conn = get_connection()
    shipments = pd.read_sql("SELECT * FROM shipments", conn)
    couriers = pd.read_sql("SELECT * FROM courier_staff", conn)
    tracking = pd.read_sql("SELECT * FROM shipment_tracking", conn)
    routes = pd.read_sql("SELECT * FROM routes", conn)
    warehouses = pd.read_sql("SELECT * FROM warehouses", conn)
    costs = pd.read_sql("SELECT * FROM costs", conn)
    conn.close()

    return shipments, couriers, tracking, routes, warehouses, costs


shipments, couriers, tracking, routes, warehouses, costs = load_data()

# =====================================================
# KPI SECTION
# =====================================================

st.subheader("Key Performance Indicators")

col1, col2, col3, col4, col5 = st.columns(5)

total_shipments = len(shipments)
delivered_shipments = len(shipments[shipments["status"] == "Delivered"])
in_transit_shipments = len(shipments[shipments["status"] == "In Transit"])
total_cost = costs[["fuel_cost", "labor_cost", "misc_cost"]].sum().sum()
avg_rating = couriers["rating"].mean()

col1.metric("Total Shipments", total_shipments)
col2.metric("Delivered", delivered_shipments)
col3.metric("In Transit", in_transit_shipments)
col4.metric("Total Operational Cost", f"{total_cost:,.2f}")
col5.metric("Avg Courier Rating", f"{avg_rating:.2f}" if not pd.isna(avg_rating) else "N/A")

st.divider()

# =====================================================
# SHIPMENT STATUS CHART
# =====================================================

st.subheader("Shipment Status Distribution")

status_counts = shipments["status"].value_counts().reset_index()
status_counts.columns = ["Status", "Count"]

fig_status = px.bar(status_counts, x="Status", y="Count",
                    color="Status", title="Shipments by Status")

st.plotly_chart(fig_status, use_container_width=True)

# =====================================================
# SHIPMENTS BY ORIGIN
# =====================================================

st.subheader("Shipments by Origin")

origin_counts = shipments["origin"].value_counts().reset_index()
origin_counts.columns = ["Origin", "Count"]

fig_origin = px.bar(origin_counts, x="Origin", y="Count",
                    title="Shipments per Origin City")

st.plotly_chart(fig_origin, use_container_width=True)

# =====================================================
# COST BREAKDOWN
# =====================================================

st.subheader("Cost Breakdown")

if not costs.empty:
    cost_sum = costs[["fuel_cost", "labor_cost", "misc_cost"]].sum()
    cost_df = cost_sum.reset_index()
    cost_df.columns = ["Cost Type", "Total Amount"]

    fig_cost = px.pie(cost_df, names="Cost Type", values="Total Amount",
                      title="Operational Cost Distribution")

    st.plotly_chart(fig_cost, use_container_width=True)
else:
    st.info("No cost data available.")

# =====================================================
# COURIER RATING DISTRIBUTION
# =====================================================

st.subheader("Courier Rating Distribution")

if not couriers.empty:
    fig_rating = px.histogram(couriers, x="rating",
                              nbins=5,
                              title="Courier Rating Histogram")

    st.plotly_chart(fig_rating, use_container_width=True)
else:
    st.info("No courier data available.")

# =====================================================
# ROUTE DISTANCE ANALYSIS
# =====================================================

st.subheader("Route Distance Analysis")

if not routes.empty:
    fig_route = px.scatter(routes,
                           x="distance_km",
                           y="avg_time_hours",
                           hover_data=["origin", "destination"],
                           title="Distance vs Average Travel Time")

    st.plotly_chart(fig_route, use_container_width=True)
else:
    st.info("No route data available.")

# =====================================================
# DATA TABLES SECTION
# =====================================================

st.divider()
st.subheader("Data Tables")

option = st.selectbox("Select Table to View",
                      ["Shipments", "Couriers", "Tracking",
                       "Routes", "Warehouses", "Costs"])

if option == "Shipments":
    st.dataframe(shipments)

elif option == "Couriers":
    st.dataframe(couriers)

elif option == "Tracking":
    st.dataframe(tracking)

elif option == "Routes":
    st.dataframe(routes)

elif option == "Warehouses":
    st.dataframe(warehouses)

elif option == "Costs":
    st.dataframe(costs)