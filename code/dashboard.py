Streamlit dashboard for Vehicle Telematics
Analytics 3.0
Displays:
- Overview stats
- Speed trends per vehicle
- Driving clusters
- Safety analytics + safety scores and risk
buckets
- Option to load live data from MongoDB
"""
import streamlit as st
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pymongo import MongoClient, errors as
mongo_errors
# Paths
REPORTS_DIR = Path("outputs/reports")
PLOTS_DIR = Path("outputs/plots")
DAILY_SUMMARY = REPORTS_DIR /
"daily_summary.csv"
DRIVING_CLUSTERS = REPORTS_DIR /
"driving_clusters.csv"
SAFETY_SUMMARY = REPORTS_DIR /
"safety_summary.csv"
SAFETY_DAILY = REPORTS_DIR /
"safety_daily.csv"
PDF_REPORT = REPORTS_DIR /
"Telematics_Dashboard.pdf"
# MongoDB config (if you want to toggle live
fetch)
MONGO_URI =
"mongodb://localhost:27017/app-vehicles"
DB_NAME = "vehicle_telematics_3"
st.set_page_config(page_title="Vehicle
Telematics Dashboard 3.0", layout="wide")
st.title(" Vehicle Telematics Dashboard
3.0")
st.markdown("Spark analytics + Safety
Analytics · CSV reports + optional MongoDB
live fetch")
# helper loader
@st.cache_data
def load_csv_safe(path):
 p = Path(path)
 if p.exists():
 try:
 return pd.read_csv(p)
 except Exception as e:
 st.error(f"Failed to read {path}: {e}")
 return pd.DataFrame()
 return pd.DataFrame()
# Option: live DB fetch
use_db = st.sidebar.checkbox("Fetch live data
from MongoDB (if available)", value=False)
daily_df = pd.DataFrame()
cluster_df = pd.DataFrame()
safety_df = pd.DataFrame()
safety_daily_df = pd.DataFrame()
if use_db:
 try:
 client = MongoClient(MONGO_URI,
serverSelectionTimeoutMS=3000)
 client.server_info()
 db = client[DB_NAME]
 # fetch collections as dataframes
 daily_df =
pd.DataFrame(list(db.daily_summary.find()))
 cluster_df =
pd.DataFrame(list(db.driving_clusters.find()))
 safety_df =
pd.DataFrame(list(db.safety_summary.find())
)
 # drop Mongo _id if present
 for d in [daily_df, cluster_df, safety_df]:
 if "_id" in d.columns:
 d.drop(columns=["_id"],
inplace=True)
 st.sidebar.success("Live DB fetch
succeeded")
 except Exception as e:
 st.sidebar.error(f"MongoDB fetch failed:
{e}")
 # fallback to CSVs
 daily_df =
load_csv_safe(DAILY_SUMMARY)
 cluster_df =
load_csv_safe(DRIVING_CLUSTERS)
 safety_df =
load_csv_safe(SAFETY_SUMMARY)
else:
 daily_df =
load_csv_safe(DAILY_SUMMARY)
 cluster_df =
load_csv_safe(DRIVING_CLUSTERS)
 safety_df =
load_csv_safe(SAFETY_SUMMARY)
 safety_daily_df =
load_csv_safe(SAFETY_DAILY)
# Overview metrics
col1, col2, col3 = st.columns(3)
num_vehicles =
int(daily_df["VehicleID"].nunique()) if not
daily_df.empty and "VehicleID" in
daily_df.columns else 0
total_records = int(daily_df.shape[0]) if not
daily_df.empty else 0
avg_safety_score =
safety_df["Safety_Score"].mean() if not
safety_df.empty and "Safety_Score" in
safety_df.columns else np.nan
col1.metric("Vehicles", num_vehicles)
col2.metric("Daily rows", total_records)
col3.metric("Avg Safety Score",
f"{avg_safety_score:.2f}" if not
np.isnan(avg_safety_score) else "N/A")
st.markdown("---")
# Vehicle selector and speed trends
st.header(" Speed Trends")
if not daily_df.empty and "VehicleID" in
daily_df.columns:
 vehicles =
list(daily_df["VehicleID"].unique())
 sel = st.selectbox("Select Vehicle",
vehicles)
 sub = daily_df[daily_df["VehicleID"] ==
sel].sort_values("Date")
 if not sub.empty:
 fig, ax = plt.subplots(figsize=(8,4))
 ax.plot(sub["Date"], sub["Avg_Speed"],
marker="o", label="Avg_Speed")
 ax.plot(sub["Date"], sub["Max_Speed"],
marker="x", label="Max_Speed")
 ax.set_xlabel("Date");
ax.set_ylabel("Speed (km/h)")
 ax.legend(); plt.xticks(rotation=30)
 st.pyplot(fig)
 else:
 st.info("No daily data for selected
vehicle")
else:
 st.info("No daily_summary data (run
analytics pipeline first)")
st.markdown("---")
# Driving clusters
st.header(" Driving Clusters (Speed vs
RPM)")
if not cluster_df.empty:
 st.write("Cluster scatter (speed vs rpm)")
 try:
 fig, ax = plt.subplots(figsize=(7,5))
 if "Cluster" in cluster_df.columns:
 sns.scatterplot(data=cluster_df,
x="gps_speed", y="rpm", hue="Cluster",
ax=ax)
 else:
 sns.scatterplot(data=cluster_df,
x="gps_speed", y="rpm", ax=ax)
 ax.set_xlabel("Speed");
ax.set_ylabel("RPM")
 st.pyplot(fig)
 except Exception as e:
 st.error("Failed to plot clusters: " +
str(e))
 st.dataframe(cluster_df.head(200))
else:
 st.info("No driving_clusters data")
st.markdown("---")
# Safety analytics
st.header("🛡 Safety Analytics")
if not safety_df.empty:
 st.subheader("Safety summary per
vehicle")

st.dataframe(safety_df.sort_values("Safety_S
core",
ascending=True).reset_index(drop=True))
 # Top unsafe vehicles
 st.subheader("Top risky vehicles")
 top_risky =
safety_df.sort_values("Safety_Score").head(1
0)

st.bar_chart(top_risky.set_index("VehicleID")
["Safety_Score"])
 # Bar comparison of event counts
 st.subheader("Event comparison (overspeed, harsh accel/brake, high RPM, idle)")
 metrics = [c for c in
["OverSpeed_Count","Harsh_Accel_Count","
Harsh_Brake_Count","HighRPM_Count","Idl
e_Count"] if c in safety_df.columns]
 if metrics:
 melted =
safety_df.melt(id_vars=["VehicleID"],
value_vars=metrics, var_name="Event",
value_name="Count")
 fig, ax = plt.subplots(figsize=(10,5))
 sns.barplot(data=melted, x="VehicleID",
y="Count", hue="Event", ax=ax)
 ax.set_xticklabels(ax.get_xticklabels(),
rotation=45)
 st.pyplot(fig)
 else:
 st.info("No event columns present in
safety summary")
 # Risk buckets pie
 if "Risk_Label" in safety_df.columns:
 st.subheader("Risk buckets distribution")
 counts =
safety_df["Risk_Label"].value_counts()
 st.write(counts)
 fig, ax = plt.subplots()
 counts.plot.pie(autopct="%1.1f%%",
ax=ax)
 ax.set_ylabel("")
 st.pyplot(fig)
 # Show per-day safety events if available
 if not safety_daily_df.empty:
 st.subheader("Safety events over time
(sample)")
 # group by date (sum)
 per_date =
safety_daily_df.groupby("Date")[["OverSpee
d_Count","Harsh_Accel_Count","Harsh_Bra
ke_Count"]].sum().reset_index()
 if not per_date.empty:
 fig, ax = plt.subplots(figsize=(10,4))
 ax.plot(per_date["Date"],
per_date["OverSpeed_Count"],
label="Overspeed")
 if "Harsh_Accel_Count" in
per_date.columns:
 ax.plot(per_date["Date"],
per_date["Harsh_Accel_Count"],
label="Harsh Accel")
 if "Harsh_Brake_Count" in
per_date.columns:
 ax.plot(per_date["Date"],
per_date["Harsh_Brake_Count"],
label="Harsh Brake")
 ax.legend(); plt.xticks(rotation=30)
 st.pyplot(fig)
else:
 st.info("No safety_summary data (run
analytics pipeline or toggle MongoDB)")
st.markdown("---")
# Show saved plots
st.header("🖼 Saved plots")
if PLOTS_DIR.exists():
 plot_files =
sorted(list(PLOTS_DIR.glob("*.png")))
 if plot_files:
 for p in plot_files:
 with st.expander(p.name):
 st.image(str(p))
 else:
 st.info("No plots found in outputs/plots")
else:
 st.info("No plots folder found. Run
analytics first.")
st.sidebar.markdown("---")
st.sidebar.markdown("Analytics pipeline
outputs are read from outputs/reports and
outputs/plots.")
st.sidebar.markdown("Use the checkbox to
fetch data directly from MongoDB if you
want a live view.")
st.success("Dashboard loaded ")
