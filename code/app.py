import os
import sys
from pathlib import Path
from datetime import timedelta
# Force headless matplotlib backend for
servers
import matplotlib
matplotlib.use("Agg")
from pyspark.sql import SparkSession,
Window
from pyspark.sql.functions import (
 expr, to_date, col, avg as _avg, max as
_max, count as _count,
 unix_timestamp, lag, when, sum as _sum, lit
)
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import
StandardScaler
from pymongo import MongoClient, errors as
mongo_errors
from reportlab.platypus import
SimpleDocTemplate, Paragraph, Image,
Spacer
from reportlab.lib.styles import
getSampleStyleSheet
# -----------------------
# CONFIG
# -----------------------
DATA_PATH = "dataset.csv" # change
if needed
OUTPUT_DIR = "outputs"
PLOTS_DIR = f"{OUTPUT_DIR}/plots"
REPORTS_DIR =
f"{OUTPUT_DIR}/reports"
os.makedirs(PLOTS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)
# MongoDB (change if needed)
MONGO_URI =
"mongodb://localhost:27017/app-vehicles"
DB_NAME = "vehicle_telematics_3"
# thresholds
OVER_SPEED_LIMIT = 80 # km/h
IDLE_LIMIT = 5 # km/h
HIGH_RPM_LIMIT = 4000 # rpm
# Harsh accel/brake thresholds (m/s^2)
HARSH_ACCEL_MPS2 = 2.5
HARSH_BRAKE_MPS2 = -2.5
# clustering params
CLUSTER_COUNT = 4
# -----------------------
# SPARK INIT
# -----------------------
spark = (

SparkSession.builder.appName("VehicleTele
matics3")
 .master("local[*]")
 .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(" Spark started")
try:
 # Read CSV
 if not Path(DATA_PATH).exists():
 print(f" Data file not found at:
{DATA_PATH}")
 sys.exit(1)
 df = spark.read.csv(DATA_PATH,
header=True, inferSchema=True)
 total_records = df.count()
 print(f" Loaded {total_records} rows
from {DATA_PATH}")
 # Normalize common column names
 if "deviceID" in df.columns and
"VehicleID" not in df.columns:
 df =
df.withColumnRenamed("deviceID",
"VehicleID")
 if "timeStamp" in df.columns and
"timestamp" not in df.columns:
 # we'll still parse into 'timestamp' below
 pass
 # -----------------------
 # Safe timestamp parsing (multiple formats)
 # -----------------------
 # if the dataset column is called 'timeStamp'
or 'timestamp' check both
 timesrc = "timeStamp" if "timeStamp" in
df.columns else ("timestamp" if "timestamp"
in df.columns else None)
 if not timesrc:
 print("⚠ No timestamp column found
(timeStamp or timestamp). Timestamps
required for derived acceleration. Some
features will be limited.")
 # create a dummy timestamp to avoid
crashes in grouping - set to null
 df = df.withColumn("timestamp",
lit(None).cast("timestamp"))
 else:
 ts_expr = (
 "coalesce("
 f"try_to_timestamp({timesrc}, 'ddMM-yyyy HH:mm:ss'),"
 f"try_to_timestamp({timesrc}, 'yyyyMM-dd HH:mm:ss'),"
 f"try_to_timestamp({timesrc}, 'ddMM-yyyy'),"
 f"try_to_timestamp({timesrc},
'MM/dd/yyyy HH:mm:ss'),"
 f"try_to_timestamp({timesrc},
'yyyy/MM/dd HH:mm:ss')"
 ")"
 )
 df = df.withColumn("timestamp",
expr(ts_expr))
 # create Date
 df = df.withColumn("Date",
to_date(col("timestamp")))
 # -----------------------
 # Cleaning & required columns check
 # -----------------------
 required = [c for c in ["VehicleID",
"gps_speed", "rpm", "timestamp"] if c in
df.columns or c == "timestamp"]
 if "VehicleID" not in df.columns or
"gps_speed" not in df.columns or "rpm" not in
df.columns:
 print(" Required columns missing.
Please ensure CSV contains VehicleID,
gps_speed, rpm, timeStamp.")
 spark.stop()
 sys.exit(1)
 # drop nulls in key columns (but keep rows
without timestamp if necessary)
 if "timestamp" in df.columns:
 df = df.dropna(subset=["VehicleID",
"gps_speed", "rpm"], how="any")
 # drop rows where timestamp could not
be parsed
 df =
df.filter(col("timestamp").isNotNull())
 else:
 df = df.dropna(subset=["VehicleID",
"gps_speed", "rpm"], how="any")
 df = df.filter(col("gps_speed") >= 0)
 cleaned = df.count()
 print(f" Cleaned -> {cleaned} rows
remain")
 # -----------------------
 # DAILY AGGREGATION
 # -----------------------
 daily = (
 df.groupBy("VehicleID", "Date")
 .agg(

_avg("gps_speed").alias("Avg_Speed"),

_max("gps_speed").alias("Max_Speed"),
 _avg("rpm").alias("Avg_RPM"),
 _max("rpm").alias("Max_RPM"),
 _count("*").alias("Records"),
 )
 ).orderBy("VehicleID", "Date")
 try:
 daily_pd = daily.toPandas()
 except Exception as e:
 print(" daily -> pandas failed:", e)
 daily_pd = pd.DataFrame()
 # -----------------------
 # Safety analytics per record (harsh
accel/brake derivation)
 # -----------------------
 # If acceleration column exists, use it; else
derive acceleration (m/s^2) from speed and
timestamp per vehicle
 has_accel_col = "acceleration" in
df.columns
 if not has_accel_col:
 # derive acceleration using window lead
of speed & timestamp
 # convert speed km/h -> m/s using factor
 factor = 1000.0 / 3600.0 # km/h -> m/s
 w =
Window.partitionBy("VehicleID").orderBy("ti
mestamp")
 df2 = df.select("VehicleID", "timestamp",
"gps_speed", "rpm").withColumn("gps_mps",
col("gps_speed") * lit(factor))
 df2 = df2.withColumn("prev_speed",
lag("gps_mps").over(w)).withColumn("prev_t
s", lag("timestamp").over(w))
 # time difference in seconds
 df2 = df2.withColumn("dt",
(unix_timestamp(col("timestamp")) -
unix_timestamp(col("prev_ts"))).cast("double
"))
 # avoid zero or null dt
 df2 = df2.withColumn("dt_safe",
when(col("dt") <= 0,
None).otherwise(col("dt")))
 df2 = df2.withColumn("acceleration",
when(col("prev_speed").isNotNull() &
col("dt_safe").isNotNull(),

(col("gps_mps") - col("prev_speed")) /
col("dt_safe")).otherwise(lit(None)))
 else:
 # assume acceleration in m/s^2 already
 df2 = df.select("VehicleID", "timestamp",
"gps_speed", "rpm", "acceleration")
 # attach flags per record
 df_flags = df2.withColumn("is_overspeed",
when(col("gps_speed") >
OVER_SPEED_LIMIT, 1).otherwise(0)) \
 .withColumn("is_idle",
when(col("gps_speed") < IDLE_LIMIT,
1).otherwise(0)) \
 .withColumn("is_high_rpm",
when(col("rpm") > HIGH_RPM_LIMIT,
1).otherwise(0)) \
 .withColumn("is_harsh_accel",
when(col("acceleration").isNotNull() &
(col("acceleration") >
HARSH_ACCEL_MPS2), 1).otherwise(0)) \
 .withColumn("is_harsh_brake",
when(col("acceleration").isNotNull() &
(col("acceleration") <
HARSH_BRAKE_MPS2), 1).otherwise(0))
 # aggregate safety metrics per vehicle per
day and overall
 safety_daily = (
 df_flags.groupBy("VehicleID",
to_date(col("timestamp")).alias("Date"))
 .agg(

_sum("is_overspeed").alias("OverSpeed_Cou
nt"),
 _sum("is_idle").alias("Idle_Count"),

_sum("is_high_rpm").alias("HighRPM_Coun
t"),

_sum("is_harsh_accel").alias("Harsh_Accel_
Count"),

_sum("is_harsh_brake").alias("Harsh_Brake_
Count"),
 _count("*").alias("Records")
 )
 ).orderBy("VehicleID", "Date")
 try:
 safety_daily_pd =
safety_daily.toPandas()
 except Exception as e:
 print(" safety_daily -> pandas failed:",
e)
 safety_daily_pd = pd.DataFrame()
 # Safety summary aggregated per vehicle
(total counts)
 safety_summary = (
 safety_daily.groupBy("VehicleID")
 .agg(

_sum("OverSpeed_Count").alias("OverSpeed
_Count"),

_sum("Idle_Count").alias("Idle_Count"),

_sum("HighRPM_Count").alias("HighRPM_
Count"),

_sum("Harsh_Accel_Count").alias("Harsh_A
ccel_Count"),

_sum("Harsh_Brake_Count").alias("Harsh_B
rake_Count"),

_sum("Records").alias("Total_Records")
 )
 ).orderBy("VehicleID")
 try:
 safety_summary_pd =
safety_summary.toPandas()
 except Exception as e:
 print(" safety_summary -> pandas
failed:", e)
 safety_summary_pd = pd.DataFrame()
 # -----------------------
 # Driver Safety Score (0-100)
 # Lower events -> higher score.
 # We'll compute a simple normalized score:
 # base_score = 100
 # subtract weighted penalties per event
normalized by records
 # weights chosen heuristically
 # -----------------------
 def compute_scores(df):
 # df is pandas safety_summary_pd
 if df.empty:
 return df
 df = df.copy()
 df["Records"] = df.get("Total_Records",
1)
 # avoid division by zero
 df["Records"] = df["Records"].replace(0,
1)
 # normalized event rates per 1000 records
 for c in ["OverSpeed_Count",
"Idle_Count", "HighRPM_Count",
"Harsh_Accel_Count",
"Harsh_Brake_Count"]:
 if c not in df.columns:
 df[c] = 0
 # weights (higher weight -> bigger
penalty)
 w = {
 "OverSpeed_Count": 3.0,
 "Harsh_Accel_Count": 4.0,
 "Harsh_Brake_Count": 4.5,
 "HighRPM_Count": 2.0,
 "Idle_Count": 0.8
 }
 # compute penalty = sum( weight * (count
/ records) )
 df["penalty"] = (
 w["OverSpeed_Count"] *
(df["OverSpeed_Count"] / df["Records"]) +
 w["Harsh_Accel_Count"] *
(df["Harsh_Accel_Count"] / df["Records"]) +
 w["Harsh_Brake_Count"] *
(df["Harsh_Brake_Count"] / df["Records"]) +
 w["HighRPM_Count"] *
(df["HighRPM_Count"] / df["Records"]) +
 w["Idle_Count"] * (df["Idle_Count"] /
df["Records"])
 )
 # scale penalty into 0-100
 # choose factor so typical penalty maps
reasonably: score = 100 - (penalty * 20)
 df["Safety_Score"] = 100 - (df["penalty"]
* 20)
 df["Safety_Score"] =
df["Safety_Score"].clip(lower=0,
upper=100).round(2)
 df.drop(columns=["penalty"],
inplace=True)
 return df
 safety_summary_scored =
compute_scores(safety_summary_pd)
 # Save safety_summary_scored to CSV
 try:

safety_summary_scored.to_csv(f"{REPORTS
_DIR}/safety_summary.csv", index=False)
 print(" Saved safety_summary.csv")
 except Exception as e:
 print(" Could not save
safety_summary.csv:", e)
 # -----------------------
 # Risk Buckets: cluster vehicles by safety
score to label risk levels
 # -----------------------
 risk_cluster_labels = None
 if not safety_summary_scored.empty and
"Safety_Score" in
safety_summary_scored.columns and
len(safety_summary_scored) >= 2:
 try:
 sc = StandardScaler()
 X =
safety_summary_scored[["Safety_Score"]].va
lues
 X_scaled = sc.fit_transform(X)
 k = min(3,
len(safety_summary_scored)) # 3 risk buckets
(low/medium/high) if possible
 km = KMeans(n_clusters=k,
random_state=42, n_init=10)
 labels = km.fit_predict(X_scaled)

safety_summary_scored["Risk_Cluster"] =
labels
 # map cluster numbers to text by avg
score
 cluster_means =
safety_summary_scored.groupby("Risk_Clust
er")["Safety_Score"].mean().sort_values()
 mapping = {}
 # lower safety_score -> higher risk
 ranks = ["High Risk", "Medium Risk",
"Low Risk"]
 for i, cid in
enumerate(cluster_means.index):
 mapping[cid] = ranks[min(i,
len(ranks)-1)]

safety_summary_scored["Risk_Label"] =
safety_summary_scored["Risk_Cluster"].map
(mapping)
 risk_cluster_labels = mapping
 except Exception as e:
 print("⚠ Risk clustering failed:", e)
 else:
 # fallback label
 if not safety_summary_scored.empty:

safety_summary_scored["Risk_Cluster"] = 0

safety_summary_scored["Risk_Label"] =
"Unknown"
 # Save reports: daily_summary.csv,
driving_clusters.csv, safety_summary.csv
already saved
 try:
 if not daily_pd.empty:

daily_pd.to_csv(f"{REPORTS_DIR}/daily_s
ummary.csv", index=False)
 print(" daily_summary.csv saved")
 else:
 pd.DataFrame(columns=["VehicleID",
"Date", "Avg_Speed", "Max_Speed",
"Avg_RPM", "Max_RPM", "Records"]) \

.to_csv(f"{REPORTS_DIR}/daily_summary.
csv", index=False)
 except Exception as e:
 print(" Failed to save
daily_summary:", e)
 # -----------------------
 # Driving clusters (speed vs rpm)
 # -----------------------
 # prepare pandas features for clustering (use
df_flags)
 try:
 df_features_pd =
df_flags.select("gps_speed",
"rpm").dropna().toPandas()
 except Exception as e:
 df_features_pd = pd.DataFrame()
 print("⚠ Could not prepare features for
driving clustering:", e)
 driving_clusters_pd = pd.DataFrame()
 cluster_plot_path = None
 if not df_features_pd.empty and
len(df_features_pd) >= 4:
 try:
 scaler = StandardScaler()
 scaled =
scaler.fit_transform(df_features_pd[["gps_spe
ed", "rpm"]])
 km =
KMeans(n_clusters=CLUSTER_COUNT,
random_state=42, n_init=10)
 df_features_pd["Cluster"] =
km.fit_predict(scaled)
 driving_clusters_pd = df_features_pd
 # save csv

driving_clusters_pd.to_csv(f"{REPORTS_DI
R}/driving_clusters.csv", index=False)
 # plot
 plt.figure(figsize=(7,5))
 sns.scatterplot(x="gps_speed",
y="rpm", hue="Cluster",
data=driving_clusters_pd)
 plt.title("Driving behavior clusters
(speed vs rpm)")
 plt.tight_layout()
 cluster_plot_path =
f"{PLOTS_DIR}/driving_clusters.png"
 plt.savefig(cluster_plot_path)
 plt.close()
 print(" driving_clusters.csv & plot
saved")
 except Exception as e:
 print("⚠ Driving clustering failed:", e)
 else:
 # ensure driving_clusters.csv exists
(empty)

pd.DataFrame(columns=["gps_speed","rpm",
"Cluster"]).to_csv(f"{REPORTS_DIR}/drivin
g_clusters.csv", index=False)
 print("⚠ Not enough data for driving
clustering")
 # -----------------------
 # Correlation heatmap (speed vs rpm)
 # -----------------------
 corr_path = None
 try:
 corr_cols = ["gps_speed", "rpm"]
 corr_df =
df.select(*corr_cols).dropna().toPandas()
 if not corr_df.empty:
 corr = corr_df.corr()
 plt.figure(figsize=(4.5,3.5))
 sns.heatmap(corr, annot=True)
 plt.tight_layout()
 corr_path =
f"{PLOTS_DIR}/correlation_heatmap.png"
 plt.savefig(corr_path)
 plt.close()
 print(" correlation_heatmap
saved")
 except Exception as e:
 print("⚠ Correlation plotting failed:", e)
 # -----------------------
 # Speed trend plots (up to 3 vehicles)
 # -----------------------
 trend_files = []
 try:
 if not daily_pd.empty and "VehicleID" in
daily_pd.columns:
 vehicle_ids =
daily_pd["VehicleID"].unique()[:3]
 for vid in vehicle_ids:
 sub =
daily_pd[daily_pd["VehicleID"] == vid]
 if sub.empty:
 continue
 plt.figure(figsize=(8,4))
 plt.plot(sub["Date"],
sub["Avg_Speed"], marker="o",
label="Avg_Speed")
 plt.plot(sub["Date"],
sub["Max_Speed"], marker="x",
label="Max_Speed")
 plt.title(f"Speed Trend - {vid}")
 plt.xlabel("Date")
 plt.ylabel("Speed")
 plt.legend()
 plt.tight_layout()
 safe_vid = str(vid).replace("/",
"").replace(" ", "")
 path =
f"{PLOTS_DIR}/speed_trend_{safe_vid}.pn
g"
 plt.savefig(path)
 plt.close()
 trend_files.append(path)
 if trend_files:
 print(" Speed trend plots saved")
 except Exception as e:
 print("⚠ Speed trend plotting failed:", e)
 # -----------------------
 # PDF Dashboard (attach existing images)
 # -----------------------
 pdf_path =
f"{REPORTS_DIR}/Telematics_Dashboard.p
df"
 doc = SimpleDocTemplate(pdf_path)
 styles = getSampleStyleSheet()
 elements = [Paragraph("Vehicle Telematics
Analytics Dashboard", styles["Title"]),
Spacer(1,20)]
 if corr_path and Path(corr_path).exists():
 elements += [Image(corr_path,
width=420, height=320), Spacer(1,15)]
 if cluster_plot_path and
Path(cluster_plot_path).exists():
 elements += [Image(cluster_plot_path,
width=420, height=320), Spacer(1,15)]
 if trend_files:
 elements += [Image(trend_files[0],
width=420, height=320), Spacer(1,15)]
 try:
 doc.build(elements)
 print(" PDF dashboard generated")
 except Exception as e:
 print(" PDF build failed:", e)
 # -----------------------
 # Save safety_daily per-day file
 # -----------------------
 try:
 if not safety_daily_pd.empty:

safety_daily_pd.to_csv(f"{REPORTS_DIR}/s
afety_daily.csv", index=False)
 print(" safety_daily.csv saved")
 else:

pd.DataFrame(columns=["VehicleID","Date",
"OverSpeed_Count","Idle_Count","HighRPM
_Count","Harsh_Accel_Count","Harsh_Brake
_Count","Records"])\

.to_csv(f"{REPORTS_DIR}/safety_daily.csv"
, index=False)
 except Exception as e:
 print(" saving safety_daily failed:", e)
 # -----------------------
 # MongoDB Upload (safe)
 # -----------------------
 try:
 client = MongoClient(MONGO_URI,
serverSelectionTimeoutMS=5000)
 client.server_info() # trigger exception if
cannot connect
 db = client[DB_NAME]
 # Upload daily summary
 try:
 if not daily_pd.empty:
 records =
daily_pd.to_dict(orient="records")
 if records:

db.daily_summary.delete_many({}) #
optional: clear old

db.daily_summary.insert_many(records)
 print(f" Inserted
{len(records)} daily_summary records into
MongoDB")
 except Exception as e:
 print("⚠ daily_summary upload
failed:", e)
 # Upload safety summary
 try:
 if not safety_summary_scored.empty:
 rec =
safety_summary_scored.to_dict(orient="recor
ds")
 if rec:

db.safety_summary.delete_many({})

db.safety_summary.insert_many(rec)
 print(f" Inserted {len(rec)}
safety_summary records into MongoDB")
 except Exception as e:
