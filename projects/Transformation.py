from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, when
from pyspark.sql.window import Window

# --------------------------------------------
# 🔹 Step 1: Initialize Spark Session
# --------------------------------------------
print("[INFO] Initializing Spark session...")

custom_tmp_dir = "E:/DataEngineering/SparkTemp"
spark = SparkSession.builder \
    .appName("Read Cleaned IPL Tables") \
    .config("spark.local.dir", custom_tmp_dir) \
    .config("spark.files.overwrite", "false") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------
# 🔹 Step 2: Read Cleaned CSV Tables into DataFrames
# --------------------------------------------
base_path = "file:///E:/DataEngineering/Ipl-Analytics/cleaned-data/"
table_names = ["Ball_By_Ball", "Match", "Player", "Player_Match", "Team"]
dataframes = {}

print("--------------------------------------------------------------------------------")
print("[INFO] Reading cleaned data tables...")

for table in table_names:
    full_path = base_path + table
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(full_path)
        df.createOrReplaceTempView(table)
        dataframes[table] = df
        print(f"[SUCCESS] Table loaded and view created: {table}")
    except Exception as e:
        print(f"[ERROR] Failed to load table {table}: {e}")

# --------------------------------------------
# 🔹 Step 3: Filter Valid Deliveries (No Wides and No Noballs)
# --------------------------------------------
df_ball_by_ball = dataframes["Ball_By_Ball"]
print("--------------------------------------------------------------------------------")
df_ball_by_ball.printSchema()

total_deliveries = df_ball_by_ball.count()
print(f"[INFO] Total Deliveries: {total_deliveries}")

df_valid_deliveries = df_ball_by_ball.filter((col("wides") == 0) & (col("noballs") == 0))
valid_delivery_count = df_valid_deliveries.count()
print(f"[INFO] Valid Deliveries (No wides/noballs): {valid_delivery_count}")

# --------------------------------------------
# 🔹 Step 4: Total & Average Runs per Match and Innings
# --------------------------------------------
print("--------------------------------------------------------------------------------")
print("[INFO] Calculating total and average runs per match and innings...")

total_and_avg_runs = df_valid_deliveries.groupBy("match_id", "innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("average_runs")
).orderBy("total_runs", ascending=False)

total_and_avg_runs.show()

# --------------------------------------------
# 🔹 Step 5: Running Total of Runs per Innings Using Window
# --------------------------------------------
print("--------------------------------------------------------------------------------")
print("[INFO] Calculating running total of runs per innings...")

window_spec = Window.partitionBy("match_id", "innings_no").orderBy("over_id")

df_valid_deliveries_with_runningtotal = df_valid_deliveries.withColumn(
    "running_total_runs",
    sum("runs_scored").over(window_spec)
)

df_valid_deliveries_with_runningtotal.select("match_id", "over_id", "running_total_runs") \
    .dropDuplicates() \
    .orderBy("running_total_runs", ascending=False) \
    .show(10)

# --------------------------------------------
# 🔹 Step 6: Orange Cap - Top Run Scorer per Season
# --------------------------------------------
print("--------------------------------------------------------------------------------")
print("[INFO] Calculating top run scorers (Orange Cap) across seasons...")

df_orangecap = df_valid_deliveries.groupBy("striker", "season").agg(
    sum("runs_scored").alias("total_runs")
)

# Join with Player table to get player name
df_Player = dataframes["Player"]

df_orangecap_named = df_orangecap.join(
    df_Player,
    df_orangecap["striker"] == df_Player["player_id"],
    how="left"
).select(
    "striker", "player_name", "season", "total_runs"
)

# Display top 10 orange cap players
df_orangecap_named.orderBy(col("total_runs").desc()).show(10)


#When was the ipl first match played and where
df_Match=dataframes["Match"]
df_valid_deliveries.alias("vdl") \
    .join(dataframes["Match"].alias("mt"), col("vdl.match_id") == col("mt.match_id"), how="left") \
    .select("mt.match_date", "mt.venue_name") \
    .orderBy("mt.match_date",ascending=False) \
    .show(1)

#Conditional Column: Flag for high impact ball (either a wicket or more than 6 runs including extras)

df_high_impact_ball = df_valid_deliveries.withColumn(
    "high_impact",
    when(
        ((col("extra_runs") + col("runs_scored")) > 6) | (col("bowler_wicket") == '0'),
        True
    ).otherwise(False)
)

df_high_impact_ball.select(
    "match_id",
    "striker",
    (col("extra_runs") + col("runs_scored")).alias("total_runs_on_high_impact_ball"),
    "high_impact"
).orderBy(col("total_runs_on_high_impact_ball"),ascending=False).show(10)