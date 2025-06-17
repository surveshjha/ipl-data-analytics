from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, when,regexp_replace,lower
from pyspark.sql.window import Window

# -----------------------------------------------------------
# Step 1: Initialize Spark Session
# -----------------------------------------------------------
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

# -----------------------------------------------------------
# Step 2: Read All Cleaned CSV Tables into a Dictionary
# -----------------------------------------------------------
base_path = "file:///E:/DataEngineering/Ipl-Analytics/cleaned-data/"
table_names = ["Ball_By_Ball", "Match", "Player", "Player_Match", "Team"]
dataframes = {}

print("------------------------------------------------------------")
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

# -----------------------------------------------------------
# Step 3: Filter to Only Valid Deliveries (No wides/noballs)
# -----------------------------------------------------------
df_ball_by_ball = dataframes["Ball_By_Ball"]
print("------------------------------------------------------------")
print("[INFO] Schema of Ball_By_Ball table:")
df_ball_by_ball.printSchema()

total_deliveries = df_ball_by_ball.count()
print(f"[INFO] Total Deliveries: {total_deliveries}")

df_valid_deliveries = df_ball_by_ball.filter(
    (col("wides") == 0) & (col("noballs") == 0)
)
valid_delivery_count = df_valid_deliveries.count()
print(f"[INFO] Valid Deliveries (no wides or no-balls): {valid_delivery_count}")

# -----------------------------------------------------------
# Step 4: Total and Average Runs per Match and Innings
# -----------------------------------------------------------
print("------------------------------------------------------------")
print("[INFO] Calculating total and average runs per match/innings...")

total_and_avg_runs = df_valid_deliveries.groupBy("match_id", "innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("average_runs")
).orderBy("total_runs", ascending=False)

total_and_avg_runs.show()

# -----------------------------------------------------------
# Step 5: Running Total of Runs per Innings using Window
# -----------------------------------------------------------
print("------------------------------------------------------------")
print("[INFO] Calculating running total of runs per innings...")

window_spec = Window.partitionBy("match_id", "innings_no").orderBy("over_id")

df_valid_deliveries_with_runningtotal = df_valid_deliveries.withColumn(
    "running_total_runs",
    sum("runs_scored").over(window_spec)
)

df_valid_deliveries_with_runningtotal.select(
    "match_id", "over_id", "running_total_runs"
).dropDuplicates().orderBy("running_total_runs", ascending=False).show(10)

# -----------------------------------------------------------
# Step 6: Orange Cap – Top Run Scorers Per Season
# -----------------------------------------------------------
print("------------------------------------------------------------")
print("[INFO] Calculating top run scorers per season (Orange Cap)...")

df_orangecap = df_valid_deliveries.groupBy("striker", "season").agg(
    sum("runs_scored").alias("total_runs")
)

# Join with Player table to fetch player names
df_player = dataframes["Player"]

df_orangecap_named = df_orangecap.join(
    df_player,
    df_orangecap["striker"] == df_player["player_id"],
    how="left"
).select(
    "striker", "player_name", "season", "total_runs"
)

print("[INFO] Top 10 Orange Cap candidates across all seasons:")
df_orangecap_named.orderBy(col("total_runs").desc()).show(10)

# -----------------------------------------------------------
# Step 7: When and Where Was the First IPL Match Played?
# -----------------------------------------------------------
print("------------------------------------------------------------")
print("[INFO] Displaying date and venue of first IPL match...")

df_match = dataframes["Match"]

df_valid_deliveries.alias("vdl") \
    .join(df_match.alias("mt"), col("vdl.match_id") == col("mt.match_id"), how="left") \
    .select("mt.match_date", "mt.venue_name") \
    .orderBy("mt.match_date", ascending=True) \
    .show(1)

# -----------------------------------------------------------
# Step 8: Identify High Impact Balls
# -----------------------------------------------------------
print("------------------------------------------------------------")
print("[INFO] Flagging high impact deliveries (wickets or >6 total runs)...")

df_high_impact_ball = df_valid_deliveries.withColumn(
    "high_impact",
    when(
        (col("extra_runs") + col("runs_scored") > 6) | (col("bowler_wicket") == '0'),
        True
    ).otherwise(False)
)

df_high_impact_ball.select(
    "match_id",
    "striker",
    (col("extra_runs") + col("runs_scored")).alias("total_runs_on_high_impact_ball"),
    "high_impact"
).orderBy(col("total_runs_on_high_impact_ball").desc()).show(10)

# -----------------------------------------------------------
# Enriching Match Dataframe with date time columns
# -----------------------------------------------------------

from pyspark.sql.functions import year,month,dayofmonth,when

#Extracting year,month,dayofmonth from the match date for detailed time based analysis

df_Match=dataframes['Match']
df_Match=df_Match.withColumn("year",year("match_date_cleaned"))
df_Match=df_Match.withColumn("month",month("match_date_cleaned"))
df_Match=df_Match.withColumn("day",dayofmonth("match_date_cleaned"))

# Win margin clolumn, High, Medium, Low 


df_Match = df_Match.withColumn(
    "win_margin_category",
    when(col("win_margin") >= 100, "High")
    .when((col("win_margin") >= 50) & (col("win_margin") < 100), "Medium")
    .otherwise("Low")
)
df_Match.show(10)

# -----------------------------------------------------------
# Cleaning Player Dataframe
# -----------------------------------------------------------

df_Player=dataframes['Player']

#Normalize and clean player names
print("------------------------------------------------------------")
print("[INFO] Normalizing and cleaning Player data")

df_Player=df_Player.withColumn("player_name",lower(regexp_replace("player_name","[^a-zA-Z0-9]",""))) \
                    .withColumn("batting_hand",lower(regexp_replace("batting_hand","[^a-zA-Z0-9]",""))) \
                    .withColumn("bowling_skill",lower(regexp_replace("bowling_skill","[^a-zA-Z0-9]","")))

df_Player.show(10)          

df_Player=df_Player.withColumn("batting_style",when( col("batting_hand").contains("left"),"Left-Handed" ).otherwise('Right-Handed') )
df_Player.show(10)