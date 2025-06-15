from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType)
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, lower, initcap, regexp_replace, col

# ------------------------------------------------------------------------------------------------------------------------
# ⚙️ 1. Spark Session Initialization
# ------------------------------------------------------------------------------------------------------------------------

print("[INFO] Initializing Spark session...")

custom_tmp_dir = "E:/DataEngineering/SparkTemp"  # Custom temp directory

spark = SparkSession.builder \
    .appName("IPL Data Analysis SPARK") \
    .config("spark.local.dir", custom_tmp_dir) \
    .config("spark.files.overwrite", "false") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", 
            "E:/DataEngineering/keys/data-with-jha-0e1c0496e4ff.json") \
    .config("spark.driver.extraClassPath", 
            "E:\\DataEngineering\\Ipl-Analytics\\jars\\gcs-connector-hadoop3-2.2.5-shaded.jar") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

print("[INFO] Spark session initialized successfully.")

# ------------------------------------------------------------------------------------------------------------------------
# 📄 2. Schema Definitions
# ------------------------------------------------------------------------------------------------------------------------

print("[INFO] Defining custom schemas for all input datasets...")

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", IntegerType(), True),
])

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),  # Year as IntegerType
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True),
])

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
])

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(20, 10), True),  # Adjust precision/scale as needed
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True),
])

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True),
])
print("[INFO] Schema definitions completed.")

# ------------------------------------------------------------------------------------------------------------------------
# 📥 3. Load DataFrames from GCS
# ------------------------------------------------------------------------------------------------------------------------

print("[INFO] Starting to load datasets from GCS...")

try:
    print("[INFO] Loading Ball_By_Ball.csv...")
    df_ball_by_ball = spark.read.schema(ball_by_ball_schema).option("header", "true").csv("gs://ipl-data-project/Ball_By_Ball.csv")
    print("[SUCCESS] Ball_By_Ball.csv loaded. Row count:", df_ball_by_ball.count())

    print("[INFO] Loading Match.csv...")
    df_match = spark.read.schema(match_schema).option("header", "true").csv("gs://ipl-data-project/Match.csv")
    print("[SUCCESS] Match.csv loaded. Row count:", df_match.count())

    print("[INFO] Loading Player.csv...")
    df_player = spark.read.schema(player_schema).option("header", "true").csv("gs://ipl-data-project/Player.csv")
    print("[SUCCESS] Player.csv loaded. Row count:", df_player.count())

    print("[INFO] Loading Player_match.csv...")
    df_player_match = spark.read.schema(player_match_schema).option("header", "true").csv("gs://ipl-data-project/Player_match.csv")
    print("[SUCCESS] Player_match.csv loaded. Row count:", df_player_match.count())

    print("[INFO] Loading Team.csv...")
    df_team = spark.read.schema(team_schema).option("header", "true").csv("gs://ipl-data-project/Team.csv")
    print("[SUCCESS] Team.csv loaded. Row count:", df_team.count())

except Exception as e:
    print("[ERROR] Failed during data loading:", e)

# ------------------------------------------------------------------------------------------------------------------------
# 🧾 4. Schema Validation
# ------------------------------------------------------------------------------------------------------------------------

print("\n[INFO] Printing all schemas for verification...")

df_ball_by_ball.printSchema()
df_match.printSchema()
df_player.printSchema()
df_player_match.printSchema()
df_team.printSchema()

# ------------------------------------------------------------------------------------------------------------------------
# ✅ Final Step
# ------------------------------------------------------------------------------------------------------------------------

print("\n[INFO] All datasets loaded and schemas verified.")
print("[COMPLETED] IPL Data Analysis environment is ready.")

# ------------------------------------------------------------------------------------------------------------------------
# DATA CLEANING
# ------------------------------------------------------------------------------------------------------------------------

ball_by_ball_count=df_ball_by_ball.count()
print("Ball By Ball Before Record Counts:")
print(ball_by_ball_count)
print("----------------------------------------------------------------------------------------------------------------------")

#Drop complete null rows
df_ball_by_ball = df_ball_by_ball.dropna(how="all")

#Filter out rows with missing essential keys
df_ball_by_ball=df_ball_by_ball.filter(F.col("Match_id").isNotNull() &
                                       F.col("Over_id").isNotNull() &
                                       F.col("ball_id").isNotNull() )
#Standardising string columns
string_cols = ["Team_Batting", "Team_Bowling", "Extra_Type","Out_type"]

for col_name in string_cols:
    df_ball_by_ball = df_ball_by_ball.withColumn(
        col_name,
        initcap(
            regexp_replace(trim(lower(col(col_name))), " +", " ")
        )
    )

#standardising Date Columns
df_ball_by_ball = df_ball_by_ball.withColumn("Match_Date", F.to_date("Match_Date", "yyyy-MM-dd"))

#Drop Duplicate values based on key columns
df_ball_by_ball = df_ball_by_ball.dropDuplicates(["Match_id", "Over_id", "Ball_id"])

ball_by_ball_count=df_ball_by_ball.count()
print("Ball By Ball After Record Counts:")
print(ball_by_ball_count)
print("----------------------------------------------------------------------------------------------------------------------")
#save the cleaned version
df_ball_by_ball_cleaned = df_ball_by_ball.cache()

df_ball_by_ball_cleaned.select("match_id", "team_batting", "team_bowling", "runs_scored").show(10, truncate=False)

# df_ball_by_ball_cleaned.createOrReplaceTempView("fact_ball_by_ball_cleaned")

# df_ball_by_ball_cleaned.write.mode("overwrite").parquet("gs://ipl-data-project/cleaned/ball_by_ball/")







# ------------------------------------------------------------------------------------------------------------------------
# DATA LOADING
# ------------------------------------------------------------------------------------------------------------------------



