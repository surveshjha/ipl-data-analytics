from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType)
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, lower, initcap, regexp_replace, col, when, lit, to_date


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
# 📄 2. Function Definitions
# ------------------------------------------------------------------------------------------------------------------------


def clean_dataframe(df, key_columns=None,string_columns=None, boolean_columns=None, integer_columns=None,date_columns=None, dedup_columns=None, table_name="Table"):
    print(f"\n Starting Cleaning for: {table_name}")
    print("----------------------------------------------------------------------------------------------------------------------")

    initial_count = df.count()
    print(f"Initial Record Count: {initial_count}")


    if integer_columns:
        print("Cleaning integer columns by replacing blanks/nulls with 0...")
        for col_name in integer_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).isNull() | (trim(col(col_name).cast("string")) == ""),
                        lit(0)
                    ).otherwise(col(col_name).cast("int"))
                )

        # Logging counts of replacements
        for col_name in integer_columns:
            if col_name in df.columns:
                count_nulls_or_blanks = df.filter(
                    col(col_name).isNull() | (trim(col(col_name).cast("string")) == "")
                ).count()
                print(f"Integer Column '{col_name}': {count_nulls_or_blanks} blanks/nulls replaced with 0")



    if boolean_columns:
        print("Cleaning boolean columns by replacing blanks/nulls with False (0)...")
        for col_name in boolean_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull() | (trim(col(col_name)) == ""), lit(False))
                    .otherwise(col(col_name).cast("boolean"))
                )
    for col_name in boolean_columns:
        if col_name in df.columns:
            count_nulls = df.filter(col(col_name).isNull() | (trim(col(col_name)) == "")).count()
            print(f"Boolean Column '{col_name}': {count_nulls} blanks/nulls replaced with False (0)")
            

    # Step X: Replace blank or null string values with "BLANK" and count replacements
    print("Replacing NULL or empty string values in string columns with 'BLANK'...")

    for col_name in string_columns:
        blank_condition = (col(col_name).isNull()) | (trim(col(col_name)) == "")
        count_blank = df.filter(blank_condition).count()

        df = df.withColumn(
            col_name,
            when(blank_condition, lit("BLANK")).otherwise(col(col_name))
        )

        print(f"Column '{col_name}': {count_blank} values replaced with 'BLANK'")

    print("String column 'BLANK' substitution complete.")
    print("--------------------------------------------------------------------------------")


    # Step 1: Drop rows where all columns are null
    df = df.dropna(how="all")
    after_null_drop = df.count()
    print(f" Step 1 - NULL row drop: {after_null_drop} | Removed: {initial_count - after_null_drop}")

    # Step 2: Filter based on essential key columns
    if key_columns:
        print(f" Step 2 - Filtering nulls in key columns: {key_columns}")
        condition = None
        for col_name in key_columns:
            if condition is None:
                condition = F.col(col_name).isNotNull()
            else:
                condition &= F.col(col_name).isNotNull()
        df = df.filter(condition)
    after_key_filter = df.count()
    print(f"Rows after key filters: {after_key_filter} | Removed: {after_null_drop - after_key_filter}")

    # Step 3: Standardize string columns
    if string_columns:
        print(f"Step 3 - Cleaning string columns: {string_columns}")
        for col_name in string_columns:
            df = df.withColumn(
                col_name,
                initcap(
                    regexp_replace(trim(lower(F.col(col_name))), " +", " ")
                )
            )
    after_string_clean = df.count()
    print(f"String columns cleaned. Record count: {after_string_clean}")

   # Step 4: Convert date columns
    if date_columns:
        print(f"Step 4 - Formatting date columns: {date_columns}")
        for col_name in date_columns:
             df = parse_date_column(df, col_name)
    after_date_conversion = df.count()
    print(f"Date formatting done. Record count: {after_date_conversion}")
    
     #Step 5: Deduplication
     if dedup_columns:
         print(f"Step 5 - Removing duplicates using: {dedup_columns}")
         before_dedup = df.count()
         df = df.dropDuplicates(dedup_columns)
         after_dedup = df.count()
         print(f"After deduplication: {after_dedup} | Duplicates removed: {before_dedup - after_dedup}")
     else:
         after_dedup = after_date_conversion
     # Final Summary
     print("Final Cleaning Summary:")
     print(f"Initial Records           : {initial_count}")
     print(f"After NULL Row Drop       : {after_null_drop}")
     print(f"After Key Filter          : {after_key_filter}")
     print(f"After String Clean        : {after_string_clean}")
     print(f"After Date Conversion     : {after_date_conversion}")
     print(f"After deduplication       : {after_dedup}")
     print(f"Final Cleaned Record Count: {after_dedup}")
     print("Cleaning Complete!")
     print("----------------------------------------------------------------------------------------------------------------------")

    return df


def map_team_names(df, column, mapping_dict):
    expr = None
    for code, name in mapping_dict.items():
        condition = (col(column) == code)
        expr = when(condition, name) if expr is None else expr.when(condition, name)
    expr = expr.otherwise(col(column))
    return df.withColumn(column, expr)

def parse_date_column(df, column):
    return df.withColumn(
        column,
        when(col(column).rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), to_date(col(column), "MM/dd/yyyy"))  # e.g., 4/22/2013
        .when(col(column).rlike(r"^\d{2}-\d{2}-\d{4}$"), to_date(col(column), "MM/dd/yyyy"))     # e.g., 05-12-2013
        .otherwise(None)
    )


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

    # print("[INFO] Loading Match.csv...")
    # df_match = spark.read.schema(match_schema).option("header", "true").csv("gs://ipl-data-project/Match.csv")
    # print("[SUCCESS] Match.csv loaded. Row count:", df_match.count())

    # print("[INFO] Loading Player.csv...")
    # df_player = spark.read.schema(player_schema).option("header", "true").csv("gs://ipl-data-project/Player.csv")
    # print("[SUCCESS] Player.csv loaded. Row count:", df_player.count())

    # print("[INFO] Loading Player_match.csv...")
    # df_player_match = spark.read.schema(player_match_schema).option("header", "true").csv("gs://ipl-data-project/Player_match.csv")
    # print("[SUCCESS] Player_match.csv loaded. Row count:", df_player_match.count())

    # print("[INFO] Loading Team.csv...")
    # df_team = spark.read.schema(team_schema).option("header", "true").csv("gs://ipl-data-project/Team.csv")
    # print("[SUCCESS] Team.csv loaded. Row count:", df_team.count())

except Exception as e:
    print("[ERROR] Failed during data loading:", e)

# ------------------------------------------------------------------------------------------------------------------------
# 🧾 4. Schema Validation
# ------------------------------------------------------------------------------------------------------------------------

print("\n[INFO] Printing all schemas for verification...")

df_ball_by_ball.printSchema()
# df_match.printSchema()
# df_player.printSchema()
# df_player_match.printSchema()
# df_team.printSchema()

# ------------------------------------------------------------------------------------------------------------------------
# ✅ Final Step
# ------------------------------------------------------------------------------------------------------------------------

print("\n[INFO] All datasets loaded and schemas verified.")
print("[COMPLETED] IPL Data Analysis environment is ready.")

# ------------------------------------------------------------------------------------------------------------------------
# DATA CLEANING STARTED
# ------------------------------------------------------------------------------------------------------------------------

#Filtering nulls in key columns
#Standardize string columns
#Convert date columns
#Dedeuplication
team_mapping = {
    "1": "Kolkata Knight Riders",
    "2": "Royal Challengers Bangalore",
    "3": "Chennai Super Kings",
    "4": "Kings XI Punjab",
    "5": "Rajasthan Royals",
    "6": "Delhi Daredevils",
    "7": "Mumbai Indians",
    "8": "Deccan Chargers",
    "9": "Kochi Tuskers Kerala",
    "10": "Pune Warriors",
    "11": "Sunrisers Hyderabad",
    "12": "Rising Pune Supergiants",
    "13": "Gujarat Lions"
}

# from pyspark.sql.functions import col, trim, isnan

# # Count NULL values
# null_count = df_ball_by_ball.filter(col("player_out").isNull()).count()
# print(f"NULL values in 'player_out': {null_count}")

# # Count Blank (empty string or spaces) values
# blank_count = df_ball_by_ball.filter(trim(col("player_out")) == "").count()
# print(f"Blank ('') values in 'player_out': {blank_count}")

# # Count Blank (empty string or spaces) values
# NULL_String_count = df_ball_by_ball.filter(trim(col("player_out")) == "NULL").count()
# print(f"NULL_String_count ('') values in 'player_out': {NULL_String_count}")


# Step X: Map numeric codes in team columns to actual team names
print(" Mapping numeric team codes to full names in 'team_batting' and 'team_bowling'...")
for col_name in ["team_batting", "team_bowling"]:
    df_ball_by_ball = map_team_names(df_ball_by_ball, col_name, team_mapping)

print("Team mapping applied.")
print("See Results:")
df_ball_by_ball.select("team_batting","team_bowling").show(10)


df_cleaned  = clean_dataframe(
    df_ball_by_ball,
    key_columns=["match_id", "over_id", "ball_id"],
    string_columns=["team_batting", "team_bowling", "extra_type", "out_type"],
    boolean_columns=[
        "caught", "bowled", "run_out", "lbw", "retired_hurt",
        "stumped", "caught_and_bowled", "hit_wicket", "obstructingfeild", "bowler_wicket"
    ],
    integer_columns=[
        'striker_batting_position', 'runs_scored', 'extra_runs', 'wides',
        'legbyes', 'byes', 'noballs', 'penalty', 'bowler_extras',
        'striker', 'non_striker', 'bowler', 'player_out', 'fielders'
    ],
    date_columns=["match_date"],
    dedup_columns=["match_id", "over_id", "ball_id"],
    table_name="Ball_By_Ball"
)


# # Define output directory
# output_dir = "E:/DataEngineering/Ipl-Analytics/cleaned-data"
# output_uri = "file:///E:/DataEngineering/Ipl-Analytics/cleaned-data"

# # Step 1: Delete existing directory if it exists
# if os.path.exists(output_dir):
#     print(f"Deleting existing directory: {output_dir}")
#     shutil.rmtree(output_dir)
# else:
#     print(f"No existing directory found at: {output_dir}")

# print("Directory cleanup complete.")
# print("--------------------------------------------------------------------------------")

# Step 2: Write the DataFrame as a single CSV file
print(f"Writing cleaned data to: output_dir")
df_cleaned.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv('E:/DataEngineering/Ipl-Analytics/cleaned-data')

print("Write complete!")
print("--------------------------------------------------------------------------------")




# df_match = clean_dataframe(
#     df_match,
#     key_columns=["match_id"],
#     string_columns=["team1", "team2", "venue_name", "city_name", "country_name", "toss_winner", "match_winner", "toss_name", "win_type", "outcome_type", "manofmach"],
#     date_columns=["match_date"],
#     dedup_columns=["match_id"],
#     table_name="Match"
# )

# df_player = clean_dataframe(
#     df_player,
#     key_columns=["player_id"],
#     string_columns=["player_name", "batting_hand", "bowling_skill", "country_name"],
#     date_columns=["dob"],
#     dedup_columns=["player_id"],
#     table_name="Player"
# )

# df_player_match = clean_dataframe(
#     df_player_match,
#     key_columns=["player_match_sk"],
#     string_columns=["player_name", "batting_hand", "bowling_skill", "country_name", "role_desc", "player_team", "opposit_team", "batting_status", "bowling_status", "player_captain", "opposit_captain", "player_keeper", "opposit_keeper"],
#     date_columns=["dob"],
#     dedup_columns=["player_match_sk"],
#     table_name="Player_Match"
# )

# df_team = clean_dataframe(
#     df_team,
#     key_columns=["team_id"],
#     string_columns=["team_name"],
#     dedup_columns=["team_id"],
#     table_name="Team"
# )


# # ------------------------------------------------------------------------------------------------------------------------
# # DATA CLEANING COMPLETED
# # ------------------------------------------------------------------------------------------------------------------------
# # Initial Record Count
# initial_count = df_ball_by_ball.count()
# print(f" Initial Record Count: {initial_count}")
# print("----------------------------------------------------------------------------------------------------------------------")

# # Step 1: Drop completely null rows
# print("Step 1: Dropping rows where all columns are NULL...")
# df_ball_by_ball = df_ball_by_ball.dropna(how="all")
# after_null_drop = df_ball_by_ball.count()
# print(f" Rows after NULL drop: {after_null_drop} | Removed: {initial_count - after_null_drop}")
# print("----------------------------------------------------------------------------------------------------------------------")

# # Step 2: Filter out rows with missing essential keys
# print("Step 2: Filtering out rows with missing Match_id, Over_id, or Ball_id...")
# df_ball_by_ball = df_ball_by_ball.filter(
#     col("Match_id").isNotNull() &
#     col("Over_id").isNotNull() &
#     col("Ball_id").isNotNull()
# )
# after_key_filter = df_ball_by_ball.count()
# print(f"Rows after key filters: {after_key_filter} | Removed: {after_null_drop - after_key_filter}")
# print("----------------------------------------------------------------------------------------------------------------------")

# # Step 3: Standardizing string columns
# print("Step 3: Standardizing string columns (Team_Batting, Team_Bowling, Extra_Type, Out_type)...")
# string_cols = ["Team_Batting", "Team_Bowling", "Extra_Type", "Out_type"]

# for col_name in string_cols:
#     df_ball_by_ball = df_ball_by_ball.withColumn(
#         col_name,
#         initcap(
#             regexp_replace(trim(lower(col(col_name))), " +", " ")
#         )
#     )
# after_string_clean = df_ball_by_ball.count()
# print(f" String formatting applied. Record count remains: {after_string_clean}")
# print("----------------------------------------------------------------------------------------------------------------------")

# # Step 4: Convert Match_Date to DateType
# print("Step 4: Converting Match_Date to DateType...")
# df_ball_by_ball = df_ball_by_ball.withColumn("Match_Date", to_date("Match_Date", "yyyy-MM-dd"))
# after_date_conversion = df_ball_by_ball.count()
# print(f"Date conversion done. Record count remains: {after_date_conversion}")
# print("----------------------------------------------------------------------------------------------------------------------")

# # Step 5: Drop duplicates based on key columns
# print("Step 5: Dropping duplicates based on Match_id, Over_id, Ball_id...")
# before_dedup = df_ball_by_ball.count()
# df_ball_by_ball = df_ball_by_ball.dropDuplicates(["Match_id", "Over_id", "Ball_id"])
# after_dedup = df_ball_by_ball.count()
# print(f" After dropping duplicates: {after_dedup} | Duplicates removed: {before_dedup - after_dedup}")
# print("----------------------------------------------------------------------------------------------------------------------")

# # Final Summary
# print(" Cleaning Summary:")
# print(f" Initial Records           : {initial_count}")
# print(f" After NULL Row Drop       : {after_null_drop}")
# print(f" After Key Filter          : {after_key_filter}")
# print(f" After String Clean        : {after_string_clean}")
# print(f" After Date Conversion     : {after_date_conversion}")
# print(f" After dropping duplicates : {after_dedup}")
# print(f" Final Cleaned Record Count: {after_dedup}")
# print("Data Cleaning Complete!")
# print("----------------------------------------------------------------------------------------------------------------------")








# ------------------------------------------------------------------------------------------------------------------------
# DATA LOADING
# ------------------------------------------------------------------------------------------------------------------------



