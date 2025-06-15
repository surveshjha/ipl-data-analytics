from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType
import tempfile

# Create a custom temp directory (optional: you can specify your own path instead of letting tempfile create one)
custom_tmp_dir = "E:/DataEngineering/SparkTemp"

# Initialize Spark session with GCS config and custom temp dir
spark = SparkSession.builder \
    .appName("IPL Data Analysis SPARK") \
    .config("spark.local.dir", custom_tmp_dir) \
    .config("spark.files.overwrite", "false") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "E:/DataEngineering/keys/data-with-jha-0e1c0496e4ff.json") \
    .config("spark.driver.extraClassPath", "E:\\DataEngineering\\Ipl-Analytics\\jars\\gcs-connector-hadoop3-2.2.5-shaded.jar") \
    .getOrCreate()

# Read CSV from GCS

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

df_ball_by_ball = spark.read.schema(ball_by_ball_schema).option("header", "true").csv("gs://ipl-data-project/Ball_By_Ball.csv")
df_ball_by_ball.printSchema()
# df_ball_by_ball.createOrReplaceTempView('ball_by_ball')
df_ball_by_ball.count()

# df_Match = spark.read.option("header", "true").csv("gs://ipl-data-project/Match.csv")

# df_Match.createOrReplaceTempView('Match')

# Match_table=spark.sql('''select count(*) from Match''')
# Match_table.show()

# # Show top 5 rows
# df_ball_by_ball.columns
# df_Match.columns
# # Optional: Print schema
# df_ball_by_ball.printSchema()
# df_Match.printSchema()

