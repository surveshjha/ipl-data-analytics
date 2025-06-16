from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,sum

# Initialize Spark Session (if not already)
print("[INFO] Initializing Spark session...")

custom_tmp_dir = "E:/DataEngineering/SparkTemp"  # Custom temp directory
spark = SparkSession.builder.appName("Read Cleaned IPL Tables").config("spark.local.dir", custom_tmp_dir).config("spark.files.overwrite", "false") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Base path where your cleaned CSVs are stored
base_path = "file:///E:/DataEngineering/Ipl-Analytics/cleaned-data/"

# List of table folders inside the cleaned-data path
table_names = [
    "Ball_By_Ball",
    "Match",
    "Player",
    "Player_Match",
    "Team"
]

# Read each CSV folder and show 5 records
print("--------------------------------------------------------------------------------")
print("Read Started ")
dataframes = {}

for table in table_names:
    full_path = base_path + table
    print(f"\n[INFO] Reading table: {table}")
    try:
        df = spark.read.option("inferschema", "true").option("header", True).csv(full_path)
        df.createOrReplaceTempView(table)
        dataframes[table] = df  # Store the dataframe by table name
        print(f"[SUCCESS] View created and DataFrame stored for: {table}")
    except Exception as e:
        print(f"[ERROR] Failed to read table {table}: {e}")

#Answering questions:

#filter to include only valid deliveries i.e. there should be no wides and no noballs
# dataframes["Ball_By_Ball"].select("match_id", "over_id", "ball_id", "wides", "noballs").show(5)


df_ball_by_ball=dataframes["Ball_By_Ball"]
df_ball_by_ball.printSchema()
print(f"Total Deliverries {df_ball_by_ball.count()}")
df_ball_by_ball=df_ball_by_ball.filter((col("wides")==0)&(col("noballs")==0))
print(f"Valid Deliverries {df_ball_by_ball.count()}")


total_and_avg_runs = df_ball_by_ball.groupBy("match_id", "innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("average_runs")
).orderBy("total_runs",ascending=False)
total_and_avg_runs.show()

# total_and_avg_runs.show()
# dataframes["Ball_By_Ball"] = dataframes["Ball_By_Ball"].filter(
#     (col('wides') == 0) & (col("noballs") == 0)
# )
# count=dataframes["Ball_By_Ball"].count()
# print(f"Valid Deliverries {count}")