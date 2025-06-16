from pyspark.sql import SparkSession

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
for table in table_names:
    full_path = base_path + table  # Example: file:///E:/.../cleaned-data/Ball_By_Ball
    print(f"\n[INFO] Reading table: {table}")
    try:
        df = spark.read.option('inferschema','true').option("header", True).csv(full_path)
        df.createOrReplaceTempView(f'{table}')
    except Exception as e:
        print(f"[ERROR] Failed to read table {table}: {e}")

df=spark.sql('''select ball_id
,match_id
,over_id
,innings_no
,team_batting
,team_bowling
,striker_batting_position
,extra_type
 from Ball_By_Ball ''')
df.show(10)