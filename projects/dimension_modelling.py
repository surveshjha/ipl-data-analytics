from pyspark.sql import SparkSession

# Initialize Spark Session (if not already)
spark = SparkSession.builder.appName("Read Cleaned IPL Tables").getOrCreate()

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
        df.show(5, truncate=False)
    except Exception as e:
        print(f"[ERROR] Failed to read table {table}: {e}")
