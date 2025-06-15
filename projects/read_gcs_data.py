from pyspark.sql import SparkSession

# Initialize Spark session with GCS config
spark = SparkSession.builder \
    .appName("ReadGCSData") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "E:/DataEngineering/keys/data-with-jha-0e1c0496e4ff.json") \
    .getOrCreate()

# Read CSV from GCS
df = spark.read.option("header", "true").csv("gs://ipl-data-project/Ball_By_Ball.csv")

# Show top 5 rows
df.show(5)

# Optional: Print schema
df.printSchema()
