import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

TRIP_PATH = "data/raw/yellow/*.parquet"
LOOKUP_PATH = "data/lookup/taxi_zone_lookup.csv"
OUTPUT_PATH = "output/spark_native/cleaned_taxi"

spark = (
    SparkSession.builder
    .appName("AIOpsAssignment3-Spark")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark master:", spark.sparkContext.master)
print("Spark application ID:", spark.sparkContext.applicationId)
print("Spark default parallelism:", spark.sparkContext.defaultParallelism)

start_time = time.time()

trip_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]

trips = (
    spark.read
    .option("mergeSchema", "false")
    .parquet(TRIP_PATH)
    .select(*trip_columns)
)

lookup = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("nullValue", "__NO_NULL_SENTINEL__")
    .csv(LOOKUP_PATH)
)

print("Trip columns:", trips.columns)
print("Lookup columns:", lookup.columns)
print("Trip partitions:", trips.rdd.getNumPartitions())

# ----------------------------
# Cleansing
# ----------------------------

required_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
]

cleaned = (
    trips
    .dropna(subset=required_columns)
    .dropDuplicates()
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
)

print("Cleansing stage created successfully")
print("Cleaned columns:", cleaned.columns)

# ----------------------------
# Timestamp normalization + duration
# ----------------------------

cleaned = (
    cleaned
    .withColumn(
        "pickup_timestamp",
        F.date_format("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "dropoff_timestamp",
        F.date_format("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "trip_duration_hours",
        (
            F.col("tpep_dropoff_datetime").cast("timestamp").cast("long")
            - F.col("tpep_pickup_datetime").cast("timestamp").cast("long")
        ) / 3600.0
    )
)

print("Timestamp normalization stage created successfully")

# ----------------------------
# Prepare lookup tables for heavy join
# ----------------------------

pickup_lookup = (
    lookup
    .select(
        F.col("LocationID").alias("PULocationID"),
        F.coalesce(F.col("Borough"), F.lit("Unknown")).alias("pickup_borough"),
        F.coalesce(F.col("Zone"), F.lit("Unknown")).alias("pickup_zone"),
        F.coalesce(F.col("service_zone"), F.lit("Unknown")).alias("pickup_service_zone"),
    )
)

dropoff_lookup = (
    lookup
    .select(
        F.col("LocationID").alias("DOLocationID"),
        F.coalesce(F.col("Borough"), F.lit("Unknown")).alias("dropoff_borough"),
        F.coalesce(F.col("Zone"), F.lit("Unknown")).alias("dropoff_zone"),
        F.coalesce(F.col("service_zone"), F.lit("Unknown")).alias("dropoff_service_zone"),
    )
)

print("Pickup/dropoff lookup tables prepared")

# ----------------------------
# Heavy join: pickup + dropoff zones
# ----------------------------

joined = (
    cleaned
    .join(pickup_lookup, on="PULocationID", how="left")
    .join(dropoff_lookup, on="DOLocationID", how="left")
)

print("Heavy join stage created successfully")
print("Joined columns:", joined.columns)

# ----------------------------
# Native Spark expression: average speed
# ----------------------------

with_udf = joined.withColumn(
    "average_speed_mph",
    F.col("trip_distance") / F.col("trip_duration_hours")
)

print("Native Spark expression stage created successfully")

# ----------------------------
# Final ML-ready dataset
# ----------------------------

final_df = with_udf.select(
    "VendorID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID",
    "pickup_timestamp",
    "dropoff_timestamp",
    "trip_duration_hours",
    "average_speed_mph",
    "pickup_borough",
    "pickup_zone",
    "pickup_service_zone",
    "dropoff_borough",
    "dropoff_zone",
    "dropoff_service_zone",
)

# Export as Parquet
final_df.write.mode("overwrite").parquet(OUTPUT_PATH)

total_time = time.time() - start_time

print(f"Spark pipeline completed in {total_time:.2f} seconds")
print("Output:", OUTPUT_PATH)

with open("results/spark_native_total_time.txt", "w") as f:
    f.write(f"{total_time:.6f}\n")

spark.stop()
