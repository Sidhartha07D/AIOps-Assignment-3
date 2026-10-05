# Generated source view; executed by quick_check.py with injected _qc_* hooks.
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
TRIP_PATH = '/home/sidhartha/AIOpsassignment-3/quick_checks/20261004T232008998532Z/input/yellow_tripdata_quick.parquet'
LOOKUP_PATH = '/home/sidhartha/AIOpsassignment-3/data/lookup/taxi_zone_lookup.csv'
OUTPUT_PATH = '/home/sidhartha/AIOpsassignment-3/quick_checks/20261004T232008998532Z/output/spark_udf'
spark = SparkSession.builder.appName('AIOpsAssignment3-Spark').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
print('Spark master:', spark.sparkContext.master)
print('Spark application ID:', spark.sparkContext.applicationId)
print('Spark default parallelism:', spark.sparkContext.defaultParallelism)
_qc_start_pipeline(globals())
start_time = time.perf_counter()
trip_columns = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'PULocationID', 'DOLocationID', 'fare_amount', 'total_amount']
trips = spark.read.option('mergeSchema', 'false').parquet(TRIP_PATH).select(*trip_columns)
lookup = spark.read.option('header', True).option('inferSchema', True).option('nullValue', '__NO_NULL_SENTINEL__').csv(LOOKUP_PATH)
print('Trip columns:', trips.columns)
print('Lookup columns:', lookup.columns)
print('Trip partitions:', trips.rdd.getNumPartitions())
required_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'PULocationID', 'DOLocationID']
cleaned = trips.dropna(subset=required_columns).dropDuplicates().filter(F.col('trip_distance') > 0).filter(F.col('tpep_dropoff_datetime') > F.col('tpep_pickup_datetime'))
print('Cleansing stage created successfully')
print('Cleaned columns:', cleaned.columns)
cleaned = cleaned.withColumn('pickup_timestamp', F.date_format('tpep_pickup_datetime', 'yyyy-MM-dd HH:mm:ss')).withColumn('dropoff_timestamp', F.date_format('tpep_dropoff_datetime', 'yyyy-MM-dd HH:mm:ss')).withColumn('trip_duration_hours', (F.col('tpep_dropoff_datetime').cast('timestamp').cast('long') - F.col('tpep_pickup_datetime').cast('timestamp').cast('long')) / 3600.0)
print('Timestamp normalization stage created successfully')
pickup_lookup = lookup.select(F.col('LocationID').alias('PULocationID'), F.coalesce(F.col('Borough'), F.lit('Unknown')).alias('pickup_borough'), F.coalesce(F.col('Zone'), F.lit('Unknown')).alias('pickup_zone'), F.coalesce(F.col('service_zone'), F.lit('Unknown')).alias('pickup_service_zone'))
dropoff_lookup = lookup.select(F.col('LocationID').alias('DOLocationID'), F.coalesce(F.col('Borough'), F.lit('Unknown')).alias('dropoff_borough'), F.coalesce(F.col('Zone'), F.lit('Unknown')).alias('dropoff_zone'), F.coalesce(F.col('service_zone'), F.lit('Unknown')).alias('dropoff_service_zone'))
print('Pickup/dropoff lookup tables prepared')
joined = cleaned.join(pickup_lookup, on='PULocationID', how='left').join(dropoff_lookup, on='DOLocationID', how='left')
print('Heavy join stage created successfully')
print('Joined columns:', joined.columns)

def average_speed_mph(distance_miles, duration_hours):
    if distance_miles is None or duration_hours is None:
        return None
    if duration_hours <= 0:
        return None
    return float(distance_miles) / float(duration_hours)
average_speed_udf = F.udf(average_speed_mph, DoubleType())
with_udf = joined.withColumn('average_speed_mph', average_speed_udf(F.col('trip_distance'), F.col('trip_duration_hours')))
print('Python UDF stage created successfully')
final_df = with_udf.select('VendorID', 'passenger_count', 'trip_distance', 'fare_amount', 'total_amount', 'PULocationID', 'DOLocationID', 'pickup_timestamp', 'dropoff_timestamp', 'trip_duration_hours', 'average_speed_mph', 'pickup_borough', 'pickup_zone', 'pickup_service_zone', 'dropoff_borough', 'dropoff_zone', 'dropoff_service_zone')
final_df.write.mode('overwrite').parquet(OUTPUT_PATH)
total_time = time.perf_counter() - start_time
_qc_finish_pipeline(globals())
print(f'Spark pipeline completed in {total_time:.2f} seconds')
print('Output:', OUTPUT_PATH)
with open('/home/sidhartha/AIOpsassignment-3/results/supplementary/20261004T232008998532Z/spark_udf_total_time.txt', 'w') as f:
    f.write(f'{total_time:.6f}\n')
_qc_profile(globals())
spark.stop()
