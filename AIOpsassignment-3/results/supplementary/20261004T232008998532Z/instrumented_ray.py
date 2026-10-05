# Generated source view; executed by quick_check.py with injected _qc_* hooks.
import glob
import os
import shutil
import time
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import ray
import ray.data
TRIP_PATH = '/home/sidhartha/AIOpsassignment-3/quick_checks/20261004T232008998532Z/input/yellow_tripdata_quick.parquet'
LOOKUP_PATH = '/home/sidhartha/AIOpsassignment-3/data/lookup/taxi_zone_lookup.csv'
OUTPUT_ROOT = '/home/sidhartha/AIOpsassignment-3/quick_checks/20261004T232008998532Z/output/ray'
RESULTS_FILE = '/home/sidhartha/AIOpsassignment-3/results/supplementary/20261004T232008998532Z/ray_monthly_results.csv'
TOTAL_TIME_FILE = '/home/sidhartha/AIOpsassignment-3/results/supplementary/20261004T232008998532Z/ray_total_time.txt'
trip_columns = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'PULocationID', 'DOLocationID', 'fare_amount', 'total_amount']
required_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'PULocationID', 'DOLocationID']
final_columns = ['VendorID', 'passenger_count', 'trip_distance', 'fare_amount', 'total_amount', 'PULocationID', 'DOLocationID', 'pickup_timestamp', 'dropoff_timestamp', 'trip_duration_hours', 'average_speed_mph', 'pickup_borough', 'pickup_zone', 'pickup_service_zone', 'dropoff_borough', 'dropoff_zone', 'dropoff_service_zone']

def remove_required_nulls(batch: pd.DataFrame) -> pd.DataFrame:
    mask = batch[required_columns].notna().all(axis=1)
    return batch.loc[mask].copy()

def remove_duplicates(batch: pd.DataFrame) -> pd.DataFrame:
    return batch.drop_duplicates().copy()

def filter_invalid_trips(batch: pd.DataFrame) -> pd.DataFrame:
    mask = (batch['trip_distance'] > 0) & (batch['tpep_dropoff_datetime'] > batch['tpep_pickup_datetime'])
    return batch.loc[mask].copy()

def add_timestamp_features(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()
    pickup = pd.to_datetime(batch['tpep_pickup_datetime'])
    dropoff = pd.to_datetime(batch['tpep_dropoff_datetime'])
    batch['pickup_timestamp'] = pickup.dt.strftime('%Y-%m-%d %H:%M:%S')
    batch['dropoff_timestamp'] = dropoff.dt.strftime('%Y-%m-%d %H:%M:%S')
    batch['trip_duration_hours'] = (dropoff - pickup).dt.total_seconds() / 3600.0
    return batch

def add_average_speed(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()
    distance = batch['trip_distance'].to_numpy(dtype='float64')
    duration = batch['trip_duration_hours'].to_numpy(dtype='float64')
    batch['average_speed_mph'] = np.divide(distance, duration, out=np.full(len(batch), np.nan, dtype='float64'), where=duration > 0)
    return batch
ray.init(address='auto')
print('Ray version:', ray.__version__)
print('Alive nodes:', len([n for n in ray.nodes() if n['Alive']]))
print('Cluster resources:', ray.cluster_resources())
_qc_start_pipeline(globals())
lookup_pd = pd.read_csv(LOOKUP_PATH, keep_default_na=False)
lookup_pd['LocationID'] = lookup_pd['LocationID'].astype('int32')
for col in ['Borough', 'Zone', 'service_zone']:
    lookup_pd[col] = lookup_pd[col].replace('', 'Unknown')
pickup_pd = lookup_pd.rename(columns={'LocationID': 'PULocationID', 'Borough': 'pickup_borough', 'Zone': 'pickup_zone', 'service_zone': 'pickup_service_zone'})
dropoff_pd = lookup_pd.rename(columns={'LocationID': 'DOLocationID', 'Borough': 'dropoff_borough', 'Zone': 'dropoff_zone', 'service_zone': 'dropoff_service_zone'})

def join_location_lookup(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()
    batch = batch.merge(pickup_pd, on='PULocationID', how='left', sort=False)
    batch = batch.merge(dropoff_pd, on='DOLocationID', how='left', sort=False)
    return batch
trip_files = sorted(glob.glob(TRIP_PATH))
print('Total monthly files:', len(trip_files))
os.makedirs(OUTPUT_ROOT, exist_ok=True)
os.makedirs('/home/sidhartha/AIOpsassignment-3/results/supplementary/20261004T232008998532Z', exist_ok=True)
monthly_results = []
pipeline_start = _qc_pipeline_started
for i, trip_file in enumerate(trip_files, start=1):
    filename = os.path.basename(trip_file)
    month = filename.replace('yellow_tripdata_', '').replace('.parquet', '')
    month_output = os.path.join(OUTPUT_ROOT, month)
    print()
    print('=' * 60)
    print(f'[{i}/{len(trip_files)}] Processing {month}')
    print('Input:', trip_file)
    print('=' * 60)
    existing_files = glob.glob(os.path.join(month_output, '*.parquet'))
    if existing_files:
        month_rows = sum((pq.ParquetFile(f).metadata.num_rows for f in existing_files))
        month_bytes = sum((os.path.getsize(f) for f in existing_files))
        monthly_results.append({'month': month, 'runtime_seconds': 0.0, 'rows': month_rows, 'output_bytes': month_bytes, 'parquet_files': len(existing_files)})
        print(f'Skipping completed {month}: {month_rows:,} rows')
        continue
    month_start = time.perf_counter()
    trips = ray.data.read_parquet(trip_file, override_num_blocks=1).select_columns(trip_columns)
    cleaned = trips.map_batches(remove_required_nulls, batch_format='pandas')
    cleaned = cleaned.map_batches(remove_duplicates, batch_format='pandas')
    cleaned = cleaned.map_batches(filter_invalid_trips, batch_format='pandas')
    cleaned = cleaned.map_batches(add_timestamp_features, batch_format='pandas')
    joined = cleaned.map_batches(join_location_lookup, batch_format='pandas')
    final_ds = joined.map_batches(add_average_speed, batch_format='pandas').select_columns(final_columns)
    final_ds.write_parquet(month_output, concurrency=2)
    month_time = time.perf_counter() - month_start
    parquet_files = glob.glob(os.path.join(month_output, '*.parquet'))
    month_rows = sum((pq.ParquetFile(f).metadata.num_rows for f in parquet_files))
    month_bytes = sum((os.path.getsize(f) for f in parquet_files))
    monthly_results.append({'month': month, 'runtime_seconds': month_time, 'rows': month_rows, 'output_bytes': month_bytes, 'parquet_files': len(parquet_files)})
    print(f'Completed {month}: {month_rows:,} rows, {month_time:.2f} seconds')
total_time = time.perf_counter() - pipeline_start
_qc_finish_pipeline(globals())
total_rows = sum((r['rows'] for r in monthly_results))
total_bytes = sum((r['output_bytes'] for r in monthly_results))
results_df = pd.DataFrame(monthly_results)
results_df.to_csv(RESULTS_FILE, index=False)
with open(TOTAL_TIME_FILE, 'w') as f:
    f.write(f'{total_time:.6f}\n')
print()
print('=' * 60)
print('RAY PIPELINE COMPLETE')
print('=' * 60)
print(f'Months processed: {len(monthly_results)}')
print(f'Total rows: {total_rows:,}')
print(f'Total runtime: {total_time:.2f} seconds')
print(f'Total output: {total_bytes / 1024 ** 3:.2f} GiB')
print('Output:', OUTPUT_ROOT)
print('Monthly metrics:', RESULTS_FILE)
_qc_profile(globals())
ray.shutdown()
