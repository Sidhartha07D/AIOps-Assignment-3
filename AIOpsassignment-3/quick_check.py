#!/usr/bin/env python3
"""Supplementary subset checks. Run from the assignment repository in Ubuntu.

Never runs or edits original scripts in place. Their ASTs are copied with
isolated paths and instrumentation hooks, then executed in the current driver.
"""
import argparse
import ast
import collections
import contextlib
import datetime
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import statistics
import subprocess
import sys
import threading
import time

TRIP_COLUMNS = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
                'passenger_count', 'trip_distance', 'PULocationID',
                'DOLocationID', 'fare_amount', 'total_amount']
FINAL_COLUMNS = ['VendorID', 'passenger_count', 'trip_distance', 'fare_amount',
                 'total_amount', 'PULocationID', 'DOLocationID',
                 'pickup_timestamp', 'dropoff_timestamp', 'trip_duration_hours',
                 'average_speed_mph', 'pickup_borough', 'pickup_zone',
                 'pickup_service_zone', 'dropoff_borough', 'dropoff_zone',
                 'dropoff_service_zone']
SOURCE_FILES = ['spark_clean.py', 'spark_clean_native.py', 'ray_clean.py']
STATE = '.quick_check_current.json'


def save_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, allow_nan=False) + '\n')


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def environment():
    versions = {}
    for name in ['pyspark', 'ray', 'numpy', 'pandas', 'pyarrow']:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return {'python': sys.version, 'platform': platform.platform(),
            'package_versions': versions}


def read_cpu(line):
    # Do not double-count guest/guest_nice, already included in user/nice.
    values = [int(x) for x in line.split()[1:9]]
    if len(values) != 8:
        raise ValueError('Unexpected /proc/stat CPU fields')
    total = sum(values)
    idle = values[3] + values[4]
    return total, idle


def read_memory(text):
    fields = {}
    for line in text.splitlines():
        key, value = line.split(':', 1)
        fields[key] = int(value.strip().split()[0])
    return {'host_ram_total_mib': fields['MemTotal'] / 1024.0,
            'host_ram_used_mib': (fields['MemTotal'] - fields['MemAvailable']) / 1024.0,
            'host_swap_used_mib': (fields['SwapTotal'] - fields['SwapFree']) / 1024.0}


class Monitor:
    """Whole-VM /proc monitoring, including OS and other running processes."""
    def __init__(self, path, phase, interval=0.5):
        self.path, self.phase, self.interval = Path(path), phase, interval
        self.rows, self.previous = [], None
        self.finished = threading.Event()
        self.error = None

    def sample(self):
        try:
            cpu = read_cpu(Path('/proc/stat').read_text().splitlines()[0])
            row = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   'monotonic_seconds': time.perf_counter(), 'phase': self.phase,
                   **read_memory(Path('/proc/meminfo').read_text())}
            row['host_cpu_busy_percent'] = None
            if self.previous:
                total = cpu[0] - self.previous[0]
                idle = cpu[1] - self.previous[1]
                if total > 0:
                    row['host_cpu_busy_percent'] = 100.0 * (total - idle) / total
            self.previous = cpu
            self.rows.append(row)
        except Exception as exc:
            self.error = f'{type(exc).__name__}: {exc}'
            self.finished.set()

    def start(self):
        if not Path('/proc/stat').is_file():
            raise RuntimeError('Run resource monitoring in the Ubuntu VM, not macOS.')
        self.sample()
        if self.error:
            raise RuntimeError(self.error)
        def loop():
            while not self.finished.wait(self.interval):
                self.sample()
        self.thread = threading.Thread(target=loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.finished.set()
        if hasattr(self, 'thread'):
            self.thread.join()
        self.sample()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(''.join(json.dumps(r, allow_nan=False) + '\n' for r in self.rows))
        cpu = [r['host_cpu_busy_percent'] for r in self.rows if r['host_cpu_busy_percent'] is not None]
        return {'scope': 'whole VM; includes OS, monitoring and other processes',
                'phase': self.phase, 'sampling_interval_seconds': self.interval,
                'samples': len(self.rows),
                'max_observed_cpu_busy_percent': max(cpu) if cpu else None,
                'max_observed_ram_used_mib': max((r['host_ram_used_mib'] for r in self.rows), default=None),
                'max_observed_swap_used_mib': max((r['host_swap_used_mib'] for r in self.rows), default=None),
                'baseline': self.rows[0] if self.rows else None,
                'ram_definition': 'MemTotal minus MemAvailable; differs from top used-memory accounting',
                'cpu_definition': 'aggregate across all virtual CPUs, 0-100%; idle and iowait excluded',
                'limitation': 'sampled maxima for this subset and phase, not true instantaneous or full-dataset peaks',
                'monitor_error': self.error}


class Run:
    def __init__(self, root):
        self.root = Path(root).resolve()
        state = json.loads((self.root / STATE).read_text())
        self.run_id = state['run_id']
        self.work = self.root / 'quick_checks' / self.run_id
        self.results = self.root / 'results' / 'supplementary' / self.run_id
        self.input = self.work / 'input' / 'yellow_tripdata_quick.parquet'
        self.manifest = json.loads((self.results / 'input_manifest.json').read_text())
        for name, digest in self.manifest['original_script_sha256'].items():
            if sha256(self.root / name) != digest:
                raise RuntimeError(f'{name} changed since input preparation. Prepare a new run.')
        if sha256(self.input) != self.manifest['subset_sha256']:
            raise RuntimeError('Subset input changed. Prepare a new run.')
        lookup = self.root / 'data' / 'lookup' / 'taxi_zone_lookup.csv'
        if sha256(lookup) != self.manifest['lookup_sha256']:
            raise RuntimeError('Lookup changed. Prepare a new run.')

    def output(self, variant):
        return self.work / 'output' / variant

    def metrics(self, variant):
        return self.results / f'{variant}_metrics.json'

    def require_fresh(self, variant):
        if self.output(variant).exists() or self.metrics(variant).exists():
            raise RuntimeError(f'{variant} already has output/results. Run prepare for a new isolated run.')


def prepare(root, row_limit):
    import pyarrow as pa
    import pyarrow.parquet as pq
    root = Path(root).resolve()
    if row_limit <= 0 or row_limit > 200000:
        raise ValueError('Use between 1 and 200000 rows for this quick check.')
    for name in SOURCE_FILES + ['data/lookup/taxi_zone_lookup.csv']:
        if not (root / name).is_file():
            raise FileNotFoundError(f'Missing {root / name}')
    sources = sorted((root / 'data/raw/yellow').glob('*.parquet'))
    if not sources:
        raise FileNotFoundError('No original input Parquet files in data/raw/yellow/.')
    if shutil.disk_usage(root).free < 1024**3:
        raise RuntimeError('Less than 1 GiB free disk. Reclaim space before making new outputs.')
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    work = root / 'quick_checks' / run_id
    results = root / 'results' / 'supplementary' / run_id
    (work / 'input').mkdir(parents=True, exist_ok=False)
    results.mkdir(parents=True, exist_ok=False)
    source = sources[0]
    parquet = pq.ParquetFile(source)
    batches, remaining = [], row_limit
    for batch in parquet.iter_batches(batch_size=min(row_limit, 65536), columns=TRIP_COLUMNS):
        taken = batch.slice(0, remaining)
        batches.append(taken)
        remaining -= len(taken)
        if remaining <= 0:
            break
    if not batches:
        raise RuntimeError('Selected source file has no rows.')
    table = pa.Table.from_batches(batches)
    target = work / 'input/yellow_tripdata_quick.parquet'
    pq.write_table(table, target, compression='snappy', row_group_size=len(table))
    manifest = {'run_id': run_id, 'scope': 'supplementary fixed subset, not the original 35-month benchmark',
                'selection': 'first rows in the first lexicographically sorted original monthly file; not random',
                'source_file': str(source.relative_to(root)),
                'source_file_rows': parquet.metadata.num_rows,
                'source_sha256': sha256(source), 'requested_rows': row_limit,
                'input_rows': len(table), 'subset_sha256': sha256(target),
                'subset_schema': str(table.schema),
                'lookup_sha256': sha256(root / 'data/lookup/taxi_zone_lookup.csv'),
                'original_script_sha256': {name: sha256(root / name) for name in SOURCE_FILES},
                'environment': environment()}
    save_json(results / 'input_manifest.json', manifest)
    save_json(root / STATE, {'run_id': run_id})
    print(f'PREPARED {len(table):,} shared rows from {source.name}')
    print('New results:', results)
    print('Original scripts, results and output folders have not been overwritten.')


class Redirect(ast.NodeTransformer):
    def __init__(self, assignments, string_paths):
        self.assignments, self.string_paths = assignments, string_paths
    def visit_Assign(self, node):
        node = self.generic_visit(node)
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in self.assignments:
                node.value = ast.Constant(self.assignments[name])
        return node
    def visit_Constant(self, node):
        if isinstance(node.value, str) and node.value in self.string_paths:
            return ast.copy_location(ast.Constant(self.string_paths[node.value]), node)
        return node
    def visit_Call(self, node):
        node = self.generic_visit(node)
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            if node.func.value.id == 'time' and node.func.attr == 'time':
                node.func.attr = 'perf_counter'
        return node


def assignment_name(node):
    if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
        return node.targets[0].id
    return None


def is_shutdown(node, module, method):
    return (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and isinstance(node.value.func.value, ast.Name)
            and node.value.func.value.id == module and node.value.func.attr == method)


def hook(code):
    return ast.parse(code).body


def adapted_tree(source, run, variant):
    tree = ast.parse(source)
    names = {assignment_name(n) for n in ast.walk(tree)}
    expected = {'TRIP_PATH', 'LOOKUP_PATH', 'joined', 'total_time'}
    expected |= {'start_time', 'with_udf'} if variant.startswith('spark') else {'pipeline_start', 'lookup_pd'}
    if not expected <= names:
        raise RuntimeError(f'Unexpected source structure: missing {sorted(expected - names)}')
    replacements = {'TRIP_PATH': str(run.input),
                    'LOOKUP_PATH': str(run.root / 'data/lookup/taxi_zone_lookup.csv')}
    if variant.startswith('spark'):
        replacements['OUTPUT_PATH'] = str(run.output(variant))
        strings = {'results/spark_total_time.txt': str(run.results / 'spark_udf_total_time.txt'),
                   'results/spark_native_total_time.txt': str(run.results / 'spark_native_total_time.txt')}
    else:
        replacements.update(OUTPUT_ROOT=str(run.output(variant)),
                            RESULTS_FILE=str(run.results / 'ray_monthly_results.csv'),
                            TOTAL_TIME_FILE=str(run.results / 'ray_total_time.txt'))
        strings = {'results': str(run.results)}
    tree = Redirect(replacements, strings).visit(tree)
    new_body = []
    start_hook = 'start_time' if variant.startswith('spark') else 'lookup_pd'
    shutdown_module, shutdown_method = ('spark', 'stop') if variant.startswith('spark') else ('ray', 'shutdown')
    starts = stops = profiles = 0
    for node in tree.body:
        name = assignment_name(node)
        if name == start_hook:
            new_body += hook('_qc_start_pipeline(globals())')
            starts += 1
        if variant == 'ray' and name == 'pipeline_start':
            node.value = ast.Name('_qc_pipeline_started', ast.Load())
        if is_shutdown(node, shutdown_module, shutdown_method):
            new_body += hook('_qc_profile(globals())')
            profiles += 1
        new_body.append(node)
        if name == 'total_time':
            new_body += hook('_qc_finish_pipeline(globals())')
            stops += 1
    if (starts, stops, profiles) != (1, 1, 1):
        raise RuntimeError('Could not insert instrumentation exactly once.')
    tree.body = new_body
    return ast.fix_missing_locations(tree)


def measure_trials(run, variant, trial_function):
    # One untimed warmup, then three timed materialized feature stages.
    warmup = trial_function()
    values, resources, checks = [], [], []
    for i in range(1, 4):
        monitor = Monitor(run.results / f'{variant}_feature_{i}_resources.jsonl', 'feature_stage')
        monitor.start()
        try:
            result = trial_function()
        finally:
            resources.append(monitor.stop())
        values.append(result['seconds'])
        checks.append({'rows': result['rows'], 'speed_sum': result['speed_sum']})
    return {'warmup_seconds_not_in_median': warmup['seconds'],
            'timed_runs_seconds': values, 'median_seconds': statistics.median(values),
            'input_rows': warmup['rows'], 'execution_checks': checks,
            'resource_samples': resources,
            'scope': 'distance, duration and speed columns materialized from cached joined input; includes scheduling, conversion and cache population; excludes joins and export; not pure function-body CPU time',
            'repetitions': 'one warmup and three timed trials in one application; not independent full-pipeline replications'}


def profile_spark(ns, run, variant):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    cached = ns['joined'].persist(StorageLevel.MEMORY_AND_DISK)
    joined_rows = cached.count()
    def trial():
        frame = ns['with_udf'].select('trip_distance', 'trip_duration_hours', 'average_speed_mph')
        frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            started = time.perf_counter()
            rows = frame.count()  # Cache population forces evaluation of the full feature projection.
            seconds = time.perf_counter() - started
            check = frame.agg(F.sum('average_speed_mph').alias('s')).collect()[0]['s']
            return {'seconds': seconds, 'rows': rows,
                    'speed_sum': check if check is None or math.isfinite(check) else str(check)}
        finally:
            frame.unpersist(blocking=True)
    try:
        result = measure_trials(run, variant, trial)
        result['cached_joined_rows'] = joined_rows
        result['execution_sink'] = 'Spark persist(MEMORY_AND_DISK) and count; sum consumes cached speed after timing'
        return result
    finally:
        cached.unpersist(blocking=True)


def profile_ray(ns, run, variant):
    cached = ns['joined'].materialize()
    joined_rows = cached.count()
    def trial():
        feature = cached.map_batches(ns['add_average_speed'], batch_format='pandas')
        feature = feature.select_columns(['trip_distance', 'trip_duration_hours', 'average_speed_mph'])
        started = time.perf_counter()
        materialized = feature.materialize()  # Executes lazy Python transformations.
        seconds = time.perf_counter() - started
        total = 0.0
        for batch in materialized.iter_batches(batch_format='pandas'):
            total += float(batch['average_speed_mph'].sum())
        return {'seconds': seconds, 'rows': materialized.count(),
                'speed_sum': total if math.isfinite(total) else str(total)}
    result = measure_trials(run, variant, trial)
    result['cached_joined_rows'] = joined_rows
    result['execution_sink'] = 'Ray materialize into object store; sum consumes cached speed after timing'
    return result


def execute_driver(root, variant):
    run = Run(root)
    run.require_fresh(variant)
    source_name = {'spark_udf': 'spark_clean.py', 'spark_native': 'spark_clean_native.py',
                   'ray': 'ray_clean.py'}[variant]
    source = (run.root / source_name).read_text()
    tree = adapted_tree(source, run, variant)
    (run.results / f'instrumented_{variant}.py').write_text(
        '# Generated source view; executed by quick_check.py with injected _qc_* hooks.\n' + ast.unparse(tree) + '\n')
    metrics = {'run_id': run.run_id, 'variant': variant,
               'scope': 'supplementary fixed subset', 'input_rows': run.manifest['input_rows'],
               'source_sha256': sha256(run.root / source_name), 'environment': environment(),
               'status': 'started'}
    monitor = None
    def start_pipeline(ns):
        nonlocal monitor
        if variant.startswith('spark'):
            sc = ns['spark'].sparkContext
            if not sc.master.startswith('spark://') or sc.defaultParallelism != 2:
                raise RuntimeError('Expected standalone Spark and two task cores. Check master and submission settings.')
            metrics['cluster'] = {'master': sc.master, 'default_parallelism': sc.defaultParallelism,
                                  'application_id': sc.applicationId,
                                  'session_timezone': ns['spark'].conf.get('spark.sql.session.timeZone')}
        else:
            ray = ns['ray']
            alive = [n for n in ray.nodes() if n['Alive']]
            workers = [n for n in alive if n.get('Resources', {}).get('CPU', 0) > 0]
            if len(alive) != 3 or len(workers) != 2 or ray.cluster_resources().get('CPU') != 2:
                raise RuntimeError('Expected the recorded Ray head (0 CPUs) and two workers (1 CPU each).')
            metrics['cluster'] = {'resources': ray.cluster_resources(),
                                  'alive_nodes': [{'address': n['NodeManagerAddress'], 'resources': n.get('Resources', {})} for n in alive]}
        monitor = Monitor(run.results / f'{variant}_pipeline_resources.jsonl', 'ingestion_to_export')
        monitor.start()
        ns['_qc_pipeline_started'] = time.perf_counter()
    def finish_pipeline(ns):
        nonlocal monitor
        metrics['pipeline_seconds'] = ns['total_time']
        metrics['pipeline_timer_scope'] = 'after engine initialization, before lookup/trip reading through successful Parquet write; excludes feature profiling and output comparison'
        metrics['pipeline_resources'] = monitor.stop()
        monitor = None
        metrics['status'] = 'pipeline_completed; profiling_pending'
        save_json(run.metrics(variant), metrics)
    def profile(ns):
        metrics['feature_stage'] = (profile_spark(ns, run, variant) if variant.startswith('spark')
                                    else profile_ray(ns, run, variant))
        metrics['status'] = 'completed'
        save_json(run.metrics(variant), metrics)
        print('\nQUICK CHECK COMPLETE:', variant)
        print('Subset pipeline seconds:', metrics['pipeline_seconds'])
        print('Feature-stage median seconds:', metrics['feature_stage']['median_seconds'])
        print('Metrics:', run.metrics(variant))
    ns = {'__name__': '__main__', '__file__': str(run.root / source_name),
          '_qc_start_pipeline': start_pipeline, '_qc_finish_pipeline': finish_pipeline,
          '_qc_profile': profile}
    try:
        exec(compile(tree, str(run.root / source_name), 'exec'), ns)
    except BaseException as exc:
        metrics['status'] = 'failed'
        metrics['error'] = f'{type(exc).__name__}: {exc}'
        if monitor is not None:
            metrics['partial_pipeline_resources'] = monitor.stop()
        save_json(run.metrics(variant), metrics)
        raise


def tee_command(command, log):
    log = Path(log)
    if log.exists():
        raise RuntimeError(f'Log already exists: {log}. Prepare a new run instead of overwriting evidence.')
    with log.open('x') as stream:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, bufsize=1, env=os.environ.copy())
        try:
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                stream.write(line)
                stream.flush()
            return process.wait()
        except KeyboardInterrupt:
            process.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=10)
            if process.poll() is None:
                process.kill()
            raise


def launch_spark(root, mode, address):
    run = Run(root)
    variant = 'spark_' + mode
    run.require_fresh(variant)
    if not shutil.which('spark-submit'):
        raise RuntimeError('spark-submit not found. Activate conda environment spark-ray-a3.')
    spill = run.work / 'spark-local'
    spill.mkdir(parents=True, exist_ok=True)
    command = ['spark-submit', '--master', f'spark://{address}:7077',
               '--driver-memory', '512m', '--conf', 'spark.executor.cores=1',
               '--conf', 'spark.executor.memory=512m', '--conf', 'spark.cores.max=2',
               '--conf', 'spark.sql.shuffle.partitions=32', '--conf', f'spark.local.dir={spill}',
               str(Path(__file__).resolve()), 'driver', variant]
    os.environ['SPARK_LOCAL_IP'] = address
    return tee_command(command, run.results / f'{variant}_run.log')


def canonical_value(value):
    if value is None:
        return ['null']
    if isinstance(value, bool):
        return ['bool', value]
    if isinstance(value, int):
        return ['integer', str(value)]
    if isinstance(value, float):
        # Hex is exact: no rounding/tolerance. Null is distinct from NaN.
        return ['float', value.hex()]
    if isinstance(value, str):
        return ['text', value]
    raise TypeError(f'Unexpected output value type: {type(value).__name__}')


def canonical_record(record):
    return json.dumps([canonical_value(record[c]) for c in FINAL_COLUMNS],
                      ensure_ascii=True, separators=(',', ':'))


def insert_records(connection, table_name, records):
    counts = collections.Counter(canonical_record(r) for r in records)
    connection.executemany(f'INSERT INTO {table_name}(record,n) VALUES (?,?) '
                           f'ON CONFLICT(record) DO UPDATE SET n={table_name}.n+excluded.n', counts.items())


def compare_tables(connection, left, right):
    left_only = connection.execute(
        f'SELECT COALESCE(SUM(MAX(a.n-COALESCE(b.n,0),0)),0) FROM {left} a '
        f'LEFT JOIN {right} b ON a.record=b.record').fetchone()[0]
    right_only = connection.execute(
        f'SELECT COALESCE(SUM(MAX(b.n-COALESCE(a.n,0),0)),0) FROM {right} b '
        f'LEFT JOIN {left} a ON b.record=a.record').fetchone()[0]
    examples = []
    for side, other in [(left, right), (right, left)]:
        rows = connection.execute(
            f'SELECT a.record,a.n,COALESCE(b.n,0) FROM {side} a LEFT JOIN {other} b '
            'ON a.record=b.record WHERE a.n>COALESCE(b.n,0) ORDER BY a.record LIMIT 3').fetchall()
        for record, own, other_count in rows:
            examples.append({'side': side, 'count_on_side': own, 'count_on_other': other_count,
                             'encoded_record': dict(zip(FINAL_COLUMNS, json.loads(record)))})
    return {'left': left, 'right': right, 'equal_values_with_multiplicity': left_only == right_only == 0,
            'left_unmatched_row_instances': left_only, 'right_unmatched_row_instances': right_only,
            'mismatch_examples': examples}


def compare(root):
    import pyarrow.parquet as pq
    run = Run(root)
    variants = ['spark_udf', 'spark_native', 'ray']
    metrics = {}
    for variant in variants:
        metrics[variant] = json.loads(run.metrics(variant).read_text())
        if metrics[variant]['status'] != 'completed':
            raise RuntimeError(f'{variant} has not completed both pipeline and profiling.')
    database = run.work / 'comparison.sqlite'
    if database.exists():
        raise RuntimeError('Comparison database already exists. Preserve it and prepare a new run.')
    summary = {'run_id': run.run_id, 'scope': 'the saved subset only; not full-dataset parity',
               'input_rows': run.manifest['input_rows'],
               'comparison_definition': 'full 17-column row multiset, preserving duplicate counts; file/row order ignored; integer widths and text/large_text representation ignored; exact float.hex values with no tolerance; signed zero distinguished; NaNs equal each other but distinct from null',
               'datasets': {}, 'benchmarks': metrics}
    with sqlite3.connect(database) as connection:
        connection.execute('PRAGMA cache_size=-32768')
        for variant in variants:
            connection.execute(f'CREATE TABLE {variant}(record TEXT PRIMARY KEY,n INTEGER NOT NULL)')
            files = sorted(run.output(variant).rglob('*.parquet'))
            if not files:
                raise RuntimeError(f'No Parquet output files for {variant}.')
            rows, schemas = 0, set()
            for filename in files:
                parquet = pq.ParquetFile(filename)
                if set(parquet.schema_arrow.names) != set(FINAL_COLUMNS):
                    raise RuntimeError(f'Unexpected columns in {filename}.')
                schemas.add(str(parquet.schema_arrow))
                for batch in parquet.iter_batches(batch_size=8192, columns=FINAL_COLUMNS):
                    records = batch.to_pylist()
                    insert_records(connection, variant, records)
                    rows += len(records)
                connection.commit()
            summary['datasets'][variant] = {'rows': rows, 'files': len(files),
                                           'inspected_file_schemas': sorted(schemas)}
        summary['comparisons'] = [compare_tables(connection, 'spark_udf', 'spark_native'),
                                  compare_tables(connection, 'spark_udf', 'ray')]
    database.unlink()
    save_json(run.results / 'summary.json', summary)
    lines = ['SUPPLEMENTARY FIXED-SUBSET CHECK', f'Input rows: {summary["input_rows"]:,}',
             'Not a rerun or validation of the full 35-month dataset.', '']
    for variant in variants:
        m = metrics[variant]
        res = m['pipeline_resources']
        lines += [f'{variant}: output rows={summary["datasets"][variant]["rows"]:,}',
                  f'  ingestion-to-export seconds={m["pipeline_seconds"]:.6f}',
                  f'  materialized feature-stage median seconds={m["feature_stage"]["median_seconds"]:.6f}',
                  f'  host CPU max observed percent={res["max_observed_cpu_busy_percent"]}',
                  f'  host RAM max observed MiB={res["max_observed_ram_used_mib"]}',
                  '  Resource scope: whole VM, 0.5-second nominal sampling; subset pipeline only.', '']
    for result in summary['comparisons']:
        lines += [f'{result["left"]} vs {result["right"]}: '
                  f'exact subset value/multiplicity match={result["equal_values_with_multiplicity"]}',
                  f'  unmatched row instances: left={result["left_unmatched_row_instances"]}, '
                  f'right={result["right_unmatched_row_instances"]}']
    text = '\n'.join(lines) + '\n'
    (run.results / 'summary.txt').write_text(text)
    print(text)
    archive_base = run.root / f'quick_check_evidence_{run.run_id}'
    archive = shutil.make_archive(str(archive_base), 'gztar', root_dir=run.root,
                                  base_dir=str(run.results.relative_to(run.root)))
    print('UPLOAD THIS EVIDENCE ARCHIVE:', archive)
    print('Small input and generated Parquet outputs remain in quick_checks/ for your own verification.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('prepare')
    p.add_argument('--rows', type=int, default=200000)
    p = sub.add_parser('spark')
    p.add_argument('mode', choices=['udf', 'native'])
    p.add_argument('--ip', default='192.168.64.2')
    sub.add_parser('ray')
    p = sub.add_parser('driver')
    p.add_argument('variant', choices=['spark_udf', 'spark_native', 'ray'])
    sub.add_parser('compare')
    args = parser.parse_args()
    root = Path.cwd()
    if args.command == 'prepare':
        prepare(root, args.rows)
    elif args.command == 'spark':
        return launch_spark(root, args.mode, args.ip)
    elif args.command == 'ray':
        run = Run(root)
        run.require_fresh('ray')
        return tee_command([sys.executable, str(Path(__file__).resolve()), 'driver', 'ray'],
                           run.results / 'ray_run.log')
    elif args.command == 'driver':
        execute_driver(root, args.variant)
    elif args.command == 'compare':
        compare(root)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'QUICK CHECK FAILED: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)
