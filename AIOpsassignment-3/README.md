# DA3408 Assignment 3: Spark vs. Ray - The Data Engineering Duel

Sidhartha Durgam · DA24B003

Preprocessing of NYC Yellow Taxi records using Spark and Ray Data on an Ubuntu ARM64 virtual machine, with two worker nodes/processes for each framework. The pipelines ingest monthly Parquet files, remove required-field nulls and duplicates, filter invalid trips, format timestamps, join pickup/dropoff locations, calculate average speed, and export Parquet.

## Submission contents

- [`spark_clean.py`](spark_clean.py): Spark pipeline with a scalar Python UDF.
- [`ray_clean.py`](ray_clean.py): Ray Data pipeline with monthly processing, local lookup merges, and resumptions.
- [`spark_clean_native.py`](spark_clean_native.py): Spark variant replacing the speed UDF with a native expression; separate output and timing files.
- [`ray_cluster.py`](ray_cluster.py): supplied launcher for one head and two worker nodes.
- [Five-page benchmark report](report/Spark_vs_Ray_Report.pdf).
- [Original cluster and job screenshots](screenshots/).
- [Results and provenance](results/README.md).



## Dataset

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

- Yellow Taxi, February 2023 through December 2025 inclusive: **35 monthly files**.
- Captured input validation: **125,135,782 rows**, **1.97 GiB** total file bytes.
- Taxi-zone lookup: **265 unique LocationIDs**, with no duplicate keys in the captured check.
- The 2025-only `cbd_congestion_fee` field is omitted consistently from both pipelines.

Download the specified Yellow Taxi monthly Parquet files and Taxi Zone Lookup CSV from the source page, and use these paths:

```text
data/
├── raw/yellow/
│   ├── yellow_tripdata_2023-02.parquet
│   ├── ...
│   └── yellow_tripdata_2025-12.parquet
└── lookup/taxi_zone_lookup.csv
```

Data, generated Parquet and spill files are excluded from Git. The report discusses the structured export; no model training was performed.

## Recorded environment and cluster topology

| Item | Recorded configuration |
|---|---|
| VM | Ubuntu 26.04 LTS, aarch64, 4 virtual CPUs |
| Host memory / swap | Approximately 3.3 GiB RAM / 3.8 GiB swap |
| Root disk | `39G` as reported by `df -h` |
| Conda environment | `spark-ray-a3` |
| Python | 3.11.17 |
| Java | OpenJDK 17.0.20.1 |
| Spark / PySpark | 3.5.7 |
| Ray | 2.59.0 |
| Spark topology | 1 master + 2 workers, 1 core and 768 MiB advertised per worker |
| Spark application | 1 core and 512 MiB per executor, 512 MiB driver; total executor cores 2; 32 shuffle partitions |
| Supplied final Ray launcher | Head: 0 task CPUs, 512 MiB logical memory, 80 MiB object store; each worker: 1 CPU, 650 MiB logical memory, 120 MiB object store |

All nodes were colocated inside the same VM. Ray logical scheduling memory is not a physical RAM measurement or a hard process-memory limit. Earlier setup logs used 256 MiB head logical memory; the final supplied launcher uses 512 MiB. The earlier Ray cluster screenshot shows 150 MB object stores per node, while later active-run evidence shows a 320 MiB total object store. These snapshots do not all describe the same session configuration.

The included [`env.yaml`](env.yaml) is a minimal, curated environment file using the direct package versions observed in the VM. It is not a complete Conda export or a tested lockfile. On a compatible Ubuntu ARM64 machine, create it with:

```bash
conda env create -f env.yaml
conda activate spark-ray-a3
```

The file includes OpenJDK 17 for Spark, plus PySpark, Ray Data, pandas, NumPy and PyArrow. The VM export warned that its pip dependencies could not be reliably locked by Conda; recreating this curated file has not been tested here.

## Reproduce the recorded workflow

Run from the repository root in the activated environment. The recorded VM address was `192.168.64.2`; replace it consistently if reproducing on another machine. Use the prepared data paths above. Keep enough disk space for output and spill, and run Spark and Ray separately.

The commands below reproduce the workflow and generate new results. Preserve the original evidence separately before a rerun, because the scripts overwrite result files and Spark output.

### Spark: master, two workers and pipeline

Prepare directories:

```bash
mkdir -p results output/spark output/spark_native \
  spark-local/driver spark-local/worker1 spark-local/worker2
```

Terminal 1 - master:

```bash
SPARK_LOCAL_IP=192.168.64.2 \
spark-class org.apache.spark.deploy.master.Master \
  --host 192.168.64.2 --port 7077 --webui-port 8080
```

Terminal 2 - first worker:

```bash
SPARK_LOCAL_IP=192.168.64.2 \
SPARK_WORKER_CORES=1 \
SPARK_WORKER_MEMORY=768m \
SPARK_LOCAL_DIRS="$PWD/spark-local/worker1" \
spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port 8081 spark://192.168.64.2:7077
```

Terminal 3 - second worker:

```bash
SPARK_LOCAL_IP=192.168.64.2 \
SPARK_WORKER_CORES=1 \
SPARK_WORKER_MEMORY=768m \
SPARK_LOCAL_DIRS="$PWD/spark-local/worker2" \
spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port 8082 spark://192.168.64.2:7077
```

Confirm two ALIVE workers in the master UI, then run the UDF pipeline in Terminal 4:

```bash
SPARK_LOCAL_IP=192.168.64.2 spark-submit \
  --master spark://192.168.64.2:7077 \
  --driver-memory 512m \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=512m \
  --conf spark.cores.max=2 \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.local.dir="$PWD/spark-local/driver" \
  spark_clean.py 2>&1 | tee results/spark_run.log
```

For the native-expression experiment, use the identical submission settings with `spark_clean_native.py` and `tee results/spark_native_run.log`. The scripts record time after Spark initialization and stop the timer after the Parquet write. The native variant writes `output/spark_native/cleaned_taxi` and `results/spark_native_total_time.txt`.

### Ray: cluster and pipeline

After stopping Spark, start the supplied cluster launcher in Terminal 1:

```bash
python ray_cluster.py
```

Leave that terminal open. In Terminal 2:

```bash
python ray_clean.py 2>&1 | tee -a results/ray_run.log
```

The pipeline connects with `ray.init(address="auto")`. It processes files in month order and writes to `output/ray/cleaned_taxi/YYYY-MM/`.

**Resume behavior:** any existing Parquet file makes the script skip that month and record `runtime_seconds=0.0` in the session CSV. This is not a completion-marker check. Interrupted/partial months therefore require inspection and removal before resuming; the experiment handled this manually. The final session's timing/CSV alone cannot reconstruct all previous work. Preserve per-month logs across sessions.

### UI evidence

Recorded VM endpoints:

- Spark Master: `http://192.168.64.2:8080`.
- Spark application jobs: `http://192.168.64.2:4040` while an application is running.
- Ray Dashboard: `http://192.168.64.2:8265`.

| Evidence file | What it establishes |
|---|---|
| [Spark Master](screenshots/spark_master_2_workers.png) | Two ALIVE Spark workers, each 1 core / 768 MiB advertised |
| [Ray cluster](screenshots/ray_cluster_2_workers.png) | One head plus two ALIVE worker nodes; earlier configuration |
| [Spark job UI](screenshots/spark_job_ui.png) | Native rerun activity, two executors, active Parquet job |
| [Ray jobs](screenshots/ray_jobs_active.png) | `python ray_clean.py` running |
| [Ray Overview](screenshots/ray_overview_active.png) | Three active nodes; 1/2 logical CPUs, 320 MiB object-store capacity; missing time-series charts |
| [Spark resource sample](screenshots/spark_resource_usage.png) | A `top` sample from the native attempt that later failed for disk capacity |
| [Ray actors diagnostic](screenshots/ray_actors_diagnostic.png) | Diagnostic snapshot; not evidence of a peak metric |

Original screenshot names and hashes are recorded in [the manifest](screenshots/evidence_manifest.json). The transferred screenshots did not include a final Ray terminal completion image; the real pasted completion text is in `results/ray_completion_terminal_excerpt.txt`.

## Benchmark results

These are evidence from the student's VM runs, not newly executed measurements during packaging.

| Experiment / calculation | Result | Scope |
|---|---:|---|
| Spark Python UDF | **612.13 s** | One successful ingestion-to-export interval; saved precision 612.133718 s |
| Spark native expression | **570.36 s** | One successful rerun with the supplied native variant |
| Ray reconstructed successful processing | **2262.83 s** | Sum across 35 completed months using mixed timing boundaries |
| Ray final resumed session | **1554.14 s** | 1554.136178 s in captured output; excludes earlier completed months |
| Spark UDF-associated whole-pipeline difference | **41.77 s / 7.32%** | Difference between successful UDF and native runs; not a direct UDF-only measurement |
| Native / UDF comparison | **1.073x** | UDF runtime divided by native runtime |
| Reconstructed Ray / Spark-UDF ratio | **3.697x**, approximately **3.70x** | Descriptive comparison, not identical timer boundaries |
| Spark UDF output rows | **121,692,576** | Captured Parquet metadata check; 32 files |
| Ray output rows | **121,692,576** | Captured Parquet metadata check; 35 files |

Ray reconstruction:

- February-November 2023: **709.74 s**, recovered successful dataset execution intervals.
- December 2023-December 2025: **1553.09 s**, 25 `Completed` per-month wall-clock lines.
- **709.74 + 1553.09 = 2262.83 s**. The [reconstruction CSV](results/ray_monthly_reconstruction.csv) has 35 unique months; its rows sum to 121,692,576.

The total excludes failed/retried work, restart downtime, inter-session gaps and some driver overhead. It is not uninterrupted full operational elapsed time. Spark was the practical winner for this constrained VM and these implementations, with the qualifications detailed in the report.

## Pipeline parity and limitations

Both implementations use nine source fields, five required-null fields and 17 output fields. They preserve literal `N/A` lookup text and perform two left joins followed by miles divided by duration in hours. However:

- Spark deduplicates globally across the selected dataset. Ray uses pandas deduplication within each monthly block; global cross-month deduplication is not established.
- Spark duration uses integer-second timestamp casts; pandas uses elapsed fractional seconds. Value-level parity has not been proven.
- Row counts match and inspected schemas have the same names/numeric types. Spark text appears as Arrow `string`; Ray text as `large_string`. Equal counts and inspected schemas are not exact record equality.
- Native output equality was not separately validated. Raw output was deleted to reclaim disk during the experiment.
- Optional-field nulls remain. Timestamp text and zone categories need model-specific treatment before training.
- Ray's one-block monthly execution restricted concurrency even though two worker CPUs were available. Global shuffle/dedup/join trials were replaced by the completed monthly/broadcast-style approach.
- Failures included storage exhaustion, GCS connection failures and Raylet termination. The saved logs do not prove OOM as the cause of every termination.

**Resource evidence:** the Spark screenshot shows 51.4% user + 9.0% system CPU, 2641.8/3380.7 MiB RAM used and 2273.6/3892.0 MiB swap used. These are sampled host values from a failed native attempt, not peaks or measurements of the original UDF run. Ray dashboard logical resources/object-store occupancy are not physical RAM. No verified run-wide CPU/RAM peaks or isolated Spark/Ray custom-function durations were retained.

## AI-first versus BI-first

For this BI-style tabular ETL workload, Spark is the supported choice from the observed results. Ray is a design option for Python model preprocessing, inference actors and flexible AI tasks. No model/GPU workload was tested, so the AI-first recommendation is not a benchmark claim.

## Performance Tuning Note and AI assistance

ChatGPT assisted with implementation/translation, broadcast-style pandas lookup joins, monthly one-block Ray processing, resumptions, spill paths, memory/object-store guidance, the timestamp cast fix and the Spark native-expression experiment. These suggestions sought successful execution under tight capacity, at the cost of reduced Ray parallelism and weaker global parity. No isolated before/after speedup for those optimizations was measured.

I ran the commands and checks manually in the VM and supplied logs/screenshots. The LLM also supplied cluster/network setup guidance and launch commands; this is disclosed rather than described as independently designed setup.  No synthetic performance values are included.



## Supplementary fixed-subset validation (5 October 2026)

The supplementary `quick_check.py` run used one saved 200,000-row subset of an
original monthly input file and retained the submitted transformations. All
three outputs contained 197,338 rows. Full-row multiset comparisons across all
17 columns, preserving duplicate counts, passed for Spark UDF/native and Spark
UDF/Ray with zero unmatched row instances in both directions. These results
validate this subset only; the full-data limitations above still apply.

| Variant | Pipeline seconds | Materialized feature-stage median seconds | Host CPU maximum observed (%) | Host RAM maximum observed (MiB) |
|---|---:|---:|---:|---:|
| Spark UDF | 21.892369 | 3.045933 | 99.505 | 2917.418 |
| Spark native | 15.953042 | 0.780352 | 99.000 | 2791.570 |
| Ray | 10.245127 | 0.385089 | 98.515 | 2664.785 |

Feature timings cover a materialized projection from cached joined input (one
warmup and three timed trials), including framework overhead. They are not pure
Python-body timings. Pipeline timings are one ingestion-to-export run per
variant after engine initialization. Whole-VM monitoring uses a nominal
0.5-second interval; RAM is MemTotal minus MemAvailable. These are sampled
subset maxima, not original full-run or instantaneous peaks.

The supplied terminal captures are preserved in
[the Spark output](results/quick_check_spark_terminal.txt) and
[the Ray/comparison output](results/quick_check_ray_and_compare_terminal.txt).
The original VM metrics, logs, instrumented source views and resource traces
are included under
[`results/supplementary/20261004T232008998532Z/`](results/supplementary/20261004T232008998532Z/).
Its [evidence audit](results/supplementary/20261004T232008998532Z/EVIDENCE_AUDIT.md)
documents the trace and timing checks. The generated Parquet outputs were not
in the archive, so the reported row-level comparison could not be rerun on
this host. The report distinguishes the full-data Spark result from Ray's
faster matched-subset result.
