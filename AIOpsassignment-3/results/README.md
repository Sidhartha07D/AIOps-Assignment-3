# Results and evidence

The original full-data run files in this directory came from the Ubuntu VM.
`spark_total_time.txt` records 612.133718 s; `spark_native_total_time.txt`
records 570.360325 s. `ray_total_time.txt` records 1554.136178 s for the
last resumed session only. `ray_reconstructed_time.txt` records 2262.830000 s
for 35 successful months using mixed timing boundaries; see
`ray_monthly_reconstruction.csv` and the report. The original validation files
record 121,692,576 rows for Spark and Ray, without proving full-record parity.

`*_terminal_excerpt.txt` files reproduce the user's pasted terminal messages;
they are distinct from the original `.log` files. `benchmark_summary.txt` is a
derived overview, not a new benchmark. The earlier Ray execution evidence was
recovered from the supplied conversation and is labelled accordingly.

`supplementary/20261004T232008998532Z/` contains the original quick-check
archive contents: input manifest, three instrumented script views, logs,
timing JSON and text files, sampled resource traces, inspected output schemas,
and the comparison summary. Its [`EVIDENCE_AUDIT.md`](supplementary/20261004T232008998532Z/EVIDENCE_AUDIT.md)
records what was checked after receipt. The original output Parquet files were
not included in that archive; the reported exact subset comparison was
generated in the VM by `quick_check.py` and could not be rerun on this host.

The supplementary check covers 200,000 input rows from one monthly file.
It does not establish parity or resource peaks for all 35 months.
