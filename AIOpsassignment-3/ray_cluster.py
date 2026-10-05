import os
import time
from ray.cluster_utils import Cluster

BASE = os.path.expanduser("~/raytmp")
TEMP_DIR = os.path.join(BASE, "session")
SPILL_DIR = os.path.join(BASE, "spill")

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(SPILL_DIR, exist_ok=True)

MiB = 1024 * 1024

cluster = Cluster(
    initialize_head=True,
    head_node_args={
        "num_cpus": 0,
        "memory": 512 * MiB,
        "object_store_memory": 80 * MiB,
        "dashboard_host": "0.0.0.0",
        "dashboard_port": 8265,
        "temp_dir": TEMP_DIR,
        "object_spilling_directory": SPILL_DIR,
    },
)

cluster.add_node(
    num_cpus=1,
    memory=650 * MiB,
    object_store_memory=120 * MiB,
    object_spilling_directory=SPILL_DIR,
)

cluster.add_node(
    num_cpus=1,
    memory=650 * MiB,
    object_store_memory=120 * MiB,
    object_spilling_directory=SPILL_DIR,
)

print("Ray cluster address:", cluster.address)
print("Dashboard: http://192.168.64.2:8265")
print("Configuration: 1 head + 2 workers")
print("Worker CPUs: 2 total")
print("Object store: 80 MiB head + 120 MiB per worker")
print("Disk spilling:", SPILL_DIR)
print("Leave this terminal running.")

while True:
    time.sleep(3600)
