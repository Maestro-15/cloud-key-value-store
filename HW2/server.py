from fastapi import FastAPI, HTTPException
import threading
import json
import time
import bisect
import requests
import os
import xxhash
import logging
import sys

app = FastAPI()

# Env. Config. Stuff.
# PORT = int(os.getenv("PORT", 8001))
PORT = int(sys.argv[-1])

NODES = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003"
]

SELF = f"http://localhost:{PORT}"

# Logging.
#logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# Consistent Hashing w/ xxHash.
class ConsistentHash:
    def __init__(self, nodes, replicas=100):
        self.replicas = replicas
        self.ring = []
        self.node_map = {}

        for node in nodes:
            self.add_node(node)

    def hash(self, key):
        return xxhash.xxh64(key).intdigest()

    def add_node(self, node):
        for i in range(self.replicas):
            h = self.hash(f"{node}:{i}")
            self.ring.append(h)
            self.node_map[h] = node
        self.ring.sort()

    def get_node(self, key):
        h = self.hash(key)
        idx = bisect.bisect(self.ring, h)
        if idx == len(self.ring):
            idx = 0
        return self.node_map[self.ring[idx]]

hash_ring = ConsistentHash(NODES)

# Store for doing disk writes in batches.
store = {}

# Lock by key not globally like before.
NUM_LOCKS = 256
locks = [threading.Lock() for _ in range(NUM_LOCKS)]

def get_lock(key):
    return locks[hash(key) % NUM_LOCKS]

# Keep the thingy open instead of having to handshake each request.
session = requests.Session()

# The disk writes.
DATA_FILE = f"data_{PORT}.json"
data_changed_bool = False

# Log file.
LOG_FILE = f"log_{PORT}.txt"

def log_write(op, key, value=None):
    with open(LOG_FILE, "a") as f:
        if op == "PUT":
            f.write(f"PUT {key} {value}\n")
        elif op == "DELETE":
            f.write(f"DELETE {key}\n")

def load_from_disk():
    global store
    store = {}

    try:
        with open(LOG_FILE, "r") as f:
            for line in f:
                parts = line.strip().split()

                if parts[0] == "PUT":
                    _, key, value = parts
                    store[key] = value
                elif parts[0] == "DELETE":
                    _, key = parts
                    store.pop(key, None)
    except FileNotFoundError:
        pass

# def load_from_disk():
#     global store
#     try:
#         with open(DATA_FILE, "r") as f:
#             store = json.load(f)
#     except FileNotFoundError:
#         store = {}

# def save_to_disk():
#     with open(DATA_FILE, "w") as f:
#         json.dump(store, f)

# def save_bg():
#     global data_changed_bool
#     while True:
#         time.sleep(2)
#         if data_changed_bool:
#             try:
#                 save_to_disk()
#                 data_changed_bool = False
#             except:
#                 pass

# threading.Thread(target=save_bg, daemon=True).start()
load_from_disk()

# Methods for Local Server w/o needing to route.
@app.post("/_local/{key}")
def local_put(key: str, body: dict):
    global data_changed_bool
    with get_lock(key):
        log_write("PUT", key, body["value"])
        store[key] = body["value"]
        data_changed_bool = True
    return {"status": "stored"}

@app.get("/_local/{key}")
def local_get(key: str):
    with get_lock(key):
        val = store.get(key)
    if val is None:
        raise HTTPException(status_code=404)
    return {"value": val}

@app.delete("/_local/{key}")
def local_delete(key: str):
    global data_changed_bool
    with get_lock(key):
        if key not in store:
            raise HTTPException(status_code=404)
        log_write("DELETE", key)
        del store[key]
        data_changed_bool = True
    return {"status": "deleted"}

# Routing.
def forward(node, method, key, body=None):
    try:
        if method == "GET":
            return session.get(f"{node}/_local/{key}", timeout=2).json()
        elif method == "POST":
            return session.post(f"{node}/_local/{key}", json=body, timeout=2).json()
        elif method == "DELETE":
            return session.delete(f"{node}/_local/{key}", timeout=2).json()
    except Exception as e:
        raise HTTPException(500, f"Forward failed: {e}")

# Making the calls to the FastAPI thingy.
@app.post("/{key}")
def put(key: str, body: dict):
    node = hash_ring.get_node(key)

    if node == SELF:
        return local_put(key, body)

    return forward(node, "POST", key, body)


@app.get("/{key}")
def get(key: str):
    node = hash_ring.get_node(key)

    if node == SELF:
        return local_get(key)

    return forward(node, "GET", key)


@app.delete("/{key}")
def delete(key: str):
    node = hash_ring.get_node(key)

    if node == SELF:
        return local_delete(key)

    return forward(node, "DELETE", key)