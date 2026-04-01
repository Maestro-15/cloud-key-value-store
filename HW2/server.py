from fastapi import FastAPI, HTTPException
import threading
import json
import time
import bisect
import requests
import os
import xxhash
import logging

app = FastAPI()

# Env. Config. Stuff.
PORT = int(os.getenv("PORT", 8001))

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
NUM_LOCKS = 64
locks = [threading.Lock() for _ in range(NUM_LOCKS)]

def get_lock(key):
    return locks[hash(key) % NUM_LOCKS]

# The disk writes.
DATA_FILE = f"data_{PORT}.json"
data_changed_bool = False

def load_from_disk():
    global store
    try:
        with open(DATA_FILE, "r") as f:
            store = json.load(f)
            logging.info("Loaded %d keys from disk", len(store))
    except FileNotFoundError:
        store = {}
        logging.info("No data.json found, starting empty")

def save_to_disk():
    with open(DATA_FILE, "w") as f:
        json.dump(store, f)

def save_bg():
    global data_changed_bool
    while True:
        time.sleep(1)
        if data_changed_bool:
            try:
                save_to_disk()
                data_changed_bool = False
            except:
                pass

threading.Thread(target=save_bg(), daemon=True).start()
load_from_disk()

# We need a 
def local_put(key, value):
    global data_changed_bool
    with get_lock(key):
        store[key] = value
        data_changed_bool = True

def local_get(key):
    with get_lock(key):
        return store.get(key)

def local_delete(key):
    global data_changed_bool
    with get_lock(key):
        if key not in store:
            return False
        del store[key]
        data_changed_bool = True
        return True


def route_request(method, key, body=None):
    node = hash_ring.get_node(key)

    if node == SELF:
        return None

    try:
        if method == "GET":
            return requests.get(f"{node}/{key}", timeout=10).json()
        elif method == "POST":
            return requests.post(f"{node}/{key}", json=body, timeout=10).json()
        elif method == "DELETE":
            return requests.delete(f"{node}/{key}", timeout=10).json()
    except:
        raise HTTPException(status_code=500, detail="Forwarding failed")

# Making the calls to the FastAPI thingy.
@app.get("/{key}")
def http_get(key: str):
    routed = route_request("GET", key)
    if routed is not None:
        return routed

    value = local_get(key)
    if value is None:
        logging.info("GET key=%s hit=%s", key, value is not None)
        raise HTTPException(status_code=404, detail="Key not found")

    return {"value": value}


@app.post("/{key}")
def http_put(key: str, body: dict):
    if "value" not in body:
        raise HTTPException(status_code=400, detail="Missing value")

    routed = route_request("POST", key, body)
    if routed is not None:
        return routed

    local_put(key, body["value"])
    logging.info("PUT key=%s", key)
    return {"status": "stored"}


@app.delete("/{key}")
def http_delete(key: str):
    routed = route_request("DELETE", key)
    if routed is not None:
        return routed

    success = local_delete(key)
    if not success:
        raise HTTPException(status_code=404, detail="Key not found")

    logging.info("DEL key=%s existed=%s", key, success)
    return {"status": "deleted"}