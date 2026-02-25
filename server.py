from fastapi import FastAPI, HTTPException
import threading
import logging
import json

app = FastAPI()

#logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

#storage
store = {}
store_lock = threading.Lock()

DATA_FILE = "data.json"

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

load_from_disk()

#logic
def put(key, value):
    with store_lock:
        store[key] = value
        save_to_disk()

def get(key):
    with store_lock:
        return store.get(key, None)

def delete(key):
    with store_lock:
        if key not in store:
            return False
        del store[key]
        save_to_disk()
        return True

#api
@app.get("/{key}")
def http_get(key: str):
    value = get(key)
    logging.info("GET key=%s hit=%s", key, value is not None)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"value": value}

@app.post("/{key}")
def http_put(key: str, body: dict):
    if "value" not in body:
        raise HTTPException(status_code=400, detail="Missing value")
    put(key, body["value"])
    logging.info("PUT key=%s", key)
    return {"status": "stored"}

@app.delete("/{key}")
def http_delete(key: str):
    success = delete(key)
    logging.info("DEL key=%s existed=%s", key, success)
    if not success:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"status": "deleted"}