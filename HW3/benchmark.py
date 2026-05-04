import bisect
import http.client
import queue
import random
import threading
import time
from urllib.parse import urlparse

import xxhash


nodes = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
    "http://127.0.0.1:8003",
]

set_ratio = 0.50
get_ratio = 0.40
del_ratio = 0.10

total_ops = 50000
thread_count = 48
keys = 50000
timeout = 2.0

jobs = queue.Queue()
times = []

success = 0
failed = 0

lock = threading.Lock()
start_sig = threading.Event()
my_thread = threading.local()


class HashRing:
    def __init__(self, node_list, copies=100):
        self.ring = []
        self.lookup = {}

        for node in node_list:
            for i in range(copies):
                h = xxhash.xxh64(node + ":" + str(i)).intdigest()
                self.ring.append(h)
                self.lookup[h] = node

        self.ring.sort()

    def get_server(self, key):
        h = xxhash.xxh64(key).intdigest()
        pos = bisect.bisect(self.ring, h)

        if pos == len(self.ring):
            pos = 0

        return self.lookup[self.ring[pos]]


def get_connections():
    if not hasattr(my_thread, "connections"):
        my_thread.connections = {}

    return my_thread.connections


def pick_op():
    x = random.random()

    if x < set_ratio:
        return "set"
    elif x < set_ratio + get_ratio:
        return "get"
    else:
        return "delete"


def send_request(ring, op, key, val, timeout):
    server = ring.get_server(key)
    connections = get_connections()

    try:
        if server not in connections:
            parsed = urlparse(server)
            connections[server] = http.client.HTTPConnection(
                parsed.hostname,
                parsed.port,
                timeout=timeout,
            )

        connection = connections[server]
        path = "/" + key

        if op == "set":
            connection.request("POST", path, body=val)

        elif op == "get":
            connection.request("GET", path)

        elif op == "delete":
            connection.request("DELETE", path)

        else:
            return False

        response = connection.getresponse()
        response.read()

        return response.status < 400

    except Exception:
        if server in connections:
            connections[server].close()
            del connections[server]
        return False


def worker(ring, timeout):
    global success
    global failed

    start_sig.wait()

    while True:
        try:
            op, key, val = jobs.get_nowait()
        except queue.Empty:
            break

        start = time.perf_counter()
        ok = send_request(ring, op, key, val, timeout)
        end = time.perf_counter()

        with lock:
            if ok:
                success += 1
                times.append(end - start)
            else:
                failed += 1

        jobs.task_done()

    if hasattr(my_thread, "connections"):
        for connection in my_thread.connections.values():
            connection.close()


if __name__ == "__main__":
    node_list = []

    for node in nodes:
        node_list.append(node.rstrip("/"))

    ring = HashRing(node_list)
    exist_key = []
    next_key = 0

    for i in range(total_ops):
        op = pick_op()

        if op == "set" or len(exist_key) == 0:
            key = "key_" + str(next_key % keys)
            next_key += 1

            if key not in exist_key:
                exist_key.append(key)

        elif op == "get":
            key = random.choice(exist_key)

        else:
            key = random.choice(exist_key)
            exist_key.remove(key)

        val = ("value_" + str(i)).encode("utf-8")
        jobs.put((op, key, val))

    print("Starting KV store benchmark")
    print("Nodes:", node_list)
    print("Threads:", thread_count)
    print("Total operations:", total_ops)
    print("Workload: 50% set, 40% get, 10% delete")
    print()

    threads = []
    start_time = time.perf_counter()
    start_sig.set()

    for i in range(thread_count):
        t = threading.Thread(target=worker, args=(ring, timeout))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    total_time = time.perf_counter() - start_time
    total_done = success + failed

    if total_time > 0:
        throughput = success / total_time
    else:
        throughput = 0

    if len(times) > 0:
        avg_latency = sum(times) / len(times)
    else:
        avg_latency = 0

    if total_done > 0:
        error_rate = (failed / total_done) * 100
    else:
        error_rate = 0

    print("Final Results:")
    print("Successful operations:", success)
    print("Failed operations:", failed)
    print("Total time:", round(total_time, 4), "seconds")
    print("Throughput:", round(throughput, 2), "operations per second")
    print("Average Latency:", round(avg_latency * 1000, 4), "ms")
    print("Error Rate:", round(error_rate, 4), "%")
