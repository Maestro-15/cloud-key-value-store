We have updated our server.py to use http, and the benchmark to use xxhash.

These were our results:-\
Starting KV store benchmark\
Nodes: ['http://127.0.0.1:8001', 'http://127.0.0.1:8002', 'http://127.0.0.1:8003']\
Threads: 48\
Total operations: 50000\
Workload: 50% set, 40% get, 10% delete

Final Results:\
Successful operations: 49956\
Failed operations: 44\
Total time: 8.0175 seconds\
Throughput: 6230.83 operations per second\
Average Latency: 5.3388 ms\
Error Rate: 0.088 %  

##

Starting KV store benchmark\
Nodes: ['http://127.0.0.1:8001', 'http://127.0.0.1:8002', 'http://127.0.0.1:8003']\
Threads: 48\
Total operations: 50000\
Workload: 50% set, 40% get, 10% delete

Final Results:\
Successful operations: 49953\
Failed operations: 47\
Total time: 7.2188 seconds\
Throughput: 6919.83 operations per second\
Average Latency: 6.7324 ms\
Error Rate: 0.094 %  


##

Starting KV store benchmark\
Nodes: ['http://127.0.0.1:8001', 'http://127.0.0.1:8002', 'http://127.0.0.1:8003']\
Threads: 48\
Total operations: 50000\
Workload: 50% set, 40% get, 10% delete

Final Results:\
Successful operations: 50000\
Failed operations: 0\
Total time: 7.2804 seconds\
Throughput: 6867.73 operations per second\
Average Latency: 6.9607 ms\
Error Rate: 0.0 %  




## Requirements

- Python 3.8+
- xxhash

Install dependencies:

pip install -r requirements.txt
(Only xxhash for this iteration).


## How to run it

Open 4 separate terminal windows. In each one, run the following commands in order (one command per window):-
python server.py 8001
python server.py 8002
python server.py 8003
python benchmark.py
