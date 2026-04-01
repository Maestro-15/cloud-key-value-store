# Simple Single Server Key Value Store

This project implements a simple singles erver key value store that supports:

- GET – Retrieve a value by key
- PUT – Store a value by key
- DEL – Delete a key

The server is implemented using Python and FastAPI.

## Requirements

- Python 3.8+
- fastapi
- uvicorn
- requests (for benchmark)
- xxhash

Install dependencies:

```bash
pip install -r requirements.txt

```

## How to run it

python -m venv venv  
venv\Scripts\activate  
python -m uvicorn server:app --port 800x --workers 1  
pything benchmark.py  
Run all of these commands again for each desired port.  
Current implementation supports ports 8001, 8002, 8003.  
Make sure to delete ports from node list depending on how many servers you want to run at a time.  
Make sure to delete the log files between runs and ctrl-c + uvicorn again between runs.
