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

Install dependencies:

```bash
pip install -r requirements.txt

```

## How to run it

python -m venv venv
venv\Scripts\activate
python -m uvicorn server:app --port 8001
pything benchmark.py
