"""Exercise the actual API without a listening socket, using an isolated test DB."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from fastapi.testclient import TestClient
from davano.app import create_app

request = json.load(sys.stdin)
with TestClient(create_app(db_path=request["db_path"])) as client:
    response = client.request(request.get("method", "GET"), request["path"], json=request.get("body"))
    print(json.dumps({"status": response.status_code, "body": response.json()}))
