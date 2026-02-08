import requests
import json
from datetime import datetime
from pathlib import Path

URL = "https://opensky-network.org/api/states/all"

def run_bronze_ingestion(**context):
    # Fetch data
    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    # Create timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    # Correct Path usage
    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    # Save JSON file
    with open(path, "w") as f:
        json.dump(data, f)

    # Push file path to XCom
    context["ti"].xcom_push(key="bronze_file", value=str(path))
