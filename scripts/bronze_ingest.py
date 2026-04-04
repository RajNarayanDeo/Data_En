import requests
import json
import os
import logging
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
def ingest_bronze(**kwargs):
    """
    Fetches real-time flight state vectors from the OpenSky Network API.
    Saves the raw response as a timestamped JSON file in /data/bronze/.
    Pushes the saved file path to XCom for the next task.
    OpenSky 'state vectors': one record per aircraft tracked by the global
    ADS-B receiver network. Each vector contains position, speed, and status.
    **kwargs: Airflow injects the task execution context here automatically.
    """
    execution_date = kwargs['execution_date']
    end_time = int(execution_date.timestamp())
    begin_time = int((execution_date - timedelta(minutes=30)).timestamp())
    logger.info(f"Fetching OpenSky data: window {begin_time} → {end_time}")
    url = "https://opensky-network.org/api/states/all"
  
    params = {'time': end_time}
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    bronze_dir = '/opt/airflow/data/bronze'
    os.makedirs(bronze_dir, exist_ok=True)
    timestamp = execution_date.strftime('%Y%m%d%H%M%S')
    bronze_path = f'{bronze_dir}/flights_{timestamp}.json'
    with open(bronze_path, 'w') as f:
        json.dump(data, f)
    state_count = len(data.get('states') or [])
    logger.info(f"Bronze saved: {bronze_path} | {state_count} flight states")
    kwargs['ti'].xcom_push(key='bronze_path', value=bronze_path)
    return bronze_path
 
