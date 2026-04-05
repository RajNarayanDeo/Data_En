import pandas as pd
# Pandas: the core Python library for tabular data manipulation.
# The DataFrame (2D table with named columns) is this pipeline's primary structure.

import json, os, logging
from datetime import datetime

logger = logging.getLogger(__name__)


# OpenSky state vectors are ordered lists — not dicts with named keys.
# This mapping translates list positions into human-readable column names.
OPENSKY_COLUMNS = [
    'icao24',           # [0]  ICAO 24-bit hex transponder address (unique per aircraft)
    'callsign',         # [1]  Airline callsign / flight number (e.g., 'AI101   ')
    'origin_country',   # [2]  Country of aircraft registration
    'time_position',    # [3]  Unix timestamp of last position update
    'last_contact',     # [4]  Unix timestamp of last ADS-B signal received
    'longitude',        # [5]  WGS-84 longitude (decimal degrees)
    'latitude',         # [6]  WGS-84 latitude (decimal degrees)
    'baro_altitude',    # [7]  Barometric altitude (metres)
    'on_ground',        # [8]  Boolean: True if the aircraft is on the ground
    'velocity',         # [9]  Ground speed (metres per second)
    'true_track',       # [10] Heading clockwise from North (decimal degrees)
    'vertical_rate',    # [11] Climb/descent rate (m/s; positive = climbing)
    'sensors',          # [12] Receiver IDs that contributed data
    'geo_altitude',     # [13] GPS-based altitude (metres)
    'squawk',           # [14] Transponder squawk code
    'spi',              # [15] Special purpose indicator flag
    'position_source',  # [16] 0=ADS-B, 1=ASTERIX, 2=MLAT
]
# Storing this as a module-level constant makes it easy to update if
# OpenSky ever adds or reorders fields in their API response.


def transform_silver(**kwargs):
    """
    Transforms the Bronze raw JSON into a cleaned Silver CSV.
    Selects: icao24, origin_country, velocity, on_ground.
    These four fields are all that's needed for Gold-layer KPI aggregation.
    """

    ti = kwargs['ti']
   
    bronze_path = ti.xcom_pull(
        task_ids='bronze_ingest',
        key='bronze_path'
    )

    if not bronze_path:
        raise ValueError("XCom key 'bronze_path' not found. Did bronze_ingest complete?")
    
    with open(bronze_path, 'r') as f:
        raw_data = json.load(f)
    
    states = raw_data.get('states', [])
    
    if not states:
        logger.warning("No flight states found. Silver transform skipped.")
        return
        
    df = pd.DataFrame(states, columns=OPENSKY_COLUMNS)
    logger.info(f"DataFrame built: {len(df)} rows × {len(df.columns)} columns")
    df = df[['icao24', 'origin_country', 'velocity', 'on_ground']]
    df = df.dropna(subset=['origin_country'])
    df['velocity'] = pd.to_numeric(df['velocity'], errors='coerce')
    df['on_ground'] = df['on_ground'].astype(bool)
    df['ingestion_date'] = datetime.utcnow().strftime('%Y-%m-%d')
   
    silver_dir = '/opt/airflow/data/silver'
    os.makedirs(silver_dir, exist_ok=True)
    date_str = kwargs['execution_date'].strftime('%Y%m%d')
    
    silver_path = f'{silver_dir}/flights_silver_{date_str}.csv'
    df.to_csv(silver_path, index=False)
    logger.info(f"Silver saved: {silver_path} ({len(df)} rows)")
    ti.xcom_push(key='silver_path', value=silver_path)
    return silver_path
