from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

import requests
import datetime as dt
import pandas as pd

def get_openaq(url, params=None):
    """
    Simple helper to call the OpenAQ v3 API with your API key.
    NOTE: you can move the API key to env/config later if needed.
    """
    headers = {
        "X-API-Key": "xxxxxx"
    }
    resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_latest_pm25() -> pd.DataFrame:
    """
    Get latest PM2.5 values for all sensors (global).
    One row per latest reading from /v3/parameters/2/latest.
    """
    base_url = "https://api.openaq.org/v3/parameters/2/latest"
    rows = []
    page = 1
    limit = 1000

    while True:
        params = {
            "limit": limit,
            "page": page,
        }
        data = get_openaq(base_url, params)
        results = data.get("results", [])
        if not results:
            break

        for r in results:
            dt_obj = r.get("datetime") or {}
            coords = r.get("coordinates") or {}

            rows.append(
                {
                    "sensors_id":   r.get("sensorsId"),
                    "locations_id": r.get("locationsId"),
                    "datetime_utc": dt_obj.get("utc"),
                    "datetime_local": dt_obj.get("local"),
                    "value_ugm3":   r.get("value"),
                    "latitude":     coords.get("latitude"),
                    "longitude":    coords.get("longitude"),
                }
            )

        if len(results) < limit:
            break
        page += 1

    return pd.DataFrame(rows)


def fetch_sensor_metadata_pm25_us() -> pd.DataFrame:
    """
    Returns a DataFrame with one row per sensor that measures PM2.5 in the US.
    Columns include sensor + location metadata (city, coords, etc.).
    """
    base_url = "https://api.openaq.org/v3/locations"
    rows = []
    page = 1
    limit = 1000

    while True:
        params = {
            "limit": limit,
            "page": page,
            "parameters_id": [2],  # PM2.5
            "iso": "US",           # United States only
        }
        data = get_openaq(base_url, params)
        results = data.get("results", [])
        if not results:
            break

        for loc in results:
            coords = loc.get("coordinates") or {}
            sensors = loc.get("sensors") or []

            for s in sensors:
                param = s.get("parameter") or {}

                rows.append(
                    {
                        # sensor-level
                        "sensors_id":      s.get("id"),
                        "parameter_name":  param.get("name"),
                        "parameter_units": param.get("units"),
                        # location-level
                        "locations_id":   loc.get("id"),
                        "location_name":  loc.get("name"),
                        "city":           loc.get("locality"),
                        "latitude":       coords.get("latitude"),
                        "longitude":      coords.get("longitude"),
                    }
                )

        if len(results) < limit:
            break
        page += 1

    df_meta = pd.DataFrame(rows)
    df_meta = df_meta.drop_duplicates(subset="sensors_id", keep="first")
    return df_meta

def schema(config):
    """
    Declare two tables:
      - airquality_latest_pm25: latest readings per sensor
      - airquality_sensor_metadata: static metadata per sensor/location
    """
    return [
        {
            "table": "airquality_latest_pm25",
            "primary_key": ["sensors_id"],
            "columns": {
                "sensors_id":   "int",
                "locations_id": "int",
                "datetime_utc": "string",
                "datetime_local": "string",
                "value_ugm3":   "float",
                "latitude":     "float",
                "longitude":    "float",
            },
        },
        {
            "table": "airquality_sensor_metadata",
            "primary_key": ["sensors_id"],
            "columns": {
                "sensors_id":      "int",
                "parameter_name":  "string",
                "parameter_units": "string",
                "locations_id":    "int",
                "location_name":   "string",
                "city":            "string",
                "latitude":        "float",
                "longitude":       "float",
            },
        },
    ]

def update(configuration: dict, state: dict):
    """
    Fivetran will call this on every sync.
    Use your fetch_latest_pm25() and fetch_sensor_metadata_pm25_us() here.
    """
    # 1) latest
    latest_df = fetch_latest_pm25()
    if not latest_df.empty:
        latest_df = (
            latest_df.sort_values("datetime_utc")
                     .drop_duplicates("sensors_id", keep="last")
        )
        for _, row in latest_df.iterrows():
            op.upsert(
                table="airquality_latest_pm25",
                data={
                    "sensors_id":   int(row["sensors_id"]) if pd.notna(row["sensors_id"]) else None,
                    "locations_id": int(row["locations_id"]) if pd.notna(row["locations_id"]) else None,
                    "datetime_utc": row["datetime_utc"],
                    "datetime_local": row["datetime_local"],
                    "value_ugm3":   float(row["value_ugm3"]) if pd.notna(row["value_ugm3"]) else None,
                    "latitude":     float(row["latitude"]) if pd.notna(row["latitude"]) else None,
                    "longitude":    float(row["longitude"]) if pd.notna(row["longitude"]) else None,
                },
            )

    # 2) metadata
    meta_df = fetch_sensor_metadata_pm25_us()
    if not meta_df.empty:
        meta_df = meta_df.drop_duplicates("sensors_id", keep="first")
        for _, row in meta_df.iterrows():
            op.upsert(
                table="airquality_sensor_metadata",
                data={
                    "sensors_id":      int(row["sensors_id"]) if pd.notna(row["sensors_id"]) else None,
                    "parameter_name":  row["parameter_name"],
                    "parameter_units": row["parameter_units"],
                    "locations_id":    int(row["locations_id"]) if pd.notna(row["locations_id"]) else None,
                    "location_name":   row["location_name"],
                    "city":            row["city"],
                    "latitude":        float(row["latitude"]) if pd.notna(row["latitude"]) else None,
                    "longitude":       float(row["longitude"]) if pd.notna(row["longitude"]) else None,
                },
            )
            
    op.checkpoint({"last_sync": pd.Timestamp.utcnow().isoformat()})
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
