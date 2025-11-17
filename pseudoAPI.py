import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from google.cloud import bigquery

import json
import tempfile

sa_key = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
if sa_key:
    try:
        json.loads(sa_key)
        sa_path = os.path.join(tempfile.gettempdir(), "gcp_sa_key.json")
        with open(sa_path, "w") as f:
            f.write(sa_key)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
    except Exception as e:
        print("Failed to load service account key:", e)

import pandas as pd

# === CONFIG ===
PROJECT_ID = os.getenv("BQ_PROJECT_ID", "the-quint-282107")

# === INITIALIZE APP ===
app = FastAPI(title="Local BigQuery Test API", version="1.0")

# === SQL QUERY ===
QUERY = """
WITH base_events AS (
  SELECT
    e.user_pseudo_id,
    DATE(TIMESTAMP_MICROS(e.event_timestamp), "Asia/Kolkata") AS event_date,
    (SELECT ep.value.int_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'ga_session_id') AS ga_session_id,
    (SELECT ep.value.int_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'session_id') AS session_id,
    e.event_bundle_sequence_id,
    (SELECT ep.value.int_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'engagement_time_msec') AS engagement_time_msec,
    e.event_name
  FROM `the-quint-282107.analytics_241044781.events_*` e
  WHERE
    DATE(TIMESTAMP_MICROS(e.event_timestamp), "Asia/Kolkata") >= '2025-10-01'
    AND _TABLE_SUFFIX BETWEEN '20251001' AND FORMAT_DATE('%Y%m%d', CURRENT_DATE('Asia/Kolkata'))
),

user_summary AS (
  SELECT
    user_pseudo_id,
    COUNTIF(event_name = 'page_view') AS page_views,
    SUM(engagement_time_msec) AS total_engagement_time_msec,
    SAFE_DIVIDE(SUM(engagement_time_msec), COUNTIF(event_name = 'page_view')) AS engagement_rate
  FROM base_events
  GROUP BY user_pseudo_id
)

SELECT
  user_pseudo_id
FROM user_summary
WHERE page_views > 0
LIMIT 10;
"""

# === API ROUTE ===
@app.get("/")
def root():
    return {"status": "ok", "message": "API is running! Use /top-users to fetch data."}

@app.get("/top-users")
def get_top_users():


    # ==== MANUAL OVERRIDE MODE FOR TESTING ONLY ====
    # Add any pseudo IDs you want to test with
    TEST_PSEUDO_IDS = [
        "1949675162.1731393103",   # your personal pseudo id
        # "1234567890.1112131415", # example extra
        # Add more here
    ]

    # If this list is NOT empty → return these IDs only
    if TEST_PSEUDO_IDS:
        return {
            "status": "success",
            "count": len(TEST_PSEUDO_IDS),
            "data": [{"user_pseudo_id": pid} for pid in TEST_PSEUDO_IDS]
        }

    # ==== DEFAULT MODE (USES BIGQUERY) ====

    try:
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(QUERY).to_dataframe()

        if df.empty:
            return JSONResponse(
                content={"message": "No data found for the given date range."},
                status_code=404
            )

        # Convert dataframe to dict for JSON response
        data = df.to_dict(orient="records")
        return {"status": "success", "count": len(data), "data": data}

    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )


# === LOCAL ENTRY POINT ===
if __name__ == "__main__":
    import uvicorn
    print("Starting local API server at http://127.0.0.1:8000/top-users")
    uvicorn.run("pseudoAPI:app", host="127.0.0.1", port=8000, reload=True)
