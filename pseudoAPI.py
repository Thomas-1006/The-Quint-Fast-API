import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from google.cloud import bigquery
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
  user_pseudo_id,
  page_views,
  engagement_rate
FROM user_summary
WHERE page_views > 0
ORDER BY
  page_views DESC,
  engagement_rate DESC
LIMIT 10;
"""

# === API ROUTE ===
@app.get("/top-users")
def get_top_users():

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
