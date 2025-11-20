import os
import json
import tempfile
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery

# =======================
# LOAD GOOGLE CREDENTIALS
# =======================
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

# =====================
# FASTAPI INITIALIZATION
# =====================
app = FastAPI(title="Quint FastAPI", version="1.0")

# CORS CONFIG
ALLOWED_ORIGINS = [
    "https://thequint-malibu-beta.quintype.io",   # Beta
    "https://www.thequint.com",                   # Prod
    "https://thequint.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# BigQuery Config
PROJECT_ID = os.getenv("BQ_PROJECT_ID", "the-quint-282107")

# ====================
# TEST ID STORAGE FILE
# ====================
TEST_IDS_FILE = "test_ids.json"

def load_test_ids():
    try:
        with open(TEST_IDS_FILE, "r") as f:
            return json.load(f).get("ids", [])
    except:
        return []

def save_test_ids(ids):
    with open(TEST_IDS_FILE, "w") as f:
        json.dump({"ids": ids}, f)


# =====================
# BIGQUERY SQL QUERY
# =====================
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
    COUNTIF(event_name = 'page_view') AS page_views
  FROM base_events
  GROUP BY user_pseudo_id
)
SELECT
  user_pseudo_id
FROM user_summary
WHERE page_views > 0
LIMIT 10;
"""


# ====================
# HOME UI PAGE
# ====================
@app.get("/", response_class=HTMLResponse)
def home():
    try:
        with open("templates/home.html", "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(content=html)
    except Exception as e:
        return HTMLResponse(content=f"Error loading HTML file: {e}", status_code=500)


# ====================
# TEST ID API ENDPOINTS
# ====================
@app.get("/test-ids")
def get_test_ids():
    return {"ids": load_test_ids()}

@app.post("/test-ids")
def add_test_id(pid: str):
    ids = load_test_ids()
    if pid not in ids:
        ids.append(pid)
        save_test_ids(ids)
    return {"status": "added", "ids": ids}

@app.delete("/test-ids/{pid}")
def delete_test_id(pid: str):
    ids = load_test_ids()
    if pid in ids:
        ids.remove(pid)
    save_test_ids(ids)
    return {"status": "deleted", "ids": ids}


# ====================
# MAIN API ENDPOINT
# ====================
@app.get("/top-users")
def get_top_users():

    # 1. Load test IDs
    TEST_PSEUDO_IDS = load_test_ids()

    # 2. If test IDs exist → return them
    if TEST_PSEUDO_IDS:
        return {
            "status": "success",
            "count": len(TEST_PSEUDO_IDS),
            "data": [{"user_pseudo_id": pid} for pid in TEST_PSEUDO_IDS]
        }

    # 3. Else → run BigQuery logic
    try:
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(QUERY).to_dataframe()

        if df.empty:
            return JSONResponse(
                content={"message": "No data found for the given date range."},
                status_code=404,
            )

        data = df.to_dict(orient="records")
        return {"status": "success", "count": len(data), "data": data}

    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500,
        )


# ====================
# LOCAL RUN
# ====================
if __name__ == "__main__":
    import uvicorn
    print("Starting local API server at http://127.0.0.1:8000/")
    uvicorn.run("pseudoAPI:app", host="127.0.0.1", port=8000, reload=True)
