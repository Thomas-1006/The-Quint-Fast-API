from fastapi import FastAPI
from google.cloud import bigquery
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

client = bigquery.Client(
    project="growthnow"
)

@app.get("/")
def home():
    return {
        "status": "running"
    }

@app.get("/top-users")
def top_users():
    with open(
        "queries/top_users.sql",
        "r",
        encoding="utf-8"
    ) as f:
        query = f.read()
    df = client.query(
        query
    ).to_dataframe()
    return {
        "status": "success",
        "project": "growthnow",
        "last_updated":
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        "count":
            len(df),
        "data":
            df.to_dict(
                orient="records"
            )
    }

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )