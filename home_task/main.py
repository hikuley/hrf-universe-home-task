from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import select
from typing import Optional

from home_task.db import get_session
from home_task.models import DaysToHireStats
from sqlalchemy.orm import Session

app = FastAPI(
    title="HRF Universe API",
    description="API for retrieving hiring statistics and metrics",
    version="1.0.0",
    docs_url="/docs",  # Swagger UI will be available at /docs
    redoc_url="/redoc"  # ReDoc will be available at /redoc
)


@app.get("/hello",
         summary="Hello World endpoint",
         description="A simple endpoint that returns a hello world message",
         response_description="Returns a hello world message")
def read_hello():
    return {"message": "Hello World"}


@app.get("/days-to-hire-stats",
         summary="Get days to hire statistics",
         description="Retrieve hiring statistics for a specific job and optionally filtered by country",
         response_description="Returns hiring statistics including min, avg, and max days to hire")
def get_days_to_hire_stats(
        standard_job_id: str,
        country_code: Optional[str] = None,
        session: Session = Depends(get_session)
):
    # If country_code is not provided, get world statistics
    query = select(DaysToHireStats).where(
        DaysToHireStats.standard_job_id == standard_job_id,
        DaysToHireStats.country_code == (country_code if country_code else None)
    )

    stats = session.execute(query).scalar_one_or_none()

    if not stats:
        raise HTTPException(
            status_code=404,
            detail=f"No statistics found for standard_job_id={standard_job_id} and country_code={country_code or 'world'}"
        )

    return {
        "standard_job_id": stats.standard_job_id,
        "country_code": stats.country_code,
        "min_days": float(stats.min_days_to_hire),
        "avg_days": float(stats.avg_days_to_hire),
        "max_days": float(stats.max_days_to_hire),
        "job_postings_number": stats.job_posting_count,
    }
