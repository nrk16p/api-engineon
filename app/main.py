# app/main.py
from fastapi import FastAPI, BackgroundTasks, HTTPException
from datetime import datetime
from pymongo import MongoClient

from app.schemas import EngineOnRunRequest, EngineOnRunResponse
from app.config import MONGO_URI, DB_TERMINUS, DB_ATMS, DB_ANALYTICS
from app.etl_engineon import process_engineon_data_optimized
from app.etl_drivercost import run_drivercost
from app.schemas import DriverCostRunRequest
from app.etl_vehiclemaster import run_vehiclemaster
from app.schemas import VehicleMasterRunRequest
from app.etl_engineon_trip_summary import (
    build_engineon_trip_summary,
    save_engineon_trip_summary,
)
from app.schemas import EngineOnTripSummaryRunRequest
# --------------------------------------------------
# App
# --------------------------------------------------
app = FastAPI(
    title="Engine-On ETL API",
    version="1.1.0"
)

# --------------------------------------------------
# Mongo (job status)
# --------------------------------------------------
mongo_client = MongoClient(MONGO_URI)
job_col = mongo_client[DB_ANALYTICS]["etl_jobs"]


# --------------------------------------------------
# Health check
# --------------------------------------------------
@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "engineon-etl"}


# --------------------------------------------------
# Background ETL runner (with status tracking)
# --------------------------------------------------
def run_etl_job(job_id: str, payload: EngineOnRunRequest):
    start_ts = datetime.utcnow()

    try:
        process_engineon_data_optimized(
            mongo_uri=MONGO_URI,
            db_terminus=DB_TERMINUS,
            db_atms=DB_ATMS,
            db_analytics=DB_ANALYTICS,
            start_date=payload.start_date,
            end_date=payload.end_date,
            max_distance=payload.max_distance,
            save_raw=payload.save_raw,
            save_summary=payload.save_summary,
            parallel_dates=payload.parallel_dates,
            max_workers=payload.max_workers,
            debug_vehicle=payload.debug_vehicle,
        )

        end_ts = datetime.utcnow()
        job_col.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "success",
                    "end_time": end_ts.isoformat(),
                    "duration_sec": int((end_ts - start_ts).total_seconds()),
                }
            },
        )

    except Exception as e:
        end_ts = datetime.utcnow()
        job_col.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "failed",
                    "end_time": end_ts.isoformat(),
                    "duration_sec": int((end_ts - start_ts).total_seconds()),
                    "error": str(e),
                }
            },
        )


# --------------------------------------------------
# Run ETL (trigger)
# --------------------------------------------------
@app.post("/engineon/run")
def run_engineon_etl(
    payload: EngineOnRunRequest,
    background_tasks: BackgroundTasks,
):
    now = datetime.utcnow()
    job_id = f"engineon_{now.strftime('%Y-%m-%d_%H%M%S')}"

    # create job record
    job_col.insert_one(
        {
            "_id": job_id,
            "job_type": "engineon",
            "status": "running",
            "start_date": payload.start_date,
            "end_date": payload.end_date,
            "start_time": now.isoformat(),
            "end_time": None,
            "duration_sec": None,
            "error": None,
        }
    )

    background_tasks.add_task(run_etl_job, job_id, payload)

    return {
        "status": "accepted",
        "job_id": job_id,
        "message": f"ETL started for {payload.start_date} → {payload.end_date}",
    }


# --------------------------------------------------
# Job status
# --------------------------------------------------
@app.get("/engineon/status/{job_id}")
def get_engineon_status(job_id: str):
    job = job_col.find_one({"_id": job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
def run_drivercost_job(job_id: str, payload: DriverCostRunRequest):
    start_ts = datetime.utcnow()

    try:
        df = run_drivercost(
            year=payload.year,
            month=payload.month,
            phpsessid=payload.phpsessid,
            base_url=payload.base_url,
            index_url=payload.index_url,
            mongo_uri=MONGO_URI,
            db_name=payload.db_name,
            collection_name=payload.collection_name,
        )

        end_ts = datetime.utcnow()
        job_col.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "success",
                    "end_time": end_ts.isoformat(),
                    "duration_sec": int((end_ts - start_ts).total_seconds()),
                    "records": 0 if df is None else len(df),
                }
            },
        )

    except Exception as e:
        end_ts = datetime.utcnow()
        job_col.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "failed",
                    "end_time": end_ts.isoformat(),
                    "duration_sec": int((end_ts - start_ts).total_seconds()),
                    "error": str(e),
                }
            },
        )
@app.post("/drivercost/run")
def run_drivercost_etl(
    payload: DriverCostRunRequest,
    background_tasks: BackgroundTasks,
):
    now = datetime.utcnow()
    job_id = f"drivercost_{now.strftime('%Y-%m-%d_%H%M%S')}"

    job_col.insert_one(
        {
            "_id": job_id,
            "job_type": "drivercost",
            "status": "running",
            "year": payload.year,
            "month": payload.month,
            "start_time": now.isoformat(),
            "end_time": None,
            "duration_sec": None,
            "records": None,
            "error": None,
        }
    )

    background_tasks.add_task(run_drivercost_job, job_id, payload)

    return {
        "status": "accepted",
        "job_id": job_id,
        "message": f"Driver cost ETL started for {payload.month}/{payload.year}",
    }

@app.get("/drivercost/status/{job_id}")
def get_drivercost_status(job_id: str):
    job = job_col.find_one({"_id": job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

def run_vehiclemaster_job(job_id: str, payload: VehicleMasterRunRequest):
    start_ts = datetime.utcnow()

    try:
        df = run_vehiclemaster(
            phpsessid=payload.phpsessid,
            base_url=payload.base_url,
            mongo_uri=MONGO_URI,
            db_name=payload.db_name,
            collection_name=payload.collection_name,
        )

        end_ts = datetime.utcnow()
        job_col.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "success",
                    "end_time": end_ts.isoformat(),
                    "duration_sec": int((end_ts - start_ts).total_seconds()),
                    "records": 0 if df is None else len(df),
                }
            },
        )

    except Exception as e:
        end_ts = datetime.utcnow()
        job_col.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "failed",
                    "end_time": end_ts.isoformat(),
                    "duration_sec": int((end_ts - start_ts).total_seconds()),
                    "error": str(e),
                }
            },
        )
@app.post("/vehiclemaster/run")
def run_vehiclemaster_etl(
    payload: VehicleMasterRunRequest,
    background_tasks: BackgroundTasks,
):
    now = datetime.utcnow()
    job_id = f"vehiclemaster_{now.strftime('%Y-%m-%d_%H%M%S')}"

    job_col.insert_one(
        {
            "_id": job_id,
            "job_type": "vehiclemaster",
            "status": "running",
            "start_time": now.isoformat(),
            "end_time": None,
            "duration_sec": None,
            "records": None,
            "error": None,
        }
    )

    background_tasks.add_task(run_vehiclemaster_job, job_id, payload)

    return {
        "status": "accepted",
        "job_id": job_id,
        "message": "Vehicle master ETL started",
    }
    
@app.get("/vehiclemaster/status/{job_id}")
def get_vehiclemaster_status(job_id: str):
    job = job_col.find_one({"_id": job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job  

def run_engineon_trip_summary_job(job_id: str, payload: EngineOnTripSummaryRunRequest):
    start_ts = datetime.utcnow()
    try:
        df = build_engineon_trip_summary(
            mongo_uri=MONGO_URI,
            year=payload.year,
            month=payload.month,
            version_type=payload.version_type,
        )
        n = save_engineon_trip_summary(MONGO_URI, df)

        end_ts = datetime.utcnow()
        job_col.update_one(
            {"_id": job_id},
            {"$set": {
                "status": "success",
                "end_time": end_ts.isoformat(),
                "duration_sec": int((end_ts - start_ts).total_seconds()),
                "records": n,
            }},
        )
    except Exception as e:
        end_ts = datetime.utcnow()
        job_col.update_one(
            {"_id": job_id},
            {"$set": {
                "status": "failed",
                "end_time": end_ts.isoformat(),
                "duration_sec": int((end_ts - start_ts).total_seconds()),
                "error": str(e),
            }},
        )
@app.post("/engineon-trip-summary/run")
def run_engineon_trip_summary(
    payload: EngineOnTripSummaryRunRequest,
    background_tasks: BackgroundTasks,
):
    now = datetime.utcnow()
    job_id = f"engineon_trip_{now.strftime('%Y-%m-%d_%H%M%S')}"

    job_col.insert_one(
        {
            "_id": job_id,
            "job_type": "engineon_trip_summary",
            "status": "running",
            "year": payload.year,
            "month": payload.month,
            "start_time": now.isoformat(),
            "end_time": None,
            "duration_sec": None,
            "records": None,
            "error": None,
        }
    )

    background_tasks.add_task(run_engineon_trip_summary_job, job_id, payload)

    return {
        "status": "accepted",
        "job_id": job_id,
        "message": f"EngineOn-Trip summary started for {payload.month}/{payload.year}",
    }
@app.get("/engineon-trip-summary/status/{job_id}")
def get_engineon_trip_summary_status(job_id: str):
    job = job_col.find_one({"_id": job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
