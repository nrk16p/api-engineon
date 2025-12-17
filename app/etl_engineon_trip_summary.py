# app/etl_engineon_trip_summary.py
import pandas as pd
import numpy as np
from pymongo import MongoClient, ReplaceOne
from typing import Optional
import warnings

warnings.filterwarnings("ignore")

DB_ATMS = "atms"
DB_ANALYTICS = "analytics"

COL_DRIVER_COST = "driver_cost_ticket"
COL_VEHICLE_MASTER = "vehiclemaster"
COL_ENGINE_ON = "summary_engineon"
COL_OUTPUT = "engineon_trip_summary"

FUEL_RATE = {
    "Mixer 10 ล้อ": 2.0,
    "Mixer 6 ล้อ": 1.0,
}


def safe_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, dayfirst=True, errors="coerce")


def to_hms(total_minutes: Optional[float]) -> Optional[str]:
    if pd.isna(total_minutes):
        return None
    sec = int(round(float(total_minutes) * 60))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}"


def clean_plate(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace("สบ.", "", regex=False)
        .str.replace("สบ", "", regex=False)
        .str.strip()
    )


def load_mongo_data(mongo_uri: str):
    client = MongoClient(mongo_uri)
    df_driver_cost = pd.DataFrame(list(client[DB_ATMS][COL_DRIVER_COST].find()))
    df_vehicle = pd.DataFrame(list(client[DB_ATMS][COL_VEHICLE_MASTER].find()))
    df_engineon = pd.DataFrame(list(client[DB_ANALYTICS][COL_ENGINE_ON].find()))
    return df_driver_cost, df_vehicle, df_engineon


def aggregate_driver_cost(df: pd.DataFrame) -> pd.DataFrame:
    df["ออก LDT_fmt"] = safe_to_datetime(df["ออก LDT_fmt"])
    df["หัว"] = clean_plate(df["หัว"])
    return (
        df.groupby(["หัว", "พจส1", "ออก LDT_fmt"], as_index=False)["LDT_unique_count"]
        .sum()
    )


def aggregate_engineon(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = safe_to_datetime(df["date"])
    df["ทะเบียนพาหนะ"] = clean_plate(df["ทะเบียนพาหนะ"])
    agg = (
        df.groupby(["ทะเบียนพาหนะ", "date", "version_type"], as_index=False)
        ["total_engine_on_min"]
        .sum()
    )
    agg["Duration_str"] = agg["total_engine_on_min"].apply(to_hms)
    return agg


def calculate_fuel(df: pd.DataFrame) -> pd.DataFrame:
    df["สำรองเวลาโหลด"] = df["#trip"] * 30
    df["ส่วนต่าง"] = df["TotalMinutes"] - df["สำรองเวลาโหลด"]
    df["ส่วนต่าง_hhmm"] = df["ส่วนต่าง"].apply(to_hms)
    df["จำนวนลิตร"] = (
        df["ประเภทยานพาหนะ"].map(FUEL_RATE).fillna(0)
        * (df["ส่วนต่าง"] / 60)
    )
    df["จำนวนลิตร"] = df["จำนวนลิตร"].clip(lower=0).round(2)
    return df


def build_engineon_trip_summary(
    mongo_uri: str,
    year: int,
    month: int,
    version_type: Optional[str] = None,
) -> pd.DataFrame:

    df_driver, df_vehicle, df_engineon = load_mongo_data(mongo_uri)

    df_driver["ออก LDT_fmt"] = safe_to_datetime(df_driver["ออก LDT_fmt"])
    df_engineon["date"] = safe_to_datetime(df_engineon["date"])

    df_driver = df_driver[
        (df_driver["ออก LDT_fmt"].dt.year == year)
        & (df_driver["ออก LDT_fmt"].dt.month == month)
    ]
    df_engineon = df_engineon[
        (df_engineon["date"].dt.year == year)
        & (df_engineon["date"].dt.month == month)
    ]

    if version_type:
        df_engineon = df_engineon[df_engineon["version_type"] == version_type]

    driver_agg = aggregate_driver_cost(df_driver)
    engine_agg = aggregate_engineon(df_engineon)

    merged = driver_agg.merge(
        engine_agg,
        left_on=["หัว", "ออก LDT_fmt"],
        right_on=["ทะเบียนพาหนะ", "date"],
        how="outer",
    )

    merged["TruckPlateNo"] = merged["หัว"].combine_first(merged["ทะเบียนพาหนะ"])
    merged["Date"] = merged["ออก LDT_fmt"].combine_first(merged["date"])

    merged = merged.drop(columns=["หัว", "ทะเบียนพาหนะ", "ออก LDT_fmt", "date"])

    merged = merged.rename(
        columns={
            "พจส1": "Supervisor",
            "LDT_unique_count": "#trip",
            "total_engine_on_min": "TotalMinutes",
        }
    )

    df_vehicle["ทะเบียน"] = clean_plate(df_vehicle["ทะเบียน"])
    merged = merged.merge(
        df_vehicle,
        left_on="TruckPlateNo",
        right_on="ทะเบียน",
        how="left",
    )

    merged = calculate_fuel(merged)

    merged["_id"] = (
        merged["TruckPlateNo"]
        + "_"
        + merged["Date"].dt.strftime("%Y-%m-%d")
    )

    merged["year"] = year
    merged["month"] = month

    return merged.reset_index(drop=True)


def save_engineon_trip_summary(mongo_uri: str, df: pd.DataFrame):
    if df.empty:
        return 0

    client = MongoClient(mongo_uri)
    col = client[DB_ANALYTICS][COL_OUTPUT]

    ops = [
        ReplaceOne({"_id": r["_id"]}, r, upsert=True)
        for r in df.to_dict("records")
    ]
    col.bulk_write(ops, ordered=False)
    return len(ops)
