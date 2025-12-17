from pymongo import MongoClient, ReplaceOne
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings("ignore")

# ============================================================
# ⚙️ Vectorized Haversine (meters)
# ============================================================
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000.0
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2.0 * R * np.arcsin(np.sqrt(a))


# ============================================================
# 🧠 Helpers
# ============================================================
def _classify_voltage_type(v):
    try:
        if str(v).strip() == "เฟิร์มแวร์ไม่รองรับ":
            return "v1"
        float(v)
        return "v2"
    except Exception:
        return None


def _classify_engine_state(v, status):
    if status != "จอดรถ":
        return "Other"
    if pd.isna(v):
        return "Unknown"
    return "Parking - Engine On" if v >= 25.0 else "Parking - Engine Off"


def _split_latlng(series):
    arr = series.astype(str).str.split(",", n=1, expand=True).to_numpy()
    lat = pd.to_numeric(arr[:, 0], errors="coerce")
    lng = pd.to_numeric(arr[:, 1], errors="coerce")
    return lat, lng


# ============================================================
# 🚀 Main ETL Function (IMPORT SAFE)
# ============================================================
def process_engineon_data_optimized(
    mongo_uri: str,
    db_terminus: str = "terminus",
    db_atms: str = "atms",
    db_analytics: str = "analytics",
    start_date: str = "01/12/2025",
    end_date: str = "01/12/2025",
    max_distance: int = 200,
    save_raw: bool = True,
    save_summary: bool = True,
    parallel_dates: bool = True,
    max_workers: int = 4,
    debug_vehicle: str | None = None,
):
    client = MongoClient(mongo_uri)
    col_log = client[db_terminus]["driving_log"]
    col_plants = client[db_atms]["plants"]
    col_raw = client[db_analytics]["raw_engineon"]
    col_sum = client[db_analytics]["summary_engineon"]

    # -------- Plants --------
    plants = pd.DataFrame(list(col_plants.find({}, {"_id": 0})))
    if plants.empty:
        raise ValueError("❌ No plant data found")

    plants["Latitude"] = pd.to_numeric(plants["Latitude"], errors="coerce")
    plants["Longitude"] = pd.to_numeric(plants["Longitude"], errors="coerce")
    plants = plants.dropna(subset=["Latitude", "Longitude"]).reset_index(drop=True)

    p_lat = plants["Latitude"].to_numpy()
    p_lng = plants["Longitude"].to_numpy()
    p_code = plants["plant_code"].to_numpy()

    # -------- Dates --------
    d0 = datetime.strptime(start_date, "%d/%m/%Y")
    d1 = datetime.strptime(end_date, "%d/%m/%Y")
    date_list = [
        (d0 + timedelta(days=i)).strftime("%d/%m/%Y")
        for i in range((d1 - d0).days + 1)
    ]

    # -------- Worker --------
    def _process_one_date(target_date: str):
        logs = list(
            col_log.find(
                {"วันที่": target_date},
                {
                    "_id": 0,
                    "ทะเบียนพาหนะ": 1,
                    "วันที่": 1,
                    "เวลา": 1,
                    "Voltage": 1,
                    "สถานะ": 1,
                    "สถานที่": 1,
                    "พิกัด": 1,
                },
            )
        )

        if not logs:
            return f"{target_date}: no data"

        df_day = pd.DataFrame(logs).dropna(subset=["ทะเบียนพาหนะ", "เวลา"])
        if df_day.empty:
            return f"{target_date}: empty"

        raw_ops, sum_ops = [], []

        for plate, dfp in df_day.groupby("ทะเบียนพาหนะ"):
            vtype = dfp["Voltage"].apply(_classify_voltage_type)
            version_type = "v1" if (vtype == "v1").any() else "v2" if (vtype == "v2").any() else None
            if not version_type:
                continue

            dt = pd.to_datetime(
                dfp["วันที่"].astype(str) + " " + dfp["เวลา"].astype(str),
                format="%d/%m/%Y %H:%M:%S",
                errors="coerce",
            )
            dfp = dfp.assign(datetime=dt).dropna(subset=["datetime"]).sort_values("datetime")

            vnum = pd.to_numeric(dfp["Voltage"], errors="coerce")
            dfp["engine_state"] = [
                _classify_engine_state(v, s)
                for v, s in zip(vnum, dfp["สถานะ"])
            ]

            dfp["prev_dt"] = dfp["datetime"].shift()
            dfp["prev_state"] = dfp["engine_state"].shift()
            dfp["prev_place"] = dfp["สถานที่"].shift()
            dfp["time_diff"] = (dfp["datetime"] - dfp["prev_dt"]).dt.total_seconds() / 60

            dfv = dfp[
                (dfp["engine_state"] == "Parking - Engine On")
                & (dfp["prev_state"] == "Parking - Engine On")
                & (dfp["สถานที่"] == dfp["prev_place"])
                & (dfp["time_diff"] > 0)
                & (dfp["time_diff"] <= 5)
            ].copy()

            if dfv.empty:
                continue

            lat, lng = _split_latlng(dfv["พิกัด"])
            dfv["lat"], dfv["lng"] = lat, lng
            dfv["prev_lat"] = dfv["lat"].shift()
            dfv["prev_lng"] = dfv["lng"].shift()
            dfv["dist"] = haversine(dfv["prev_lat"], dfv["prev_lng"], dfv["lat"], dfv["lng"])
            dfv["event_id"] = ((dfv["dist"] > max_distance) | dfv["dist"].isna()).cumsum()

            events = dfv.groupby("event_id").agg(
                total_engine_on_min=("time_diff", "sum"),
                lat=("lat", "mean"),
                lng=("lng", "mean"),
            ).reset_index()

            dist_matrix = haversine(
                events["lat"].to_numpy()[:, None],
                events["lng"].to_numpy()[:, None],
                p_lat[None, :],
                p_lng[None, :],
            )

            idx = np.nanargmin(dist_matrix, axis=1)
            min_dist = dist_matrix[np.arange(len(events)), idx]
            events["nearest_plant"] = np.where(min_dist <= max_distance, p_code[idx], None)

            date_key = datetime.strptime(target_date, "%d/%m/%Y").strftime("%Y-%m-%d")

            for i, r in events.iterrows():
                _id = f"{plate}_{date_key}_{i}"
                raw_ops.append(
                    ReplaceOne(
                        {"_id": _id},
                        {
                            "_id": _id,
                            "ทะเบียนพาหนะ": plate,
                            "date": target_date,
                            **r.to_dict(),
                        },
                        upsert=True,
                    )
                )

            valid = events[events["nearest_plant"].notna()]
            if not valid.empty:
                total_min = valid["total_engine_on_min"].sum()
                sum_ops.append(
                    ReplaceOne(
                        {"_id": f"{plate}_{date_key}"},
                        {
                            "_id": f"{plate}_{date_key}",
                            "ทะเบียนพาหนะ": plate,
                            "date": target_date,
                            "total_engine_on_min": total_min,
                            "total_engine_on_hr": total_min / 60,
                            "version_type": version_type,
                        },
                        upsert=True,
                    )
                )

        if save_raw and raw_ops:
            col_raw.bulk_write(raw_ops, ordered=False)
        if save_summary and sum_ops:
            col_sum.bulk_write(sum_ops, ordered=False)

        return f"{target_date}: raw={len(raw_ops)}, summary={len(sum_ops)}"

    if parallel_dates:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for r in as_completed([ex.submit(_process_one_date, d) for d in date_list]):
                print(r.result())
    else:
        for d in date_list:
            print(_process_one_date(d))

    print("🎉 ETL Completed")
