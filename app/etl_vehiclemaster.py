import requests
import warnings
import urllib3
import pandas as pd
import logging
from pathlib import Path
from typing import Optional
from pymongo import MongoClient

import requests
import warnings
import urllib3
import pandas as pd
import logging
from pathlib import Path
from typing import Optional
from pymongo import MongoClient

# ── GLOBAL SETTINGS ───────────────────────────────────────────────────────────
warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# ── MAIN FUNCTION ─────────────────────────────────────────────────────────────
def run_vehiclemaster(
    phpsessid: str,
    base_url: str,
    mongo_uri: str,
    db_name: str,
    collection_name: str
) -> Optional[pd.DataFrame]:
    """
    Fetch vehicle master table from mena-atms, save as JSON,
    and upload to MongoDB.

    Args:
        phpsessid (str): PHP session ID from logged-in mena-atms.com
        base_url (str): The export page URL (mena-atms vehicle index)
        mongo_uri (str): MongoDB connection string
        db_name (str): Target MongoDB database
        collection_name (str): Target MongoDB collection
    Returns:
        Optional[pd.DataFrame]: Cleaned vehicle master DataFrame
    """
    # --- Path detection (serverless vs local) ---
    if Path("/tmp").exists():
        save_dir = Path("/tmp") / "data"
    else:
        save_dir = Path.cwd() / "data"
    save_dir.mkdir(parents=True, exist_ok=True)

    headers = {
        "Referer": base_url,
        "Cookie": f"PHPSESSID={phpsessid}",
        "User-Agent": "Mozilla/5.0",
    }

    # ── STEP 1: Fetch Data ────────────────────────────────────────────────────
    try:
        resp = requests.get(base_url, headers=headers, verify=False, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"❌ Failed to fetch vehicle master: {e}")
        return None

    # Ensure proper encoding for Thai characters
    if not resp.encoding:
        resp.encoding = resp.apparent_encoding

    # ── STEP 2: Parse HTML tables ─────────────────────────────────────────────
    try:
        tables = pd.read_html(resp.text, displayed_only=False)
    except ValueError:
        logging.error("❌ No HTML tables found in vehicle master response.")
        return None

    if not tables:
        logging.warning("⚠️ No tables parsed from vehicle master HTML.")
        return None

    # Pick the largest table (most likely the main one)
    df = max(tables, key=lambda t: t.shape[0] * t.shape[1])

    # ── STEP 3: Clean Data ────────────────────────────────────────────────────
    df.columns = df.columns.map(lambda c: str(c).strip())
    df = df.loc[:, ~df.columns.astype(str).str.contains(r"^Unnamed", case=False)]
    df = df.astype(str)

    #keep_cols = ["ทะเบียน", "เลขรถ", "ประเภทรถร่วม", "ประเภทยานพาหนะ", "ประเภทยานพาหนะเพิ่มเติม"]
    #existing_cols = [col for col in keep_cols if col in df.columns]
    df 

    # ── STEP 4: Save locally ──────────────────────────────────────────────────
    filename = save_dir / "vehicleMaster.json"
    df.to_json(filename, orient="records", force_ascii=False)
    logging.info("✅ Saved vehicle master to %s", filename)

    # ── STEP 5: Save to MongoDB (replace existing) ────────────────────────────
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Remove all old data before inserting
        delete_result = collection.delete_many({})
        logging.info("🧹 Removed %d old records from %s.%s",
                     delete_result.deleted_count, db_name, collection_name)

        # Insert new records
        records = df.to_dict(orient="records")
        if records:
            collection.insert_many(records)
            logging.info("✅ Inserted %d new records into MongoDB (%s.%s)",
                         len(records), db_name, collection_name)
        else:
            logging.warning("⚠️ No records to insert")

    except Exception as e:
        logging.error(f"❌ MongoDB insert failed: {e}")

    return df


