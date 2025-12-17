# app/etl_drivercost.py
import requests, io, re, urllib.parse, warnings, urllib3, logging
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
from pymongo import MongoClient


import requests, io, re, urllib.parse, warnings, urllib3, logging
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
from pymongo import MongoClient

# ── GLOBAL SETTINGS ───────────────────────────────────────────────────────────
warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ── HELPERS ───────────────────────────────────────────────────────────────────
def _absolutize(href: str, base_url: str) -> str:
    """Convert relative href to absolute URL."""
    return urllib.parse.urljoin(base_url, href)

def _fetch_index_page(session: requests.Session, headers: dict, params: dict, index_url: str) -> str:
    """Fetch the HTML content of the index page."""
    r = session.get(index_url, params=params, headers=headers, verify=False, timeout=60)
    r.raise_for_status()
    return r.text

def _parse_download_links(html: str, base_url: str) -> List[Dict[str, str]]:
    """Parse HTML for downloadable report links."""
    DL_RE = re.compile(r"/cms/file/download/id/(\d+)")
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tr")

    items = []
    for tr in rows:
        tds = tr.find_all("td")
        if not tds:
            continue

        a = tds[0].find("a", href=True)
        if not a:
            continue

        m = DL_RE.search(a["href"])
        if not m:
            continue

        items.append({
            "download_id": m.group(1),
            "download_url": _absolutize(a["href"], base_url),
            "title": a.get_text(strip=True),
            "year": tds[1].get_text(strip=True) if len(tds) > 1 else "",
            "month": tds[2].get_text(strip=True) if len(tds) > 2 else "",
            "created_at": tds[3].get_text(strip=True) if len(tds) > 3 else "",
        })

    # Deduplicate
    seen, uniq = set(), []
    for it in items:
        if it["download_id"] not in seen:
            uniq.append(it)
            seen.add(it["download_id"])
    return uniq

def _download_excel(session: requests.Session, url: str, headers: dict) -> pd.DataFrame:
    """Download an Excel file and return as DataFrame."""
    r = session.get(url, headers=headers, verify=False, timeout=60)
    r.raise_for_status()
    with io.BytesIO(r.content) as f:
        return pd.read_excel(f, sheet_name=0, dtype=str, skiprows=1)


# ── MAIN FUNCTION ─────────────────────────────────────────────────────────────
def run_drivercost(
    year: str,
    month: str,
    phpsessid: str,
    base_url: str,
    index_url: str,
    mongo_uri: str,
    db_name: str,
    collection_name: str
) -> Optional[pd.DataFrame]:
    """
    Download, filter, and save driver cost report for a given year and month.
    Replace previous month's records in MongoDB (delete + insert).
    """

    # --- Save path setup ---
    base_dir = Path("/tmp") if Path("/tmp").exists() else Path.cwd()
    save_dir = base_dir / "data"
    save_dir.mkdir(parents=True, exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": index_url,
        "Cookie": f"PHPSESSID={phpsessid}",
    }
    params = {
        "type": "monthly-driver-cost",
        "use_as": "batch-report",
        "year": year,
        "month": month,
        "ref_id": "1",
        "search_fields": "year,month",
        "submit": "ค้นหา",
    }

    with requests.Session() as s:
        # Step 1: Fetch index page
        html = _fetch_index_page(s, headers, params, index_url)

        # Step 2: Parse download links
        links = _parse_download_links(html, base_url)
        if not links:
            raise RuntimeError("❌ No reports found. Check PHPSESSID or parameters.")

        logging.info("Found downloads: %s", [i["download_id"] for i in links])

        # Step 3: Download & merge Excel reports
        dfs = []
        for it in links:
            df = _download_excel(s, it["download_url"], headers)
            df["download_id"] = it["download_id"]
            df["report_title"] = it["title"]
            df["year"] = it["year"]
            df["month"] = it["month"]
            dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)

    # ── FILTER ────────────────────────────────────────────────────────────────
    target_services = [
        'Mixer UMO (CPAC) - MIXB', 'Mixer นครหลวง โม่เล็ก - MIXS',
        'Mixer นครหลวง โม่ใหญ่ - MIXB', 'Mixer ORC - MIXB',
        'Mixer CPAC โม่เล็ก - MIXS', 'Mixer CPAC (คิว) - MIXB',
        'Mixer ACON อยุธยาคอนกรีต - MIXB', 'Mixer KPAC - MIXB',
        'Mixer KPAC โม่เล็ก - MIXS', 'Mixer ฟาสท์ คอนกรีต - MIXB',
        'Mixer เอเชีย - MIXB', 'Mixer ที.เอ็น.ซีเมนต์บล็อค - MIXB',
        'Mixer มหาทรัพย์ซีเมนต์ - MIXB'
    ]

    filtered = combined[combined["บริการ"].isin(target_services)]

    result = (
        filtered.groupby(['ออก LDT', 'เลขรถ', 'หัว', 'พจส1'], as_index=False)
        .agg(LDT_unique_count=('LDT', 'nunique'))
    )

    # ── CLEANUP ───────────────────────────────────────────────────────────────
    result["หัว"] = result["หัว"].str.replace("สบ.", "", regex=False).str.strip()
    result['ออก LDT'] = pd.to_datetime(result['ออก LDT'], format='%d/%m/%Y', errors='coerce')
    result['ออก LDT_fmt'] = result['ออก LDT'].dt.strftime('%d/%m/%Y')
    result['mmyy'] = result['ออก LDT'].dt.strftime('%m/%Y')

    # ── SAVE JSON LOCALLY ─────────────────────────────────────────────────────
    filename = save_dir / f"driver_cost_{year}_{month}.json"
    result.to_json(filename, orient="records", force_ascii=False)
    logging.info("✅ Saved result to %s", filename)

    # ── SAVE TO MONGODB (delete + insert) ─────────────────────────────────────
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        records = result.to_dict(orient="records")
        if records:
            # Delete existing records for same month/year
            delete_result = collection.delete_many({"mmyy": f"{month}/{year}"})
            logging.info("🧹 Removed %d old records for %s/%s",
                         delete_result.deleted_count, month, year)

            collection.insert_many(records)
            logging.info("✅ Inserted %d new records into MongoDB (%s.%s)",
                         len(records), db_name, collection_name)
        else:
            logging.warning("⚠️ No records to insert")

    except Exception as e:
        logging.error(f"❌ MongoDB operation failed: {e}")

    return result




    if df_result is not None:
        print("✅ Completed successfully!")
    else:
        print("❌ Failed to process driver cost data.")
