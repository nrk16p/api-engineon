# app/config.py
import os
from dotenv import load_dotenv

load_dotenv()  # 👈 โหลด .env

MONGO_URI = os.getenv("MONGO_URI")

DB_TERMINUS = os.getenv("DB_TERMINUS", "terminus")
DB_ATMS = os.getenv("DB_ATMS", "atms")
DB_ANALYTICS = os.getenv("DB_ANALYTICS", "analytics")

if not MONGO_URI:
    raise RuntimeError("❌ MONGO_URI is not set in .env")
