# app/schemas.py
from pydantic import BaseModel
from typing import Optional

class EngineOnRunRequest(BaseModel):
    start_date: str
    end_date: str
    max_distance: int = 200
    save_raw: bool = True
    save_summary: bool = True
    parallel_dates: bool = False
    max_workers: int = 4
    debug_vehicle: Optional[str] = None


class EngineOnRunResponse(BaseModel):
    status: str
    message: str

class DriverCostRunRequest(BaseModel):
    year: str            # "2025"
    month: str           # "12"
    phpsessid: str = "nn0jiufk4njcd956rovb0isk8u"
    base_url: str = "https://www.mena-atms.com"
    index_url: str = "https://www.mena-atms.com/cms/file/index"
    db_name: str = "atms"
    collection_name: str = "driver_cost_ticket"
    
class VehicleMasterRunRequest(BaseModel):
    phpsessid: str = "nn0jiufk4njcd956rovb0isk8u"
    base_url: str = (
        "https://www.mena-atms.com/veh/vehicle/index.export/"
        "?page=1&order_by=v.code%20asc&search-toggle-status=&order_by=v.code%20asc"
    )
    db_name: str = "atms"
    collection_name: str = "vehiclemaster"
    
class EngineOnTripSummaryRunRequest(BaseModel):
    year: int
    month: int
    version_type: Optional[str] = None