from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
from bson import json_util
import json
from config import get_db

router = APIRouter(prefix="/api/strategies", tags=["strategies"])
db = get_db()


@router.get("/")
def get_strategies():
    """Fetch all available strategies"""
    try:
        data = list(db.strategies.find({}, {"_id": 0, "strategy": 1, "segment": 1, "type": 1}))
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# @router.get("/{strategy_name}/mtm")
# def get_strategy_mtm(
#     strategy_name: str,
#     start: str | None = Query(None, description="Start date (YYYY-MM-DD)"),
#     end: str | None = Query(None, description="End date (YYYY-MM-DD)"),
#     limit: int = Query(10000, description="Maximum number of records to fetch (default 10k)")
# ):
#     """Generate OHLC data from mtmPnl series (15-min candles) efficiently"""
#     try:
#         # Build query filters dynamically
#         query = {"strategy": strategy_name}
#         if start and end:
#             query["Date"] = {
#                 "$gte": datetime.fromisoformat(start),
#                 "$lte": datetime.fromisoformat(end)
#             }

#         # Fetch only necessary fields
#         cursor = (
#             db.strategies_mtm_data.find(query, {"_id": 0, "Date": 1, "mtmPnl": 1})
#             .sort("Date", 1)
#             .limit(limit)
#         )

#         ohlc_data = []
#         prev_close = None

#         # Stream directly from cursor (no full list load)
#         for record in cursor:
#             mtm_value = record.get("mtmPnl", 0)
#             timestamp = record["Date"]

#             open_val = prev_close if prev_close is not None else mtm_value
#             close_val = mtm_value
#             high_val = max(open_val, close_val)
#             low_val = min(open_val, close_val)

#             ohlc_data.append({
#                 "time": timestamp,
#                 "open": open_val,
#                 "high": high_val,
#                 "low": low_val,
#                 "close": close_val
#             })

#             prev_close = close_val

#         return ohlc_data

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

@router.get("/{strategy_name}/mtm")
def get_strategy_mtm(strategy_name: str):
    """Generate OHLC data from mtmPnl series (15-min candles) efficiently"""
    try:
        # Query all records for the given strategy
        cursor = (
            db.strategies_mtm_data.find({"strategy": strategy_name}, {"_id": 0, "Date": 1, "mtmPnl": 1})
            .sort("Date", 1)
        )

        ohlc_data = []
        prev_close = None

        # Stream directly from cursor (no full list load)
        for record in cursor:
            mtm_value = record.get("mtmPnl", 0)
            timestamp = record["Date"]

            open_val = prev_close if prev_close is not None else mtm_value
            close_val = mtm_value
            high_val = max(open_val, close_val)
            low_val = min(open_val, close_val)

            ohlc_data.append({
                "time": timestamp,
                "open": open_val,
                "high": high_val,
                "low": low_val,
                "close": close_val
            })

            prev_close = close_val

        return ohlc_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

"""Logic Output:
[
  {"Date": {"$date": "2020-01-02T09:15:00.000Z"}, "mtmPnl": -128088.9},
  {"Date": {"$date": "2020-01-02T09:30:00.000Z"}, "mtmPnl": -130687.9}
]
Response Output:
[
  {
    "time": "2020-01-02T09:15:00.000Z",
    "open": -128088.9,
    "high": -128088.9,
    "low": -128088.9,
    "close": -128088.9
  },
  {
    "time": "2020-01-02T09:30:00.000Z",
    "open": -128088.9,
    "high": -128088.9,
    "low": -130687.9,
    "close": -130687.9
  }
]

"""