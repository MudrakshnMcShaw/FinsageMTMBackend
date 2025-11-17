from fastapi import APIRouter, HTTPException
from datetime import datetime
from config import get_finsage_db
from logger_setup import logger  
import pandas as pd

router = APIRouter(prefix="/api", tags=["strategies"])
db = get_finsage_db()


@router.get("/strategies")
def get_strategies():
    """Fetch all available strategies"""
    try:
        logger.info("Fetching list of strategies from MongoDB...")
        data = list(db.strategies.find({}, {"_id": 0, "strategy": 1, "segment": 1, "type": 1}))
        logger.info(f"Fetched {len(data)} strategies successfully.")
        return data

    except Exception as e:
        logger.error(f"Error while fetching strategies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies/{strategy_name}/mtm")
def get_strategy_mtm(strategy_name: str):
    """Generate OHLC from CumulativePnl (15-min candles) using pandas for speed"""
    try:
        logger.info(f"Fetching MTM data for strategy: {strategy_name}")

        # ---- 1. Load data into Pandas DataFrame directly ---- #
        cursor = db.strategies_mtm_data.find(
            {"strategy": strategy_name},
            {"_id": 0, "Date": 1, "CumulativePnl": 1}
        ).sort("Date", 1)

        df = pd.DataFrame(list(cursor))
        if df.empty:
            return []

        # ---- 2. Convert datetime to UNIX timestamp ---- #
        df["time"] = df["Date"].astype("int64") // 10**9 -19800   # Faster than .timestamp()
        
        # ---- 3. Compute OHLC using vectorized operations ---- #
        # OPEN = previous close or current if it's first row
        df["open"] = df["CumulativePnl"].shift(1).fillna(df["CumulativePnl"])

        # CLOSE = current CumulativePnl
        df["close"] = df["CumulativePnl"]

        # HIGH & LOW
        df["high"] = df[["open", "close"]].max(axis=1)
        df["low"]  = df[["open", "close"]].min(axis=1)
        # df.to_csv('test.csv')
        # ---- 4. Select final required columns ---- #
        out = df[["time", "open", "high", "low", "close"]].to_dict(orient="records")

        logger.info(f"Generated {len(out)} OHLC candles for {strategy_name}")

        return out

    except Exception as e:
        logger.exception(f"Error while generating OHLC for '{strategy_name}'")
        raise HTTPException(status_code=500, detail=str(e))


# @router.get("/strategies/{strategy_name}/mtms")
# def get_strategy_mtm(strategy_name: str):
#     """Generate OHLC data from CumulativePnl series (15-min candles) efficiently"""
#     try:
#         logger.info(f"Starting OHLC generation for strategy: {strategy_name}")

#         cursor = (
#             db.strategies_mtm_data.find(
#                 {"strategy": strategy_name}, {"_id": 0, "Date": 1, "CumulativePnl": 1}
#             )
#             .sort("Date", 1)
#         )

#         ohlc_data = []
#         prev_close = None
#         count = 0

#         for record in cursor:
#             # print(record)
#             dt = record["Date"]   
#             unix_time = int(dt.timestamp()) # Convert to UNIX timestamp (epoch time)

#             mtm_value = record.get("CumulativePnl", 0)
#             # timestamp = record["Date"]

#             open_val = prev_close if prev_close is not None else mtm_value
#             close_val = mtm_value
#             high_val = max(open_val, close_val)
#             low_val = min(open_val, close_val)

#             ohlc_data.append({
#                 "time": unix_time,
#                 "open": open_val,
#                 "high": high_val,
#                 "low": low_val,
#                 "close": close_val
#             })

#             prev_close = close_val
#             count += 1

#             # if count % 1000 == 0:
#                 # logger.info(f"Processed {count} MTM records so far for '{strategy_name}'")

#         logger.info(f"Completed OHLC generation for '{strategy_name}' — total {count} records processed.")
#         # logger.info(f"Generated {ohlc_data} OHLC data points for strategy '{strategy_name}'")
#         # logger.info(ohlc_data[0])
#         return ohlc_data

#     except Exception as e:
#         logger.exception(f"Error while generating OHLC for '{strategy_name}'")
#         raise HTTPException(status_code=500, detail=str(e))
