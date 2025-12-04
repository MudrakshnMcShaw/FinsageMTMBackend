from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import datetime
from logger_setup import logger  
import pandas as pd
from database import get_finsage_db

router = APIRouter(prefix="/api", tags=["strategies"])

# In your routers
def get_db():
    try:
        db = get_finsage_db()
        return db
    except Exception as e:
        logger.error(f"DB unavailable: {e}")
        raise HTTPException(
            status_code=503,
            detail="Finsage Database is down"
        )

@router.get("/strategies")
def get_strategies(db=Depends(get_db)):
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
def get_strategy_mtm(strategy_name: str, db=Depends(get_db)):
    """Generate OHLC from CumulativePnl (15-min candles) using pandas for speed"""
    try:
        logger.info(f"Fetching MTM data for strategy: {strategy_name}")

        # ---- 1. Load data into Pandas DataFrame directly ---- #
        cursor = db.strategies_mtm_data.find(
            {
                "strategy": strategy_name,
            },
            {"_id": 0, "Date": 1, "CumulativePnl": 1}
        )
# @router.get("/strategies/{strategy_name}/mtm")
# def get_strategy_mtm(
#     strategy_name: str,
#     from_: int = Query(..., alias="from"),
#     to: int = Query(..., alias="to"),
#     db=Depends(get_db)
# ):
#     try:
#         from_sec = from_
#         to_sec = to

#         cursor = db.strategies_mtm_data.find({
#             "strategy": strategy_name,
#             "Date": {
#                 "$gte": pd.Timestamp(from_sec, unit='s', tz='UTC'),
#                 "$lte": pd.Timestamp(to_sec, unit='s', tz='UTC')
#             }
#         }, {"_id": 00, "Date": 1, "CumulativePnl": 1}).sort("Date", 1)
        
        df = pd.DataFrame(list(cursor))
        # df.to_csv('sorted.csv')
        if df.empty:
            return []
            
        # ---- 2. Convert datetime to UNIX timestamp ---- #
        df["time"] = (df["Date"].astype("int64") // 10**9 -19800) * 1000   # Faster than .timestamp()
        
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