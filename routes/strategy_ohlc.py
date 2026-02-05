from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import datetime, timezone
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

def normalize_to_min(ts):
    return ts - (ts % 60)

def convert_to_datetime_str(ts):
    norm_ts = normalize_to_min(ts)
    dt = datetime.fromtimestamp(norm_ts, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds")

@router.get("/strategies")
def get_strategies( 
    db=Depends(get_db)):
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
def get_strategy_mtm(
    strategy_name: str,
    from_ts: int = Query(None, alias="from"),
    to_ts: int = Query(None, alias="to"),
    count_back: int = Query(None, alias="countBack"),
    db=Depends(get_db)
    ):
    """Generate OHLC from CumulativePnl (15-min candles) using pandas for speed"""
    try:
        logger.info(f"Fetching MTM data for strategy: {strategy_name}")
        # base_filter = {'strategy': strategy_name}

        # if from_ts is not None:

        #     from_dt = datetime.fromtimestamp(from_ts)
        #     base_filter.setdefault('Date', {})["$gte"] = from_dt
        #     print(f'Tradingview from: {from_dt}')

        # if to_ts is not None:
        #     to_dt = datetime.fromtimestamp(to_ts)
        #     base_filter.setdefault('Date', {})["$lt"] = to_dt
        #     print(f'Tradingview to: {to_dt}')

        # if count_back is not None:
        #     print(f'query: {base_filter}')
        #     cursor = db.strategies_mtm_data.find(base_filter, {"_id": 0, "Date": 1, "CumulativePnl": 1}).sort('Date', -1).limit(count_back)
        #     data = list(cursor)
        #     data.reverse()
        # else:
        #     cursor = db.strategies_mtm_data.find(
        #         base_filter, 
        #         {"_id": 0, "Date": 1, "CumulativePnl": 1}
        #     ).sort('Date', 1)
        #     data = list(cursor)
        # ---- 1. Load data into Pandas DataFrame directly ---- #
        cursor = db.strategies_mtm_data.find(
            {
                "strategy": strategy_name,
            },
            {"_id": 0, "Date": 1, "CumulativePnl": 1}
        )

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