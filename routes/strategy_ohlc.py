from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import datetime, timezone
from logger_setup import logger  
import pandas as pd
from database import get_finsage_db
from services.strategy_ohlc_service import get_strategy_ohlc

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


@router.get("/strategies/mtm")
def get_strategy_mtm(
    strategy_name: str,
    from_ts: int = Query(None, alias="from"),
    to_ts: int = Query(None, alias="to"),
    count_back: int = Query(None, alias="countBack"),
    db=Depends(get_db)
    ):
    """Generate OHLC from CumulativePnl (15-min candles) using pandas for speed"""
    return get_strategy_ohlc(strategy_name, db)