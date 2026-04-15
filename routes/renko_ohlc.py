from fastapi import HTTPException, APIRouter, UploadFile, Depends, Query
from fastapi.responses import JSONResponse
from bson import ObjectId
import pandas as pd
import numpy as np
import json
import datetime
from io import StringIO
from logger_setup import logger
from pydantic import BaseModel
from typing import List
from database import get_infra_db, get_finsage_db
from services.file_ohlc import get_file_ohlc
from services.strategy_ohlc_service import get_strategy_ohlc
from services.portfolio_ohlc_service import get_portfolio_ohlc
from helpers.make_renko import generate_renko

import psutil, os

def log_mem(stage):
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / (1024 ** 2)
    print(f"{stage}: {mem:.2f} MB")

router = APIRouter(prefix="/api", tags=["renko"])

BATCH_SIZE = 5000   # Insert 5000 rows at a time (best performance)

class RenkoRequest(BaseModel):
    brick_type: str
    method: str
    value: float
    ohlc: List[dict]

# In your routers
def get_db():
    try:
        db = get_infra_db()
        return db
    except Exception as e:
        logger.error(f"DB unavailable: {e}")
        raise HTTPException(
            status_code=503,
            detail="Infra tools Database is down"
        )

def get_fin_db():
    try:
        db = get_finsage_db()
        return db
    except Exception as e:
        logger.error(f"DB unavailable: {e}")
        raise HTTPException(
            status_code=503,
            detail="Finsage Database is down"
        )

def get_collections(db=Depends(get_db)):
    return {
        "files": db.files,
        "timeseries": db.timeseries_mtm
    }

@router.get("/get-renko")
def make_renko_chart(
    brick_type,
    method,
    value: float,
    type,
    name,
    margin: float
):  
    ohlc_data = []
    # get the ohlc data first
    if type == 'strategy':
        db = get_fin_db()
        ohlc_data = get_strategy_ohlc(name, db)
    elif type == 'portfolio':
        db = get_fin_db()
        ohlc_data = get_portfolio_ohlc(name, db)
    elif type == 'file':
        db = get_db()
        ohlc_data = get_file_ohlc(name, db)

    df = pd.DataFrame(ohlc_data)

    renko_df, brick_size = generate_renko(df, brick_type, method, value, margin)

    renko_df["date"] = pd.to_datetime(renko_df["date"])

    # Convert to ms for TradingView (consistent with all other endpoints)
    renko_df["time"] = renko_df["date"].astype("int64") // 10**6

    # ✅ Readable IST date string for display
    renko_df["date"] = (
        renko_df["date"]
        .dt.tz_localize("UTC")
        .dt.tz_convert("Asia/Kolkata")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )    
    out = renko_df[["date", "time", "open", "high", "low", "close"]].to_dict("records")

    return out