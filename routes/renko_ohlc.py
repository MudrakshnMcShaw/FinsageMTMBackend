from fastapi import HTTPException, APIRouter, UploadFile, Depends, Query
from fastapi.responses import JSONResponse
from bson import ObjectId
import pandas as pd
import json
import datetime
from io import StringIO
from logger_setup import logger
from database import get_infra_db
from services.file_ohlc import get_file_ohlc
from helpers.make_renko import generate_renko

router = APIRouter(prefix="/api", tags=["renko"])

BATCH_SIZE = 5000   # Insert 5000 rows at a time (best performance)
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

def get_collections(db=Depends(get_db)):
    return {
        "files": db.files,
        "timeseries": db.timeseries_mtm
    }

@router.get("/get-renko")
def make_renko_chart(
    file_id: str,
    brick_type: str,
    method: str,
    value: float,
    db=Depends(get_db)
):
    ohlc = get_file_ohlc(file_id, db)
    df = pd.DataFrame(ohlc)
    logger.info(df.info())
    logger.info(df.head(10))
    renko_df, brick_size = generate_renko(df, brick_type, method, value)
    out = renko_df[["time", "open", "high", "low", "close"]].to_dict("records")
    # logger.info(out)

    return out