from logger_setup import logger
from fastapi import HTTPException, APIRouter, UploadFile, Depends, Query
from bson import ObjectId
import pandas as pd

def get_file_ohlc(
        file_id: str,
        db
):
    try:
        cursor = db.timeseries_mtm.find(
            {"file_id": ObjectId(file_id)},
            {"timestamp": 1, "CumulativePnl": 1}
        ).sort("timestamp", 1)
        
        data = list(cursor)
        print('lenght of data before process: {len(data)}')
        df = pd.DataFrame(data)
        if df.empty:
            logger.warning("Empty dataframe, returning empty array")
            return []

        df["time"] = (df["timestamp"]) * 1000
        df["open"] = df["CumulativePnl"].shift(1).fillna(df["CumulativePnl"])
        df["close"] = df["CumulativePnl"]
        df["high"] = df[["open", "close"]].max(axis=1)
        df["low"] = df[["open", "close"]].min(axis=1)
        df.to_csv('2025.csv')

        out = df[["time", "open", "high", "low", "close"]].to_dict(orient="records")
        # logger.info(f"Generated {len(out)} OHLC records for file_id: {file_id}")
        
        return out
    except Exception as e:
        logger.error(f"Error while fetching MTM data for file_id {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))