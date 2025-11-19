from fastapi import HTTPException, APIRouter, UploadFile, Depends
from fastapi.responses import JSONResponse
# from db import files_collection, timeseries_collection
# from config import get_infra_tools_db
from bson import ObjectId
import pandas as pd
from io import StringIO
import datetime
from logger_setup import logger
from database import get_infra_db

router = APIRouter(prefix="/api", tags=["file"])

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

@router.post("/file/upload")
async def upload_file(file: UploadFile, db=Depends(get_db)):
    files_collection = db.files
    timeseries_collection = db.timeseries_mtm

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    try:
        csv_content = (await file.read()).decode("utf-8")
        df = pd.read_csv(StringIO(csv_content))

        # Insert metadata
        file_doc = {
            "filename": file.filename,
            "content_type": file.content_type,
            "upload_date": datetime.datetime.utcnow(),
            "total_rows": len(df),
        }
        result = files_collection.insert_one(file_doc)
        file_id = result.inserted_id

        # Convert rows to dicts
        records = df.to_dict(orient="records")

        batch = []
        for r in records:
            # Convert to timestamp
            try:
                ts = datetime.datetime.strptime(
                    f"{r['date']} {r['time']}",
                    "%Y-%m-%d %H:%M:%S"
                )
            except:
                ts = None

            doc = {
                "file_id": file_id,
                **r,
                "timestamp": ts
            }

            batch.append(doc)

            # When batch full → insert
            if len(batch) >= BATCH_SIZE:
                timeseries_collection.insert_many(batch)
                batch = []

        # insert remaining rows
        if batch:
            timeseries_collection.insert_many(batch)

        return {"file_id": str(file_id), "rows": len(df)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/file")
def list_uploaded_files(db=Depends(get_db)):
    files_collection = db.files
    """List all uploaded files with metadata"""
    try:
        files = list(files_collection.find({}, {"_id": 1, "filename": 1, "upload_date": 1, "total_rows": 1}))
        for f in files:
            f["file_id"] = str(f["_id"])
            del f["_id"]
        return files
    except Exception as e:
        logger.error(f"Error while listing uploaded files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/file/{file_id}/mtm")
def get_mtm_from_file(file_id: str, db=Depends(get_db)):
    """Fetch MTM timeseries data for a given uploaded file ID"""
    try:
        logger.info(f"Fetching MTM data for file_id: {file_id}")
        
        # Load data into Pandas DataFrame
        cursor = (
            # timeseries_collection.find(
            db.timeseries_mtm.find(
                {"file_id": ObjectId(file_id)},
                {"_id": 0, "timestamp": 1, "new_cum_sum_mtm": 1}
            )
            # .sort("timestamp", 1)
        )


        df = pd.DataFrame(list(cursor))
        if df.empty:
            return []
        
        # Convert dataframe to UNIX timestamp
        df["time"] = df["timestamp"].astype("int64") // 10**9 -19800

        # Compute OHLC using vectorized ops
        df["open"] = df["new_cum_sum_mtm"].shift(1).fillna(df["new_cum_sum_mtm"])

        df["close"] = df["new_cum_sum_mtm"]

        df["high"] = df[["open", "close"]].max(axis=1)
        df["low"] = df[["open", "close"]].min(axis=1)

        out = df[["time", "open", "high", "low", "close"]].to_dict(orient="records")
        logger.info(f"Generated {len(out)} OHLC records for file_id: {file_id}")


        return out

    except Exception as e:
        logger.error(f"Error while fetching MTM data for file_id {file_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))