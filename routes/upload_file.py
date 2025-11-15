from fastapi import HTTPException, APIRouter, UploadFile
from fastapi.responses import JSONResponse
from db import files_collection, timeseries_collection
from config import get_infra_tools_db
from bson import ObjectId
import pandas as pd
from io import StringIO
import datetime
from logger_setup import logger

router = APIRouter(prefix="/api", tags=["file"])

BATCH_SIZE = 5000   # Insert 5000 rows at a time (best performance)
db = get_infra_tools_db()

@router.post("/file/upload")
async def upload_file(file: UploadFile):
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
def list_uploaded_files():
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
def get_mtm_from_file(file_id: str):
    """Fetch MTM timeseries data for a given uploaded file ID"""
    try:
        logger.info(f"Fetching MTM data for file_id: {file_id}")

        cursor = (
            db.timeseries_mtm.find(
                {"file_id": ObjectId(file_id)}, {"_id": 0, "timestamp": 1, "new_cum_sum_mtm": 1}
            )
            .sort("timestamp", 1)
        )

        ohlc_data = []
        prev_close = None
        count = 0

        for record in cursor:
            dt = record["timestamp"]   
            unix_time = int(dt.timestamp()) # Convert to UNIX timestamp (epoch time)

            mtm_value = record.get("new_cum_sum_mtm", 0)

            open_val = prev_close if prev_close is not None else mtm_value
            close_val = mtm_value
            high_val = max(open_val, close_val)
            low_val = min(open_val, close_val)

            ohlc_data.append({
                "time": unix_time,
                "open": open_val,
                "high": high_val,
                "low": low_val,
                "close": close_val
            })

            prev_close = close_val
            count += 1
        logger.info(f"Fetched {count} MTM records successfully for file_id: {file_id}")
        return ohlc_data
    except Exception as e:
        logger.error(f"Error while fetching MTM data for file_id {file_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))