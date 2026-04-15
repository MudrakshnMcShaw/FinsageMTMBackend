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

def normalize_to_min(ts):
    return ts - (ts % 60)

def normalize_to_min_ceil(ts):
    reminder = ts % 60
    if reminder == 0:
        return ts
    return ts + (60 - reminder)

@router.post("/file/upload")
async def upload_file(file: UploadFile, db=Depends(get_db)):
    files_collection = db.files
    timeseries_collection = db.timeseries_mtm

    if not file.filename.endswith((".csv", ".json")):
        raise HTTPException(status_code=400, detail="Only CSV and JSON files allowed")

    try:
        file_content = await file.read()
        
        # Determine file type and parse accordingly
        if file.filename.endswith(".csv"):
            df = pd.read_csv(StringIO(file_content.decode("utf-8")))
            records = df.to_dict(orient="records")
            row_count = len(df)

        elif file.filename.endswith(".json"):
            json_data = json.loads(file_content.decode("utf-8"))

            if "mtm" not in json_data:
                raise HTTPException(
                    status_code=400,
                    detail="JSON must contain 'mtm' key with array of records"
                )

            records = []
            for item in json_data["mtm"]:
                record = {
                    "Date": item.get("Date"),
                    "CumulativePnl": item.get("CumulativePnl"),
                }
                records.append(record)

            row_count = len(records)

        for i, r in enumerate(records):
            pnl = r.get("CumulativePnl")
            if pnl is None or pd.isna(pnl):
                logger.error("User tried to upload currupt file")
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid CumulativePnl at row {i+1}"
                )
            
        # Insert metadata
        file_doc = {
            "filename": file.filename,
            "content_type": file.content_type,
            "upload_date": datetime.datetime.utcnow(),
            "total_rows": row_count,
            "file_type": "json" if file.filename.endswith(".json") else "csv",
        }
        result = files_collection.insert_one(file_doc)
        file_id = result.inserted_id

        batch = []
        for r in records:
            # Create timestamp directly from "Date"
            raw_date = r.get("Date")
            try:
                dt = datetime.datetime.strptime(r["Date"], "%Y-%m-%d %H:%M:%S")
                epoc_time = int(dt.timestamp())
            except:
                dt = None
                epoc_time = None

            doc = {
                "file_id": file_id,
                "timestamp": epoc_time,
                "Date": raw_date,
                "CumulativePnl": r.get("CumulativePnl"),
            }

            batch.append(doc)

            if len(batch) >= BATCH_SIZE:
                timeseries_collection.insert_many(batch)
                batch = []

        if batch:
            timeseries_collection.insert_many(batch)


        return {
            "file_id": str(file_id), 
            "rows": row_count,
            "file_type": file_doc["file_type"]
        }

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/file")
def list_uploaded_files(db=Depends(get_db)):
    files_collection = db.files
    """List all uploaded files with metadata"""
    try:
        files = list(files_collection.find(
            {"file_type": "csv"}, 
            {"_id": 1, "filename": 1, "upload_date": 1, "total_rows": 1}))
        for f in files:
            f["file_id"] = str(f["_id"])
            del f["_id"]
        return files
    except Exception as e:
        logger.error(f"Error while listing uploaded files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/find_file")
def get_file_by_id(
    file_id: str,
    db=Depends(get_db)):
    files_collection = db.files
    """List all uploaded files with metadata"""
    try:
        file = files_collection.find_one(
            {"_id": ObjectId(file_id)}, 
            {"_id": 1, "filename": 1,}
            )
        
        return file["filename"]
    except Exception as e:
        logger.error(f"Error while listing uploaded files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/json-file")
def list_uploaded_json(db=Depends(get_db)):
    files_collection = db.files
    """List all json files"""
    try:
        files = list(files_collection.find(
            {"file_type": "json"},
            {"_id": 1, "filename": 1}
            ))
        for f in files:
            f["file_id"] = str(f["_id"])
            del f["_id"]
        return files
    except Exception as e:
        logger.error(f"Error while listing uploaded files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/file/{file_id}/mtm")
def get_mtm_from_file(
    file_id: str,
    from_ts: int = Query(None, alias="from"), 
    to_ts: int = Query(None, alias="to"),      
    count_back: int = Query(None, alias="countBack"),
    db=Depends(get_db)
):
    return get_file_ohlc(file_id, db)

@router.delete("/file/{file_id}")
def delete_file(file_id: str, db=Depends(get_db)):
    files_collection = db.files
    timeseries_collection = db.timeseries_mtm

    try:
        oid = ObjectId(file_id)
    except:
        raise HTTPException(status_code=400, detail="Invalid file_id format")

    # Check if file exists
    file_doc = files_collection.find_one({"_id": oid})
    if not file_doc:
        raise HTTPException(status_code=404, detail="File not found")

    # Delete file metadata
    files_collection.delete_one({"_id": oid})

    # Delete associated timeseries rows
    result = timeseries_collection.delete_many({"file_id": oid})

    return {
        "message": "File deleted successfully",
        "file_id": file_id,
        "deleted_timeseries_rows": result.deleted_count
    }
