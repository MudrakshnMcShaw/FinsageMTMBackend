from fastapi import HTTPException, APIRouter, UploadFile, Depends, Query
from fastapi.responses import JSONResponse
from bson import ObjectId
import pandas as pd
import json
import datetime
from io import StringIO
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

# @router.post("/file/upload")
# async def upload_file(file: UploadFile, db=Depends(get_db)):
#     files_collection = db.files
#     timeseries_collection = db.timeseries_mtm

#     if not file.filename.endswith(".csv"):
#         raise HTTPException(status_code=400, detail="Only CSV files allowed")

#     try:
#         csv_content = (await file.read()).decode("utf-8")
#         df = pd.read_csv(StringIO(csv_content))

#         # Insert metadata
#         file_doc = {
#             "filename": file.filename,
#             "content_type": file.content_type,
#             "upload_date": datetime.datetime.utcnow(),
#             "total_rows": len(df),
#         }
#         result = files_collection.insert_one(file_doc)
#         file_id = result.inserted_id

#         # Convert rows to dicts
#         records = df.to_dict(orient="records")

#         batch = []
#         for r in records:
#             # Convert to timestamp
#             try:
#                 ts = datetime.datetime.strptime(
#                     f"{r['date']} {r['time']}",
#                     "%Y-%m-%d %H:%M:%S"
#                 )
#             except:
#                 ts = None

#             doc = {
#                 "file_id": file_id,
#                 **r,
#                 "timestamp": ts
#             }

#             batch.append(doc)

#             # When batch full → insert
#             if len(batch) >= BATCH_SIZE:
#                 timeseries_collection.insert_many(batch)
#                 batch = []

#         # insert remaining rows
#         if batch:
#             timeseries_collection.insert_many(batch)

#         return {"file_id": str(file_id), "rows": len(df)}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

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


# @router.get("/file/{file_id}/mtm")
# def get_mtm_from_file(file_id: str, db=Depends(get_db)):
#     """Fetch MTM timeseries data for a given uploaded file ID"""
#     try:
#         logger.info(f"Fetching MTM data for file_id: {file_id}")
        
#         # Load data into Pandas DataFrame
#         cursor = (
#             # timeseries_collection.find(
#             db.timeseries_mtm.find(
#                 {"file_id": ObjectId(file_id)},
#                 {"_id": 0, "timestamp": 1, "CumulativePnl": 1}
#             )
#             .sort("timestamp", 1)
#         )


@router.get("/file/{file_id}/mtm")
def get_mtm_from_file(
    file_id: str,
    from_ts: int = Query(..., alias="from"),   # Unix seconds
    to_ts: int = Query(..., alias="to"),
    db=Depends(get_db)
):
    try:

        cursor = db.timeseries_mtm.find({
            "file_id": ObjectId(file_id),
            # "timestamp": {"$lte": to_ts}  # or adjust based on your field
        }).sort("timestamp", 1)

        df = pd.DataFrame(list(cursor))
        if df.empty:
            return []

        # Convert dataframe to UNIX timestamp
        # df["time"] = (df["timestamp"].astype("int64") // 10**9 -19800) * 1000
        df["time"] =  (df["timestamp"]) * 1000
        # Compute OHLC using vectorized ops
        df["open"] = df["CumulativePnl"].shift(1).fillna(df["CumulativePnl"])

        df["close"] = df["CumulativePnl"]

        df["high"] = df[["open", "close"]].max(axis=1)
        df["low"] = df[["open", "close"]].min(axis=1)

        out = df[["time", "open", "high", "low", "close"]].to_dict(orient="records")
        logger.info(f"Generated {len(out)} OHLC records for file_id: {file_id}")

        return out
    except Exception as e:
        logger.error(f"Error while fetching MTM data for file_id {file_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
