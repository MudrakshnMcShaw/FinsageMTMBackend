from fastapi import HTTPException, APIRouter, UploadFile, Depends, Query, Request, Form, File
from fastapi.responses import JSONResponse
from bson import ObjectId
from typing import Optional
import pandas as pd
import json
import datetime
from io import StringIO
from logger_setup import logger
from database import get_infra_db

router = APIRouter(prefix="/api", tags=["chart_layout"])

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
        "chart_layouts": db.chart_layouts
    }

API_VERSION = "1.1"
@router.post(f"/{API_VERSION}/charts")
async def save_chart(
    request: Request,
    client_id: str = Query(..., alias="client"),
    user_id: str = Query(..., alias="user"),
    # These come either as form fields OR as JSON fields
    name: Optional[str] = Form(None),
    content: Optional[str] = Form(None),
    symbol: Optional[str] = Form(None),
    resolution: Optional[str] = Form(None),
):  
    db = get_infra_db()

    # If it was sent as JSON (some older TradingView versions / Firefox)
    if name is None and content is None:
        try:
            body = await request.json()
            logger.info(f"Received JSON body: {body}")
            name = body.get("name")
            content = body.get("content")
            symbol = body.get("symbol")
            resolution = body.get("resolution")
        except Exception as e:
            logger.warning(f"Failed to parse as JSON, falling back to form: {e}")
            raise HTTPException(status_code=400, detail="Invalid payload")

    # If still empty → TradingView sometimes sends empty multipart on init
    if not name or not content:
        logger.info("Empty or incomplete save request from TradingView (ping?)")
        return JSONResponse({"status": "ok", "id": None})

    # Validate content is proper JSON string (TradingView sends it as string)
    try:
        json.loads(content) if content else None
    except json.JSONDecodeError:
        logger.error(f"Invalid 'content' JSON from TradingView: {content[:200]}")
        raise HTTPException(status_code=400, detail="Field 'content' must be valid JSON string")

    doc = {
        "client_id": client_id,
        "user_id": user_id,
        "name": name,
        "content": content,
        "symbol": symbol or None,
        "resolution": resolution or None,
        "saved_at": datetime.datetime.utcnow(),
    }

    try:
        result = db.charts_layout.insert_one(doc)
        logger.info(f"Chart saved successfully: {name}, id={result.inserted_id}")
        return JSONResponse({"status": "ok", "id": str(result.inserted_id)})
    except Exception as e:
        logger.error(f"DB insert failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to save chart")

@router.get(f"/{API_VERSION}/charts")
async def charts_endpoint(
    client_id: str = Query(..., alias="client"),
    user_id: str = Query(..., alias="user"),
    chart: Optional[str] = Query(None, alias="chart")
):
    db = get_infra_db()
    col = db.charts_layout
    logger.info(f"Charts request: client={client_id}, user={user_id}, chart={chart}")

    if chart:
        # Load single chart
        try:
            doc = col.find_one({
                "_id": ObjectId(chart),
                "client_id": client_id,
                "user_id": user_id
            })
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid chart id")

        if not doc:
            raise HTTPException(status_code=404, detail="Chart not found")

        return JSONResponse({
            "status": "ok",
            "data": {
                "content": doc["content"],
                "name": doc["name"],
                "symbol": doc.get("symbol"),
                "resolution": doc.get("resolution", "15")
            }
        })

    else:
        # List all
        charts = []
        for doc in col.find({"client_id": client_id, "user_id": user_id}):
            charts.append({
                "id": str(doc["_id"]),
                "name": doc["name"],
                "symbol": doc.get("symbol"),
                "resolution": doc.get("resolution"),
                "timestamp": int(doc["saved_at"].timestamp())
            })
        return JSONResponse({"status": "ok", "data": charts})


@router.delete(f"/{API_VERSION}/charts")
async def delete_chart(
    client_id: str = Query(..., alias="client"),
    user_id: str = Query(..., alias="user"),
    chart: str = Query(..., alias="chart")
):
    db = get_infra_db()
    try:
        result = await db.charts_layout.delete_one({
            "_id": ObjectId(chart),
            "client_id": client_id,
            "user_id": user_id
        })
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid chart id")

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Chart not found")

    return JSONResponse({"status": "ok"})
