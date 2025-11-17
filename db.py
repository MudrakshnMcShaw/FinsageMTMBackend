# db.py
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from logger_setup import logger

load_dotenv()
MONGO_URL = os.environ.get("MONGO_URL_INFRA_TOOLS", "mongodb://localhost:27017")

DB_NAME = 'FinSageAI_V2_Files'
COLLECTION_NAME = 'files'  # Single collection


try:
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]
    files_collection = db[COLLECTION_NAME]
    timeseries_collection = db["timeseries_mtm"]

    # Ensure collection exists
    if COLLECTION_NAME not in db.list_collection_names():
        db.create_collection(COLLECTION_NAME)

except Exception as e:
    logger.error("Failed to connect MongoDB", exc_info=True)
    raise e