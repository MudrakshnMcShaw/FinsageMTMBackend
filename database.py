import os
from pymongo import MongoClient
from dotenv import load_dotenv
from logger_setup import logger

load_dotenv()

MONGO_URL_MTM_DATA = os.getenv("MONGO_URL_FINSAGE_V2")
MONGO_URL_INFRA_TOOLS = os.getenv("MONGO_URL_INFRA_TOOLS")

mongo_clients = {}      # Healthy clients only
mongo_failed = set()    # Mark permanently failed DBs


def get_mongo_client(url: str, db_name: str):
    # Instant reject if we know it's down
    if db_name in mongo_failed:
        raise ConnectionError(f"MongoDB ({db_name}) is offline")

    # Return cached healthy client
    if db_name in mongo_clients:
        return mongo_clients[db_name]

    try:
        logger.info(f"[MongoDB] Connecting to {db_name}...")
        client = MongoClient(
            url,
            serverSelectionTimeoutMS=3000,
            connectTimeoutMS=3000,
            socketTimeoutMS=5000,
            heartbeatFrequencyMS=10000,
        )
        client.admin.command("ping")
        logger.info(f"Connected to MongoDB: {db_name}")
        mongo_clients[db_name] = client
        return client

    except Exception as e:
        logger.error(f"MongoDB connection failed ({db_name}): {e}", exc_info=True)
        mongo_failed.add(db_name)        # Remember it's dead
        raise ConnectionError(f"Cannot connect to MongoDB ({db_name})") from e


def get_finsage_db():
    client = get_mongo_client(MONGO_URL_MTM_DATA, "FinSageAI_V2")
    return client["FinSageAI_V2"]


def get_infra_db():
    client = get_mongo_client(MONGO_URL_INFRA_TOOLS, "FinSageAI_V2_Files")
    return client["FinSageAI_V2_Files"]
