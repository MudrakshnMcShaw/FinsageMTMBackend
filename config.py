from pymongo import MongoClient
from dotenv import load_dotenv
import os
load_dotenv()

MONGO_URL_FINSAGE_V2 = os.getenv("MONGO_URL_FINSAGE_V2", "mongodb://localhost:27017")
DB_NAME = "FinSageAI_V2"

MONGO_URL_INFRA_TOOLS = os.getenv("MONGO_URL_INFRA_TOOLS", "mongodb://localhost:27017")
DB_NAME_INFRA = "FinSageAI_V2_Files"

def get_finsage_db():
    client = MongoClient(MONGO_URL_FINSAGE_V2)
    return client[DB_NAME]

def get_infra_tools_db():
    client = MongoClient(MONGO_URL_INFRA_TOOLS)
    return client[DB_NAME_INFRA]
