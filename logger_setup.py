# logger_setup.py
import os
import logging
from datetime import datetime

# --- Create date-based folder ---
today = datetime.now().strftime("%Y-%m-%d")
log_dir = os.path.join("logs", today)
os.makedirs(log_dir, exist_ok=True)

# --- Log file path ---
log_file = os.path.join(log_dir, "app.log")

# --- Configure logger ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# --- Create and export a logger instance ---
logger = logging.getLogger("AppLogger")