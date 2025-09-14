import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
SQL_DIR = BASE_DIR / "sql"
LOG_DIR = BASE_DIR / "logs"

# Database Configuration
DB_CONFIG = {
    "dev" : {
        "host": 192.168.10.94",
        "port": 5432,
        "database": "ootp_dev",
        "user": "ootp_etl",
        "password": os.environ.get("OOTP_ETL_PASSWORD", "")
    },
    "stage": {
        "host": 192.168.10.94",
        "port": 5432,
        "database": "ootp_stage",
        "user": "ootp_etl",
        "password": os.environ.get("OOTP_ETL_PASSWORD", "")
    }
}

# Game data source
GAME_DATA_PATH = os.environ.get("OOTP_GAME_DATA_PATH", "")

# ETL Settings
BATCH_SIZE = 1000
ENABLE_CHANGE_DETECTION = True
ARCHIVE_AFTER_DATYS = 3650

