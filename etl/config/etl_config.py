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

# Message Filtering Configuration
# Messages will be excluded from loading if they match these criteria
MESSAGE_FILTERS = {
    # Filter by message_type (list of message_type values to exclude)
    'exclude_message_types': [
        # Add message type IDs here that should be filtered out
        # Example: 100, 101, 102
    ],

    # Filter by sender_id (list of sender IDs to exclude)
    'exclude_sender_ids': [
        # Add sender IDs here (e.g., specific managers/teams you don't want messages from)
        # Example: 1, 5, 10
    ],

    # Filter by importance (exclude messages below this threshold)
    'min_importance': None,  # Set to an integer to filter low-importance messages

    # Filter by deleted flag (default: exclude deleted messages)
    'exclude_deleted': True,
}

