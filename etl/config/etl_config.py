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
        "host": "192.168.10.94",
        "port": 5432,
        "database": "ootp_dev",
        "user": "ootp_etl",
        "password": os.environ.get("OOTP_ETL_PASSWORD", "")
    },
    "stage": {
        "host": "192.168.10.94",
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

# Ollama Configuration
OLLAMA_CONFIG = {
    # Ollama API endpoint
    'base_url': os.environ.get('OLLAMA_BASE_URL', 'http://localhost:11434'),

    # Default timeout for generation requests (seconds)
    'timeout': 120,

    # Model selection by priority tier
    'models': {
        'MUST_GENERATE': 'qwen2.5:14b',      # Best quality for exceptional games
        'SHOULD_GENERATE': 'qwen2.5:7b',     # Good quality for solid performances
        'COULD_GENERATE': 'qwen2.5:3b',      # Fast generation for routine games
    },

    # Temperature settings by priority tier (lower = more conservative)
    'temperatures': {
        'MUST_GENERATE': 0.6,
        'SHOULD_GENERATE': 0.7,
        'COULD_GENERATE': 0.75,
    },

    # Generation parameters
    'default_temperature': 0.7,
    'default_max_tokens': 400,  # ~250-word articles

    # Retry configuration
    'max_retries': 3,
    'backoff_multiplier': 2.0,
}

# Newspaper Article Generation Configuration
NEWSPAPER_CONFIG = {
    # Priority thresholds (newsworthiness scores)
    'priority_thresholds': {
        'MUST_GENERATE': 80,      # Exceptional performances, milestones
        'SHOULD_GENERATE': 50,    # Solid performances
        'COULD_GENERATE': 20,     # Routine performances
        'SKIP': 0,                # Below 20 - skip generation
    },

    # Auto-publish threshold (future use)
    'auto_publish_threshold': 60,

    # Editorial workflow
    'default_status': 'draft',  # Articles start as drafts
    'auto_publish_enabled': False,  # Require manual review

    # Game logs archive configuration
    'game_logs': {
        'active_csv_path': DATA_DIR / 'incoming' / 'csv' / 'game_logs.csv',
        'archive_path': DATA_DIR / 'archive' / 'game_logs',
        'compression': 'gzip',
    },

    # Staging table configuration
    'staging': {
        'truncate_after_run': True,  # Clean up staging tables after each run
        'keep_last_n_runs': 0,       # Set to >0 to keep history for debugging
    },

    # Messages integration (for reprints)
    'messages': {
        'worthy_message_types': [1, 5, 12],  # Trade, Awards, Milestones (TBD based on data analysis)
        'minimum_importance': 5,
        'minimum_hype': 3,
        'min_body_length': 100,  # Characters
        'auto_publish_reprints': True,
    },
}

