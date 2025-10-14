# OOTP ETL Pipeline

## Overview

This ETL (Extract, Transform, Load) pipeline extracts data from Out of the Park Baseball 25, transforms it, and loads it into a PostgreSQL database. The pipeline supports both full and incremental data loading with comprehensive change detection and batch tracking.

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Initial Setup](#initial-setup)
- [Configuration](#configuration)
- [Common Operations](#common-operations)
- [Maintenance](#maintenance)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Architecture

### Components

- **Data Extraction**: Syncs CSV files and player images from remote OOTP game machine via rsync
- **Change Detection**: MD5 checksum-based file tracking to enable incremental loading
- **Database Layer**: PostgreSQL with SQLAlchemy for schema management and data loading
- **Batch Tracking**: Complete audit trail of all ETL operations via metadata tables
- **Loaders**: Specialized loaders for different data types (reference, players, statistics)

### Directory Structure

```
etl/
├── config/              # Configuration files
│   └── etl_config.py    # Main ETL configuration
├── data/                # Data directories
│   ├── incoming/        # Synced CSV files from game
│   │   └── csv/
│   └── images/          # Player photos
│       └── players/
├── docs/                # Documentation
├── logs/                # ETL execution logs (auto-created)
├── scripts/             # Shell scripts for data fetching
│   └── fetch_game_data.sh
├── sql/                 # Database schema SQL files
│   ├── tables/          # Table creation scripts (executed in order)
│   └── maintenance/     # Maintenance scripts
├── src/                 # Python source code
│   ├── database/        # Database connection and schema management
│   ├── loaders/         # Data loader classes
│   ├── transformers/    # Data transformation logic
│   └── utils/           # Utility functions
├── tests/               # Test files
├── .env                 # Environment configuration (DO NOT COMMIT)
└── main.py              # CLI entry point
```

### Database Schema

The database schema is created from SQL files in `sql/tables/` and executed in numerical order:

1. `00_etl_metadata.sql` - ETL tracking tables (batch runs, file tracking, load history)
2. `01_core_reference.sql` - Core reference data (leagues, teams, divisions, etc.)
3. `02_persons.sql` - Player and person tables
4. `03_statistics_complete.sql` - Batting and pitching statistics tables
5. `04_calculation_tables.sql` - League constants and calculated metrics
6. `05_team_league_history.sql` - Team and league historical data
7. `06_web_support.sql` - Web application support tables
8. `07_newspaper.sql` - Newspaper and article tables
9. `08_search_indexes.sql` - Full-text search indexes
10. `09_leaderboard_views.sql` - Leaderboard views
11. `10_calculation_functions.sql` - PostgreSQL functions for calculations

## Prerequisites

### Software Requirements

- Python 3.9+
- PostgreSQL 14+
- rsync (for data fetching)
- SSH access to OOTP game machine

### Python Packages

```bash
pip install -r requirements.txt
```

Key dependencies:
- `sqlalchemy` - Database ORM
- `psycopg2-binary` - PostgreSQL adapter
- `click` - CLI framework
- `loguru` - Logging
- `python-dotenv` - Environment variable management

## Initial Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd rb2/etl
```

### 2. Create Virtual Environment

```bash
# On Pop OS / Linux
python3 -m venv /home/jayco/virtual-envs/ootp
source /home/jayco/virtual-envs/ootp/bin/activate

# On Mac
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example `.env` file and configure for your environment:

```bash
cp .env.example .env  # If example exists, otherwise create from scratch
```

Edit `.env` with your settings (see [Configuration](#configuration) section).

### 5. Configure SSH Access

Ensure passwordless SSH access to the OOTP game machine:

```bash
# Test SSH connection
ssh Jayco@192.168.0.9 "echo 'Connection successful'"
```

If you need to set up SSH keys:

```bash
ssh-keygen -t rsa -b 4096
ssh-copy-id Jayco@192.168.0.9
```

### 6. Initialize Database

```bash
# Create all database tables
python main.py init-db

# Verify connection
python main.py check-status
```

### 7. Perform Initial Data Load

```bash
# Fetch data from game machine
python main.py fetch-data

# Load reference data
python main.py load-reference --force

# Load statistics and player data
python main.py load-stats
```

## Configuration

### Environment Variables (.env)

The `.env` file contains all configuration for the ETL pipeline. **Never commit this file to version control.**

#### Required Variables

```bash
# Local Paths
OOTP_GAME_DATA_PATH=/home/jayco/hdd/PycharmProjects/rb2/etl/data/incoming
OOTP_IMAGES_PATH=/home/jayco/hdd/PycharmProjects/rb2/etl/data/images/players

# Game Configuration
OOTP_GAME_NAME="Your Game Name"
OOTP_GAME_MACHINE=user@hostname.or.ip

# Database Configuration (Minotaur PostgreSQL server)
DB_HOST=192.168.10.94
DB_PORT=5432
DB_NAME_DEV=ootp_dev
DB_NAME_STAGING=ootp_staging
DB_USER_ETL=ootp_etl
DB_USER_APP=ootp_app
OOTP_ETL_PASSWORD=your_password_here

# Remote Game Paths (on OOTP machine)
OOTP_REMOTE_GAME_BASE="/path/to/OOTP Baseball 25/saved_games"
OOTP_REMOTE_DATA_PATH="/path/to/OOTP Baseball 25/saved_games/Your Game Name.lg/import_export/csv"
OOTP_REMOTE_PICTURES_PATH="/path/to/OOTP Baseball 25/saved_games/Your Game Name.lg/news/html/images/person_pictures"
```

#### Important Notes

- Update `OOTP_GAME_NAME` to match your actual game save name
- Update all paths with `"Your Game Name.lg"` to use your actual game name
- Ensure the database user has appropriate permissions
- For Mac paths, use the full Container path as shown in the example

### Python Configuration (config/etl_config.py)

Additional ETL settings can be configured in `config/etl_config.py`:

```python
# ETL Settings
BATCH_SIZE = 1000                    # Records per batch for bulk inserts
ENABLE_CHANGE_DETECTION = True       # Enable checksum-based change detection
ARCHIVE_AFTER_DAYS = 3650            # Keep batch history for 10 years

# Message Filtering Configuration
MESSAGE_FILTERS = {
    'exclude_message_types': [],     # Message type IDs to exclude
    'exclude_sender_ids': [],        # Sender IDs to exclude
    'min_importance': None,          # Minimum importance threshold
    'exclude_deleted': True,         # Exclude deleted messages
}
```

## Common Operations

### CLI Commands

The ETL pipeline is controlled via the `main.py` CLI:

```bash
# Show all available commands
python main.py --help

# Enable debug logging for any command
python main.py --debug <command>
```

### Fetching Data from Game Machine

```bash
# Fetch latest CSV and image data
python main.py fetch-data

# Dry run (show what would be fetched without executing)
python main.py fetch-data --dry-run
```

This command:
- Syncs CSV files from game machine to `data/incoming/csv/`
- Syncs player pictures to `data/images/players/`
- Uses `--delete` for CSVs (complete refresh)
- Uses `--update` for images (preserves newer local files)

### Loading Reference Data

Reference data includes leagues, teams, divisions, conferences, etc.

```bash
# Load all reference tables (incremental by default)
python main.py load-reference

# Force full reload of all reference tables
python main.py load-reference --force

# Load specific reference table
python main.py load-reference --file leagues.csv

# Force reload of specific table
python main.py load-reference --file leagues.csv --force
```

Reference tables are loaded in this order:
1. `leagues.csv`
2. `divisions.csv`
3. `conferences.csv`
4. `team_names.csv`
5. `teams.csv`

### Loading Statistics and Player Data

```bash
# Load all player data and statistics
python main.py load-stats

# Force recalculation of league constants for all years
python main.py load-stats --force-all-constants
```

This command loads:
1. `players.csv`
2. `players_career_batting_stats.csv`
3. `players_career_pitching_stats.csv`
4. League constants (for advanced metrics calculation)
5. `coaches.csv`
6. `team_roster.csv`
7. `team_roster_staff.csv`

**IMPORTANT:** After loading stats, you must refresh the materialized views for optimal web performance:

```bash
# Refresh materialized views (REQUIRED after load-stats)
python main.py refresh-views
```

This refreshes all leaderboard materialized views used by the web application. Without this step, leaderboards will be empty or stale.

### Checking Pipeline Status

```bash
# Check database connection and pipeline status
python main.py check-status
```

### Database Operations

```bash
# Initialize/create all database tables
python main.py init-db

# Create only metadata tables
python main.py init-db --metadata-only
```

## Maintenance

### Complete Database Refresh

When you need to completely refresh the database with new test data or a different game:

#### Step 1: Update Configuration

Edit `.env` to point to your new game:

```bash
# Update game name
OOTP_GAME_NAME="New Game 2"

# Update remote paths to use new game name
OOTP_REMOTE_DATA_PATH="/path/to/saved_games/New Game 2.lg/import_export/csv"
OOTP_REMOTE_PICTURES_PATH="/path/to/saved_games/New Game 2.lg/news/html/images/person_pictures"
```

#### Step 2: Reset Database

**Option A: Using the reset script (recommended)**

From the database server:

```bash
# If PostgreSQL is in Docker with sudo required
sudo docker exec -it <postgres-container> psql -U ootp_etl -d ootp_dev -f /path/to/reset_database.sql

# If direct PostgreSQL access
PGPASSWORD=your_password psql -h 192.168.10.94 -U ootp_etl -d ootp_dev -f sql/maintenance/reset_database.sql
```

**Option B: Manual SQL execution**

Connect to the database and run the SQL from `sql/maintenance/reset_database.sql`.

#### Step 3: Reinitialize Database

```bash
python main.py init-db
```

#### Step 4: Fetch and Load Data

```bash
# Fetch data from game machine
python main.py fetch-data

# Verify connection
python main.py check-status

# Load reference data
python main.py load-reference --force

# Load statistics
python main.py load-stats --force-all-constants

# CRITICAL: Refresh materialized views (required for web performance)
python main.py refresh-views
```

**Note:** The `refresh-views` command MUST be run after loading stats to populate the leaderboard materialized views. Without this step, the web application's leaderboard pages will not function properly.

#### Step 5: Verify Data Load

Connect to the database and verify:

```sql
-- Check table row counts
SELECT
    schemaname,
    tablename,
    n_tup_ins as row_count
FROM pg_stat_user_tables
ORDER BY schemaname, tablename;

-- Check recent batch runs
SELECT * FROM etl_batch_runs ORDER BY started_at DESC LIMIT 10;

-- Check file tracking
SELECT * FROM etl_file_tracking ORDER BY processed_at DESC LIMIT 10;
```

### Log Management

Logs are stored in `logs/` directory:

```bash
# View today's log
tail -f logs/etl_$(date +%Y-%m-%d).log

# View specific date
tail -f logs/etl_2024-10-13.log

# Search logs for errors
grep -i error logs/etl_$(date +%Y-%m-%d).log
```

Log rotation:
- Daily rotation (new file each day)
- 30 day retention
- Debug level logged to file
- Info level logged to console

### Monitoring Batch Runs

Query the `etl_batch_runs` table to monitor ETL operations:

```sql
-- Recent batch runs with status
SELECT
    batch_id,
    batch_type,
    status,
    started_at,
    completed_at,
    records_processed,
    records_inserted,
    records_updated,
    records_failed
FROM etl_batch_runs
ORDER BY started_at DESC
LIMIT 20;

-- Failed batches
SELECT * FROM etl_batch_runs WHERE status = 'failed';

-- Batch performance metrics
SELECT
    batch_type,
    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration_seconds,
    MAX(records_processed) as max_records,
    COUNT(*) as total_runs
FROM etl_batch_runs
WHERE status = 'completed'
GROUP BY batch_type;
```

### File Change Detection

The ETL tracks file changes using MD5 checksums:

```sql
-- Recently processed files
SELECT
    file_name,
    file_checksum,
    file_size,
    records_in_file,
    processed_at,
    load_strategy
FROM etl_file_tracking
ORDER BY processed_at DESC;

-- Files that haven't changed recently
SELECT
    file_name,
    processed_at,
    load_strategy
FROM etl_file_tracking
WHERE processed_at < NOW() - INTERVAL '7 days'
ORDER BY processed_at;
```

### Database Maintenance Scripts

Additional maintenance scripts in `sql/maintenance/`:

```bash
# Reset entire database (drops all tables, views, functions)
psql -U ootp_etl -d ootp_dev -f sql/maintenance/reset_database.sql
```

## Troubleshooting

### Common Issues

#### 1. Database Connection Failed

**Symptoms:**
```
✗ Database connection failed
```

**Solutions:**
- Verify database is running: `sudo docker ps` (if using Docker)
- Check `.env` configuration (host, port, credentials)
- Test connection manually: `psql -h 192.168.10.94 -U ootp_etl -d ootp_dev`
- Check network connectivity: `ping 192.168.10.94`

#### 2. SSH/rsync Failures

**Symptoms:**
```
rsync: connection unexpectedly closed
Permission denied (publickey,password)
```

**Solutions:**
- Verify SSH key setup: `ssh-copy-id user@host`
- Test SSH connection: `ssh Jayco@192.168.0.9`
- Check `OOTP_GAME_MACHINE` in `.env`
- Verify remote paths exist on game machine

#### 3. File Not Found Errors

**Symptoms:**
```
File /path/to/file.csv not found
```

**Solutions:**
- Run `python main.py fetch-data` to sync data
- Verify `OOTP_GAME_DATA_PATH` in `.env`
- Check that game has exported CSV data
- Verify directory permissions

#### 4. Import Errors

**Symptoms:**
```
ModuleNotFoundError: No module named 'xyz'
```

**Solutions:**
- Activate virtual environment
- Reinstall dependencies: `pip install -r requirements.txt`
- Check Python version: `python --version` (requires 3.9+)

#### 5. Table Already Exists

**Symptoms:**
```
relation "table_name" already exists
```

**Solutions:**
- Reset database: Run `sql/maintenance/reset_database.sql`
- Or drop specific table: `DROP TABLE table_name CASCADE;`
- Then run: `python main.py init-db`

#### 6. Syntax Error in etl_config.py

**Symptoms:**
```
SyntaxError: invalid syntax in etl_config.py
```

**Solutions:**
- Check for missing quotes around IP addresses
- Verify all strings are properly quoted
- Review Python syntax (commas, brackets, etc.)

### Viewing Detailed Logs

```bash
# Enable debug mode for any command
python main.py --debug <command>

# View real-time log
tail -f logs/etl_$(date +%Y-%m-%d).log

# Search for specific errors
grep -B5 -A5 "ERROR" logs/etl_$(date +%Y-%m-%d).log
```

### Database Diagnostic Queries

```sql
-- Check table sizes
SELECT
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Check index usage
SELECT
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- Check for locks
SELECT * FROM pg_locks WHERE NOT granted;

-- Check active connections
SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    query
FROM pg_stat_activity
WHERE datname = 'ootp_dev';
```

## Development

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_incremental_loading.py

# Run with coverage
pytest --cov=src tests/
```

### Code Structure

#### Loaders

Base loader class: `src/loaders/base_loader.py`

All loaders inherit from `BaseLoader` and implement:
- Change detection via checksums
- Incremental vs full load strategies
- Batch tracking
- Error handling

Specialized loaders:
- `ReferenceLoader` - Reference data tables
- `PlayersLoader` - Player information
- `BattingStatsLoader` - Batting statistics
- `PitchingStatsLoader` - Pitching statistics
- `StatsLoader` - Generic statistics loader

#### Transformers

- `LeagueConstantsTransformer` - Calculates league-wide constants for advanced metrics (wOBA, FIP, etc.)

#### Utilities

- `checksum.py` - MD5 checksum calculation for change detection
- `batch.py` - Batch ID generation and tracking
- `fetch.py` - Wrapper for fetch_game_data.sh script
- `csv_preprocessor.py` - CSV cleaning and validation
- `message_filter.py` - Message filtering logic

### Adding New Loaders

1. Create new loader class in `src/loaders/`
2. Inherit from `BaseLoader`
3. Implement required methods:
   - `get_table_name()` - Target table name
   - `get_load_strategy()` - 'full' or 'incremental'
   - `transform_row()` - Row-level transformations (optional)

Example:

```python
from .base_loader import BaseLoader

class MyNewLoader(BaseLoader):
    def get_table_name(self):
        return 'my_table'

    def get_load_strategy(self):
        return 'incremental'

    def transform_row(self, row):
        # Optional: transform data before insert
        row['new_column'] = row['old_column'].upper()
        return row
```

4. Add CLI command in `main.py` if needed

### Contributing

1. Create feature branch: `git checkout -b feature/my-feature`
2. Make changes and test thoroughly
3. Update documentation as needed
4. Commit with clear messages
5. Push and create pull request

### Coding Standards

- Follow PEP 8 style guide
- Use type hints where appropriate
- Write docstrings for all functions/classes
- Add unit tests for new functionality
- Keep functions focused and single-purpose
- Use meaningful variable names
- Comment complex logic

---

## Support

For issues, questions, or contributions:
- Check logs in `logs/` directory
- Review troubleshooting section above
- Check existing documentation in `docs/`
- Create GitHub issue with:
  - Error messages
  - Log excerpts
  - Steps to reproduce
  - Environment details

## License

[Add license information]

## Changelog

See `CHANGELOG.md` for version history and changes.
