# Incremental Loading & Data Preservation

## Overview

This ETL pipeline now supports incremental loading for historical data tables, ensuring that records are never deleted even if they disappear from the source system.

## Protected Tables

The following tables now use incremental loading with UPSERT logic:

### 1. `trade_history`
- **Strategy**: Incremental (UPSERT on `trade_id`)
- **Behavior**:
  - New trades are inserted
  - Existing trades are updated if data changes
  - Old trades are NEVER deleted, even if removed from source CSV
- **Primary Key**: `trade_id`

### 2. `messages`
- **Strategy**: Incremental (UPSERT on `message_id`)
- **Behavior**:
  - New messages are inserted
  - Existing messages are updated if data changes
  - Old messages are NEVER deleted, even if trimmed from source CSV
  - Messages can be filtered during load (see Message Filtering below)
- **Primary Key**: `message_id`

## How It Works

### Standard Load (Other Reference Tables)
```
CSV File → Check Checksum → If Changed:
  1. TRUNCATE target table
  2. Load all CSV data
  3. Result: Only current CSV data remains
```

### Incremental Load (trade_history, messages)
```
CSV File → Always Load:
  1. Create staging table
  2. Load CSV data to staging
  3. UPSERT from staging to target
     - INSERT if message_id/trade_id doesn't exist
     - UPDATE if it exists
  4. Drop staging table
  5. Result: Historical data preserved + current data updated
```

## Message Filtering

Messages can be filtered during the ETL process to exclude unwanted content.

### Configuration

Edit `/etl/config/etl_config.py` and modify the `MESSAGE_FILTERS` dictionary:

```python
MESSAGE_FILTERS = {
    # Filter by message_type (list of message_type values to exclude)
    'exclude_message_types': [
        100,  # Example: Filter out message type 100
        101,  # Example: Filter out message type 101
    ],

    # Filter by sender_id (list of sender IDs to exclude)
    'exclude_sender_ids': [
        5,   # Example: Filter out messages from sender 5
        10,  # Example: Filter out messages from sender 10
    ],

    # Filter by importance (exclude messages below this threshold)
    'min_importance': 3,  # Example: Only load messages with importance >= 3

    # Filter by deleted flag (default: exclude deleted messages)
    'exclude_deleted': True,
}
```

### Filter Behavior

- **Filters are applied BEFORE loading**: Filtered messages are never inserted into the database
- **Filters do NOT delete existing messages**: If a message already exists in the database, filters won't remove it
- **Multiple filters are AND'ed**: A message must pass ALL filters to be loaded

### Finding Message Types and Senders

To identify which message types or senders to filter:

```sql
-- Find all unique message types
SELECT DISTINCT message_type, COUNT(*) as count
FROM messages
GROUP BY message_type
ORDER BY count DESC;

-- Find all unique senders
SELECT DISTINCT sender_id, COUNT(*) as count
FROM messages
GROUP BY sender_id
ORDER BY count DESC;

-- Find messages by importance
SELECT importance, COUNT(*) as count
FROM messages
GROUP BY importance
ORDER BY importance DESC;
```

## Image Preservation

Images are already protected via rsync configuration.

### Current Configuration (fetch_game_data.sh)

```bash
# Player pictures use --update flag (preserves local files)
rsync -avz --update --progress --stats \
    "${GAME_MACHINE}:${REMOTE_PICTURES_PATH}/" \
    "${LOCAL_PICTURES}/"
```

### Behavior

- `--update`: Skip files that are **newer** on the receiver
- When a player becomes a coach and their image is deleted from source:
  - rsync sees the local file is newer
  - The local file is preserved
  - Image remains available on your system

## Running the ETL

### Load All Reference Tables (including incremental)

```bash
cd etl
./main.py load-reference
```

### Load Specific Tables

```bash
# Load only trades
./main.py load-reference --file trade_history.csv

# Load only messages (with filtering)
./main.py load-reference --file messages.csv

# Force reload even if unchanged
./main.py load-reference --file messages.csv --force
```

## Monitoring

### Check Last Load Status

```sql
SELECT
    filename,
    last_status,
    rows_processed,
    last_processed,
    processing_time_seconds
FROM etl_file_metadata
WHERE filename IN ('trade_history.csv', 'messages.csv')
ORDER BY last_processed DESC;
```

### Verify Data Preservation

```sql
-- Check oldest trade date
SELECT MIN(date) as oldest_trade, MAX(date) as newest_trade, COUNT(*) as total_trades
FROM trade_history;

-- Check oldest message date
SELECT MIN(date) as oldest_message, MAX(date) as newest_message, COUNT(*) as total_messages
FROM messages;

-- After running ETL multiple times, these counts should only increase, never decrease
```

## Troubleshooting

### Messages Not Being Filtered

1. Check your configuration syntax in `etl_config.py`
2. Look for warnings in the ETL logs:
   ```bash
   tail -f logs/etl_*.log | grep -i filter
   ```
3. Verify the filter is active:
   ```bash
   tail -f logs/etl_*.log | grep "Active filters"
   ```

### Historical Data Appears to Be Missing

1. Check if data was loaded before the incremental strategy was implemented:
   ```sql
   SELECT * FROM etl_file_metadata WHERE filename = 'messages.csv';
   ```
2. If `last_processed` date is before the incremental implementation, you may need to re-load historical CSVs if you have backups

### Performance Concerns

- Incremental loads are slower than TRUNCATE+INSERT for small datasets
- But they preserve history and prevent data loss
- Indexes on `message_id` and `trade_id` ensure UPSERT performance
- Staging tables are dropped after each load to avoid bloat

## Technical Implementation

### Key Files Modified

1. **`/etl/config/etl_config.py`**
   - Added `MESSAGE_FILTERS` configuration

2. **`/etl/src/utils/message_filter.py`** (NEW)
   - Message filtering logic

3. **`/etl/src/loaders/reference_loader.py`**
   - Updated table configs to use `'load_strategy': 'incremental'`
   - Added `_handle_incremental_load()` method
   - Added `_apply_message_filters()` method
   - Modified `get_load_strategy()` to respect config
   - Modified `get_update_columns()` to support wildcard '*'

4. **`/etl/src/loaders/base_loader.py`**
   - Updated `_upsert_from_staging()` to handle '*' wildcard in update_columns
   - Expands '*' to all non-primary-key columns

### Database Requirements

- Tables must have proper primary keys for UPSERT to work
- `ON CONFLICT (primary_key) DO UPDATE SET ...` is used
- PostgreSQL 9.5+ required for UPSERT support

## Future Enhancements

Potential improvements for consideration:

1. **Soft Deletes**: Add `deleted_at` timestamp instead of filtering
2. **Audit Trail**: Track when records were first inserted vs. updated
3. **Archival**: Periodically archive very old messages to separate tables
4. **Change Detection**: Log what changed in each update
5. **Rollback**: Keep staging tables for N days to enable rollback
