# Quick Start: Incremental Loading & Data Preservation

## What Changed?

Your ETL now **preserves all historical data** for trades and messages, even if OOTP trims these files.

## Summary

✅ **Images**: Already protected (rsync `--update` flag)
✅ **Trade History**: Now using incremental UPSERT (never deletes)
✅ **Messages**: Now using incremental UPSERT + optional filtering (never deletes)
❌ **Injury History**: Ignored per your request

## How to Use

### 1. Configure Message Filtering (Optional)

Edit `etl/config/etl_config.py`:

```python
MESSAGE_FILTERS = {
    'exclude_message_types': [100, 101],  # Add types to exclude
    'exclude_sender_ids': [5, 10],        # Add senders to exclude
    'min_importance': 3,                   # Only load importance >= 3
    'exclude_deleted': True,               # Exclude deleted messages
}
```

To find message types/senders to filter:
```sql
SELECT message_type, COUNT(*) FROM messages GROUP BY message_type ORDER BY COUNT(*) DESC;
SELECT sender_id, COUNT(*) FROM messages GROUP BY sender_id ORDER BY COUNT(*) DESC;
```

### 2. Run the ETL

```bash
cd etl

# Test current state
python tests/test_incremental_loading.py

# Load trades and messages
./main.py load-reference --file trade_history.csv
./main.py load-reference --file messages.csv

# Verify preservation
python tests/test_incremental_loading.py
```

### 3. Verify Data Preservation

After running ETL multiple times:

```sql
-- Counts should NEVER decrease
SELECT COUNT(*) FROM trade_history;
SELECT COUNT(*) FROM messages;

-- Check date ranges (should expand over time)
SELECT MIN(date), MAX(date) FROM trade_history;
SELECT MIN(date), MAX(date) FROM messages;
```

## Key Behavior Changes

### Before (TRUNCATE + INSERT)
```
1. CSV has 1000 records
2. TRUNCATE table → 0 records
3. INSERT from CSV → 1000 records
4. Next run: CSV has 800 records (200 trimmed by OOTP)
5. TRUNCATE table → 0 records
6. INSERT from CSV → 800 records
❌ Lost 200 historical records!
```

### After (INCREMENTAL UPSERT)
```
1. CSV has 1000 records
2. UPSERT into table → 1000 records (all new)
3. Next run: CSV has 800 records (200 trimmed by OOTP)
4. UPSERT into table → 0 new, 800 updated
✅ Still have 1000 records (200 preserved even though not in CSV)
```

## Files Modified

1. `etl/config/etl_config.py` - Added MESSAGE_FILTERS config
2. `etl/src/utils/message_filter.py` - NEW: Filtering logic
3. `etl/src/loaders/reference_loader.py` - Incremental load implementation
4. `etl/src/loaders/base_loader.py` - UPSERT wildcard support
5. `etl/docs/INCREMENTAL_LOADING.md` - Full documentation
6. `etl/tests/test_incremental_loading.py` - NEW: Test script

## Troubleshooting

**Q: Messages aren't being filtered**
- Check `etl_config.py` syntax
- Run test script to verify config
- Check logs: `tail -f logs/etl_*.log | grep -i filter`

**Q: Data seems to be missing**
- Historical data from before this change won't magically appear
- Only new ETL runs will preserve data going forward
- If you have old CSV backups, you can reload them

**Q: Performance is slow**
- Incremental UPSERT is slower than TRUNCATE+INSERT
- But it preserves history (worth it!)
- Ensure indexes exist on message_id and trade_id

## Need More Details?

See `etl/docs/INCREMENTAL_LOADING.md` for full documentation.