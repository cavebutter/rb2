# Fix: Messages Table FK Constraint Issue

**Date:** 2025-10-17
**Issue:** Messages table not populating during ETL process
**Root Cause:** Invalid foreign key constraint on `messages.trade_id`

## Problem Summary

The `messages` table was failing to load during the ETL process with a foreign key violation error:

```
insert or update on table "messages" violates foreign key constraint "messages_trade_id_fkey"
```

## Root Cause Analysis

The messages table had an FK constraint: `messages.trade_id → trade_history.trade_id`

However, this constraint was fundamentally broken because:

1. **messages.trade_id** contains OOTP's internal trade IDs (values like 2, 3, 7 from the game export)
2. **trade_history.trade_id** is our auto-generated SERIAL primary key (1, 2, 3, ..., 287)
3. These two ID systems are **completely unrelated** - OOTP doesn't export its internal trade_id values in the trade_history.csv file

### Data Evidence

- Only 3 unique positive trade_id values in messages.csv: [2, 3, 7]
- trade_history has 287 rows with auto-generated trade_ids (1-287)
- trade_history.csv from OOTP has `message_id` column but **no trade_id column**
- The calculated_fields config was trying to convert invalid values to NULL, but valid positive values (2, 3, 7) still failed the FK check

### Correct Relationship

The proper relationship is:
- `trade_history.message_id` → `messages.message_id` (already exists and works correctly)

The `messages.trade_id` field should remain for reference but **without an FK constraint** since it contains OOTP internal IDs that don't correspond to our database values.

## Solution Implemented

### 1. Dropped FK Constraint from Live Database

```sql
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_trade_id_fkey;
```

Executed via: `psql -h 192.168.10.94 -U ootp_etl -d ootp_dev`

### 2. Updated SQL Table Creation Script

**File:** `etl/sql/tables/07_newspaper.sql:277`

Removed the FK constraint and added explanatory comment:

```sql
body TEXT NOT NULL
-- Note: No FK constraint on trade_id - this field contains OOTP internal trade IDs
-- which don't correspond to our auto-generated trade_history.trade_id values.
-- The proper relationship is: trade_history.message_id -> messages.message_id
);
```

### 3. Updated Reference Loader Configuration

**File:** `etl/src/loaders/reference_loader.py:278`

Added clarifying comment to calculated_fields:

```python
'calculated_fields': {
    # Convert 0 and negative values to NULL (non-trade messages use 0, -1, -5, etc.)
    # Note: trade_id contains OOTP internal IDs with no FK constraint - kept for reference only
    'trade_id': 'CASE WHEN trade_id > 0 THEN trade_id ELSE NULL END'
}
```

### 4. Created Maintenance Script

**File:** `etl/sql/maintenance/drop_messages_trade_fk.sql`

Created a documented SQL script that can be re-run safely to drop the constraint in new environments.

## Verification

After applying the fix:

```bash
./main.py load-reference --file messages.csv
```

**Results:**
- ✅ Successfully loaded 2,810 messages (after filtering 729 deleted messages)
- ✅ No FK violations
- ✅ Messages with trade_id values retained (2 messages with trade_ids 2 and 7)
- ✅ Invalid trade_id values (0, negative) converted to NULL as expected

```sql
SELECT COUNT(*) FROM messages;
-- Result: 2810

SELECT COUNT(*), COUNT(DISTINCT trade_id)
FROM messages WHERE trade_id IS NOT NULL;
-- Result: 2 messages, 2 unique trade_ids
```

## Impact

- **Fixed:** Messages table now loads successfully during ETL
- **No Data Loss:** All messages (including those with trade_id references) load correctly
- **Backwards Compatible:** The calculated_fields logic still converts invalid trade_ids to NULL
- **Future-Proof:** New database instances will be created without this invalid FK constraint

## Files Modified

1. `etl/sql/tables/07_newspaper.sql` - Removed FK constraint from CREATE TABLE
2. `etl/src/loaders/reference_loader.py` - Updated comment in calculated_fields
3. `etl/sql/maintenance/drop_messages_trade_fk.sql` - Created maintenance script

## Lessons Learned

When adding an auto-generated SERIAL primary key to a table (like trade_history.trade_id), ensure that:
1. Any FK references to that table use the correct column (message_id, not trade_id)
2. OOTP's internal IDs in CSV exports may not match our database-generated IDs
3. FK constraints should only reference columns that have a valid data relationship

## Related Tables

- `messages` - Fixed (no longer has invalid FK)
- `trade_history` - Correct relationship via message_id column