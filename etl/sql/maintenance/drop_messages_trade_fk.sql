-- =====================================================
-- Drop Invalid FK Constraint from messages.trade_id
-- =====================================================
--
-- ISSUE: The messages table had an FK constraint to trade_history.trade_id, but this is invalid because:
--
--   1. messages.trade_id contains OOTP's internal trade IDs (values like 2, 3, 7 from the game export)
--   2. trade_history.trade_id is our auto-generated SERIAL primary key (1, 2, 3, ..., 287)
--   3. These two ID systems are COMPLETELY UNRELATED - OOTP doesn't export its trade_id values
--      in trade_history.csv
--
-- CORRECT RELATIONSHIP:
--   - trade_history.message_id -> messages.message_id (already exists, works correctly)
--   - messages.trade_id remains as a reference field but with NO FK constraint
--
-- IMPACT: This constraint was causing messages.csv to fail during ETL with FK violations
--
-- EXECUTION: This script was already run on 2025-10-17 and can be safely re-run
-- =====================================================

ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_trade_id_fkey;

-- Verify the constraint is gone
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN '✓ FK constraint successfully removed'
        ELSE '✗ WARNING: FK constraint still exists!'
    END AS status
FROM information_schema.table_constraints
WHERE table_name = 'messages'
AND constraint_type = 'FOREIGN KEY'
AND constraint_name = 'messages_trade_id_fkey';
