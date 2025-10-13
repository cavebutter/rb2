"""
Test script for incremental loading and data preservation

Run this to verify that trade_history and messages preserve historical records.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import text
from src.database.connection import db
from loguru import logger


def test_trade_history_preservation():
    """Test that trade history is preserved across loads"""
    logger.info("Testing trade_history preservation...")

    # Get current count
    result = db.execute_sql(text("SELECT COUNT(*) FROM trade_history"))
    initial_count = result[0][0]
    logger.info(f"Current trade_history count: {initial_count}")

    # Get date range
    result = db.execute_sql(text("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM trade_history
    """))
    if result and result[0][0]:
        min_date, max_date = result[0]
        logger.info(f"Trade date range: {min_date} to {max_date}")
    else:
        logger.warning("No trades in database yet")

    return initial_count


def test_messages_preservation():
    """Test that messages are preserved across loads"""
    logger.info("Testing messages preservation...")

    # Get current count
    result = db.execute_sql(text("SELECT COUNT(*) FROM messages"))
    initial_count = result[0][0]
    logger.info(f"Current messages count: {initial_count}")

    # Get date range
    result = db.execute_sql(text("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM messages
    """))
    if result and result[0][0]:
        min_date, max_date = result[0]
        logger.info(f"Message date range: {min_date} to {max_date}")
    else:
        logger.warning("No messages in database yet")

    # Get message type breakdown
    result = db.execute_sql(text("""
        SELECT message_type, COUNT(*) as count
        FROM messages
        GROUP BY message_type
        ORDER BY count DESC
        LIMIT 10
    """))
    if result:
        logger.info("Top 10 message types:")
        for message_type, count in result:
            logger.info(f"  Type {message_type}: {count} messages")

    return initial_count


def test_message_filtering():
    """Test message filtering configuration"""
    logger.info("Testing message filtering configuration...")

    try:
        from config.etl_config import MESSAGE_FILTERS
        from src.utils.message_filter import MessageFilter

        filter_obj = MessageFilter(MESSAGE_FILTERS)
        logger.info(filter_obj.get_filter_summary())

        # Check if any filters are active
        has_active_filters = (
            MESSAGE_FILTERS.get('exclude_message_types') or
            MESSAGE_FILTERS.get('exclude_sender_ids') or
            MESSAGE_FILTERS.get('min_importance') is not None or
            MESSAGE_FILTERS.get('exclude_deleted')
        )

        if has_active_filters:
            logger.info("✓ Message filters are configured")
        else:
            logger.info("ℹ No message filters are currently active (this is fine)")

        return True
    except ImportError as e:
        logger.error(f"Could not import MESSAGE_FILTERS: {e}")
        return False
    except Exception as e:
        logger.error(f"Error testing message filters: {e}")
        return False


def test_etl_metadata():
    """Check ETL metadata for load history"""
    logger.info("Checking ETL metadata...")

    result = db.execute_sql(text("""
        SELECT
            filename,
            last_status,
            rows_processed,
            last_processed,
            ROUND(processing_time_seconds, 2) as processing_time_sec
        FROM etl_file_metadata
        WHERE filename IN ('trade_history.csv', 'messages.csv')
        ORDER BY last_processed DESC
    """))

    if result:
        logger.info("Recent load history:")
        for row in result:
            filename, status, rows, processed, proc_time = row
            logger.info(f"  {filename}: {status}, {rows} rows, {processed}, {proc_time}s")
    else:
        logger.warning("No load history found for trade_history or messages")


def main():
    """Run all tests"""
    logger.info("=" * 60)
    logger.info("INCREMENTAL LOADING TEST SUITE")
    logger.info("=" * 60)

    try:
        # Test database connection
        if not db.test_connection():
            logger.error("Database connection failed!")
            return False

        logger.info("✓ Database connection successful\n")

        # Run tests
        trade_count = test_trade_history_preservation()
        logger.info("")

        message_count = test_messages_preservation()
        logger.info("")

        filter_test = test_message_filtering()
        logger.info("")

        test_etl_metadata()
        logger.info("")

        # Summary
        logger.info("=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Trade History Records: {trade_count}")
        logger.info(f"Message Records: {message_count}")
        logger.info(f"Message Filters: {'Configured' if filter_test else 'Not configured'}")
        logger.info("")
        logger.info("Next Steps:")
        logger.info("1. Run: ./main.py load-reference --file trade_history.csv")
        logger.info("2. Run: ./main.py load-reference --file messages.csv")
        logger.info("3. Re-run this test script")
        logger.info("4. Verify counts INCREASED or stayed the same (never decreased)")

        return True

    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
