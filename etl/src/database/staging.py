"""Staging table management for ETL pipline"""
from sqlalchemy import text, inspect
from loguru import logger
from .connection import db
class StagingTableManager:

    def __init__(self, connection=None):
        self.db = connection or db
        self.inspector = inspect(self.db.engine)

    def create_staging_table(self, source_table: str, staging_prefix: str = "staging_"):
        """Create a staging table with the same structure as the source table"""
        staging_table = f"{staging_prefix}{source_table}"

        logger.info(f"Creating staging table: {staging_table}")

        try:
            # Drop existing staging table
            self.drop_staging_table(staging_table)

            # Create staging table as copy of source structure
            sql = text(f"""
            CREATE TABLE {staging_table} (LIKE {source_table} INCLUDING ALL)""")
            self.db.execute_sql(sql)
            logger.success(f"Successfully created staging table: {staging_table}")
            return True

        except Exception as e:
            logger.error(f"Error creating staging table {staging_table}: {e}")
            raise


    def create_staging_from_csv_structure(self, table_name: str, columns: dict, staging_prefix: str = "staging_"):
        """Create staging table from CSV column definitions"""
        staging_table = f"{staging_prefix}{table_name}"
        logger.info(f"Creating staging table: {staging_table}")
        try:
            # Drop existing staging table
            self.drop_staging_table(staging_table)

           # Build column definitions
            column_defs = []
            for col_name, col_type in columns.items():
                column_defs.append(f"{col_name} {col_type}")

            # Create SQL
            sql = f"""
                CREATE TABLE {staging_table} (
                    {', '.join(column_defs)}
                )"""
            self.db.execute_sql(sql)
            logger.success(f"Successfully created staging table: {staging_table}")
            return staging_table

        except Exception as e:
            logger.error(f"Error creating staging table {staging_table}: {e}")
            raise


    def analyze_staging_changes(self, staging_table: str, target_table: str, key_columns: list):
        """Analyze differences between staging and target tbles"""
        logger.info(f"Analyzing changes between {staging_table} and {target_table}")
        key_join = ' AND '.join([f"s.{col} = t.{col}" for col in key_columns])

        # Count new records
        new_records_sql = text(f"""
            SELECT COUNT(*) FROM {staging_table} s
            WHERE NOT EXISTS (
                SELECT 1 FROM {target_table} t
                WHERE {key_join}
            )
        """)

        changed_records_sql = text(f"""
        SELECT COUNT(*) FROM {staging_table} s
        INNER JOIN {target_table} t ON {key_join}
        WHERE s != t --this needs proper column-wise comparison""")

        with self.db.get_session() as session:
            new_count = session.execute(new_records_sql).scalar()
            # changed_count = session.execute(changed_records_sql).scalar()

        logger.info(f"Found {new_count} new records")
        return {
            "new_records": new_count,
            # "changed_records": changed_count
            'staging_table': staging_table,
            'target_table': target_table
        }