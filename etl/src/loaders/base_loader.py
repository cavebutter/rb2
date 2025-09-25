"""Base loader class for ETL process."""
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger
from ..database.connection import db
from ..database.staging import StagingTableManager
from sqlalchemy import text

class BaseLoader(ABC):
    """Base class for all data loaders"""

    def __init__(self, batch_id: str = None):
        self.db = db
        self.staging_mgr = StagingTableManager()
        self.batch_id = batch_id
        self.stats = {
            'rows_read': 0,
            'rows_inserted': 0,
            'rows_updated': 0,
            'rows_deleted': 0,
            'errors': []
        }

    @abstractmethod
    def get_load_strategy(self) -> str:
        """Return the load strategy for this loader."""
        pass

    @abstractmethod
    def get_primary_keys(self) -> List[str]:
        """Return a list of primary keys for this loader."""
        pass

    @abstractmethod
    def get_target_table(self) -> str:
        """Return the target table name for this loader."""
        pass

    @abstractmethod
    def get_column_mapping(self) -> Optional[Dict[str, str]]:
        """Return a mapping of CSV columns to database columns, if any."""
        pass


    @abstractmethod
    def get_calculated_fields(self) -> Dict[str, str]:
        """Return dictionary of calculated fields and their SQL expressions"""
        pass


    @abstractmethod
    def get_upsert_keys(self) -> List[str]:
        """Return list of columns to use as keys for UPSERT operations."""
        pass


    @abstractmethod
    def get_update_columns(self) -> List[str]:
        """Return list of columns to update on conflict during UPSERT."""
        pass

    def load_csv(self, csv_path: Path) -> bool:
        """Load data from a CSV file."""
        target_table = self.get_target_table()
        strategy = self.get_load_strategy()

        logger.info(f"Loading {csv_path} into {target_table} using {strategy} strategy")
        try:
            self._create_batch_run()
            self._record_file_start(csv_path)

            if strategy == 'skip':
                return self._handle_skip_strategy(csv_path)
            elif strategy == 'full':
                return self._handle_full_load(csv_path)
            elif strategy == 'incremental':
                return self._handle_incremental_load(csv_path)
            elif strategy == 'append':
                return self._handle_append_load(csv_path)
            else:
                raise ValueError(f"Unknown load strategy: {strategy}")
        except Exception as e:
            import traceback
            logger.error(f"Error loading {csv_path}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.stats['errors'].append(str(e))
            self._record_file_completion(csv_path, 'failed', str(e))
            return False


    def _handle_skip_strategy(self, csv_path: Path) -> bool:
        """Handle skip strategy - only load if checksum changed"""
        # TODO - implement checksum comparison
        logger.info(f"Skipping {csv_path.name} - skip strategy")
        self._record_file_completion(csv_path, 'skipped')
        return True

    def _handle_full_load(self, csv_path: Path) -> bool:
        """Handle full load - truncate and reload"""
        target_table = self.get_target_table()
        staging_table = f"staging_{target_table}"
        column_mapping = self.get_column_mapping()

        # Read CSV and apply column mapping if needed
        df = pd.read_csv(csv_path)

        # Filter columns based on column mapping
        if column_mapping:
            # Only keep columns that are in the mapping
            csv_columns = list(column_mapping.keys())
            df_to_load = df[csv_columns].copy()
            # Rename columns according to mapping
            df_to_load = df_to_load.rename(columns=column_mapping)
        else:
            df_to_load = df

        # Create staging table based on filtered columns
        columns = self._infer_column_types(df_to_load)
        self.staging_mgr.create_staging_from_csv_structure(target_table, columns)

        # Load data into staging table - pass the filtered df
        row_count = self.staging_mgr.copy_csv_to_staging(str(csv_path), staging_table, df=df_to_load)
        self.stats['rows_read'] = row_count

        # Calculate derived fields (like current_date_year and parent_league_id transformations)
        self._calculate_derived_fields(staging_table)

        # Truncate target and insert from staging
        with self.db.get_session() as session:
            session.execute(text(f"TRUNCATE TABLE {target_table} CASCADE"))

            if column_mapping:
                # Build explicit column list for mapped tables
                target_cols = list(column_mapping.values())
                cols_str = ', '.join(target_cols)
                session.execute(text(f"""
                    INSERT INTO {target_table} ({cols_str})
                    SELECT {cols_str} FROM {staging_table}
                """))
            else:
                # Get column names from staging table to ensure proper order
                cols_sql = text("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = :table_name
                    ORDER BY ordinal_position
                """)
                result = self.db.execute_sql(cols_sql, {'table_name': staging_table})
                staging_columns = [row[0] for row in result]

                # Get target table columns
                result = self.db.execute_sql(cols_sql, {'table_name': target_table})
                target_columns = [row[0] for row in result]

                # Find common columns (staging might have extra calculated fields)
                common_columns = [col for col in staging_columns if col in target_columns]
                cols_str = ', '.join(common_columns)

                session.execute(text(f"""
                    INSERT INTO {target_table} ({cols_str})
                    SELECT {cols_str} FROM {staging_table}
                """))

            self.stats['rows_inserted'] = row_count

        # Cleanup staging table
        self.staging_mgr.drop_staging_table(staging_table)
        self._record_file_completion(csv_path, 'success')
        return True

    def _handle_incremental_load(self, csv_path: Path) -> bool:
        """Handle incremental load - only insert/update changed records"""
        # TODO Implement incremental logic with UPSERT
        logger.warning("Incremental load not yet implemented, using full load")
        return self._handle_full_load(csv_path)


    def _handle_append_load(self, csv_path: Path) -> bool:
        """Handle append load - only insert new records"""
        # TODO implement append logic with watermark
        logger.warning("Append load not yet implemented, using full load")
        return self._handle_full_load(csv_path)


    def _infer_column_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infer PostgreSQL column types from DataFrame dtypes"""
        type_mapping = {
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'TEXT'
        }

        columns = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
            pg_type = type_mapping.get(dtype, 'TEXT')
            columns[col] = pg_type
        return columns


    def _record_file_start(self, csv_path: Path):
        """Record file processing start in metadata"""
        sql = text("""
        INSERT INTO etl_file_metadata (filename, last_status, last_batch_id, last_processed)
        VALUES (:filename, 'in_progress', :batch_id, CURRENT_TIMESTAMP)
        ON CONFLICT (filename) DO UPDATE SET
        last_status = 'in_progress',
        last_batch_id = :batch_id,
        last_processed = CURRENT_TIMESTAMP
        """)

        self.db.execute_sql(sql, {
            'batch_id': self.batch_id,
            'filename': csv_path.name
        })


    def _record_file_completion(self, csv_path: Path, status: str, error: str = None):
        """Record file processing completion in metadata"""
        sql = text("""
            INSERT INTO etl_file_metadata (filename, last_status, rows_processed, rows_updated, rows_deleted, error_message, processing_time_seconds)
            VALUES (:filename, :status, :rows_processed, :rows_updated, :rows_deleted, :error_message, 0)
            ON CONFLICT (filename) DO UPDATE SET
            last_status = :status,
            rows_processed = :rows_processed,
            rows_updated = :rows_updated,
            rows_deleted = :rows_deleted,
            error_message = :error_message,
            processing_time_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - etl_file_metadata.last_processed))         
            """)
        self.db.execute_sql(sql, {
            'filename': csv_path.name,
            'status': status,
            'rows_processed': self.stats['rows_inserted'],
            'rows_updated': self.stats['rows_updated'],
            'rows_deleted': self.stats['rows_deleted'],
            'error_message': error
        })

    def _create_batch_run(self):
        """Create a batch run record if batch_id is provided"""
        if not self.batch_id:
            return

        sql = text("""
             INSERT INTO etl_batch_runs (batch_id, batch_type, environment, status, triggered_by)
             VALUES (:batch_id, :batch_type, :environment, 'running', :triggered_by)
             ON CONFLICT (batch_id) DO NOTHING
         """)

        self.db.execute_sql(sql, {
            'batch_id': self.batch_id,
            'batch_type': 'incremental',  # or could be based on load strategy
            'environment': 'dev',  # could be from config
            'triggered_by': 'etl_pipeline'
        })

    def _calculate_derived_fields(self, staging_table: str):
        """Calculate derived fields based on loader's get_calculated_fields"""
        calculated_fields = self.get_calculated_fields()

        if not calculated_fields:
            return

        logger.info(f"Calculating derived fields for {staging_table}")

        # First, add columns if they don't exist
        for field, expression in calculated_fields.items():
            # Determine column type based on expression
            if 'CURRENT_TIMESTAMP' in expression:
                col_type = 'TIMESTAMP'
            elif 'INTEGER' in expression:
                col_type = 'INTEGER'
            elif 'DECIMAL' in expression or 'ROUND' in expression:
                col_type = 'DECIMAL(4,3)'
            elif 'TO_DATE' in expression:
                col_type = 'DATE'
            else:
                col_type = 'DECIMAL(8,3)'

            add_col_sql = text(f"""
                ALTER TABLE {staging_table}
                ADD COLUMN IF NOT EXISTS {field} {col_type}
            """)
            self.db.execute_sql(add_col_sql)

        # Build UPDATE statement for calculated fields
        set_clauses = []
        for field, expression in calculated_fields.items():
            set_clauses.append(f"{field} = {expression}")

        if set_clauses:
            update_sql = text(f"""
                UPDATE {staging_table}
                SET {', '.join(set_clauses)}
            """)

            self.db.execute_sql(update_sql)
            logger.info(f"Calculated fields updated in {staging_table}")

    def _upsert_from_staging(self, staging_table: str, target_table: str):
        """Perform UPSERT from staging to target table"""
        upsert_keys = self.get_upsert_keys()
        update_columns = self.get_update_columns()
        calculated_fields = self.get_calculated_fields()

        # Get columns from both staging and target tables
        cols_sql = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name
            ORDER BY ordinal_position
        """)

        # Get target columns
        result = self.db.execute_sql(cols_sql, {'table_name': target_table})
        target_columns = [row[0] for row in result]

        # Build SELECT clause with calculated expressions where needed
        select_clauses = []
        column_mapping = self.get_column_mapping() or {}
        reverse_mapping = {v: k for k, v in column_mapping.items()}
        for col in target_columns:
            if col in calculated_fields:
                # Use the calculated expression
                select_clauses.append(f"({calculated_fields[col]}) AS {col}")
            else:
                # Use direct column reference
                staging_col = reverse_mapping.get(col, col)
                select_clauses.append(f"s.{staging_col} AS {col}")

        # Build INSERT statement
        insert_cols = ', '.join(target_columns)
        select_cols = ', '.join(select_clauses)
        conflict_keys = ', '.join(upsert_keys)

        # Build UPDATE SET clause for conflicts
        update_set_clauses = []
        for col in update_columns:
            if col in target_columns and col not in upsert_keys:
                update_set_clauses.append(f"{col} = EXCLUDED.{col}")

        if update_set_clauses:
            upsert_sql = text(f"""
                INSERT INTO {target_table} ({insert_cols})
                SELECT {select_cols}
                FROM {staging_table} s
                ON CONFLICT ({conflict_keys}) DO UPDATE SET
                {', '.join(update_set_clauses)}
            """)
        else:
            upsert_sql = text(f"""
                INSERT INTO {target_table} ({insert_cols})
                SELECT {select_cols}
                FROM {staging_table} s
                ON CONFLICT ({conflict_keys}) DO NOTHING
            """)

        with self.db.get_session() as session:
            result = session.execute(upsert_sql)
            row_count = result.rowcount
            session.commit()

        logger.info(f"Upserted {row_count} rows from {staging_table} to {target_table}")
        return row_count


