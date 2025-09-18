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


    def load_csv(self, csv_path: Path) -> bool:
        """Load data from a CSV file."""
        target_table = self.get_target_table()
        strategy = self.get_load_strategy()

        logger.info(f"Loading {csv_path} into {target_table} using {strategy} strategy")
        try:
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

        # Check if column mapping exists
        column_mapping = self.get_column_mapping()

        # Read CSV
        df = pd.read_csv(csv_path)

        # Apply column filtering if mapping exists
        if column_mapping:
            csv_columns = list(column_mapping.keys())
            df_filtered = df[csv_columns]

            # Rename columns if needed
            df_filtered = df_filtered.rename(columns=column_mapping)

            # Use filtered dataframe
            df_to_load = df_filtered
        else:
            # Use original dataframe
            df_to_load = df

        # Create staging table based on filtered columns
        columns = self._infer_column_types(df_to_load)
        self.staging_mgr.create_staging_from_csv_structure(target_table, columns)

        # Load data into staging table - pass the filtered df
        row_count = self.staging_mgr.copy_csv_to_staging(str(csv_path), staging_table, df=df_to_load)
        self.stats['rows_read'] = row_count

        # Truncate target and insert from staging
        with self.db.get_session() as session:
            session.execute(text(f"TRUNCATE TABLE {target_table} CASCADE"))
            session.execute(text(f"""
                INSERT INTO {target_table}
                SELECT * FROM {staging_table}
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