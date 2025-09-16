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
    def load_csv(self, csv_path: Path) -> bool:
        """Load data from a CSV file."""
        target_table = self.get_target_table()
        strategy = self.get_load_strategy()

        logger.info(f"Loading {csv_path} into {target_table} using {strategy} strategy")
        try:
            self._record_file_start(csv_path)

            if strategy == 'skip':
                return self.handle_skip_strategy(csv_path)
            elif strategy == 'full':
                return self.handle_full_load(csv_path)
            elif strategy == 'incremental':
                return self.handle_incremental_load(csv_path)
            elif strategy == 'append':
                return self.handle_append_load(csv_path)
            else:
                raise ValueError(f"Unknown load strategy: {strategy}")
        except Exception as e:
            logger.error(f"Error loading {csv_path}: {e}")
            self.stats['errors'].append(str(e))
            self._record_file_commpletion(csv_path, 'failed', str(e))
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

        # Create staging table
        df = pd.read_csv(csv_path, nrows=5)  # Read a few rows to infer schema
        columns = self._infer_column_types(df)
        self.staging_mgr.create_staging_from_csv_structure(target_table, columns)

        # Load data into staging table
        row_count = self.staging_mgr.copy_csv_to_staging(str(csv_path), staging_table)
        self.stats['rows_read'] = row_count

        # Truncate target and insert from staging
        with self.db.get_session() as session:
            session.execute(text(f"TRUNCATE TABLE {target_table} CASCADE"))
            session.execute(text(f"""
                INSERT INTO {target_table}
                SELECT * FROM {staging_table}
                """))
            self.stats['rows_inserted'] = row_count

        # Cleanup
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
        """Infer PostreSQL column types from DataFrame dtypes"""
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
        UPDATE etl_file_metadata
        SET last_status = 'in_progress',
        last_batch_id = :batch_id,
        last_processed = CURRENT_TIMESTAMP
        WHERE filename = :filename
        """)

        self.db.execute_sql(sql, {
            'batch_id': self.batch_id,
            'filename': csv_path.name
        })


    def _record_file_completion(self, csv_path: Path, status: str, error: str = None):
        """Record file processing completion in metadata"""
        sql = text("""
            UPDATE etl_file_metadata
            SET last_status = :status,
            rows_inserted = :rows_inserted,
            rows_updated = :rows_updated,
            rows_deleted = :rows_deleted,
            error_message = :error_message,
            processing_time_seconds = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - last_processed)
            
            """)
        self.db.execute_sql(sql, {
            'filename': csv_path.name,
            'status': status,
            'rows_inserted': self.stats['rows_inserted'],
            'rows_updated': self.stats['rows_updated'],
            'rows_deleted': self.stats['rows_deleted'],
            'error_message': error
        })