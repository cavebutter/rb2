from pathlib import Path
import pandas as pd
from loaders.base_loader import BaseLoader
from loguru import logger
from sqlalchemy import text

class StatsLoader(BaseLoader):
    """Base loader for player statistics tables"""

    def __init__(self, batch_id: str = None):
        super().__init__(batch_id)
        self._team_relations_cache = {} # Cache for sub_league lookups

    def get_load_strategy(self) -> str:
        return 'incremental'

    def _populate_subleague_id(self, staging_table: str):
        """Populate sub_league_id from team_relations"""
        logger.info(f"Populating sub_league_id in {staging_table} from team_relations")

        update_sql = text(f"""
            UPDATE {staging_table} s
            SET sub_league_id = tr.sub_league_id
            FROM team_relations tr
            WHERE s.team_id = tr.team_id
             """)

        self.db.execute(update_sql)
        logger.info("Sub_league ID populated successfully")

    def _handle_incremental_load(self, csv_path: Path) -> bool:
        """Stats-specific incremental load with sub_league population"""
        target_table = self.get_target_table()
        staging_table = f"staging_{target_table}"

        # Standard column mapping and staging load
        column_mapping = self.get_column_mapping()
        df = pd.read_csv(csv_path)

        if column_mapping:
            csv_columns = list(column_mapping.keys())
            df_filtered = df[csv_columns]
            df_filtered = df_filtered.rename(columns=column_mapping)
            df = df_filtered

        # Create staging table
        columns = self._infer_column_types(df)
        self.staging_mgr.create_staging_from_csv_structure(target_table, columns)

        # Load to staging
        row_count = self.staging_mgr.copy_csv_to_staging(str(csv_path), staging_table, df=df)
        self.stats['rows_read'] = row_count

        # Add subleague BEFORE calculating stats
        self._populate_subleague_id(staging_table)

        # Calculate basic rate stats
        self._calculate_derived_fields(staging_table)

        # UPSERT from staging
        upserted = self._upsert_from_staging(staging_table, target_table)
        self.stats['rows_inserted'] = upserted

        # Cleanup
        self.staging_mgr.drop_staging_table(staging_table)
        self._record_file_completion(csv_path, 'success')
        return True

