"""
Game-Level Statistics Loaders

Loads individual game performance data for batting and pitching.
These tables support newspaper article generation by providing
game-by-game player performance context.
"""
from typing import List, Dict, Optional
from pathlib import Path
from loguru import logger
import pandas as pd
from sqlalchemy import text
from .stats_loader import StatsLoader
from ..utils.csv_preprocessor import CSVPreprocessor


class GameBattingStatsLoader(StatsLoader):
    """Loader for game-level batting statistics"""

    def get_target_table(self) -> str:
        return 'players_game_batting_stats'

    def get_primary_keys(self) -> List[str]:
        return ['player_id', 'year', 'game_id']

    def get_upsert_keys(self) -> List[str]:
        return ['player_id', 'year', 'game_id']

    def get_column_mapping(self) -> Optional[Dict[str, str]]:
        """Map CSV columns to database columns if needed"""
        # CSV has all the columns we need, no mapping required
        return None

    def get_calculated_fields(self) -> Dict[str, str]:
        """No calculated fields for game stats - raw data only"""
        return {}

    def get_update_columns(self) -> List[str]:
        """Columns to update on UPSERT"""
        return [
            'team_id', 'ab', 'h', 'd', 't', 'hr', 'r', 'rbi',
            'bb', 'k', 'sb', 'cs', 'sf', 'sh', 'hp', 'gdp'
        ]

    def _handle_incremental_load(self, csv_path: Path) -> bool:
        """
        Game stats use incremental loading strategy.

        Strategy:
        - Load all game stats (no split_id filtering like career stats)
        - Upsert based on (player_id, year, game_id)
        - Replace existing game records if they exist
        """
        logger.info(f"Loading game batting stats from: {csv_path}")

        # Read full CSV and deduplicate based on upsert keys
        df = pd.read_csv(csv_path, low_memory=False)

        # Deduplicate using upsert keys (player_id, year, game_id)
        df = CSVPreprocessor.deduplicate_rows(df, subset=['player_id', 'year', 'game_id'])

        # Infer column types from deduplicated data
        columns = self._infer_column_types(df.head(1))

        # Create staging table from CSV structure
        staging_table = f"staging_{self.get_target_table()}"

        if not self.staging_mgr.create_staging_from_csv_structure(
            table_name=self.get_target_table(),
            columns=columns,
            staging_prefix="staging_"
        ):
            logger.error(f"Failed to create staging table: {staging_table}")
            return False

        # Load deduplicated CSV into staging
        if not self.staging_mgr.copy_csv_to_staging(csv_path, staging_table, df=df):
            logger.error(f"Failed to load CSV into staging: {staging_table}")
            return False

        # Populate calculated fields (if any)
        self._calculate_derived_fields(staging_table)

        # Upsert from staging to target
        upserted = self._upsert_from_staging(staging_table, self.get_target_table())
        logger.info(f"Upserted {upserted} rows from {staging_table} to {self.get_target_table()}")

        return True


class GamePitchingStatsLoader(StatsLoader):
    """Loader for game-level pitching statistics"""

    def get_target_table(self) -> str:
        return 'players_game_pitching_stats'

    def get_primary_keys(self) -> List[str]:
        return ['player_id', 'year', 'game_id']

    def get_upsert_keys(self) -> List[str]:
        return ['player_id', 'year', 'game_id']

    def get_column_mapping(self) -> Optional[Dict[str, str]]:
        """Map CSV columns to database columns"""
        # The CSV might have different column names
        return {
            'ipf': 'ip',  # innings pitched fraction -> decimal IP
            'ha': 'h',    # hits allowed -> h
            'pi': 'pc',   # pitches -> pitch count
        }

    def get_calculated_fields(self) -> Dict[str, str]:
        """Convert IPF to decimal IP if needed"""
        # IP is stored as decimal in DB (e.g., 6.2 = 6 2/3 innings)
        # If CSV has ipf (outs), convert: ipf / 3 = decimal IP
        return {
            # 'ip': 'ROUND((ipf::decimal / 3), 1)'  # Uncomment if CSV has ipf instead of ip
        }

    def get_update_columns(self) -> List[str]:
        """Columns to update on UPSERT"""
        return [
            'team_id', 'ip', 'h', 'r', 'er', 'bb', 'k', 'hr', 'bf', 'pc',
            'w', 'l', 'sv', 'hld', 'bs', 'cg', 'sho', 'qs'
        ]

    def _handle_incremental_load(self, csv_path: Path) -> bool:
        """
        Game stats use incremental loading strategy.

        Strategy:
        - Load all game stats
        - Upsert based on (player_id, year, game_id)
        - Replace existing game records if they exist
        """
        logger.info(f"Loading game pitching stats from: {csv_path}")

        # Read full CSV and deduplicate based on upsert keys
        df = pd.read_csv(csv_path, low_memory=False)

        # Deduplicate using upsert keys (player_id, year, game_id)
        df = CSVPreprocessor.deduplicate_rows(df, subset=['player_id', 'year', 'game_id'])

        # Infer column types from deduplicated data
        columns = self._infer_column_types(df.head(1))

        # Create staging table from CSV structure
        staging_table = f"staging_{self.get_target_table()}"

        if not self.staging_mgr.create_staging_from_csv_structure(
            table_name=self.get_target_table(),
            columns=columns,
            staging_prefix="staging_"
        ):
            logger.error(f"Failed to create staging table: {staging_table}")
            return False

        # Load deduplicated CSV into staging
        if not self.staging_mgr.copy_csv_to_staging(csv_path, staging_table, df=df):
            logger.error(f"Failed to load CSV into staging: {staging_table}")
            return False

        # Populate calculated fields (if any)
        self._calculate_derived_fields(staging_table)

        # Upsert from staging to target
        upserted = self._upsert_from_staging(staging_table, self.get_target_table())
        logger.info(f"Upserted {upserted} rows from {staging_table} to {self.get_target_table()}")

        return True