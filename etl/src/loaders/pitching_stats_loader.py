from typing import List, Dict, Optional
from pathlib import Path
from loguru import logger
import pandas as pd
from sqlalchemy import text
from .stats_loader import StatsLoader

class PitchingStatsLoader(StatsLoader):
    """Loader for pitching statistics"""
    def get_target_table(self) -> str:
        return "players_career_pitching_stats"

    def get_primary_keys(self) -> List[str]:
        return ['player_id', 'year', 'team_id', 'split_id', 'stint']

    def get_upsert_keys(self) -> List[str]:
        return ['player_id', 'year', 'team_id', 'split_id', 'stint']

    def get_column_mapping(self) -> Optional[Dict[str, str]]:
        return None

    def get_calculated_fields(self) -> Dict[str, str]:
        return {
            # Cast to proper target types to avoid staging table precision issues
            'era': 'CASE WHEN outs > 0 THEN (ROUND((er * 9.0) / (outs / 3.0), 2))::DECIMAL(5,2) ELSE 0::DECIMAL(5,2) END',
            'whip': 'CASE WHEN outs > 0 THEN (ROUND((bb + ha) / (outs / 3.0), 2))::DECIMAL(4,2) ELSE 0::DECIMAL(4,2) END',
            'k9': 'CASE WHEN outs > 0 THEN (ROUND((k * 9.0) / (outs / 3.0), 1))::DECIMAL(4,1) ELSE 0::DECIMAL(4,1) END',
            'bb9': 'CASE WHEN outs > 0 THEN (ROUND((bb * 9.0) / (outs / 3.0), 1))::DECIMAL(4,1) ELSE 0::DECIMAL(4,1) END',
            'hr9': 'CASE WHEN outs > 0 THEN (ROUND((hra * 9.0) / (outs / 3.0), 1))::DECIMAL(4,1) ELSE 0::DECIMAL(4,1) END',
            'h9': 'CASE WHEN outs > 0 THEN (ROUND((ha * 9.0) / (outs / 3.0), 1))::DECIMAL(4,1) ELSE 0::DECIMAL(4,1) END',
            'babip': '''CASE 
                    WHEN (bf - bb - k - hra + sf) > 0 AND (ha - hra) >= 0 
                    THEN (LEAST(0.999, ROUND((ha - hra)::numeric / (bf - bb - k - hra + sf), 3)))::DECIMAL(4,3)
                    ELSE 0::DECIMAL(4,3)
                END''',
            'fip': 'NULL::DECIMAL(4,2)',
            'era_plus': 'NULL::INTEGER',
            'fip_plus': 'NULL::INTEGER',
            'constants_version': 'NULL::INTEGER',
            'last_updated': 'CURRENT_TIMESTAMP'
        }

    def _add_calculated_columns(self, staging_table: str):
        """Add calculated columns to staging table with proper types"""
        from sqlalchemy import text

        # ADD columns first (they don't exist in CSV)
        alter_sql = text(f"""
            ALTER TABLE {staging_table}
            ADD COLUMN IF NOT EXISTS era DECIMAL(5,2),
            ADD COLUMN IF NOT EXISTS whip DECIMAL(4,2),
            ADD COLUMN IF NOT EXISTS k9 DECIMAL(4,1),
            ADD COLUMN IF NOT EXISTS bb9 DECIMAL(4,1), 
            ADD COLUMN IF NOT EXISTS hr9 DECIMAL(4,1),
            ADD COLUMN IF NOT EXISTS h9 DECIMAL(4,1),
            ADD COLUMN IF NOT EXISTS babip DECIMAL(4,3),
            ADD COLUMN IF NOT EXISTS fip DECIMAL(4,2),
            ADD COLUMN IF NOT EXISTS era_plus INTEGER,
            ADD COLUMN IF NOT EXISTS fip_plus INTEGER,
            ADD COLUMN IF NOT EXISTS constants_version INTEGER,
            ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP;
        """)

        self.db.execute_sql(alter_sql)

    def get_update_columns(self) -> List[str]:
        """What to update on UPSERT"""
        # Always update counting stats
        base_columns = [
            'ip', 'ab', 'tb', 'ha', 'k', 'bf', 'rs', 'bb', 'r', 'er',
            'gb', 'fb', 'pi', 'ipf', 'g', 'gs', 'w', 'l', 's', 'sa',
            'da', 'sh', 'sf', 'ta', 'hra', 'bk', 'ci', 'iw', 'wp', 'hp',
            'gf', 'dp', 'qs', 'svo', 'bs', 'ra', 'cg', 'sho', 'sb', 'cs',
            'hld', 'ir', 'irs', 'wpa', 'li', 'outs', 'war', 'sub_league_id'
        ]

        # Add calculated fields for current season
        if self.should_update_calculated_fields():
            base_columns.extend([
                'era', 'whip', 'k9', 'bb9', 'h9', 'hr9', 'babip',
                'fip', 'era_plus', 'fip_plus', 'last_updated'
            ])

        return base_columns

    def _handle_incremental_load(self, csv_path: Path) -> bool:
        """Handle incremental load with staging table column fix"""
        staging_table = f"staging_{self.get_target_table()}"

        # Create staging and load data
        df = pd.read_csv(csv_path)

        # CREATE FRESH STAGING TABLE - This was missing!
        target_table = self.get_target_table()
        columns = self._infer_column_types(df)
        self.staging_mgr.create_staging_from_csv_structure(target_table, columns)

        row_count = self.staging_mgr.copy_csv_to_staging(str(csv_path), staging_table, df=df)

        # Populate sub_league_id
        self._populate_subleague_id(staging_table)

        # Fix column types BEFORE calculating derived fields
        self._add_calculated_columns(staging_table)

        # Now calculate derived fields with proper column types
        self._calculate_derived_fields(staging_table)

        # Complete the UPSERT
        upserted = self._upsert_from_staging(staging_table, target_table)

        self.stats["rows_inserted"] = upserted
        logger.info(f"Upserted {upserted} rows from {staging_table} to {target_table}")

        return True

    def should_update_calculated_fields(self) -> bool:
        """Update calculated fields for current season only"""
        return True
