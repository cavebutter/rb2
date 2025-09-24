from typing import List, Dict, Optional
from pathlib import Path
from loguru import logger
import pandas as pd
from sqlalchemy import text
from .stats_loader import StatsLoader
from ..utils.batch import generate_batch_id

class BattingStatsLoader(StatsLoader):
    """Loader for batting statistics"""
    
    def get_target_table(self) -> str:
        return 'players_career_batting_stats'

    def get_primary_keys(self) -> List[str]:
        return ['player_id', 'year', 'team_id', 'split_id', 'stint']

    def get_upsert_keys(self) -> List[str]:
        return ['player_id', 'year', 'team_id', 'split_id', 'stint']

    def get_column_mapping(self) -> Optional[Dict[str, str]]:
        return None

    def get_calculated_fields(self) -> Dict[str, str]:
        # Basic rate stats - always recalculated
        return {
            # Traditional stats
            'batting_average': 'CASE WHEN ab > 0 THEN ROUND(h::numeric / ab, 3) ELSE 0 END',
            'on_base_percentage': 'CASE WHEN (ab + bb + hp + sf) > 0 THEN ROUND((h + bb + hp)::numeric / (ab + bb + hp + sf), 3) ELSE 0 END',
            'slugging_percentage': 'CASE WHEN ab > 0 THEN ROUND(((h - d - t - hr) + (2 * d) + (3 * t) + (4 * hr))::numeric / ab, 3) ELSE 0 END',
            'ops': 'ROUND(on_base_percentage + slugging_percentage, 3)',
            'iso': 'ROUND(slugging_percentage - batting_average, 3)',
            'babip': 'CASE WHEN (ab - k - hr + sf) > 0 THEN ROUND((h - hr)::numeric / (ab - k - hr + sf), 3 ) ELSE 0 END',
            # Advanced stats placeholders - these are calculated post-load
            'woba': 'NULL::DECIMAL(4,3)',
            'wrc': 'NULL::INTEGER',
            'wrc_plus': 'NULL::INTEGER',
            # Version Tracking for constants calculation
            'constants_version': 'NULL::INTEGER',
            # Metadata
            'last_updated': 'CURRENT_TIMESTAMP'
        }

    def get_update_columns(self) -> List[str]:
        """What to update on UPSERT"""
        # Always update counting stats
        base_columns = [
            'ab', 'h', 'k', 'pa', 'pitches_seen', 'g', 'gs',
            'd', 't', 'hr', 'r', 'rbi', 'sb', 'cs', 'bb', 'ibb',
            'gdp', 'sh', 'sf', 'hp', 'ci', 'wpa', 'ubr', 'war',
            'sub_league_id'
        ]

        # Add calculated fields for current season
        if self.should_update_calculated_fields():
            base_columns.extend([
                'batting_average', 'on_base_percentage',
                'slugging_percentage', 'ops', 'iso', 'babip',
                'woba', 'wrc', 'wrc_plus', 'last_updated'
            ])

        return base_columns

    def should_update_calculated_fields(self) -> bool:
        """Update calculated fields for current season only"""
        # Simple implementation - could be enhanced to check current season
        return True

