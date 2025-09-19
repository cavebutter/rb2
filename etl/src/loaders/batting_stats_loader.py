from loaders.stats_loader import StatsLoader
from typing import List, Dict, Optional

class BattingStatsLoader(StatsLoader):
    """Loader for batting statistics"""
    def get_target_table(self) -> str:
        return 'players_career_batting_stats'

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
            # Metadata
            'last_updated': 'CURRENT_TIMESTAMP'
        }

    def get_update_columns(self) -> List[str]:
        """What to update on UPSERT"""
        current_season = self.get_current_season()

        # Always update counting stats
        base_columns = [
            'ab', 'h', 'k', 'pa', 'pitches-seen', 'g', 'gs',
            'd', 't', 'hr', 'r', 'rbi', 'sb', 'cs', 'bb', 'ibb',
            'gdp', 'sh', 'sf', 'hp', 'ci', 'wpa', 'ubr', 'war'
        ]

        # Add calculated fields for current season
        if self.should_update_calculated_fields():
            base_columns.extend([
                'batthing_average', 'on_base_percentage',
                'slugging_percentage', 'ops', 'iso', 'babip',
                'woba', 'wrc', 'wrc_plus', 'last_updated'
            ])

        return base_columns

