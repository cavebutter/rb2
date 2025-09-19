from typing import List, Dict
from loaders.stats_loader import StatsLoader

class PlayersLoader(StatsLoader):
    """Loader for players master table"""

    def get_target_table(self) -> str:
        return 'players'

    def get_primary_keys(self) -> List[str]:
        return ['player_id']

    def get_upsert_keys(self) -> List[str]:
        return ['player_id']

    def get_column_mapping(self) -> Dict[str, str]:
        # Map CSV columns to DB columns if names differ
        # In this case, they are the same
        return None

    def get_update_columns(self) -> List[str]:
        """Columns to update on conflict"""
        return [
            'team_id', 'league_id', 'position', 'role',
            'weight', 'height', 'age', 'uniform_number',
            'free_agent', 'retired', 'organization_id',
            'injury_is_injured', 'injury_left', 'fatigue_points',
            'morale', 'expectation', 'last_updated'
        ]

    def get_calculated_fields(self) -> Dict[str, str]:
        """Simple calculated fields"""
        return {}