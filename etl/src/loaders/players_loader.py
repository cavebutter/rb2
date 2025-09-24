"""Multi-target loader for normalized players tables"""
from typing import List, Dict, Optional
from pathlib import Path
from loguru import logger
import pandas as pd
import json
from sqlalchemy import text
from .base_loader import BaseLoader
from ..utils.batch import generate_batch_id

class PlayersLoader(BaseLoader):
    """Loader for normalized players tables"""

    def __init__(self, batch_id: str = None):
        super().__init__(batch_id)
        self.current_season = 2024

    def get_target_table(self) -> str:
        return 'players_core'

    def get_primary_keys(self) -> List[str]:
        return ['player_id']

    def get_upsert_keys(self) -> List[str]:
        return ['player_id']

    def get_column_mapping(self) -> Optional[Dict[str, str]]:
        return None

    def get_update_columns(self) -> List[str]:
        return [] # Handled per table

    def get_calculated_fields(self) -> Dict[str, str]:
        return {}  # Handled per table

    def get_load_strategy(self) -> str:
        return 'incremental'

    def should_update_calculated_fields(self) -> bool:
        return True

    def _handle_incremental_load(self, csv_path: Path) -> bool:
        """Handle  multi-table incremental load"""
        logger.info(f"Loading players CSV into normalized tables: {csv_path}")

        try:
            # Read and prepare data
            df = pd.read_csv(csv_path)
            self.stats["rows_read"] = len(df)

            # Split data for each target table
            core_data = self._prepare_core_data(df)
            status_data = self._prepare_status_data(df)
            contracts_data = self._prepare_contracts_data(df)
            ratings_data = self._prepare_ratings_data(df)

            # Laod each table in dependency order
            with self.db.get_session() as session:
                # 1. Load core data first
                core_count = self._load_core_table(core_data, session)

                # 2. Load dependent tables
                status_count = self._load_status_table(status_data, session)
                contracts_count = self._load_contracts_table(contracts_data, session)
                ratings_count = self._load_ratings_table(ratings_data, session)

                session.commit()

                total_rows = core_count + status_count + contracts_count + ratings_count
                self.stats["rows_inserted"] = total_rows
                logger.info(f"Successfully loaded players data: core={core_count}, status={status_count}, contracts={contracts_count}, ratings={ratings_count}")
            self._record_file_completion(csv_path, 'success')
            return True
        except Exception as e:
            logger.error(f"Error in multi-table players load: {e}")
            self._record_file_completion(csv_path, 'failed', str(e))
            raise

    def _prepare_core_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare data for players_core table"""
        core_columns = [
            'player_id', 'first_name', 'last_name', 'nick_name', 'date_of_birth',
            'city_of_birth_id', 'nation_id', 'second_nation_id', 'height', 'weight',
            'bats', 'throws', 'person_type', 'language_ids0', 'language_ids1',
            'historical_id', 'historical_team_id', 'college', 'acquired', 'acquired_date',
            'draft_year', 'draft_round', 'draft_supplemental', 'draft_pick', 'draft_overall_pick',
            'draft_eligible', 'hsc_status', 'redshirt', 'picked_in_draft', 'school',
            'commit_school', 'draft_league_id', 'draft_team_id'
        ]

        core_df = df[core_columns].copy()

        # Handle date conversions
        core_df['date_of_birth'] = pd.to_datetime(core_df['date_of_birth'], errors='coerce')
        core_df['acquired_date'] = pd.to_datetime(core_df['acquired_date'], errors='coerce')

        # Add timestamps
        core_df['created_at'] = pd.Timestamp.now()
        core_df['updated_at'] = pd.Timestamp.now()

        return core_df



    def _prepare_status_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare data for players_current_status table"""
        status_columns = [
            'player_id', 'team_id', 'league_id', 'position', 'role', 'uniform_number',
            'age', 'retired', 'free_agent', 'hall_of_fame', 'inducted', 'turned_coach',
            'last_league_id', 'last_team_id', 'organization_id', 'last_organization_id',
            'experience', 'hidden', 'rust', 'local_pop', 'national_pop', 'draft_protected',
            'on_loan', 'loan_league_id', 'loan_team_id'
        ]

        status_df = df[status_columns].copy()
        status_df['season_year'] = self.current_season
        status_df['last_updated'] = pd.Timestamp.now()

        return status_df


    def _prepare_contracts_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare data for players_contracts table"""
        contracts_columns = [
            'player_id', 'best_contract_offer_id', 'morale', 'morale_mod',
            'morale_player_performance', 'morale_team_performance', 'morale_team_transactions',
            'morale_team_chemistry', 'morale_player_role', 'expectation'
        ]

        contracts_df = df[contracts_columns].copy()
        contracts_df['season_year'] = self.current_season
        contracts_df['team_id'] = df['team_id']  # Add team_id for context

        return contracts_df

    def _prepare_ratings_data(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare JSONB ratings data for players_ratings table"""
        ratings_records = []

        for _, row in df.iterrows():
            player_id = row['player_id']

            # Personality ratings
            personality_data = {
                'greed': row['personality_greed'],
                'loyalty': row['personality_loyalty'],
                'play_for_winner': row['personality_play_for_winner'],
                'work_ethic': row['personality_work_ethic'],
                'intelligence': row['personality_intelligence'],
                'leader': row['personality_leader']
            }
            ratings_records.append({
                'player_id': player_id,
                'season_year': self.current_season,
                'rating_type': 'personality',
                'ratings': json.dumps(personality_data)
            })

            # Injury ratings
            injury_data = {
                'is_injured': row['injury_is_injured'],
                'dtd_injury': row['injury_dtd_injury'],
                'career_ending': row['injury_career_ending'],
                'dl_left': row['injury_dl_left'],
                'dl_playoff_round': row['injury_dl_playoff_round'],
                'injury_left': row['injury_left'],
                'dtd_injury_effect': row['dtd_injury_effect'],
                'dtd_injury_effect_hit': row['dtd_injury_effect_hit'],
                'dtd_injury_effect_throw': row['dtd_injury_effect_throw'],
                'dtd_injury_effect_run': row['dtd_injury_effect_run'],
                'injury_id': row['injury_id'],
                'injury_id2': row['injury_id2'],
                'injury_dtd_injury2': row['injury_dtd_injury2'],
                'injury_left2': row['injury_left2'],
                'dtd_injury_effect2': row['dtd_injury_effect2'],
                'dtd_injury_effect_hit2': row['dtd_injury_effect_hit2'],
                'dtd_injury_effect_throw2': row['dtd_injury_effect_throw2'],
                'dtd_injury_effect_run2': row['dtd_injury_effect_run2'],
                'prone_overall': row['prone_overall'],
                'prone_leg': row['prone_leg'],
                'prone_back': row['prone_back'],
                'prone_arm': row['prone_arm']
            }
            ratings_records.append({
                'player_id': player_id,
                'season_year': self.current_season,
                'rating_type': 'injury',
                'ratings': json.dumps(injury_data)
            })

            # Fatigue ratings
            fatigue_data = {
                'pitches0': row['fatigue_pitches0'],
                'pitches1': row['fatigue_pitches1'],
                'pitches2': row['fatigue_pitches2'],
                'pitches3': row['fatigue_pitches3'],
                'pitches4': row['fatigue_pitches4'],
                'pitches5': row['fatigue_pitches5'],
                'fatigue_points': row['fatigue_points'],
                'played_today': row['fatigue_played_today']
            }
            ratings_records.append({
                'player_id': player_id,
                'season_year': self.current_season,
                'rating_type': 'fatigue',
                'ratings': json.dumps(fatigue_data)
            })

            # Strategy ratings
            strategy_data = {
                'override_team': row['strategy_override_team'],
                'stealing': row['strategy_stealing'],
                'running': row['strategy_running'],
                'bunt_for_hit': row['strategy_bunt_for_hit'],
                'sac_bunt': row['strategy_sac_bunt'],
                'hit_run': row['strategy_hit_run'],
                'hook_start': row['strategy_hook_start'],
                'hook_relief': row['strategy_hook_relief'],
                'pitch_count': row['strategy_pitch_count'],
                'pitch_around': row['strategy_pitch_around'],
                'never_pinch_hit': row['strategy_never_pinch_hit'],
                'defensive_sub': row['strategy_defensive_sub'],
                'dtd_sit_min': row['strategy_dtd_sit_min'],
                'dtd_allow_ph': row['strategy_dtd_allow_ph']
            }
            ratings_records.append({
                'player_id': player_id,
                'season_year': self.current_season,
                'rating_type': 'strategy',
                'ratings': json.dumps(strategy_data)
            })

        return ratings_records



    def _load_core_table(self, core_df: pd.DataFrame, session) -> int:
        """Load data into players_core table"""
        logger.info("Loading players_core table")
        core_df.to_sql('players_core', self.db.engine, if_exists='append', index=False, method='multi')
        return len(core_df)


    def _load_status_table(self, status_df: pd.DataFrame, session) -> int:
        """Load data into players_current_status table"""
        logger.info("Loading players_current_status table")
        status_df.to_sql('players_current_status', self.db.engine, if_exists='append', index=False, method='multi')
        return len(status_df)


    def _load_contracts_table(self, contracts_df: pd.DataFrame, session) -> int:
        """Load data into players_contracts table"""
        logger.info("Loading players_contracts table")
        contracts_df.to_sql('players_contracts', self.db.engine, if_exists='append', index=False, method='multi')
        return len(contracts_df)

    def _load_ratings_table(self, ratings_data: List[Dict], session) -> int:
        """Load data into players_ratings table"""
        logger.info("Loading players_ratings table")
        ratings_df = pd.DataFrame(ratings_data)
        ratings_df.to_sql('players_ratings', self.db.engine, if_exists='append', index=False, method='multi')
        return len(ratings_df)

    def _get_current_season(self) -> int:
        """Get current season from leagues table"""
        try:
            sql = text("SELECT MAX(current_date_year) FROM leagues WHERE current_date_year IS NOT NULL")
            result = self.db.execute_sql(sql)
            season = result[0][0] if result and result[0][0] else 2024
            logger.info(f"Detected current season: {season}")
            return season
        except Exception as e:
            logger.warning(f"Could not detect season from leagues, using default 2024: {e}")
            return 2024