"""Loader for static reference tables"""
from typing import List
from pathlib import Path
from loguru import logger
from .base_loader import BaseLoader
from ..utils.checksum import calculate_file_checksum
from ..utils.message_filter import MessageFilter
from sqlalchemy import text
from typing import Optional, Dict
import pandas as pd



class ReferenceLoader(BaseLoader):
    """Loader for static reference tables that rarely change"""

    # Map CSV filenames to database tables and their keys
    REFERENCE_TABLES = {
        'continents.csv': {
            'table': 'continents',
            'primary_keys': ['continent_id'],
            'load_order': 1
        },
        'nations.csv': {
            'table': 'nations',
            'primary_keys': ['nation_id'],
            'load_order': 2
        },
        'states.csv': {
            'table': 'states',
            'primary_keys': ['state_id', 'nation_id'],
            'load_order': 3
        },
        'cities.csv': {
            'table': 'cities',
            'primary_keys': ['city_id'],
            'load_order': 4,
            'column_mapping': {
                'city_id': 'city_id',
                'nation_id': 'nation_id',
                'state_id': 'state_id',
                'name': 'name',
                'abbreviation': 'abbreviation',
                'population': 'population',
                'main_language_id': 'main_language_id',
            },
            'calculated_fields': {
                'state_id': 'NULLIF(state_id, 0)',
                 'main_language_id': 'NULLIF(main_language_id, 0)'


                # Excluding latitude and longitude due to bad data
            }
        },
        'languages.csv': {
            'table': 'languages',
            'primary_keys': ['language_id'],
            'load_order': 5
        },
        'parks.csv': {
            'table': 'parks',
            'primary_keys': ['park_id'],
            'load_order': 6,
            'column_mapping': {
                'park_id': 'park_id',
                'name': 'name',
                'nation_id': 'nation_id',
                'capacity': 'capacity',
                'type': 'type',
                'foul_ground': 'foul_ground',
                'turf': 'turf',
                'distances0': 'distances0',
                'distances1': 'distances1',
                'distances2': 'distances2',
                'distances3': 'distances3',
                'distances4': 'distances4',
                'distances5': 'distances5',
                'distances6': 'distances6',
                'wall_heights0': 'wall_heights0',
                'wall_heights1': 'wall_heights1',
                'wall_heights2': 'wall_heights2',
                'wall_heights3': 'wall_heights3',
                'wall_heights4': 'wall_heights4',
                'wall_heights5': 'wall_heights5',
                'wall_heights6': 'wall_heights6',
                'avg': 'avg',
                'd': 'd',
                't': 't',
                'hr': 'hr',
            }
        },
          'leagues.csv': {
              'table': 'leagues',
              'primary_keys': ['league_id'],
              'load_order': 7,
              'column_mapping': {
                  'league_id': 'league_id',
                  'name': 'name',
                  'abbr': 'abbr',
                  'nation_id': 'nation_id',
                  'language_id': 'language_id',
                  'logo_file_name': 'logo_file_name',
                  'parent_league_id': 'parent_league_id',
                  'league_state': 'league_state',
                  'season_year': 'season_year',
                  'league_level': 'league_level',
                  'current_date': 'game_date',
              },
              'calculated_fields': {
                  'current_date_year': 'EXTRACT(YEAR FROM current_date::date)',
                  'parent_league_id': 'NULLIF(parent_league_id, 0)'
              }
        },
        'teams.csv': {
            'table': 'teams',
            'primary_keys': ['team_id'],
            'load_order': 8,
            'calculated_fields': {
                'parent_team_id': 'NULLIF(parent_team_id, 0)',
                'city_id': 'NULLIF(city_id, 0)',
                'park_id': 'NULLIF(park_id, 0)',
                'league_id': 'NULLIF(league_id, 0)',
                'sub_league_id': 'NULLIF(sub_league_id, 0)',
                'division_id': 'NULLIF(division_id, 0)',
                'nation_id': 'NULLIF(nation_id, 0)',
                'human_id': 'NULLIF(human_id, 0)',
                }
        },
        'sub_leagues.csv': {
            'table': 'sub_leagues',
            'primary_keys': ['league_id', 'sub_league_id'],
            'load_order': 9,
        },
        'divisions.csv': {
            'table': 'divisions',
            'primary_keys': ['league_id', 'sub_league_id', 'division_id'],
            'load_order': 10,
            'calculated_fields': {
                'name': "CASE WHEN name = '' OR name IS NULL THEN 'No Division' ELSE name END"
            }
        },
        'team_relations.csv': {
            'table': 'team_relations',
            'primary_keys': ['team_id'],
            'load_order': 11
        },
        'team_record.csv': {
            'table': 'team_record',
            'primary_keys': ['team_id'],
            'load_order': 12,
        },
        # League History Tables
        'league_history.csv': {
            'table': 'league_history',
            'primary_keys': ['league_id', 'sub_league_id', 'year'],
            'load_order': 13,
        },
        'league_history_batting_stats.csv': {
            'table': 'league_history_batting_stats',
            'primary_keys': ['year', 'team_id', 'game_id', 'league_id', 'level_id', 'split_id'],
            'load_order': 14,
        },
        'league_history_pitching_stats.csv': {
            'table': 'league_history_pitching_stats',
            'primary_keys': ['year', 'team_id', 'game_id', 'level_id', 'split_id'],
            'load_order': 15,
        },
        # Team history tables
        'team_history.csv': {
            'table': 'team_history',
            'primary_keys': ['team_id', 'year'],
            'load_order': 16,
        },
        'team_history_batting_stats.csv': {
            'table': 'team_history_batting_stats',
            'primary_keys': ['team_id', 'year'],
            'load_order': 17,
        },
        'team_history_pitching_stats.csv': {
            'table': 'team_history_pitching_stats',
            'primary_keys': ['team_id', 'year'],
            'load_order': 18,
        },
        'team_history_record.csv': {
            'table': 'team_history_record',
            'primary_keys': ['team_id', 'year'],
            'load_order': 19,
        },
        # Newspaper/transaction tables (no player FKs)
        'trade_history.csv': {
            'table': 'trade_history',
            'primary_keys': ['trade_id'],
            'load_order': 20,
            'load_strategy': 'incremental',  # Never delete historical trades
            'column_mapping': {
                # Exclude trade_id - it's auto-generated SERIAL
                'date': 'date',
                'summary': 'summary',
                'message_id': 'message_id',
                'team_id_0': 'team_id_0',
                'player_id_0_0': 'player_id_0_0',
                'player_id_0_1': 'player_id_0_1',
                'player_id_0_2': 'player_id_0_2',
                'player_id_0_3': 'player_id_0_3',
                'player_id_0_4': 'player_id_0_4',
                'player_id_0_5': 'player_id_0_5',
                'player_id_0_6': 'player_id_0_6',
                'player_id_0_7': 'player_id_0_7',
                'player_id_0_8': 'player_id_0_8',
                'player_id_0_9': 'player_id_0_9',
                'draft_round_0_0': 'draft_round_0_0',
                'draft_team_0_0': 'draft_team_0_0',
                'draft_round_0_1': 'draft_round_0_1',
                'draft_team_0_1': 'draft_team_0_1',
                'draft_round_0_2': 'draft_round_0_2',
                'draft_team_0_2': 'draft_team_0_2',
                'draft_round_0_3': 'draft_round_0_3',
                'draft_team_0_3': 'draft_team_0_3',
                'draft_round_0_4': 'draft_round_0_4',
                'draft_team_0_4': 'draft_team_0_4',
                'cash_0': 'cash_0',
                'iafa_cap_0': 'iafa_cap_0',
                'team_id_1': 'team_id_1',
                'player_id_1_0': 'player_id_1_0',
                'player_id_1_1': 'player_id_1_1',
                'player_id_1_2': 'player_id_1_2',
                'player_id_1_3': 'player_id_1_3',
                'player_id_1_4': 'player_id_1_4',
                'player_id_1_5': 'player_id_1_5',
                'player_id_1_6': 'player_id_1_6',
                'player_id_1_7': 'player_id_1_7',
                'player_id_1_8': 'player_id_1_8',
                'player_id_1_9': 'player_id_1_9',
                'draft_round_1_0': 'draft_round_1_0',
                'draft_team_1_0': 'draft_team_1_0',
                'draft_round_1_1': 'draft_round_1_1',
                'draft_team_1_1': 'draft_team_1_1',
                'draft_round_1_2': 'draft_round_1_2',
                'draft_team_1_2': 'draft_team_1_2',
                'draft_round_1_3': 'draft_round_1_3',
                'draft_team_1_3': 'draft_team_1_3',
                'draft_round_1_4': 'draft_round_1_4',
                'draft_team_1_4': 'draft_team_1_4',
                'cash_1': 'cash_1',
                'iafa_cap_1': 'iafa_cap_1'
            }
        },
        'messages.csv': {
            'table': 'messages',
            'primary_keys': ['message_id'],
            'load_order': 21,
            'load_strategy': 'incremental',  # Never delete historical messages
            'apply_filters': True,  # Enable message filtering
            'calculated_fields': {
                # Convert 0 and negative values to NULL (non-trade messages use 0, -1, -5, etc.)
                'trade_id': 'CASE WHEN trade_id > 0 THEN trade_id ELSE NULL END'
            }
        },
        # Coaches and rosters (loaded manually after players in load-stats command)
        'coaches.csv': {
            'table': 'coaches',
            'primary_keys': ['coach_id'],
            'load_order': 99,  # Manual load only
            'calculated_fields': {
                'former_player_id': 'NULLIF(former_player_id, 0)'
            }
        },
        'team_roster.csv': {
            'table': 'team_roster',
            'primary_keys': ['team_id', 'player_id'],
            'load_order': 100  # Manual load only
        },
        'team_roster_staff.csv': {
            'table': 'team_roster_staff',
            'primary_keys': ['team_id'],
            'load_order': 101  # Manual load only
        }

    }



    def __init__(self, csv_filename: str, batch_id: str = None):
        super().__init__(batch_id)
        self.csv_filename = csv_filename

        if csv_filename not in self.REFERENCE_TABLES:
            raise ValueError(f"Unknown reference table CSV: {csv_filename}")
        self.config = self.REFERENCE_TABLES[csv_filename]
        self.table_name = self.config['table']


    def get_column_mapping(self) -> Optional[Dict[str, str]]:
        """Return column mapping if defined in config"""
        return self.config.get('column_mapping')


    def get_load_strategy(self) -> str:
        """Return load strategy from config, default to 'skip' for reference tables."""
        return self.config.get('load_strategy', 'skip')


    def get_primary_keys(self) -> List[str]:
        """Return primary key columns for this table"""
        return self.config['primary_keys']


    def get_target_table(self) -> str:
        """Return the target table name"""
        return self.table_name

    def get_calculated_fields(self) -> Dict[str, str]:
        """Reference tables don't need calculated fields"""
        return self.config.get('calculated_fields', {})

    def get_upsert_keys(self) -> List[str]:
        """Use primary keys for UPSERT operations"""
        return self.get_primary_keys()

    def get_update_columns(self) -> List[str]:
        """For incremental loads, specify which columns to update on conflict"""
        # For messages and trades, we want to update all non-key columns
        if self.table_name in ['messages', 'trade_history']:
            return ['*']  # Update all columns except primary key
        return []  # Other reference tables: insert-only

    def _handle_skip_strategy(self, csv_path: Path) -> bool:
        """Override to implement checksum comparison"""
        logger.info(f"Checking if {csv_path.name} has changed...")

        # Calculate current file checksum
        current_checksum = calculate_file_checksum(csv_path)

        # Get stored checksum from metadata
        stored_checksum = self._get_stored_checksum(csv_path.name)

        if stored_checksum and current_checksum == stored_checksum:
            logger.info(f"File {csv_path.name} unchanged (checksum: {current_checksum[:8]}...")
            self._record_file_completion(csv_path, 'skipped')
            return True

        # File has changed or is new, perform full load
        logger.info(f"File {csv_path.name} has changed, performing full load")
        success = self._handle_full_load(csv_path)

        if success:
            # Update stored checksum
            self._update_stored_checksum(csv_path.name, current_checksum)
        return success

    def _handle_incremental_load(self, csv_path: Path) -> bool:
        """Handle incremental load with UPSERT - preserves historical records"""
        target_table = self.get_target_table()
        staging_table = f"staging_{target_table}"
        column_mapping = self.get_column_mapping()

        logger.info(f"Performing incremental load for {target_table} (preserves all historical data)")

        # Read CSV
        try:
            df = pd.read_csv(csv_path)
        except pd.errors.ParserError as e:
            logger.warning(f"Malformed CSV detected, attempting to skip bad lines: {e}")
            df = pd.read_csv(csv_path, on_bad_lines='skip', engine='python')
            logger.info(f"Successfully loaded CSV with {len(df)} rows (skipped bad lines)")

        # Apply message filtering if configured
        if self.config.get('apply_filters') and self.csv_filename == 'messages.csv':
            df = self._apply_message_filters(df)

        # Apply CSV preprocessing
        from ..utils.csv_preprocessor import CSVPreprocessor
        primary_keys = self.get_primary_keys()

        dedup_subset = None
        if primary_keys and column_mapping:
            reverse_mapping = {v: k for k, v in column_mapping.items()}
            dedup_subset = [reverse_mapping.get(pk) for pk in primary_keys if reverse_mapping.get(pk)]
            if not dedup_subset or len(dedup_subset) != len(primary_keys):
                dedup_subset = None
        elif primary_keys:
            dedup_subset = primary_keys

        df = CSVPreprocessor.preprocess(df, config={
            'clean_quoted_strings': True,
            'deduplicate': True,
            'dedup_subset': dedup_subset
        })

        # Filter columns based on column mapping
        if column_mapping:
            csv_columns = list(column_mapping.keys())
            df_to_load = df[csv_columns].copy()
            df_to_load = df_to_load.rename(columns=column_mapping)
        else:
            df_to_load = df

        # Create staging table
        columns = self._infer_column_types(df_to_load)
        self.staging_mgr.create_staging_from_csv_structure(target_table, columns)

        # Load data into staging
        row_count = self.staging_mgr.copy_csv_to_staging(str(csv_path), staging_table, df=df_to_load)
        self.stats['rows_read'] = row_count

        # Calculate derived fields
        self._calculate_derived_fields(staging_table)

        # Perform UPSERT from staging to target
        upserted_count = self._upsert_from_staging(staging_table, target_table)
        self.stats['rows_inserted'] = upserted_count

        # Cleanup staging table
        self.staging_mgr.drop_staging_table(staging_table)
        self._record_file_completion(csv_path, 'success')

        logger.info(f"Incremental load complete: {upserted_count} rows upserted (no historical data removed)")
        return True

    def _apply_message_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply message filtering based on configuration"""
        try:
            from config.etl_config import MESSAGE_FILTERS

            message_filter = MessageFilter(MESSAGE_FILTERS)
            logger.info(message_filter.get_filter_summary())

            filtered_df = message_filter.filter_messages(df)
            return filtered_df
        except ImportError:
            logger.warning("Could not import MESSAGE_FILTERS from config, skipping filters")
            return df
        except Exception as e:
            logger.error(f"Error applying message filters: {e}")
            return df

    def _handle_full_load(self, csv_path: Path) -> bool:
        """Override to handle special post-load operations"""
        # Call parent's full load method
        success = super()._handle_full_load(csv_path)

        if success and self.csv_filename == 'nations.csv':
            # Add nation_id=0 record for parks table foreign key
            self._add_placeholder_nation()

        return success

    def _add_placeholder_nation(self):
        """Add nation_id=0 placeholder record"""
        logger.info("Adding nation_id=0 placeholder record")
        sql = text("""
            INSERT INTO nations (nation_id, name, abbreviation, continent_id)
            VALUES (0, 'Unknown', 'UNK', 1)
            ON CONFLICT (nation_id) DO NOTHING
        """)
        self.db.execute_sql(sql)
        logger.info("Nation placeholder record added")


    def _get_stored_checksum(self, filename: str) -> str:
        """Get stored checksum from metadata table"""
        sql = text("""
        SELECT checksum
        FROM etl_file_metadata
        WHERE filename = :filename
        """)

        with self.db.get_session() as session:
            result = session.execute(sql, {'filename': filename}).scalar()
            return result


    def _update_stored_checksum(self, filename: str, checksum: str):
        """Update stored checksum in metadata table"""
        sql = text(f"""INSERT INTO etl_file_metadata (filename, checksum, load_strategy, last_processed)
        VALUES (:filename, :checksum, :strategy, CURRENT_TIMESTAMP)
        ON CONFLICT (filename) DO UPDATE SET
        checksum = EXCLUDED.checksum,
        last_processed = EXCLUDED.last_processed""")

        self.db.execute_sql(sql, {
            'filename': filename,
            'checksum': checksum,
            'strategy': self.get_load_strategy()
        })


    @classmethod
    def get_load_order(cls) -> List[str]:
        """Return CSV files in dependency order (excludes manual-load-only tables with load_order >= 99)"""
        return sorted(
            [k for k, v in cls.REFERENCE_TABLES.items() if v['load_order'] < 99],
            key=lambda x: cls.REFERENCE_TABLES[x]['load_order']
        )
