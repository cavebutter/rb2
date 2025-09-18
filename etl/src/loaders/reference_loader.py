"""Loader for static reference tables"""
from typing import List
from pathlib import Path
from loguru import logger
from .base_loader import BaseLoader
from ..utils.checksum import calculate_file_checksum
from sqlalchemy import text
from typing import Optional, Dict



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
                'main_language_id': 'main_language_id'
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
        """Reference tables use skip strategy - only load if changed."""
        return 'skip'


    def get_primary_keys(self) -> List[str]:
        """Return primary key columns for this table"""
        return self.config['primary_keys']


    def get_target_table(self) -> str:
        """Return the target table name"""
        return self.table_name


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
        """Return CSV files in dependency order"""
        return sorted(
            cls.REFERENCE_TABLES.keys(),
            key=lambda x: cls.REFERENCE_TABLES[x]['load_order']
        )
