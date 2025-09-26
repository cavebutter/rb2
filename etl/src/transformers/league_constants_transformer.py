"""Transformer for calculating league-wide constants needed for advanced metrics"""
from typing import List, Optional, Dict
from loguru import logger
from sqlalchemy import text
from ..loaders.base_loader import BaseLoader
from ..utils.batch import generate_batch_id

class LeagueConstantsTransformer(BaseLoader):
    """
    Calculates league constants required for advanced metrics:
    - League runs per out
    - Run values (for wOBA calculations)
    - FIP constants
    - Sub_league batting and pitching environments
    """
    def __init__(self, batch_id: str = None, force_all: bool = False):
        """Initialize the transformer
        Args:
            batch_id: Batch identifier for tracking
            force_all: If True, recalculate all years (for initial loads/rebuilds) If False, only calculate current.
            season.
        """
        super().__init__(batch_id or generate_batch_id())
        self.force_all = force_all

    def get_load_strategy(self) -> str:
        """Return load strategy - incremental for year-specific processing"""
        return "incremental"

    def get_primary_keys(self) -> List[str]:
        """Not applicable for transformer"""
        return []

    def get_target_table(self) -> str:
        """Multiple target tables - return primary one for logging purposes"""
        return "league_constants"

    def get_column_mapping(self) -> Optional[Dict[str, str]]:
        """Not applicable for transformer"""
        return None

    def get_calculated_fields(self) -> Dict[str, str]:
        """Not applicable for transformer"""
        return {}

    def get_upsert_keys(self) -> List[str]:
        """Not applicable for transformer"""
        return []

    def get_update_columns(self) -> List[str]:
        """Not applicable for transformer"""
        return []

    def transform_constants(self) -> bool:
        """Main entry point to calculate all league constants.
        Returns:
            True if successful, False otherwise.
        """
        try:
            years_to_process = self._get_years_to_process()

            if not years_to_process:
                logger.warning("No years found to process for constants calculation")
                return False

            logger.info(f"Processing constants for {len(years_to_process)} years")

            for year in years_to_process:
                logger.info(f"Processing year {year}")

                # Validate prerequisites
                if not self._validate_prerequisites(year):
                    logger.error(f"Prerequisites validation failed for year {year}")
                    return False

                # Calculate constants for this year
                if not self._calculate_year_constants(year):
                    logger.error(f"Year constants calculation failed for year {year}")
                    return False

                # Record metadata
                self._record_year_calculation(year)

            logger.info("All constants calculations complete")
            return True
        except Exception as e:
            logger.error(f"Constants transformation failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _get_years_to_process(self) -> List[int]:
        """
        Determine which years need to be processed.
        :return:
        List of years to process
        """
        if self.force_all:
            sql = text("""
            SELECT DISTINCT year
            FROM players_career_batting_stats
            WHERE year IS NOT NULL
            ORDER BY year
            """)

            result = self.db.execute_sql(sql)
            years = [row[0] for row in result]
            logger.info(f"Force all mode: Processing all {len(years)} years")

        else:
            # Only current season
            sql = text("""
            SELECT MAX(season_year)
            FROM leagues
            """)
            current_year = self.db.execute_sql(sql).scalar()
            years = [current_year] if current_year else []
            logger.info(f"Incremental mode: Processing current year {current_year}")
        return years

    def _validate_prerequisites(self, year: int) -> bool:
        """
        Ensure required data exists before calculating constants.
        :param year: Year to validate
        :return: True if prerequisites validation is successful, False otherwise.
        """
        # Check for batting stats
        sql = text("""
        SELECT COUNT(*) FROM players_career_batting_stats
        WHERE year = :year AND split_id = 1
        """)

        batting_count = self.db.execute_sql(sql, {'year': year}).scalar()
        if batting_count == 0:
            logger.error(f"Prerequisites validation failed for year {year} - Batting")
            return False

        # Check pitching stats
        sql = text("""
        SELECT COUNT(*) FROM players_career_pitching_stats
        WHERE year = :year AND split_id = 1
        """)
        pitching_count = self.db.execute_sql(sql, {'year': year}).scalar()
        if pitching_count == 0:
            logger.error(f"Prerequisites validation failed for year {year} - Pitching")
            return False

        # Check for player positions which are needed for batting environment
        sql = text("""
        SELECT COUNT(*) FROM players_current_status
        """)
        current_status = self.db.execute_sql(sql).scalar()
        if current_status == 0:
            logger.error(f"Prerequisites validation failed for year {year} - Player Current Status")
            return False

        logger.debug(f"Prerequisites validated for year {year}: {batting_count} batters, {pitching_count} pitchers.")
        return True

    def _calculate_year_constants(self, year: int) -> bool:
        """
        Execute all calculation functions for a specific year.
        :param year: Year to calculate constants for
        :return: True, if successful.
        """
        with self.db.get_session() as session:
            try:
                # Call the master function with year parameter
                logger.debug(f"Calling refresh_all_calculations({year})")
                session.execute(text("SELECT refresh_all_calculations(:year)"), {"year": year})
                session.commit()

                # Verify calculations succeeded
                if not self._verify_calculations(year):
                    raise Exception(f"Constants verification failed for year {year}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"Failed to calculate constants for year {year}: {e}")
                return False

    def _verify_calculations(self, year: int) -> bool:
        """
        Verify that constants were calculated successfully
        :param year: Year to verify
        :return: True if verification passes
        """
        # Check league_runs_per_out
        sql = text("""
        SELECT COUNT(*) FROM league_runs_per_out WHERE year = :year
        """)
        count = self.db.execute_sql(sql, {"year": year}).scalar()
        if count == 0:
            logger.error(f"No league_runs_per_out records for year {year}")
            return False

        # Check run_values
        sql = text("""
        SELECT COUNT(*) FROM run_values WHERE year = :year
        """)
        count = self.db.execute_sql(sql, {"year": year}).scalar()
        if count == 0:
            logger.error(f"No run_values records for year {year}")
            return False

        # Check FIP constants
        sql = text("""
        SELECT COUNT(*) FROM fip_constants WHERE year = :year
        """)
        count = self.db.execute_sql(sql, {"year": year}).scalar()
        if count == 0:
            logger.error(f"No fip_constants records for year {year}")
            return False

        logger.info(f"Constants verified successfully for year {year}")
        return True

    def _record_year_calculation(self, year: int):
        """
        Record metadata about the calculation
        :param year: Year that was calculated
        :return:
        """
        sql = text("""
        INSERT INTO etl_file_metadata (
        filename,
        last_status,
        last_batch_id,
        rows_processed,
        last_processed
        ) VALUES (
        :filename,
        'success',
        :batch_id,
        :rows,
        CURRENT_TIMESTAMP
        )
        ON CONFLICT (filename) DO UPDATE SET
        last_status = 'success',
        last_batch_id = :batch_id,
        rows_processed = :rows,
        last_processed = CURRENT_TIMESTAMP
        """)

        # Get total rows processed
        count_sql = text("""
        SELECT
            (SELECT COUNT(*) FROM league_runs_per_out WHERE year = :year) +
            (SELECT COUNT(*) FROM run_values WHERE year = :year) +
            (SELECT COUNT(*) FROM fip_constants WHERE year = :year)
            """)
        total_rows = self.db.execute_sql(count_sql, {"year": year}).scalar()

        self.db.execute_sql(sql, {
            'filename': f'constants_year_{year}',
            'batch_id': self.batch_id,
            'rows': total_rows
        })
        logger.debug(f"Recorded calculation metadata for year {year}: {total_rows} rows")

    @classmethod
    def is_initial_load(cls) -> bool:
        """
        Check if this is the first time calculating constants
        :return: True if no constants exist yet.
        """
        from ..database.connection import db
        sql = text("SELECT COUNT(*) FROM league_runs_per_out")
        count = db.execute_sql(sql).scalar()
        return count == 0