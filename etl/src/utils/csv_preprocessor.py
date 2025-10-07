"""
CSV Preprocessing Utilities
Handles data cleansing for dirty CSV files
"""
import pandas as pd
from pathlib import Path
from loguru import logger
from typing import Optional


class CSVPreprocessor:
    """Cleans CSV files before loading into database"""

    @staticmethod
    def clean_quoted_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
        """Replace quoted empty strings ('') with actual empty strings"""
        logger.debug("Cleaning quoted empty strings")
        return df.replace("''", "", regex=False)

    @staticmethod
    def deduplicate_rows(df: pd.DataFrame, subset: Optional[list] = None) -> pd.DataFrame:
        """Remove duplicate rows, keeping first occurrence"""
        initial_count = len(df)
        df_clean = df.drop_duplicates(subset=subset, keep='first')
        removed = initial_count - len(df_clean)
        if removed > 0:
            logger.warning(f"Removed {removed} duplicate rows")
        return df_clean

    @staticmethod
    def fix_malformed_csv(csv_path: Path, expected_columns: int) -> pd.DataFrame:
        """
        Attempt to fix CSV files with unescaped commas in text fields.
        This is a best-effort approach - may not work for all edge cases.
        """
        logger.warning(f"Attempting to fix malformed CSV: {csv_path}")

        # Try reading with error_bad_lines=False (pandas < 2.0) or on_bad_lines='skip'
        try:
            df = pd.read_csv(csv_path, on_bad_lines='skip', engine='python')
            logger.info(f"Successfully read CSV with {len(df)} rows (skipped bad lines)")
            return df
        except Exception as e:
            logger.error(f"Could not fix malformed CSV: {e}")
            raise

    @classmethod
    def preprocess(cls, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        Apply all preprocessing steps based on config

        Args:
            df: DataFrame to process
            config: Optional dict with preprocessing options:
                - clean_quoted_strings: bool (default True)
                - deduplicate: bool (default True)
                - dedup_subset: list of columns for deduplication (default None = all)
        """
        if config is None:
            config = {}

        # Default: clean quoted empty strings
        if config.get('clean_quoted_strings', True):
            df = cls.clean_quoted_empty_strings(df)

        # Default: deduplicate rows
        if config.get('deduplicate', True):
            subset = config.get('dedup_subset')
            df = cls.deduplicate_rows(df, subset=subset)

        return df