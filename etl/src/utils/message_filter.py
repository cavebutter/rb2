"""Message filtering utility for ETL preprocessing"""
import pandas as pd
from loguru import logger
from typing import Dict, Any


class MessageFilter:
    """Filters messages based on configured criteria"""

    def __init__(self, filter_config: Dict[str, Any]):
        """
        Initialize message filter with configuration

        Args:
            filter_config: Dictionary containing filter criteria
                - exclude_message_types: List of message_type values to exclude
                - exclude_sender_ids: List of sender_id values to exclude
                - min_importance: Minimum importance threshold (exclude below)
                - exclude_deleted: Whether to exclude deleted=1 messages
        """
        self.exclude_message_types = filter_config.get('exclude_message_types', [])
        self.exclude_sender_ids = filter_config.get('exclude_sender_ids', [])
        self.min_importance = filter_config.get('min_importance')
        self.exclude_deleted = filter_config.get('exclude_deleted', True)

    def filter_messages(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all configured filters to messages DataFrame

        Args:
            df: DataFrame containing messages data

        Returns:
            Filtered DataFrame with excluded messages removed
        """
        initial_count = len(df)
        filtered_df = df.copy()

        # Filter by message_type
        if self.exclude_message_types:
            before = len(filtered_df)
            filtered_df = filtered_df[~filtered_df['message_type'].isin(self.exclude_message_types)]
            excluded = before - len(filtered_df)
            if excluded > 0:
                logger.info(f"Filtered {excluded} messages by message_type (excluded types: {self.exclude_message_types})")

        # Filter by sender_id
        if self.exclude_sender_ids:
            before = len(filtered_df)
            filtered_df = filtered_df[~filtered_df['sender_id'].isin(self.exclude_sender_ids)]
            excluded = before - len(filtered_df)
            if excluded > 0:
                logger.info(f"Filtered {excluded} messages by sender_id (excluded IDs: {self.exclude_sender_ids})")

        # Filter by importance threshold
        if self.min_importance is not None:
            before = len(filtered_df)
            filtered_df = filtered_df[filtered_df['importance'] >= self.min_importance]
            excluded = before - len(filtered_df)
            if excluded > 0:
                logger.info(f"Filtered {excluded} messages below importance threshold {self.min_importance}")

        # Filter deleted messages
        if self.exclude_deleted and 'deleted' in filtered_df.columns:
            before = len(filtered_df)
            filtered_df = filtered_df[filtered_df['deleted'] == 0]
            excluded = before - len(filtered_df)
            if excluded > 0:
                logger.info(f"Filtered {excluded} deleted messages")

        total_filtered = initial_count - len(filtered_df)
        if total_filtered > 0:
            logger.info(f"Total messages filtered: {total_filtered} ({initial_count} -> {len(filtered_df)})")

        return filtered_df

    def get_filter_summary(self) -> str:
        """Return a human-readable summary of active filters"""
        filters = []

        if self.exclude_message_types:
            filters.append(f"Excluding message types: {self.exclude_message_types}")

        if self.exclude_sender_ids:
            filters.append(f"Excluding sender IDs: {self.exclude_sender_ids}")

        if self.min_importance is not None:
            filters.append(f"Minimum importance: {self.min_importance}")

        if self.exclude_deleted:
            filters.append("Excluding deleted messages")

        if not filters:
            return "No message filters active"

        return "Active filters: " + "; ".join(filters)
