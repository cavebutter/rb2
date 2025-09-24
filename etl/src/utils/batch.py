"""Batch utilities for ETL operations."""

import uuid
from datetime import datetime
from typing import Optional


def generate_batch_id(prefix: Optional[str] = None) -> str:
    """
    Generate a unique batch ID for ETL operations.
    
    Args:
        prefix: Optional prefix for the batch ID
        
    Returns:
        Unique batch ID string
    """
    return str(uuid.uuid4())


def get_current_batch_timestamp() -> str:
    """Get current timestamp for batch operations."""
    return datetime.now().isoformat()