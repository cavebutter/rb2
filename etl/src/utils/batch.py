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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_suffix = str(uuid.uuid4())[:8]
    
    if prefix:
        return f"{prefix}_{timestamp}_{unique_suffix}"
    return f"batch_{timestamp}_{unique_suffix}"


def get_current_batch_timestamp() -> str:
    """Get current timestamp for batch operations."""
    return datetime.now().isoformat()