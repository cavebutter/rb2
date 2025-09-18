"""File Checksum Calculation Utilities"""
import hashlib
from pathlib import Path
from loguru import logger

def calculate_file_checksum(file_path: Path, algorithm: str = 'sha256') -> str:
    """Calculate checksum of a file"""
    hash_func = hashlib.new(algorithm) # Create a new hash object

    try:
        with open(file_path, 'rb') as f:
            # Read in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)

        checksum = hash_func.hexdigest()
        logger.debug(f"Calculated {algorithm} checksum for {file_path.name}: {checksum[:16]}...")
        return checksum
    except Exception as e:
        logger.error(f"Error calculating checksum for {file_path}: {e}")
        raise

