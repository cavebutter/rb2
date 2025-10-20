"""
Game Log Archiver Module

Manages game_logs.csv lifecycle to prevent unbounded growth:
- Archives historical seasons as compressed files
- Keeps only current season in active CSV
- Provides on-demand retrieval from archives for article regeneration

Strategy:
- At end of each season: compress and archive that season's game logs
- Active game_logs.csv contains only current season
- Archives stored as game_logs_YYYY.csv.gz in data/archive/game_logs/
- On-demand decompression for regenerating historical articles
"""

import os
import gzip
import shutil
import csv
from pathlib import Path
from typing import List, Optional
from datetime import date
from loguru import logger


def get_archive_path(base_dir: str, year: int) -> Path:
    """
    Get path to archived game log file for a specific year.

    Args:
        base_dir: Base data directory (e.g., 'etl/data')
        year: Season year

    Returns:
        Path to compressed archive file
    """
    archive_dir = Path(base_dir) / 'archive' / 'game_logs'
    return archive_dir / f'game_logs_{year}.csv.gz'


def ensure_archive_directory(base_dir: str) -> Path:
    """
    Create archive directory if it doesn't exist.

    Args:
        base_dir: Base data directory

    Returns:
        Path to archive directory
    """
    archive_dir = Path(base_dir) / 'archive' / 'game_logs'
    archive_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Archive directory ready: {archive_dir}")
    return archive_dir


def archive_season_game_logs(
    csv_path: str,
    base_dir: str,
    season_year: int,
    remove_from_active: bool = True
) -> Optional[Path]:
    """
    Archive a specific season's game logs to compressed file.

    Workflow:
    1. Read active game_logs.csv
    2. Filter to specified season
    3. Write to compressed archive file
    4. Optionally remove archived entries from active CSV

    Args:
        csv_path: Path to active game_logs.csv
        base_dir: Base data directory for archives
        season_year: Year to archive
        remove_from_active: If True, remove archived entries from active CSV

    Returns:
        Path to created archive file, or None if no games found
    """
    ensure_archive_directory(base_dir)
    archive_path = get_archive_path(base_dir, season_year)

    if archive_path.exists():
        logger.warning(f"Archive already exists: {archive_path}")
        return archive_path

    # Load all entries from active CSV
    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            all_entries = list(reader)
    except FileNotFoundError:
        logger.error(f"Active game log CSV not found: {csv_path}")
        return None

    # Need to query database to map game_id -> year
    # For now, we'll need to pass game_ids or query separately
    # This is a simplified implementation that assumes we have year data

    # Filter to season entries (simplified - would need DB query in practice)
    season_entries = []
    remaining_entries = []

    for entry in all_entries:
        # TODO: Query game year from database using game_id
        # For now, this is a placeholder
        # if game_year == season_year:
        #     season_entries.append(entry)
        # else:
        #     remaining_entries.append(entry)
        pass

    if not season_entries:
        logger.warning(f"No game log entries found for season {season_year}")
        return None

    # Write to compressed archive
    with gzip.open(archive_path, 'wt', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(season_entries)

    logger.info(f"Archived {len(season_entries)} game log entries for season {season_year} to {archive_path}")

    # Remove archived entries from active CSV if requested
    if remove_from_active:
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(remaining_entries)

        logger.info(f"Removed {len(season_entries)} archived entries from active CSV, {len(remaining_entries)} remaining")

    return archive_path


def prune_game_logs(
    conn,
    csv_path: str,
    base_dir: str,
    current_season_year: int
) -> List[Path]:
    """
    Archive all historical seasons, keeping only current season in active CSV.

    Run this at end of each season to prevent game_logs.csv from growing too large.

    Args:
        conn: psycopg2 database connection (to query game years)
        csv_path: Path to active game_logs.csv
        base_dir: Base data directory for archives
        current_season_year: Current season year to keep active

    Returns:
        List of archive file paths created
    """
    ensure_archive_directory(base_dir)

    # Get all game_ids and years from active CSV
    game_ids_by_year = {}

    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            game_ids = set(int(row['game_id']) for row in reader)
    except FileNotFoundError:
        logger.error(f"Active game log CSV not found: {csv_path}")
        return []

    if not game_ids:
        logger.warning("No game IDs found in active game log CSV")
        return []

    # Query database to get years for all games
    with conn.cursor() as cur:
        cur.execute("""
            SELECT game_id, EXTRACT(YEAR FROM date) as year
            FROM games
            WHERE game_id = ANY(%s)
        """, (list(game_ids),))

        for row in cur.fetchall():
            game_id, year = row[0], int(row[1])
            if year not in game_ids_by_year:
                game_ids_by_year[year] = []
            game_ids_by_year[year].append(game_id)

    # Archive each historical season
    archived_files = []

    for year in sorted(game_ids_by_year.keys()):
        if year >= current_season_year:
            continue  # Keep current season active

        archive_path = get_archive_path(base_dir, year)

        if archive_path.exists():
            logger.info(f"Archive already exists for {year}: {archive_path}")
            continue

        # Filter and archive this season
        season_game_ids = set(game_ids_by_year[year])

        # Read active CSV
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                all_entries = list(reader)
        except Exception as e:
            logger.error(f"Error reading active CSV: {e}")
            continue

        # Split into season vs remaining
        season_entries = [e for e in all_entries if int(e['game_id']) in season_game_ids]
        remaining_entries = [e for e in all_entries if int(e['game_id']) not in season_game_ids]

        if not season_entries:
            continue

        # Write archive
        with gzip.open(archive_path, 'wt', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(season_entries)

        logger.info(f"Archived {len(season_entries)} entries for season {year} to {archive_path}")
        archived_files.append(archive_path)

        # Update active CSV
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(remaining_entries)

    total_entries = sum(len(game_ids_by_year[y]) for y in game_ids_by_year if y < current_season_year)
    logger.info(f"Pruning complete: archived {total_entries} entries across {len(archived_files)} seasons")

    return archived_files


def get_game_log_from_archive(
    base_dir: str,
    game_id: int,
    game_year: int
) -> List[dict]:
    """
    Retrieve game log entries from archive for article regeneration.

    Args:
        base_dir: Base data directory
        game_id: Game ID to retrieve
        game_year: Year of the game (to locate correct archive)

    Returns:
        List of log entry dicts for the game
    """
    archive_path = get_archive_path(base_dir, game_year)

    if not archive_path.exists():
        logger.error(f"Archive not found for year {game_year}: {archive_path}")
        return []

    entries = []

    try:
        with gzip.open(archive_path, 'rt', newline='') as f:
            reader = csv.DictReader(f)

            for row in reader:
                if int(row['game_id']) == game_id:
                    entries.append({
                        'game_id': int(row['game_id']),
                        'type': int(row['type']),
                        'line': int(row['line']),
                        'text': row['text']
                    })

    except Exception as e:
        logger.error(f"Error reading archive for game_id={game_id}, year={game_year}: {e}")
        return []

    logger.debug(f"Retrieved {len(entries)} log entries for game_id={game_id} from {game_year} archive")
    return entries


def get_game_log_entries(
    conn,
    csv_path: str,
    base_dir: str,
    game_id: int
) -> List[dict]:
    """
    Get game log entries from either active CSV or archive.

    Automatically determines if game is in active CSV or needs archive retrieval.

    Args:
        conn: psycopg2 database connection
        csv_path: Path to active game_logs.csv
        base_dir: Base data directory for archives
        game_id: Game ID to retrieve

    Returns:
        List of log entry dicts for the game
    """
    # Try active CSV first
    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)

            entries = []
            for row in reader:
                if int(row['game_id']) == game_id:
                    entries.append({
                        'game_id': int(row['game_id']),
                        'type': int(row['type']),
                        'line': int(row['line']),
                        'text': row['text']
                    })

            if entries:
                logger.debug(f"Found {len(entries)} entries for game_id={game_id} in active CSV")
                return entries

    except FileNotFoundError:
        logger.warning(f"Active CSV not found: {csv_path}")

    # Not in active CSV, check archives
    # Query game year
    with conn.cursor() as cur:
        cur.execute("SELECT EXTRACT(YEAR FROM date) FROM games WHERE game_id = %s", (game_id,))
        row = cur.fetchone()

        if not row:
            logger.error(f"Game not found in database: game_id={game_id}")
            return []

        game_year = int(row[0])

    # Try archive
    entries = get_game_log_from_archive(base_dir, game_id, game_year)

    if not entries:
        logger.warning(f"No game log entries found for game_id={game_id} (year {game_year})")

    return entries


def list_archived_seasons(base_dir: str) -> List[int]:
    """
    List all seasons that have been archived.

    Args:
        base_dir: Base data directory

    Returns:
        List of archived season years (sorted)
    """
    archive_dir = Path(base_dir) / 'archive' / 'game_logs'

    if not archive_dir.exists():
        return []

    years = []
    for archive_file in archive_dir.glob('game_logs_*.csv.gz'):
        # Extract year from filename
        match = archive_file.stem.replace('.csv', '')  # Remove .csv from game_logs_YYYY.csv
        year_str = match.split('_')[-1]
        try:
            years.append(int(year_str))
        except ValueError:
            logger.warning(f"Unexpected archive filename format: {archive_file}")

    return sorted(years)


def get_archive_stats(base_dir: str) -> dict:
    """
    Get statistics about archived game logs.

    Args:
        base_dir: Base data directory

    Returns:
        Dict with keys: seasons (list), total_size_mb (float), file_count (int)
    """
    archive_dir = Path(base_dir) / 'archive' / 'game_logs'

    if not archive_dir.exists():
        return {'seasons': [], 'total_size_mb': 0.0, 'file_count': 0}

    seasons = list_archived_seasons(base_dir)
    total_size = sum(f.stat().st_size for f in archive_dir.glob('*.csv.gz'))

    return {
        'seasons': seasons,
        'total_size_mb': round(total_size / (1024 * 1024), 2),
        'file_count': len(seasons)
    }
