"""
Branch Game Detection Module

Identifies games featuring Branch family members by loading per-game stats
from CSV files into staging tables, then detecting and prioritizing games
for article generation.

Data Flow:
1. Load players_game_batting.csv → staging_branch_game_batting (filtered to Branch players)
2. Load players_game_pitching_stats.csv → staging_branch_game_pitching (filtered to Branch players)
3. Detect games where Branch players appeared
4. Merge multi-Branch games into single article candidates
5. Cleanup staging tables after processing
"""

import csv
from typing import List, Dict, Optional, Set
from datetime import date
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger


def get_branch_family_ids(conn) -> List[int]:
    """
    Query all Branch family player IDs.

    Checks branch_family_members table first, falls back to
    players_core WHERE last_name='Branch' if empty.

    Args:
        conn: psycopg2 database connection

    Returns:
        List of player_id integers
    """
    with conn.cursor() as cur:
        # Try branch_family_members table first
        cur.execute("SELECT player_id FROM branch_family_members")
        results = cur.fetchall()

        if results:
            player_ids = [row[0] for row in results]
            logger.info(f"Found {len(player_ids)} Branch family members from branch_family_members table")
            return player_ids

        # Fallback to players_core
        cur.execute("SELECT player_id FROM players_core WHERE last_name = 'Branch'")
        results = cur.fetchall()
        player_ids = [row[0] for row in results]
        logger.info(f"Found {len(player_ids)} Branch family members from players_core (last_name='Branch')")
        return player_ids


def load_game_stats_to_staging(
    conn,
    csv_path: str,
    branch_ids: List[int],
    stats_type: str = 'batting'
) -> int:
    """
    Load players_game_batting.csv or players_game_pitching_stats.csv
    to staging tables, filtered to Branch players only.

    Args:
        conn: psycopg2 database connection
        csv_path: Path to CSV file
        branch_ids: List of Branch family player IDs to filter
        stats_type: 'batting' or 'pitching'

    Returns:
        Number of records loaded

    Raises:
        ValueError: If stats_type is invalid
        FileNotFoundError: If CSV file doesn't exist
    """
    if stats_type not in ('batting', 'pitching'):
        raise ValueError(f"stats_type must be 'batting' or 'pitching', got '{stats_type}'")

    table_name = f"staging_branch_game_{stats_type}"
    branch_ids_set = set(branch_ids)
    records_to_insert = []

    logger.info(f"Loading {stats_type} stats from {csv_path} for {len(branch_ids)} Branch players")

    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)

            for row in reader:
                player_id = int(row['player_id'])

                # Filter to Branch players only
                if player_id not in branch_ids_set:
                    continue

                # Parse row based on stats type
                if stats_type == 'batting':
                    record = (
                        player_id,
                        int(row['year']) if row['year'] else None,
                        int(row['team_id']) if row['team_id'] else None,
                        int(row['game_id']),
                        int(row['league_id']) if row['league_id'] else None,
                        int(row['level_id']) if row['level_id'] else None,
                        int(row['split_id']) if row['split_id'] else None,
                        int(row['position']) if row['position'] else None,
                        int(row['ab']) if row['ab'] else 0,
                        int(row['h']) if row['h'] else 0,
                        int(row['k']) if row['k'] else 0,
                        int(row['pa']) if row['pa'] else 0,
                        int(row['g']) if row['g'] else 0,
                        int(row['d']) if row['d'] else 0,
                        int(row['t']) if row['t'] else 0,
                        int(row['hr']) if row['hr'] else 0,
                        int(row['r']) if row['r'] else 0,
                        int(row['rbi']) if row['rbi'] else 0,
                        int(row['sb']) if row['sb'] else 0,
                        int(row['bb']) if row['bb'] else 0,
                        float(row['wpa']) if row['wpa'] else 0.0,
                    )
                else:  # pitching
                    record = (
                        player_id,
                        int(row['year']) if row['year'] else None,
                        int(row['team_id']) if row['team_id'] else None,
                        int(row['game_id']),
                        int(row['league_id']) if row['league_id'] else None,
                        int(row['level_id']) if row['level_id'] else None,
                        int(row['split_id']) if row['split_id'] else None,
                        int(row['g']) if row['g'] else 0,
                        int(row['gs']) if row['gs'] else 0,
                        float(row['ip']) if row['ip'] else 0.0,
                        int(row['h']) if row['h'] else 0,
                        int(row['r']) if row['r'] else 0,
                        int(row['er']) if row['er'] else 0,
                        int(row['hr']) if row['hr'] else 0,
                        int(row['bb']) if row['bb'] else 0,
                        int(row['k']) if row['k'] else 0,
                        int(row['w']) if row['w'] else 0,
                        int(row['l']) if row['l'] else 0,
                        int(row['sv']) if row['sv'] else 0,
                        int(row['hld']) if row['hld'] else 0,
                        float(row['wpa']) if row['wpa'] else 0.0,
                    )

                records_to_insert.append(record)

        if not records_to_insert:
            logger.warning(f"No {stats_type} records found for Branch players")
            return 0

        # Bulk insert into staging table
        with conn.cursor() as cur:
            if stats_type == 'batting':
                insert_sql = f"""
                    INSERT INTO {table_name}
                    (player_id, year, team_id, game_id, league_id, level_id, split_id,
                     position, ab, h, k, pa, g, d, t, hr, r, rbi, sb, bb, wpa)
                    VALUES %s
                    ON CONFLICT (player_id, game_id) DO UPDATE SET
                        ab = EXCLUDED.ab,
                        h = EXCLUDED.h,
                        hr = EXCLUDED.hr,
                        rbi = EXCLUDED.rbi,
                        loaded_at = NOW()
                """
            else:  # pitching
                insert_sql = f"""
                    INSERT INTO {table_name}
                    (player_id, year, team_id, game_id, league_id, level_id, split_id,
                     g, gs, ip, h, r, er, hr, bb, k, w, l, sv, hld, wpa)
                    VALUES %s
                    ON CONFLICT (player_id, game_id) DO UPDATE SET
                        ip = EXCLUDED.ip,
                        k = EXCLUDED.k,
                        er = EXCLUDED.er,
                        loaded_at = NOW()
                """

            execute_values(cur, insert_sql, records_to_insert)
            conn.commit()

        logger.info(f"Loaded {len(records_to_insert)} {stats_type} records for Branch players")
        return len(records_to_insert)

    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading {stats_type} stats: {e}")
        conn.rollback()
        raise


def detect_branch_games(
    conn,
    branch_ids: List[int],
    date_range: Optional[tuple] = None,
    exclude_existing: bool = True
) -> List[Dict]:
    """
    Scan staging tables for Branch appearances and return game candidates.

    Args:
        conn: psycopg2 database connection
        branch_ids: List of Branch family player IDs
        date_range: Optional (start_date, end_date) tuple to filter games
        exclude_existing: If True, exclude games that already have articles

    Returns:
        List of dicts with keys: game_id, player_id, stats_type, stats (dict)
    """
    branch_games = []

    # Query batting performances
    with conn.cursor() as cur:
        sql = """
            SELECT
                player_id, game_id, year, team_id, ab, h, d, t, hr, r, rbi, sb, bb, k, wpa
            FROM staging_branch_game_batting
            WHERE player_id = ANY(%s)
        """
        params = [branch_ids]

        if date_range:
            # We need to join with games table to filter by date
            sql = """
                SELECT
                    b.player_id, b.game_id, b.year, b.team_id, b.ab, b.h, b.d, b.t,
                    b.hr, b.r, b.rbi, b.sb, b.bb, b.k, b.wpa
                FROM staging_branch_game_batting b
                JOIN games g ON b.game_id = g.game_id
                WHERE b.player_id = ANY(%s)
                  AND g.date BETWEEN %s AND %s
            """
            params.extend([date_range[0], date_range[1]])

        if exclude_existing:
            sql += """
                AND NOT EXISTS (
                    SELECT 1 FROM newspaper_articles na
                    WHERE na.game_id = staging_branch_game_batting.game_id
                      AND na.status != 'rejected'
                )
            """

        cur.execute(sql, params)
        batting_results = cur.fetchall()

        for row in batting_results:
            branch_games.append({
                'game_id': row[1],
                'player_id': row[0],
                'year': row[2],
                'team_id': row[3],
                'stats_type': 'batting',
                'stats': {
                    'ab': row[4],
                    'h': row[5],
                    'd': row[6],
                    't': row[7],
                    'hr': row[8],
                    'r': row[9],
                    'rbi': row[10],
                    'sb': row[11],
                    'bb': row[12],
                    'k': row[13],
                    'wpa': float(row[14]) if row[14] else 0.0,
                }
            })

    # Query pitching performances
    with conn.cursor() as cur:
        sql = """
            SELECT
                player_id, game_id, year, team_id, gs, ip, h, r, er, hr, bb, k, w, l, sv, wpa
            FROM staging_branch_game_pitching
            WHERE player_id = ANY(%s)
        """
        params = [branch_ids]

        if date_range:
            sql = """
                SELECT
                    p.player_id, p.game_id, p.year, p.team_id, p.gs, p.ip, p.h, p.r,
                    p.er, p.hr, p.bb, p.k, p.w, p.l, p.sv, p.wpa
                FROM staging_branch_game_pitching p
                JOIN games g ON p.game_id = g.game_id
                WHERE p.player_id = ANY(%s)
                  AND g.date BETWEEN %s AND %s
            """
            params.extend([date_range[0], date_range[1]])

        if exclude_existing:
            sql += """
                AND NOT EXISTS (
                    SELECT 1 FROM newspaper_articles na
                    WHERE na.game_id = staging_branch_game_pitching.game_id
                      AND na.status != 'rejected'
                )
            """

        cur.execute(sql, params)
        pitching_results = cur.fetchall()

        for row in pitching_results:
            branch_games.append({
                'game_id': row[1],
                'player_id': row[0],
                'year': row[2],
                'team_id': row[3],
                'stats_type': 'pitching',
                'stats': {
                    'gs': row[4],
                    'ip': float(row[5]) if row[5] else 0.0,
                    'h': row[6],
                    'r': row[7],
                    'er': row[8],
                    'hr': row[9],
                    'bb': row[10],
                    'k': row[11],
                    'w': row[12],
                    'l': row[13],
                    'sv': row[14],
                    'wpa': float(row[15]) if row[15] else 0.0,
                }
            })

    logger.info(f"Detected {len(branch_games)} Branch game performances ({len(batting_results)} batting, {len(pitching_results)} pitching)")
    return branch_games


def detect_multi_branch_games(branch_games: List[Dict]) -> List[Dict]:
    """
    Identify games where multiple Branch players appeared and merge them.

    Combines multiple performances from the same game into a single record
    for generating a combined article.

    Args:
        branch_games: List of individual Branch game performances

    Returns:
        Deduplicated list with player_ids array and performances list
    """
    # Group by game_id
    games_by_id = {}
    for perf in branch_games:
        game_id = perf['game_id']
        if game_id not in games_by_id:
            games_by_id[game_id] = {
                'game_id': game_id,
                'year': perf['year'],
                'player_ids': [],
                'team_ids': set(),
                'performances': []
            }

        games_by_id[game_id]['player_ids'].append(perf['player_id'])
        games_by_id[game_id]['team_ids'].add(perf['team_id'])
        games_by_id[game_id]['performances'].append({
            'player_id': perf['player_id'],
            'stats_type': perf['stats_type'],
            'stats': perf['stats']
        })

    # Convert to list and log multi-Branch games
    merged_games = []
    multi_branch_count = 0

    for game_id, game_data in games_by_id.items():
        game_data['team_ids'] = list(game_data['team_ids'])
        merged_games.append(game_data)

        if len(game_data['player_ids']) > 1:
            multi_branch_count += 1
            logger.info(f"Multi-Branch game detected: game_id={game_id}, players={game_data['player_ids']}")

    logger.info(f"Merged {len(branch_games)} performances into {len(merged_games)} unique games ({multi_branch_count} multi-Branch)")
    return merged_games


def cleanup_staging_tables(conn) -> None:
    """
    Truncate staging tables after article generation completes.
    Ensures fresh data on next run.

    Args:
        conn: psycopg2 database connection
    """
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging_branch_game_batting")
        cur.execute("TRUNCATE TABLE staging_branch_game_pitching")
        conn.commit()

    logger.info("Staging tables truncated successfully")
