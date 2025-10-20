"""
Game Context Module

Fetches complete game metadata and player details for article generation.
Provides rich context including teams, scores, key players, and player bios.
"""

from typing import Dict, Optional, List
from loguru import logger


def get_game_context(conn, game_id: int) -> Optional[Dict]:
    """
    Fetch complete game metadata for article generation.

    Retrieves:
    - Teams (home/away with names)
    - Final score
    - Date and attendance
    - Innings played
    - Hits and errors
    - Winning, losing, and save pitchers (with names)

    Args:
        conn: psycopg2 database connection
        game_id: Game ID to fetch

    Returns:
        Dict with game context, or None if game not found
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                g.game_id,
                g.date,
                g.attendance,
                g.innings,
                g.home_team,
                ht.name as home_team_name,
                ht.nickname as home_team_nickname,
                ht.abbr as home_team_abbr,
                g.away_team,
                at.name as away_team_name,
                at.nickname as away_team_nickname,
                at.abbr as away_team_abbr,
                g.runs_0 as away_runs,
                g.runs_1 as home_runs,
                g.hits_0 as away_hits,
                g.hits_1 as home_hits,
                g.errors_0 as away_errors,
                g.errors_1 as home_errors,
                g.winning_pitcher,
                wp.first_name as wp_first_name,
                wp.last_name as wp_last_name,
                g.losing_pitcher,
                lp.first_name as lp_first_name,
                lp.last_name as lp_last_name,
                g.save_pitcher,
                sp.first_name as sp_first_name,
                sp.last_name as sp_last_name,
                g.starter_0 as away_starter,
                g.starter_1 as home_starter,
                g.league_id
            FROM games g
            LEFT JOIN teams ht ON g.home_team = ht.team_id
            LEFT JOIN teams at ON g.away_team = at.team_id
            LEFT JOIN players_core wp ON g.winning_pitcher = wp.player_id
            LEFT JOIN players_core lp ON g.losing_pitcher = lp.player_id
            LEFT JOIN players_core sp ON g.save_pitcher = sp.player_id
            WHERE g.game_id = %s
        """, (game_id,))

        row = cur.fetchone()

        if not row:
            logger.warning(f"Game not found: game_id={game_id}")
            return None

        context = {
            'game_id': row[0],
            'date': row[1],
            'attendance': row[2],
            'innings': row[3],
            'home_team': {
                'team_id': row[4],
                'name': row[5],
                'nickname': row[6],
                'abbr': row[7],
            },
            'away_team': {
                'team_id': row[8],
                'name': row[9],
                'nickname': row[10],
                'abbr': row[11],
            },
            'score': {
                'away': row[12],
                'home': row[13],
            },
            'hits': {
                'away': row[14],
                'home': row[15],
            },
            'errors': {
                'away': row[16],
                'home': row[17],
            },
            'winning_pitcher': {
                'player_id': row[18],
                'name': f"{row[19]} {row[20]}" if row[19] else None
            } if row[18] else None,
            'losing_pitcher': {
                'player_id': row[21],
                'name': f"{row[22]} {row[23]}" if row[22] else None
            } if row[21] else None,
            'save_pitcher': {
                'player_id': row[24],
                'name': f"{row[25]} {row[26]}" if row[25] else None
            } if row[24] else None,
            'starters': {
                'away': row[27],
                'home': row[28],
            },
            'league_id': row[29]
        }

        logger.debug(f"Fetched game context for game_id={game_id}: {context['away_team']['abbr']} @ {context['home_team']['abbr']}")
        return context


def get_player_details(conn, player_id: int) -> Optional[Dict]:
    """
    Get player biographical information for article generation.

    Retrieves:
    - Full name
    - Birth date and age
    - Height, weight
    - Bats/Throws
    - Current team

    Args:
        conn: psycopg2 database connection
        player_id: Player ID to fetch

    Returns:
        Dict with player details, or None if player not found
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                pc.player_id,
                pc.first_name,
                pc.last_name,
                pc.nick_name,
                pc.date_of_birth,
                pc.height,
                pc.weight,
                pc.bats,
                pc.throws,
                pcs.team_id,
                t.name as team_name,
                t.abbr as team_abbr,
                pcs.position
            FROM players_core pc
            LEFT JOIN players_current_status pcs ON pc.player_id = pcs.player_id
            LEFT JOIN teams t ON pcs.team_id = t.team_id
            WHERE pc.player_id = %s
        """, (player_id,))

        row = cur.fetchone()

        if not row:
            logger.warning(f"Player not found: player_id={player_id}")
            return None

        # Convert bats/throws codes to text
        bats_map = {0: 'R', 1: 'L', 2: 'S'}  # Switch
        throws_map = {0: 'R', 1: 'L'}

        details = {
            'player_id': row[0],
            'first_name': row[1],
            'last_name': row[2],
            'nick_name': row[3],
            'full_name': f"{row[1]} {row[2]}",
            'date_of_birth': row[4],
            'height': row[5],
            'weight': row[6],
            'bats': bats_map.get(row[7], 'R') if row[7] is not None else None,
            'throws': throws_map.get(row[8], 'R') if row[8] is not None else None,
            'team': {
                'team_id': row[9],
                'name': row[10],
                'abbr': row[11],
            } if row[9] else None,
            'position': row[12]
        }

        logger.debug(f"Fetched player details for {details['full_name']} (player_id={player_id})")
        return details


def get_branch_player_game_stats(conn, player_id: int, game_id: int) -> Dict:
    """
    Get Branch player's full game statistics from staging tables.

    Checks both batting and pitching staging tables.

    Args:
        conn: psycopg2 database connection
        player_id: Player ID
        game_id: Game ID

    Returns:
        Dict with keys: batting (dict or None), pitching (dict or None)
    """
    result = {
        'batting': None,
        'pitching': None
    }

    # Check batting
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ab, h, d, t, hr, r, rbi, sb, bb, k, wpa
            FROM staging_branch_game_batting
            WHERE player_id = %s AND game_id = %s
        """, (player_id, game_id))

        row = cur.fetchone()
        if row:
            result['batting'] = {
                'ab': row[0],
                'h': row[1],
                'd': row[2],
                't': row[3],
                'hr': row[4],
                'r': row[5],
                'rbi': row[6],
                'sb': row[7],
                'bb': row[8],
                'k': row[9],
                'wpa': float(row[10]) if row[10] else 0.0
            }

    # Check pitching
    with conn.cursor() as cur:
        cur.execute("""
            SELECT gs, ip, h, r, er, hr, bb, k, w, l, sv, wpa
            FROM staging_branch_game_pitching
            WHERE player_id = %s AND game_id = %s
        """, (player_id, game_id))

        row = cur.fetchone()
        if row:
            result['pitching'] = {
                'gs': row[0],
                'ip': float(row[1]) if row[1] else 0.0,
                'h': row[2],
                'r': row[3],
                'er': row[4],
                'hr': row[5],
                'bb': row[6],
                'k': row[7],
                'w': row[8],
                'l': row[9],
                'sv': row[10],
                'wpa': float(row[11]) if row[11] else 0.0
            }

    return result


def enrich_game_with_context(conn, game: Dict) -> Dict:
    """
    Enrich a game dict with full context for article generation.

    Takes a game from detect_multi_branch_games() and adds:
    - Full game metadata (teams, score, etc.)
    - Player biographical details for all Branch players
    - Complete game stats for each player

    Args:
        conn: psycopg2 database connection
        game: Game dict with keys: game_id, year, player_ids, team_ids, performances

    Returns:
        Enriched game dict with added 'context' and 'players' keys
    """
    game_id = game['game_id']

    # Get game context
    context = get_game_context(conn, game_id)
    if not context:
        logger.error(f"Could not fetch game context for game_id={game_id}")
        game['context'] = None
        game['players'] = []
        return game

    game['context'] = context

    # Get player details for all Branch players in this game
    players = []
    for player_id in game['player_ids']:
        player_details = get_player_details(conn, player_id)
        if not player_details:
            logger.warning(f"Could not fetch player details for player_id={player_id}")
            continue

        # Add game stats
        game_stats = get_branch_player_game_stats(conn, player_id, game_id)
        player_details['game_stats'] = game_stats

        # Find the performance in the original game dict
        performance = next(
            (p for p in game['performances'] if p['player_id'] == player_id),
            None
        )
        if performance:
            player_details['performance'] = performance

        players.append(player_details)

    game['players'] = players

    logger.info(f"Enriched game {game_id} with context for {len(players)} Branch players")
    return game