"""
Newspaper Article Generation Pipeline

End-to-end orchestration of Branch family article generation:
1. Detect Branch games
2. Score newsworthiness
3. Generate articles with Ollama
4. Save to database

This is the main entry point for automated article generation,
called by ETL after successful data import.
"""

import psycopg2
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from loguru import logger

from src.newspaper.prompt_builder import build_article_prompt, build_multi_branch_prompt
from src.newspaper.ollama_client import OllamaClient, get_fallback_model
from src.newspaper.article_processor import create_processor
from config.etl_config import OLLAMA_CONFIG, NEWSPAPER_CONFIG, DB_CONFIG


def get_branch_family_ids(db_config: Dict) -> List[int]:
    """
    Get all Branch family player IDs.

    Args:
        db_config: Database configuration dict

    Returns:
        List of player_id integers
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        # Query all players with last_name='Branch'
        cursor.execute("""
            SELECT player_id
            FROM players_core
            WHERE last_name = 'Branch'
            ORDER BY player_id
        """)

        branch_ids = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(branch_ids)} Branch family members")

        return branch_ids

    finally:
        cursor.close()
        conn.close()


def detect_branch_games(
    branch_ids: List[int],
    db_config: Dict,
    date_range: Optional[Tuple[date, date]] = None
) -> List[Dict]:
    """
    Detect games featuring Branch family members from staging tables.

    Note: This assumes staging tables have been loaded by Task 2.1
    (branch_detector.py). For now, we'll query players_game_batting_stats
    and players_game_pitching_stats directly.

    Args:
        branch_ids: List of Branch player IDs
        db_config: Database configuration
        date_range: Optional (start_date, end_date) tuple

    Returns:
        List of dicts with game_id, player_id, stats, performance_type
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    games = []

    try:
        # Query batting performances
        batting_query = """
            SELECT
                player_id,
                year,
                team_id,
                game_id,
                ab, h, hr, rbi, r, bb, k, d, t
            FROM players_game_batting_stats
            WHERE player_id = ANY(%s)
        """

        params = [branch_ids]

        if date_range:
            batting_query += " AND year BETWEEN %s AND %s"
            params.extend([date_range[0].year, date_range[1].year])

        cursor.execute(batting_query, params)

        for row in cursor.fetchall():
            games.append({
                'player_id': row[0],
                'year': row[1],
                'team_id': row[2],
                'game_id': row[3],
                'performance_type': 'batting',
                'stats': {
                    'ab': row[4],
                    'h': row[5],
                    'hr': row[6],
                    'rbi': row[7],
                    'r': row[8],
                    'bb': row[9],
                    'k': row[10],
                    'd': row[11],
                    't': row[12]
                }
            })

        # Query pitching performances
        pitching_query = """
            SELECT
                player_id,
                year,
                team_id,
                game_id,
                ip, h, er, hr, bb, k, w, l, sv
            FROM players_game_pitching_stats
            WHERE player_id = ANY(%s)
        """

        params = [branch_ids]

        if date_range:
            pitching_query += " AND year BETWEEN %s AND %s"
            params.extend([date_range[0].year, date_range[1].year])

        cursor.execute(pitching_query, params)

        for row in cursor.fetchall():
            games.append({
                'player_id': row[0],
                'year': row[1],
                'team_id': row[2],
                'game_id': row[3],
                'performance_type': 'pitching',
                'stats': {
                    'ip': float(row[4]) if row[4] else 0.0,
                    'h': row[5],
                    'er': row[6],
                    'hr': row[7],
                    'bb': row[8],
                    'k': row[9],
                    'w': row[10],
                    'l': row[11],
                    'sv': row[12]
                }
            })

        logger.info(f"Found {len(games)} Branch game performances")
        return games

    finally:
        cursor.close()
        conn.close()


def calculate_newsworthiness(performance: Dict) -> int:
    """
    Calculate newsworthiness score (0-100) for a performance.

    Args:
        performance: Performance dict with 'stats' and 'performance_type'

    Returns:
        Score 0-100
    """
    score = 0
    stats = performance['stats']
    perf_type = performance['performance_type']

    if perf_type == 'batting':
        # Multi-HR games
        hr = stats.get('hr', 0)
        if hr >= 3:
            score += 80
        elif hr == 2:
            score += 50

        # High hit games
        h = stats.get('h', 0)
        if h >= 5:
            score += 50
        elif h >= 4:
            score += 35
        elif h == 3:
            score += 20

        # High RBI
        rbi = stats.get('rbi', 0)
        if rbi >= 6:
            score += 40
        elif rbi >= 5:
            score += 30
        elif rbi >= 4:
            score += 20

        # Cycle check (1B + 2B + 3B + HR = 4)
        singles = h - stats.get('d', 0) - stats.get('t', 0) - hr
        if singles >= 1 and stats.get('d', 0) >= 1 and stats.get('t', 0) >= 1 and hr >= 1:
            score = max(score, 90)  # Cycle is always high priority

    elif perf_type == 'pitching':
        ip = stats.get('ip', 0.0)
        h = stats.get('h', 0)
        er = stats.get('er', 0)
        k = stats.get('k', 0)
        w = stats.get('w', 0)
        sv = stats.get('sv', 0)

        # No-hitter
        if ip >= 9.0 and h == 0:
            score = 95

        # Shutout
        elif ip >= 9.0 and er == 0:
            score = 70

        # Complete game
        elif ip >= 9.0:
            score += 50

        # Quality start (6+ IP, ≤3 ER)
        elif ip >= 6.0 and er <= 3:
            score += 35

        # High strikeouts
        if k >= 15:
            score += 50
        elif k >= 12:
            score += 35
        elif k >= 10:
            score += 25

        # Win bonus
        if w == 1:
            score += 15

        # Save bonus
        if sv == 1:
            score += 20

    # Cap at 100
    return min(score, 100)


def prioritize_games(games: List[Dict]) -> List[Dict]:
    """
    Add newsworthiness score and priority tier to each game.

    Args:
        games: List of game performance dicts

    Returns:
        List of games with 'newsworthiness_score' and 'priority' fields added
    """
    for game in games:
        score = calculate_newsworthiness(game)
        game['newsworthiness_score'] = score

        # Determine priority tier
        if score >= NEWSPAPER_CONFIG['priority_thresholds']['MUST_GENERATE']:
            game['priority'] = 'MUST_GENERATE'
        elif score >= NEWSPAPER_CONFIG['priority_thresholds']['SHOULD_GENERATE']:
            game['priority'] = 'SHOULD_GENERATE'
        elif score >= NEWSPAPER_CONFIG['priority_thresholds']['COULD_GENERATE']:
            game['priority'] = 'COULD_GENERATE'
        else:
            game['priority'] = 'SKIP'

    # Sort by score descending
    games.sort(key=lambda x: x['newsworthiness_score'], reverse=True)

    return games


def check_existing_article(game_id: int, player_id: int, db_config: Dict) -> bool:
    """
    Check if article already exists for this game/player combination.

    Args:
        game_id: Game ID
        player_id: Player ID
        db_config: Database configuration

    Returns:
        True if article exists, False otherwise
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        # Check if article exists for this game with this player tagged
        cursor.execute("""
            SELECT COUNT(*)
            FROM newspaper_articles a
            JOIN article_player_tags apt ON a.article_id = apt.article_id
            WHERE a.game_id = %s
              AND apt.player_id = %s
              AND a.status != 'rejected'
        """, (game_id, player_id))

        count = cursor.fetchone()[0]
        return count > 0

    finally:
        cursor.close()
        conn.close()


def get_game_context(game_id: int, db_config: Dict) -> Optional[Dict]:
    """
    Fetch game context for prompt building.

    This is a simplified version - Task 2.1 (game_context.py) would
    have a more complete implementation.

    Args:
        game_id: Game ID
        db_config: Database configuration

    Returns:
        Game context dict or None if not found
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        # Query games table (if it exists and is populated)
        # For now, return a minimal context
        # TODO: Replace with proper game_context.py implementation from Task 2.1

        return {
            'game_id': game_id,
            'date': date.today(),  # Placeholder
            'home_team': {
                'team_id': 0,
                'name': 'Home Team',
                'abbr': 'HOM'
            },
            'away_team': {
                'team_id': 0,
                'name': 'Away Team',
                'abbr': 'AWY'
            },
            'score': {
                'home': 0,
                'away': 0
            }
        }

    finally:
        cursor.close()
        conn.close()


def get_player_details(player_id: int, game_id: int, performance: Dict, db_config: Dict) -> Dict:
    """
    Get player biographical and game performance details.

    Args:
        player_id: Player ID
        game_id: Game ID
        performance: Performance dict with stats
        db_config: Database configuration

    Returns:
        Player details dict
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        # Get player bio
        cursor.execute("""
            SELECT pc.first_name, pc.last_name, pcs.position
            FROM players_core pc
            LEFT JOIN players_current_status pcs ON pc.player_id = pcs.player_id
            WHERE pc.player_id = %s
        """, (player_id,))

        row = cursor.fetchone()
        if not row:
            logger.warning(f"Player {player_id} not found in players_core")
            return None

        first_name, last_name, position = row

        # Build player details
        player_details = {
            'player_id': player_id,
            'full_name': f"{first_name} {last_name}",
            'first_name': first_name,
            'last_name': last_name,
            'position': position,
            'team': {
                'team_id': performance.get('team_id'),
                'name': 'Unknown Team',  # TODO: Get from teams table
                'abbr': 'UNK'
            },
            'game_stats': {}
        }

        # Add stats based on performance type
        if performance['performance_type'] == 'batting':
            player_details['game_stats']['batting'] = performance['stats']
        else:
            player_details['game_stats']['pitching'] = performance['stats']

        return player_details

    finally:
        cursor.close()
        conn.close()


def select_model_for_priority(priority: str) -> str:
    """
    Select Ollama model based on priority tier.

    Args:
        priority: Priority string (MUST_GENERATE, SHOULD_GENERATE, etc.)

    Returns:
        Model name
    """
    models = OLLAMA_CONFIG['models']

    if priority == 'MUST_GENERATE':
        return models['MUST_GENERATE']
    elif priority == 'SHOULD_GENERATE':
        return models['SHOULD_GENERATE']
    elif priority == 'COULD_GENERATE':
        return models['COULD_GENERATE']
    else:
        return models['SHOULD_GENERATE']  # Default


def select_temperature_for_priority(priority: str) -> float:
    """
    Select temperature based on priority tier.

    Args:
        priority: Priority string

    Returns:
        Temperature (0.0-1.0)
    """
    temps = OLLAMA_CONFIG['temperatures']

    if priority == 'MUST_GENERATE':
        return temps['MUST_GENERATE']
    elif priority == 'SHOULD_GENERATE':
        return temps['SHOULD_GENERATE']
    elif priority == 'COULD_GENERATE':
        return temps['COULD_GENERATE']
    else:
        return OLLAMA_CONFIG['default_temperature']


def generate_branch_articles_pipeline(
    date_range: Optional[Tuple[date, date]] = None,
    force_regenerate: bool = False,
    priority_filter: Optional[List[str]] = None
) -> Dict:
    """
    End-to-end pipeline for Branch family article generation.

    Workflow:
    1. Get Branch family player IDs
    2. Detect Branch games in date_range
    3. Check for existing articles (skip unless force_regenerate)
    4. Prioritize by newsworthiness
    5. Filter to MUST_GENERATE and SHOULD_GENERATE (unless specified)
    6. For each game:
        a. Gather game context
        b. Get player details
        c. Build prompt
        d. Select model based on priority
        e. Generate article via Ollama
        f. Parse and validate output
        g. Save to database as draft
        h. Log results
    7. Return summary statistics

    Args:
        date_range: Optional (start_date, end_date) tuple
        force_regenerate: If True, regenerate even if article exists
        priority_filter: List of priority tiers to generate (default: MUST_GENERATE, SHOULD_GENERATE)

    Returns:
        Dict with counts: {
            'detected': int,
            'generated': int,
            'failed': int,
            'skipped': int,
            'errors': List[str]
        }
    """
    logger.info("=" * 80)
    logger.info("STARTING BRANCH ARTICLE GENERATION PIPELINE")
    logger.info("=" * 80)

    results = {
        'detected': 0,
        'generated': 0,
        'failed': 0,
        'skipped': 0,
        'errors': []
    }

    # Default priority filter
    if priority_filter is None:
        priority_filter = ['MUST_GENERATE', 'SHOULD_GENERATE']

    try:
        # Step 1: Get Branch family IDs
        logger.info("\n[Step 1] Retrieving Branch family player IDs...")
        branch_ids = get_branch_family_ids(DB_CONFIG['dev'])

        if not branch_ids:
            logger.warning("No Branch family members found")
            return results

        # Step 2: Detect Branch games
        logger.info(f"\n[Step 2] Detecting Branch games (date_range={date_range})...")
        games = detect_branch_games(branch_ids, DB_CONFIG['dev'], date_range)
        results['detected'] = len(games)

        if not games:
            logger.info("No Branch games found in date range")
            return results

        # Step 3: Prioritize games
        logger.info("\n[Step 3] Scoring newsworthiness and prioritizing...")
        games = prioritize_games(games)

        # Log priority distribution
        priority_counts = {}
        for game in games:
            priority = game['priority']
            priority_counts[priority] = priority_counts.get(priority, 0) + 1

        logger.info("Priority distribution:")
        for priority, count in sorted(priority_counts.items()):
            logger.info(f"  {priority}: {count} games")

        # Step 4: Filter by priority
        filtered_games = [g for g in games if g['priority'] in priority_filter]
        logger.info(f"\nFiltered to {len(filtered_games)} games in priority tiers: {priority_filter}")

        if not filtered_games:
            logger.info("No games meet priority filter criteria")
            return results

        # Step 5: Initialize clients
        logger.info("\n[Step 5] Initializing Ollama client and article processor...")

        ollama_client = OllamaClient(
            base_url=OLLAMA_CONFIG['base_url'],
            timeout=OLLAMA_CONFIG['timeout']
        )

        # Health check
        if not ollama_client.health_check():
            error_msg = "Ollama service is not available"
            logger.error(f"✗ {error_msg}")
            results['errors'].append(error_msg)
            return results

        article_processor = create_processor(DB_CONFIG['dev'])

        # Step 6: Generate articles
        logger.info(f"\n[Step 6] Generating articles for {len(filtered_games)} games...")

        for i, game in enumerate(filtered_games, 1):
            game_id = game['game_id']
            player_id = game['player_id']
            priority = game['priority']
            score = game['newsworthiness_score']

            logger.info(f"\n--- Game {i}/{len(filtered_games)} ---")
            logger.info(f"Game ID: {game_id}, Player ID: {player_id}")
            logger.info(f"Priority: {priority}, Score: {score}")

            try:
                # Check if article already exists
                if not force_regenerate and check_existing_article(game_id, player_id, DB_CONFIG['dev']):
                    logger.info(f"  ⏭  Article already exists, skipping")
                    results['skipped'] += 1
                    continue

                # Get game context
                game_context = get_game_context(game_id, DB_CONFIG['dev'])
                if not game_context:
                    logger.warning(f"  ⚠  Could not retrieve game context, skipping")
                    results['skipped'] += 1
                    continue

                # Get player details
                player_details = get_player_details(player_id, game_id, game, DB_CONFIG['dev'])
                if not player_details:
                    logger.warning(f"  ⚠  Could not retrieve player details, skipping")
                    results['skipped'] += 1
                    continue

                # Build prompt (without play-by-play for now - Task 2.2 integration pending)
                logger.info(f"  Building prompt...")
                prompt = build_article_prompt(
                    game_context=game_context,
                    player_details=player_details
                )

                # Select model and temperature
                model = select_model_for_priority(priority)
                temperature = select_temperature_for_priority(priority)

                # Check model availability and get fallback if needed
                model = get_fallback_model(model, ollama_client)

                logger.info(f"  Generating article with {model} (temp={temperature})...")

                # Generate article
                article_text, metadata = ollama_client.generate_with_retry(
                    prompt=prompt,
                    model=model,
                    temperature=temperature,
                    max_tokens=OLLAMA_CONFIG['default_max_tokens'],
                    max_retries=OLLAMA_CONFIG['max_retries']
                )

                logger.info(f"  ✓ Article generated in {metadata['total_time']:.2f}s")

                # Process and save article
                logger.info(f"  Processing and saving article...")

                article_id, process_result = article_processor.process_and_save(
                    raw_article_text=article_text,
                    game_context=game_context,
                    generation_metadata=metadata,
                    newsworthiness_score=score,
                    category_name='Game Recap',
                    player_ids=[player_id],
                    team_ids=[game_context['home_team']['team_id'], game_context['away_team']['team_id']]
                )

                if process_result['success']:
                    logger.info(f"  ✓ Article saved: article_id={article_id}")
                    logger.info(f"     Headline: {process_result['headline'][:60]}...")
                    logger.info(f"     Word count: {process_result['word_count']}")
                    results['generated'] += 1
                else:
                    error_msg = f"Game {game_id}: {process_result.get('error')}"
                    logger.error(f"  ✗ Article processing failed: {process_result.get('error')}")
                    results['failed'] += 1
                    results['errors'].append(error_msg)

            except Exception as e:
                error_msg = f"Game {game_id}: {str(e)}"
                logger.error(f"  ✗ Error generating article: {e}")
                results['failed'] += 1
                results['errors'].append(error_msg)
                import traceback
                traceback.print_exc()

        # Close processor
        article_processor.close()

        # Step 7: Summary
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Detected: {results['detected']} games")
        logger.info(f"Generated: {results['generated']} articles")
        logger.info(f"Failed: {results['failed']} articles")
        logger.info(f"Skipped: {results['skipped']} articles")

        if results['errors']:
            logger.info(f"\nErrors ({len(results['errors'])}):")
            for error in results['errors'][:5]:  # Show first 5
                logger.info(f"  - {error}")
            if len(results['errors']) > 5:
                logger.info(f"  ... and {len(results['errors']) - 5} more")

        return results

    except Exception as e:
        logger.error(f"Pipeline failed with exception: {e}")
        import traceback
        traceback.print_exc()
        results['errors'].append(f"Pipeline exception: {str(e)}")
        return results