"""
Newsworthiness Scoring Module

Calculates newsworthiness scores (0-100) for Branch family game performances
to prioritize article generation. Higher scores indicate more noteworthy games
that warrant better LLM models and priority processing.

Scoring Criteria:
- Batting: Multi-HR games, hitting for cycle, 4+ hits, 5+ RBI
- Pitching: Shutouts, no-hitters, complete games, quality starts with high K
- Combined performances get bonus points
"""

from typing import Dict, List
from loguru import logger


# Priority thresholds for article generation
PRIORITY_MUST_GENERATE = 80      # Milestones, exceptional performances
PRIORITY_SHOULD_GENERATE = 50    # Solid performances worth covering
PRIORITY_COULD_GENERATE = 20     # Routine performances, generate if time permits
PRIORITY_SKIP = 0                # Not worth generating


def calculate_batting_newsworthiness(stats: Dict) -> int:
    """
    Score batting performance 0-100 based on quality.

    Scoring rubric:
    - Home runs: 25 points per HR (max 75 for 3+)
    - Cycle (1B, 2B, 3B, HR): 90 points
    - 4+ hits: 40 points
    - 3 hits: 25 points
    - 5+ RBI: 45 points
    - 3-4 RBI: 25 points
    - Grand slam (4 RBI + HR): Bonus 10 points
    - 2+ stolen bases: 10 points
    - Perfect game (H >= AB, no K): 15 points

    Args:
        stats: Dict with keys: ab, h, d, t, hr, r, rbi, sb, bb, k

    Returns:
        Score from 0-100
    """
    score = 0

    ab = stats.get('ab', 0)
    h = stats.get('h', 0)
    d = stats.get('d', 0)  # doubles
    t = stats.get('t', 0)  # triples
    hr = stats.get('hr', 0)
    rbi = stats.get('rbi', 0)
    sb = stats.get('sb', 0)
    k = stats.get('k', 0)

    # Skip if didn't play (0 AB and 0 walks)
    bb = stats.get('bb', 0)
    if ab == 0 and bb == 0:
        return 0

    # Home runs (major scoring factor)
    if hr >= 3:
        score += 75  # 3+ HR game is exceptional
    elif hr == 2:
        score += 50  # 2-HR game is very newsworthy
    elif hr == 1:
        score += 25  # Single HR is notable

    # Cycle detection (single, double, triple, homer)
    singles = h - d - t - hr
    if singles >= 1 and d >= 1 and t >= 1 and hr >= 1:
        score = 90  # Hitting for cycle is extremely rare

    # Hits
    if h >= 5:
        score += 50
    elif h == 4:
        score += 40
    elif h == 3:
        score += 25
    elif h == 2:
        score += 10

    # RBI
    if rbi >= 6:
        score += 50
    elif rbi >= 5:
        score += 45
    elif rbi >= 4:
        score += 30
    elif rbi >= 3:
        score += 25
    elif rbi >= 2:
        score += 10

    # Grand slam bonus (4 RBI + HR, assuming typical grand slam)
    if hr >= 1 and rbi >= 4:
        score += 10

    # Stolen bases
    if sb >= 3:
        score += 15
    elif sb >= 2:
        score += 10

    # Perfect performance (all hits, no strikeouts)
    if ab > 0 and h == ab and k == 0:
        score += 15

    # Cap at 100
    return min(score, 100)


def calculate_pitching_newsworthiness(stats: Dict) -> int:
    """
    Score pitching performance 0-100 based on quality.

    Scoring rubric:
    - No-hitter: 95 points
    - One-hitter: 70 points
    - Shutout (0 ER, 6+ IP): 70 points
    - Complete game (9 IP): 50 points
    - Complete game shutout: 85 points
    - Win: 15 points
    - Save: 25 points
    - Quality start (6+ IP, <= 3 ER): 30 points
    - 10+ strikeouts: 30 points
    - 15+ strikeouts: 50 points

    Args:
        stats: Dict with keys: gs, ip, h, r, er, hr, bb, k, w, l, sv

    Returns:
        Score from 0-100
    """
    score = 0

    ip = stats.get('ip', 0.0)
    h = stats.get('h', 0)
    er = stats.get('er', 0)
    k = stats.get('k', 0)
    w = stats.get('w', 0)
    sv = stats.get('sv', 0)
    gs = stats.get('gs', 0)

    # Skip if didn't pitch
    if ip == 0:
        return 0

    # No-hitter (complete game with 0 hits)
    if ip >= 9.0 and h == 0:
        score = 95  # No-hitter is headline news

    # One-hitter
    elif ip >= 9.0 and h == 1:
        score = 70

    # Complete game shutout
    elif ip >= 9.0 and er == 0:
        score = 85

    # Shutout (not complete game)
    elif ip >= 6.0 and er == 0 and gs > 0:
        score += 70

    # Complete game (not shutout)
    elif ip >= 9.0:
        score += 50

    # Quality start (6+ IP, 3 or fewer ER)
    elif ip >= 6.0 and er <= 3 and gs > 0:
        score += 30

    # Win
    if w > 0:
        score += 15

    # Save
    if sv > 0:
        score += 25

    # Strikeouts
    if k >= 15:
        score += 50
    elif k >= 12:
        score += 40
    elif k >= 10:
        score += 30
    elif k >= 8:
        score += 15

    # Dominant relief appearance (3+ IP, 0 ER, 5+ K)
    if gs == 0 and ip >= 3.0 and er == 0 and k >= 5:
        score += 35

    # Cap at 100
    return min(score, 100)


def calculate_newsworthiness(performance: Dict) -> int:
    """
    Calculate newsworthiness score for a single performance.

    Args:
        performance: Dict with keys: player_id, stats_type, stats

    Returns:
        Score from 0-100
    """
    stats_type = performance.get('stats_type')
    stats = performance.get('stats', {})

    if stats_type == 'batting':
        return calculate_batting_newsworthiness(stats)
    elif stats_type == 'pitching':
        return calculate_pitching_newsworthiness(stats)
    else:
        logger.warning(f"Unknown stats_type: {stats_type}")
        return 0


def calculate_combined_newsworthiness(performances: List[Dict]) -> int:
    """
    Calculate newsworthiness for games with multiple Branch player performances.

    Takes the highest individual score and adds bonus points for multiple contributors.

    Args:
        performances: List of performance dicts with keys: player_id, stats_type, stats

    Returns:
        Combined score from 0-100
    """
    if not performances:
        return 0

    # Calculate individual scores
    scores = [calculate_newsworthiness(perf) for perf in performances]
    max_score = max(scores)

    # Bonus for multiple Branch players in same game
    if len(performances) > 1:
        # Add 10 points for family angle
        bonus = 10
        logger.info(f"Multi-Branch bonus: +{bonus} points for {len(performances)} players")
        max_score = min(max_score + bonus, 100)

    return max_score


def prioritize_games(branch_games: List[Dict]) -> List[Dict]:
    """
    Add priority tier and newsworthiness score to each game.

    Tiers:
    - MUST_GENERATE (>= 80): Exceptional performances, use best model
    - SHOULD_GENERATE (50-79): Solid performances, use standard model
    - COULD_GENERATE (20-49): Routine performances, generate if capacity allows
    - SKIP (< 20): Not worth generating

    Args:
        branch_games: List of game dicts from detect_multi_branch_games()
                      Each has: game_id, year, player_ids, team_ids, performances

    Returns:
        Same list with added fields: newsworthiness_score, priority
        Sorted by newsworthiness_score descending
    """
    for game in branch_games:
        performances = game.get('performances', [])

        # Calculate score
        if len(performances) > 1:
            score = calculate_combined_newsworthiness(performances)
        else:
            score = calculate_newsworthiness(performances[0]) if performances else 0

        game['newsworthiness_score'] = score

        # Assign priority tier
        if score >= PRIORITY_MUST_GENERATE:
            game['priority'] = 'MUST_GENERATE'
        elif score >= PRIORITY_SHOULD_GENERATE:
            game['priority'] = 'SHOULD_GENERATE'
        elif score >= PRIORITY_COULD_GENERATE:
            game['priority'] = 'COULD_GENERATE'
        else:
            game['priority'] = 'SKIP'

    # Sort by score descending
    branch_games.sort(key=lambda g: g['newsworthiness_score'], reverse=True)

    # Log summary
    priority_counts = {}
    for game in branch_games:
        priority = game['priority']
        priority_counts[priority] = priority_counts.get(priority, 0) + 1

    logger.info(f"Game prioritization complete: {priority_counts}")

    return branch_games


def filter_by_priority(branch_games: List[Dict], min_priority: str = 'SHOULD_GENERATE') -> List[Dict]:
    """
    Filter games to only those meeting minimum priority threshold.

    Args:
        branch_games: Prioritized list from prioritize_games()
        min_priority: Minimum priority tier to include
                      Options: 'MUST_GENERATE', 'SHOULD_GENERATE', 'COULD_GENERATE'

    Returns:
        Filtered list of games
    """
    priority_order = {
        'MUST_GENERATE': 3,
        'SHOULD_GENERATE': 2,
        'COULD_GENERATE': 1,
        'SKIP': 0
    }

    min_level = priority_order.get(min_priority, 2)
    filtered = [g for g in branch_games if priority_order.get(g['priority'], 0) >= min_level]

    logger.info(f"Filtered to {len(filtered)}/{len(branch_games)} games with priority >= {min_priority}")
    return filtered