"""Player service layer - Handles all player-related business logic and queries.

This service centralizes player data access and calculations, enabling:
- Cleaner route handlers
- Better testability
- Future caching integration
- Performance optimization through efficient queries
"""
from datetime import date
from sqlalchemy import func, and_
from app.extensions import db
from app.models import (
    Player, PlayerBattingStats, PlayerPitchingStats,
    PlayerFieldingStats, Team, League, TradeHistory, Message
)


def calculate_age_for_season(birth_date, season_year):
    """Calculate player's age during a specific season.

    Uses June 30th as the reference date (mid-season).

    Args:
        birth_date: Player's date of birth (date object)
        season_year: Season year (int)

    Returns:
        int: Player's age, or None if birth_date is missing
    """
    if not birth_date or not season_year:
        return None

    # Use June 30th as reference (middle of season)
    mid_season = date(season_year, 6, 30)

    age = mid_season.year - birth_date.year
    # Adjust if birthday hasn't occurred yet
    if (mid_season.month, mid_season.day) < (birth_date.month, birth_date.day):
        age -= 1

    return age


def get_player_career_batting_stats(player_id):
    """Get player's yearly batting statistics with career totals.

    Returns yearly stats ordered by year (most recent first) plus calculated
    career totals. Includes team and league information for links.

    Args:
        player_id: Player ID (int)

    Returns:
        dict: {
            'yearly_stats': [PlayerBattingStats objects with age added],
            'career_totals': dict of career totals with calculated rate stats
        }
    """
    # Get player for age calculation
    player = Player.query.get(player_id)
    if not player:
        return {'yearly_stats': [], 'career_totals': None}

    # Query yearly stats with relationship loading disabled (avoid N+1)
    # Only get overall stats (split_id=1)
    # Order by year ascending (earliest to most recent)
    # OPTIMIZATION: Block automatic eager loading of player/team relationships
    from sqlalchemy.orm import lazyload

    yearly_stats = (
        PlayerBattingStats.query
        .options(
            lazyload(PlayerBattingStats.player),  # Don't load player (we already have it)
            lazyload(PlayerBattingStats.team)     # Don't load team (causes cascade)
        )
        .filter_by(player_id=player_id, split_id=1)
        .order_by(PlayerBattingStats.year.asc())
        .all()
    )

    # Add age to each stat row
    for stat in yearly_stats:
        stat.age = calculate_age_for_season(player.date_of_birth, stat.year)

    # Calculate career totals using SQL aggregation (performance)
    totals_query = db.session.query(
        # Counting stats - sum them
        func.sum(PlayerBattingStats.g).label('g'),
        func.sum(PlayerBattingStats.pa).label('pa'),
        func.sum(PlayerBattingStats.ab).label('ab'),
        func.sum(PlayerBattingStats.r).label('r'),
        func.sum(PlayerBattingStats.h).label('h'),
        func.sum(PlayerBattingStats.d).label('d'),  # doubles
        func.sum(PlayerBattingStats.t).label('t'),  # triples
        func.sum(PlayerBattingStats.hr).label('hr'),
        func.sum(PlayerBattingStats.rbi).label('rbi'),
        func.sum(PlayerBattingStats.sb).label('sb'),
        func.sum(PlayerBattingStats.cs).label('cs'),
        func.sum(PlayerBattingStats.bb).label('bb'),
        func.sum(PlayerBattingStats.ibb).label('ibb'),
        func.sum(PlayerBattingStats.k).label('k'),
        func.sum(PlayerBattingStats.hp).label('hp'),
        func.sum(PlayerBattingStats.sh).label('sh'),
        func.sum(PlayerBattingStats.sf).label('sf'),
        func.sum(PlayerBattingStats.gdp).label('gdp'),
        # Advanced stats - sum the counting stats (WAR, wRC, wRAA, WPA, UBR)
        func.sum(PlayerBattingStats.war).label('war'),
        func.sum(PlayerBattingStats.wrc).label('wrc'),
        func.sum(PlayerBattingStats.wraa).label('wraa'),
        func.sum(PlayerBattingStats.wpa).label('wpa'),
        func.sum(PlayerBattingStats.ubr).label('ubr'),
        # Note: Rate stats (ISO, BABIP, wOBA) are in yearly_stats, calculated from career totals below
        # Note: wRC+ cannot be summed (context-dependent), show '-' in career row
    ).filter(
        PlayerBattingStats.player_id == player_id,
        PlayerBattingStats.split_id == 1
    ).first()

    if not totals_query or totals_query.ab is None or totals_query.ab == 0:
        # No batting stats
        return {'yearly_stats': yearly_stats, 'career_totals': None}

    # Convert to dict for easier manipulation
    # Note: Use bracket notation for 't' because Row.t returns tuple
    career_totals = {
        'g': totals_query.g or 0,
        'pa': totals_query.pa or 0,
        'ab': totals_query.ab or 0,
        'r': totals_query.r or 0,
        'h': totals_query.h or 0,
        'd': totals_query.d or 0,
        't': totals_query[6] or 0,  # Use index because .t returns tuple
        'hr': totals_query.hr or 0,
        'rbi': totals_query.rbi or 0,
        'sb': totals_query.sb or 0,
        'cs': totals_query.cs or 0,
        'bb': totals_query.bb or 0,
        'ibb': totals_query.ibb or 0,
        'k': totals_query.k or 0,
        'hp': totals_query.hp or 0,
        'sh': totals_query.sh or 0,
        'sf': totals_query.sf or 0,
        'gdp': totals_query.gdp or 0,
        # Advanced stats (summed)
        'war': totals_query.war,
        'wrc': totals_query.wrc,
        'wraa': totals_query.wraa,
        'wpa': totals_query.wpa,
        'ubr': totals_query.ubr,
    }

    # Calculate rate stats from totals
    career_totals['batting_average'] = _calculate_batting_average(career_totals)
    career_totals['on_base_percentage'] = _calculate_obp(career_totals)
    career_totals['slugging_percentage'] = _calculate_slg(career_totals)

    # OPS = OBP + SLG
    if career_totals['on_base_percentage'] and career_totals['slugging_percentage']:
        career_totals['ops'] = career_totals['on_base_percentage'] + career_totals['slugging_percentage']
    else:
        career_totals['ops'] = None

    # Total Bases = 1B + (2�2B) + (3�3B) + (4�HR)
    singles = career_totals['h'] - career_totals['d'] - career_totals['t'] - career_totals['hr']
    career_totals['total_bases'] = (
        singles +
        (2 * career_totals['d']) +
        (3 * career_totals['t']) +
        (4 * career_totals['hr'])
    )

    # Calculate advanced rate stats from career totals
    # (These aren't stored for career in DB, must calculate from totals)
    career_totals['iso'] = _calculate_iso(career_totals)
    career_totals['babip'] = _calculate_babip(career_totals)
    career_totals['woba'] = _calculate_woba(career_totals)
    # wRC+ cannot be calculated for career (context-dependent), will show '-' in template

    return {
        'yearly_stats': yearly_stats,
        'career_totals': career_totals
    }


def get_player_career_pitching_stats(player_id):
    """Get player's yearly pitching statistics with career totals.

    Returns yearly stats ordered by year (most recent first) plus calculated
    career totals. Includes team and league information for links.

    Args:
        player_id: Player ID (int)

    Returns:
        dict: {
            'yearly_stats': [PlayerPitchingStats objects with age added],
            'career_totals': dict of career totals with calculated rate stats
        }
    """
    # Get player for age calculation
    player = Player.query.get(player_id)
    if not player:
        return {'yearly_stats': [], 'career_totals': None}

    # Query yearly stats (split_id=1 for overall)
    # Order by year ascending (earliest to most recent)
    # OPTIMIZATION: Block automatic eager loading of player/team relationships
    from sqlalchemy.orm import lazyload

    yearly_stats = (
        PlayerPitchingStats.query
        .options(
            lazyload(PlayerPitchingStats.player),  # Don't load player (we already have it)
            lazyload(PlayerPitchingStats.team)     # Don't load team (causes cascade)
        )
        .filter_by(player_id=player_id, split_id=1)
        .order_by(PlayerPitchingStats.year.asc())
        .all()
    )

    # Add age to each stat row
    for stat in yearly_stats:
        stat.age = calculate_age_for_season(player.date_of_birth, stat.year)

    # Calculate career totals using SQL aggregation
    totals_query = db.session.query(
        # Counting stats
        func.sum(PlayerPitchingStats.g).label('g'),
        func.sum(PlayerPitchingStats.gs).label('gs'),
        func.sum(PlayerPitchingStats.gf).label('gf'),
        func.sum(PlayerPitchingStats.w).label('w'),
        func.sum(PlayerPitchingStats.l).label('l'),
        func.sum(PlayerPitchingStats.s).label('s'),  # saves
        func.sum(PlayerPitchingStats.cg).label('cg'),
        func.sum(PlayerPitchingStats.sho).label('sho'),
        func.sum(PlayerPitchingStats.outs).label('outs'),  # Total outs (for IP calculation)
        func.sum(PlayerPitchingStats.ha).label('ha'),  # hits allowed
        func.sum(PlayerPitchingStats.r).label('r'),
        func.sum(PlayerPitchingStats.er).label('er'),
        func.sum(PlayerPitchingStats.hra).label('hra'),  # HR allowed
        func.sum(PlayerPitchingStats.bb).label('bb'),
        func.sum(PlayerPitchingStats.iw).label('iw'),  # intentional walks
        func.sum(PlayerPitchingStats.k).label('k'),
        func.sum(PlayerPitchingStats.hp).label('hp'),
        func.sum(PlayerPitchingStats.bk).label('bk'),
        func.sum(PlayerPitchingStats.wp).label('wp'),
        func.sum(PlayerPitchingStats.bf).label('bf'),  # batters faced
        # WAR - sum it
        func.sum(PlayerPitchingStats.war).label('war'),
    ).filter(
        PlayerPitchingStats.player_id == player_id,
        PlayerPitchingStats.split_id == 1
    ).first()

    if not totals_query or totals_query.outs is None or totals_query.outs == 0:
        # No pitching stats
        return {'yearly_stats': yearly_stats, 'career_totals': None}

    # Convert to dict
    career_totals = {
        'g': totals_query.g or 0,
        'gs': totals_query.gs or 0,
        'gf': totals_query.gf or 0,
        'w': totals_query.w or 0,
        'l': totals_query.l or 0,
        's': totals_query.s or 0,
        'cg': totals_query.cg or 0,
        'sho': totals_query.sho or 0,
        'outs': totals_query.outs or 0,
        'ha': totals_query.ha or 0,
        'r': totals_query.r or 0,
        'er': totals_query.er or 0,
        'hra': totals_query.hra or 0,
        'bb': totals_query.bb or 0,
        'iw': totals_query.iw or 0,
        'k': totals_query.k or 0,
        'hp': totals_query.hp or 0,
        'bk': totals_query.bk or 0,
        'wp': totals_query.wp or 0,
        'bf': totals_query.bf or 0,
        'war': totals_query.war,
    }

    # Calculate IP from total outs
    # IP displayed as XXX.Y where Y is 0, 1, or 2 (the fractional outs)
    total_outs = career_totals['outs']
    career_totals['ip'] = total_outs // 3  # Whole innings
    career_totals['ipf'] = total_outs % 3   # Fractional outs (0, 1, or 2)

    # Calculate innings as decimal for rate stat calculations
    innings_pitched = total_outs / 3.0

    # Calculate rate stats from totals
    career_totals['era'] = _calculate_era(career_totals['er'], innings_pitched)
    career_totals['whip'] = _calculate_whip(career_totals['ha'], career_totals['bb'], innings_pitched)
    career_totals['k9'] = _calculate_per_nine(career_totals['k'], innings_pitched)
    career_totals['bb9'] = _calculate_per_nine(career_totals['bb'], innings_pitched)
    career_totals['hr9'] = _calculate_per_nine(career_totals['hra'], innings_pitched)
    career_totals['h9'] = _calculate_per_nine(career_totals['ha'], innings_pitched)

    # SO/W ratio
    if career_totals['bb'] > 0:
        career_totals['so_w'] = round(career_totals['k'] / career_totals['bb'], 2)
    else:
        career_totals['so_w'] = None

    return {
        'yearly_stats': yearly_stats,
        'career_totals': career_totals
    }


# ===== HELPER FUNCTIONS FOR RATE STAT CALCULATIONS =====

def _calculate_batting_average(totals):
    """Calculate batting average from totals."""
    if totals['ab'] > 0:
        return round(totals['h'] / totals['ab'], 3)
    return None


def _calculate_obp(totals):
    """Calculate on-base percentage from totals.

    Formula: (H + BB + HBP) / (AB + BB + HBP + SF)
    """
    numerator = totals['h'] + totals['bb'] + totals['hp']
    denominator = totals['ab'] + totals['bb'] + totals['hp'] + totals['sf']

    if denominator > 0:
        return round(numerator / denominator, 3)
    return None


def _calculate_slg(totals):
    """Calculate slugging percentage from totals.

    Formula: TB / AB
    """
    if totals['ab'] > 0:
        # Total Bases = 1B + (2�2B) + (3�3B) + (4�HR)
        singles = totals['h'] - totals['d'] - totals['t'] - totals['hr']
        total_bases = singles + (2 * totals['d']) + (3 * totals['t']) + (4 * totals['hr'])
        return round(total_bases / totals['ab'], 3)
    return None


def _calculate_era(earned_runs, innings_pitched):
    """Calculate ERA from earned runs and innings pitched.

    Formula: (ER * 9) / IP
    """
    if innings_pitched > 0:
        return round((earned_runs * 9) / innings_pitched, 2)
    return None


def _calculate_whip(hits, walks, innings_pitched):
    """Calculate WHIP from hits, walks, and innings pitched.

    Formula: (H + BB) / IP
    """
    if innings_pitched > 0:
        return round((hits + walks) / innings_pitched, 2)
    return None


def _calculate_per_nine(stat_value, innings_pitched):
    """Calculate per-9-innings rate (K/9, BB/9, etc.)

    Formula: (Stat * 9) / IP
    """
    if innings_pitched > 0:
        return round((stat_value * 9) / innings_pitched, 1)
    return None


def _calculate_iso(totals):
    """Calculate Isolated Power (ISO) from totals.

    Formula: SLG - AVG
    """
    slg = _calculate_slg(totals)
    avg = _calculate_batting_average(totals)

    if slg is not None and avg is not None:
        return round(slg - avg, 3)
    return None


def _calculate_babip(totals):
    """Calculate Batting Average on Balls in Play (BABIP) from totals.

    Formula: (H - HR) / (AB - K - HR + SF)
    """
    numerator = totals['h'] - totals['hr']
    denominator = totals['ab'] - totals['k'] - totals['hr'] + totals['sf']

    if denominator > 0:
        return round(numerator / denominator, 3)
    return None


def _calculate_woba(totals):
    """Calculate Weighted On-Base Average (wOBA) from totals.

    This is a simplified calculation using standard weights.
    For exact wOBA, we'd need league-specific constants.

    Simplified formula using standard weights:
    wOBA = (0.69*BB + 0.72*HBP + 0.89*1B + 1.27*2B + 1.62*3B + 2.10*HR) / (AB + BB - IBB + SF + HBP)
    """
    singles = totals['h'] - totals['d'] - totals['t'] - totals['hr']

    numerator = (
        (0.69 * totals['bb']) +
        (0.72 * totals['hp']) +
        (0.89 * singles) +
        (1.27 * totals['d']) +
        (1.62 * totals['t']) +
        (2.10 * totals['hr'])
    )

    denominator = totals['ab'] + totals['bb'] - totals['ibb'] + totals['sf'] + totals['hp']

    if denominator > 0:
        return round(numerator / denominator, 3)
    return None


def get_player_trade_history(player_id):
    """Get all trades involving a specific player.

    Returns trades in chronological order (oldest to newest).

    Args:
        player_id: Player ID (int)

    Returns:
        list: List of TradeHistory objects where player was traded
    """
    # Query for trades where player appears in any of the 20 player slots
    trades = (
        TradeHistory.query
        .filter(
            db.or_(
                TradeHistory.player_id_0_0 == player_id,
                TradeHistory.player_id_0_1 == player_id,
                TradeHistory.player_id_0_2 == player_id,
                TradeHistory.player_id_0_3 == player_id,
                TradeHistory.player_id_0_4 == player_id,
                TradeHistory.player_id_0_5 == player_id,
                TradeHistory.player_id_0_6 == player_id,
                TradeHistory.player_id_0_7 == player_id,
                TradeHistory.player_id_0_8 == player_id,
                TradeHistory.player_id_0_9 == player_id,
                TradeHistory.player_id_1_0 == player_id,
                TradeHistory.player_id_1_1 == player_id,
                TradeHistory.player_id_1_2 == player_id,
                TradeHistory.player_id_1_3 == player_id,
                TradeHistory.player_id_1_4 == player_id,
                TradeHistory.player_id_1_5 == player_id,
                TradeHistory.player_id_1_6 == player_id,
                TradeHistory.player_id_1_7 == player_id,
                TradeHistory.player_id_1_8 == player_id,
                TradeHistory.player_id_1_9 == player_id
            )
        )
        .order_by(TradeHistory.date.asc())
        .all()
    )

    return trades


def get_player_news(player_id, limit=None):
    """Get player-related news stories (contracts, injuries, awards, highlights, career milestones).

    Filters to relevant message types only:
    - Type 2: Contract signings
    - Type 3: Retirements & suspensions
    - Type 4: Performance highlights
    - Type 7: Awards (Player of the Week, etc.)
    - Type 8: Injuries

    Excludes:
    - Type 0: General announcements
    - Type 1: Trades (shown separately via trade_history)
    - Type 6: General
    - Type 11: Trade rumors

    Args:
        player_id: Player ID
        limit: Maximum number of messages to return (None = all)

    Returns:
        list: List of Message objects ordered by date (most recent first)
    """
    # Relevant message types for player pages
    RELEVANT_TYPES = [2, 3, 4, 7, 8]

    query = (
        Message.query
        .filter(
            and_(
                Message.deleted == 0,  # Exclude deleted messages
                Message.message_type.in_(RELEVANT_TYPES)  # Only relevant types
            )
        )
        .filter(
            # Check if player appears in any of the 10 player slots
            (Message.player_id_0 == player_id) |
            (Message.player_id_1 == player_id) |
            (Message.player_id_2 == player_id) |
            (Message.player_id_3 == player_id) |
            (Message.player_id_4 == player_id) |
            (Message.player_id_5 == player_id) |
            (Message.player_id_6 == player_id) |
            (Message.player_id_7 == player_id) |
            (Message.player_id_8 == player_id) |
            (Message.player_id_9 == player_id)
        )
        .order_by(Message.date.desc())  # Most recent first
    )

    if limit:
        query = query.limit(limit)

    return query.all()
