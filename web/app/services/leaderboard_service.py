"""Leaderboard service layer - Handles all leaderboard queries and caching.

This service centralizes leaderboard data access, enabling:
- High-performance queries against pre-aggregated materialized views
- In-memory caching with automatic TTL
- Flexible filtering by league, year, active status
- Pagination support for large result sets
- Future Redis integration for distributed caching

Architecture:
- Queries materialized views (no runtime aggregation)
- Filters applied at database level (indexed columns)
- Top-level league filtering only (maintains simplicity)
- Results cached by query signature for performance
"""
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from sqlalchemy import desc, asc
from app.extensions import db
from app.models import (
    LeaderboardCareerBatting,
    LeaderboardCareerPitching,
    LeaderboardSingleSeasonBatting,
    LeaderboardSingleSeasonPitching,
    LeaderboardYearlyBatting,
    LeaderboardYearlyPitching,
    League
)


# =====================================================
# In-Memory Cache Configuration
# =====================================================
# Simple in-memory cache until Redis is available
# Cache structure: {cache_key: {'data': result, 'expires': timestamp}}
_CACHE = {}
_CACHE_TTL = timedelta(minutes=15)  # Cache expires after 15 minutes


def _get_cache_key(prefix: str, **kwargs) -> str:
    """Generate cache key from function name and arguments.

    Args:
        prefix: Function/query identifier
        **kwargs: Query parameters to include in key

    Returns:
        str: Unique cache key
    """
    # Sort kwargs for consistent key generation
    params = '_'.join(f"{k}={v}" for k, v in sorted(kwargs.items()) if v is not None)
    return f"{prefix}:{params}" if params else prefix


def _get_cached(cache_key: str) -> Optional[Any]:
    """Retrieve cached data if not expired.

    Args:
        cache_key: Cache key to lookup

    Returns:
        Cached data if found and not expired, None otherwise
    """
    if cache_key in _CACHE:
        cached = _CACHE[cache_key]
        if datetime.now() < cached['expires']:
            return cached['data']
        else:
            # Expired, remove from cache
            del _CACHE[cache_key]
    return None


def _set_cached(cache_key: str, data: Any) -> None:
    """Store data in cache with TTL.

    Args:
        cache_key: Cache key
        data: Data to cache
    """
    _CACHE[cache_key] = {
        'data': data,
        'expires': datetime.now() + _CACHE_TTL
    }


def clear_cache() -> int:
    """Clear all cached leaderboard data.

    Returns:
        int: Number of cache entries cleared
    """
    count = len(_CACHE)
    _CACHE.clear()
    return count


# =====================================================
# League Helper Functions
# =====================================================

def get_top_level_leagues() -> List[League]:
    """Get all top-level leagues (league_level = 1).

    Returns:
        List of League objects for top-level leagues only
    """
    cache_key = _get_cache_key('top_level_leagues')
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    leagues = League.query.filter_by(league_level=1).order_by(League.name).all()
    _set_cached(cache_key, leagues)
    return leagues


def get_league_options() -> List[Dict[str, Any]]:
    """Get league filter options for UI dropdowns.

    Returns:
        List of dicts with league_id, name, abbr for all top-level leagues
        Includes "All Leagues" option at the beginning
    """
    cache_key = _get_cache_key('league_options')
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    leagues = get_top_level_leagues()
    options = [{'league_id': None, 'name': 'All Leagues', 'abbr': 'ALL'}]
    options.extend([
        {'league_id': l.league_id, 'name': l.name, 'abbr': l.abbr}
        for l in leagues
    ])

    _set_cached(cache_key, options)
    return options


# =====================================================
# Career Leaderboards
# =====================================================

def get_career_batting_leaders(
    stat: str = 'hr',
    league_id: Optional[int] = None,
    active_only: bool = False,
    limit: int = 100,
    offset: int = 0
) -> Dict[str, Any]:
    """Get career batting leaders for a specific stat.

    Args:
        stat: Stat to sort by (hr, avg, rbi, sb, war, etc.)
        league_id: Filter by league (None = all leagues)
        active_only: If True, only show active players
        limit: Number of results to return
        offset: Pagination offset

    Returns:
        Dict with 'leaders' (list of LeaderboardCareerBatting) and 'total' count
    """
    cache_key = _get_cache_key('career_batting', stat=stat, league_id=league_id,
                                active_only=active_only, limit=limit, offset=offset)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # Build query
    query = LeaderboardCareerBatting.query

    # Apply filters
    if active_only:
        query = query.filter_by(is_active=True)

    # League filtering: For career stats, we need to join to player's stats
    # Since career view doesn't have league_id, we filter on universe (top-level leagues only)
    # If a specific league is requested, we'll need to query the underlying stats table
    if league_id is not None:
        # Filter to players who have stats in this league
        # Subquery approach: Find player_ids with stats in target league
        from app.models import PlayerBattingStats
        subquery = db.session.query(PlayerBattingStats.player_id).filter(
            PlayerBattingStats.league_id == league_id,
            PlayerBattingStats.split_id == 1
        ).distinct().subquery()
        query = query.filter(LeaderboardCareerBatting.player_id.in_(subquery))

    # Determine sort order (descending for counting stats, ascending for rate stats)
    rate_stats = ['avg', 'obp', 'slg']
    sort_column = getattr(LeaderboardCareerBatting, stat, LeaderboardCareerBatting.hr)

    if stat in rate_stats:
        # Rate stats: higher is better, but need minimum PA threshold
        query = query.filter(LeaderboardCareerBatting.pa >= 1000)
        query = query.order_by(desc(sort_column))
    else:
        # Counting stats: higher is better
        query = query.order_by(desc(sort_column))

    # Get total count before pagination
    total = query.count()

    # Apply pagination
    leaders = query.limit(limit).offset(offset).all()

    result = {'leaders': leaders, 'total': total}
    _set_cached(cache_key, result)
    return result


def get_career_pitching_leaders(
    stat: str = 'w',
    league_id: Optional[int] = None,
    active_only: bool = False,
    limit: int = 100,
    offset: int = 0
) -> Dict[str, Any]:
    """Get career pitching leaders for a specific stat.

    Args:
        stat: Stat to sort by (w, sv, so, era, whip, war, etc.)
        league_id: Filter by league (None = all leagues)
        active_only: If True, only show active players
        limit: Number of results to return
        offset: Pagination offset

    Returns:
        Dict with 'leaders' (list of LeaderboardCareerPitching) and 'total' count
    """
    cache_key = _get_cache_key('career_pitching', stat=stat, league_id=league_id,
                                active_only=active_only, limit=limit, offset=offset)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # Build query
    query = LeaderboardCareerPitching.query

    # Apply filters
    if active_only:
        query = query.filter_by(is_active=True)

    # League filtering
    if league_id is not None:
        from app.models import PlayerPitchingStats
        subquery = db.session.query(PlayerPitchingStats.player_id).filter(
            PlayerPitchingStats.league_id == league_id,
            PlayerPitchingStats.split_id == 1
        ).distinct().subquery()
        query = query.filter(LeaderboardCareerPitching.player_id.in_(subquery))

    # Determine sort order (lower is better for ERA/WHIP, higher for others)
    lower_better = ['era', 'whip']
    rate_stats = ['era', 'whip', 'k_per_9', 'k_bb_ratio']
    sort_column = getattr(LeaderboardCareerPitching, stat, LeaderboardCareerPitching.w)

    if stat in rate_stats:
        # Rate stats: need minimum IP threshold
        query = query.filter(LeaderboardCareerPitching.ip >= 500)

        if stat in lower_better:
            query = query.order_by(asc(sort_column))
        else:
            query = query.order_by(desc(sort_column))
    else:
        # Counting stats: higher is better
        query = query.order_by(desc(sort_column))

    # Get total count
    total = query.count()

    # Apply pagination
    leaders = query.limit(limit).offset(offset).all()

    result = {'leaders': leaders, 'total': total}
    _set_cached(cache_key, result)
    return result


# =====================================================
# Single-Season Leaderboards
# =====================================================

def get_single_season_batting_leaders(
    stat: str = 'hr',
    league_id: Optional[int] = None,
    year: Optional[int] = None,
    active_only: bool = False,
    limit: int = 100,
    offset: int = 0
) -> Dict[str, Any]:
    """Get single-season batting records for a specific stat.

    Args:
        stat: Stat to sort by (hr, avg, rbi, sb, war, etc.)
        league_id: Filter by league (None = all leagues)
        year: Filter by year (None = all years)
        active_only: If True, only show active players
        limit: Number of results to return
        offset: Pagination offset

    Returns:
        Dict with 'leaders' (list of LeaderboardSingleSeasonBatting) and 'total' count
    """
    cache_key = _get_cache_key('single_season_batting', stat=stat, league_id=league_id,
                                year=year, active_only=active_only, limit=limit, offset=offset)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # Build query
    query = LeaderboardSingleSeasonBatting.query

    # CRITICAL: Exclude team_id = 0 (college/HS players whose stats don't count)
    query = query.filter(LeaderboardSingleSeasonBatting.team_id != 0)

    # Apply filters
    if league_id is not None:
        query = query.filter_by(league_id=league_id)

    if year is not None:
        query = query.filter_by(year=year)

    if active_only:
        query = query.filter_by(is_active=True)

    # Determine sort order
    rate_stats = ['avg', 'obp', 'slg']
    sort_column = getattr(LeaderboardSingleSeasonBatting, stat, LeaderboardSingleSeasonBatting.hr)

    # All rate stats: higher is better
    query = query.order_by(desc(sort_column))

    # Get total count
    total = query.count()

    # Apply pagination
    leaders = query.limit(limit).offset(offset).all()

    result = {'leaders': leaders, 'total': total}
    _set_cached(cache_key, result)
    return result


def get_single_season_pitching_leaders(
    stat: str = 'w',
    league_id: Optional[int] = None,
    year: Optional[int] = None,
    active_only: bool = False,
    limit: int = 100,
    offset: int = 0
) -> Dict[str, Any]:
    """Get single-season pitching records for a specific stat.

    Args:
        stat: Stat to sort by (w, sv, so, era, whip, war, etc.)
        league_id: Filter by league (None = all leagues)
        year: Filter by year (None = all years)
        active_only: If True, only show active players
        limit: Number of results to return
        offset: Pagination offset

    Returns:
        Dict with 'leaders' (list of LeaderboardSingleSeasonPitching) and 'total' count
    """
    cache_key = _get_cache_key('single_season_pitching', stat=stat, league_id=league_id,
                                year=year, active_only=active_only, limit=limit, offset=offset)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # Build query
    query = LeaderboardSingleSeasonPitching.query

    # CRITICAL: Exclude team_id = 0 (college/HS players whose stats don't count)
    query = query.filter(LeaderboardSingleSeasonPitching.team_id != 0)

    # Apply filters
    if league_id is not None:
        query = query.filter_by(league_id=league_id)

    if year is not None:
        query = query.filter_by(year=year)

    if active_only:
        query = query.filter_by(is_active=True)

    # Determine sort order
    lower_better = ['era', 'whip']
    sort_column = getattr(LeaderboardSingleSeasonPitching, stat, LeaderboardSingleSeasonPitching.w)

    if stat in lower_better:
        query = query.order_by(asc(sort_column))
    else:
        query = query.order_by(desc(sort_column))

    # Get total count
    total = query.count()

    # Apply pagination
    leaders = query.limit(limit).offset(offset).all()

    result = {'leaders': leaders, 'total': total}
    _set_cached(cache_key, result)
    return result


# =====================================================
# Yearly League Leaders
# =====================================================

def get_yearly_batting_leaders(
    stat: str,
    year: int,
    league_id: Optional[int] = None,
    limit: int = 10
) -> Dict[str, Any]:
    """Get top batting leaders for a specific year/league/stat.

    The yearly views already contain top 10 per year/league/stat, so we just
    need to filter and return the pre-ranked results.

    Args:
        stat: Stat to return leaders for (hr, rbi, sb, h, avg, war)
        year: Season year
        league_id: League to filter by (None = all leagues)
        limit: Number of results (default 10, max from view)

    Returns:
        Dict with 'leaders' (list of LeaderboardYearlyBatting) and 'total' count
    """
    cache_key = _get_cache_key('yearly_batting', stat=stat, year=year,
                                league_id=league_id, limit=limit)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # Map stat name to rank column and stat column
    rank_columns = {
        'hr': LeaderboardYearlyBatting.hr_rank,
        'rbi': LeaderboardYearlyBatting.rbi_rank,
        'sb': LeaderboardYearlyBatting.sb_rank,
        'h': LeaderboardYearlyBatting.h_rank,
        'avg': LeaderboardYearlyBatting.avg_rank,
        'war': LeaderboardYearlyBatting.war_rank,
    }

    stat_columns = {
        'hr': LeaderboardYearlyBatting.hr,
        'rbi': LeaderboardYearlyBatting.rbi,
        'sb': LeaderboardYearlyBatting.sb,
        'h': LeaderboardYearlyBatting.h,
        'avg': LeaderboardYearlyBatting.avg,
        'war': LeaderboardYearlyBatting.war,
    }

    rank_column = rank_columns.get(stat, LeaderboardYearlyBatting.hr_rank)
    stat_column = stat_columns.get(stat, LeaderboardYearlyBatting.hr)

    query = LeaderboardYearlyBatting.query.filter(
        LeaderboardYearlyBatting.year == year,
        rank_column <= limit,
        stat_column.isnot(None)  # Filter out NULL stat values
    )

    if league_id is not None:
        query = query.filter_by(league_id=league_id)
        # For single league, order by rank
        query = query.order_by(rank_column)
    else:
        # For all leagues, order by actual stat value (desc for most stats, asc for ERA/WHIP)
        # Determine sort direction based on stat
        if stat in ['era', 'whip']:
            query = query.order_by(stat_column.asc())
        else:
            query = query.order_by(stat_column.desc())

    leaders = query.limit(limit).all()
    result = {'leaders': leaders, 'total': len(leaders)}
    _set_cached(cache_key, result)
    return result


def get_yearly_pitching_leaders(
    stat: str,
    year: int,
    league_id: Optional[int] = None,
    limit: int = 10
) -> Dict[str, Any]:
    """Get top pitching leaders for a specific year/league/stat.

    Args:
        stat: Stat to return leaders for (w, sv, so, era, whip, war)
        year: Season year
        league_id: League to filter by (None = all leagues)
        limit: Number of results (default 10, max from view)

    Returns:
        Dict with 'leaders' (list of LeaderboardYearlyPitching) and 'total' count
    """
    cache_key = _get_cache_key('yearly_pitching', stat=stat, year=year,
                                league_id=league_id, limit=limit)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # Map stat name to rank column and stat column
    rank_columns = {
        'w': LeaderboardYearlyPitching.w_rank,
        'sv': LeaderboardYearlyPitching.sv_rank,
        'so': LeaderboardYearlyPitching.so_rank,
        'era': LeaderboardYearlyPitching.era_rank,
        'whip': LeaderboardYearlyPitching.whip_rank,
        'war': LeaderboardYearlyPitching.war_rank,
    }

    stat_columns = {
        'w': LeaderboardYearlyPitching.w,
        'sv': LeaderboardYearlyPitching.sv,
        'so': LeaderboardYearlyPitching.so,
        'era': LeaderboardYearlyPitching.era,
        'whip': LeaderboardYearlyPitching.whip,
        'war': LeaderboardYearlyPitching.war,
    }

    rank_column = rank_columns.get(stat, LeaderboardYearlyPitching.w_rank)
    stat_column = stat_columns.get(stat, LeaderboardYearlyPitching.w)

    query = LeaderboardYearlyPitching.query.filter(
        LeaderboardYearlyPitching.year == year,
        rank_column <= limit,
        stat_column.isnot(None)  # Filter out NULL stat values
    )

    if league_id is not None:
        query = query.filter_by(league_id=league_id)
        # For single league, order by rank
        query = query.order_by(rank_column)
    else:
        # For all leagues, order by actual stat value (desc for most stats, asc for ERA/WHIP)
        # Determine sort direction based on stat
        if stat in ['era', 'whip']:
            query = query.order_by(stat_column.asc())
        else:
            query = query.order_by(stat_column.desc())

    leaders = query.limit(limit).all()
    result = {'leaders': leaders, 'total': len(leaders)}
    _set_cached(cache_key, result)
    return result


# =====================================================
# Utility Functions
# =====================================================

def get_available_years() -> List[int]:
    """Get list of years with data in leaderboards.

    Returns:
        List of years (integers) in descending order
    """
    cache_key = _get_cache_key('available_years')
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # Query distinct years from single-season batting view
    years = db.session.query(
        LeaderboardSingleSeasonBatting.year
    ).distinct().order_by(desc(LeaderboardSingleSeasonBatting.year)).all()

    year_list = [y[0] for y in years]
    _set_cached(cache_key, year_list)
    return year_list


def get_stat_metadata() -> Dict[str, Dict[str, Any]]:
    """Get metadata about all available stats for leaderboards.

    Returns:
        Dict mapping stat abbreviation to metadata (name, description, format, etc.)
    """
    return {
        # Batting stats
        'hr': {
            'name': 'Home Runs',
            'category': 'batting',
            'type': 'counting',
            'format': 'integer',
            'sort': 'desc',
            'min_pa': 0
        },
        'avg': {
            'name': 'Batting Average',
            'category': 'batting',
            'type': 'rate',
            'format': 'avg',
            'sort': 'desc',
            'min_pa': 300
        },
        'rbi': {
            'name': 'RBI',
            'category': 'batting',
            'type': 'counting',
            'format': 'integer',
            'sort': 'desc',
            'min_pa': 0
        },
        'sb': {
            'name': 'Stolen Bases',
            'category': 'batting',
            'type': 'counting',
            'format': 'integer',
            'sort': 'desc',
            'min_pa': 0
        },
        'obp': {
            'name': 'On-Base Percentage',
            'category': 'batting',
            'type': 'rate',
            'format': 'avg',
            'sort': 'desc',
            'min_pa': 300
        },
        'slg': {
            'name': 'Slugging Percentage',
            'category': 'batting',
            'type': 'rate',
            'format': 'avg',
            'sort': 'desc',
            'min_pa': 300
        },
        'h': {
            'name': 'Hits',
            'category': 'batting',
            'type': 'counting',
            'format': 'integer',
            'sort': 'desc',
            'min_pa': 0
        },

        # Pitching stats
        'w': {
            'name': 'Wins',
            'category': 'pitching',
            'type': 'counting',
            'format': 'integer',
            'sort': 'desc',
            'min_ip': 0
        },
        'sv': {
            'name': 'Saves',
            'category': 'pitching',
            'type': 'counting',
            'format': 'integer',
            'sort': 'desc',
            'min_ip': 0
        },
        'so': {
            'name': 'Strikeouts',
            'category': 'pitching',
            'type': 'counting',
            'format': 'integer',
            'sort': 'desc',
            'min_ip': 0
        },
        'era': {
            'name': 'ERA',
            'category': 'pitching',
            'type': 'rate',
            'format': 'era',
            'sort': 'asc',
            'min_ip': 100
        },
        'whip': {
            'name': 'WHIP',
            'category': 'pitching',
            'type': 'rate',
            'format': 'whip',
            'sort': 'asc',
            'min_ip': 100
        },
        'k_per_9': {
            'name': 'K/9',
            'category': 'pitching',
            'type': 'rate',
            'format': 'rate',
            'sort': 'desc',
            'min_ip': 100
        },

        # Universal
        'war': {
            'name': 'WAR',
            'category': 'both',
            'type': 'advanced',
            'format': 'decimal1',
            'sort': 'desc',
            'min_pa': 0,
            'min_ip': 0
        },
    }


def get_year_by_year_leaders(stat: str, category: str, league_id: Optional[int] = None, limit: int = 10) -> Dict[str, Any]:
    """Get top N leaders for each year for a specific stat.

    Args:
        stat: Stat abbreviation (hr, avg, w, era, etc.)
        category: 'batting' or 'pitching'
        league_id: Optional league filter (None = all leagues)
        limit: Number of leaders per year (default 10)

    Returns:
        Dict with years as keys, each containing list of top N leaders
        {
            1980: [{'player_name': 'John Doe', 'value': 50, 'team_abbr': 'NYY', ...}, ...],
            1981: [...],
            ...
        }
    """
    cache_key = _get_cache_key('year_by_year', stat=stat, category=category, league_id=league_id, limit=limit)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # Get stat metadata
    stat_meta = get_stat_metadata().get(stat, {})
    sort_order = desc if stat_meta.get('sort') == 'desc' else asc

    # Choose the right model and column
    if category == 'batting':
        model = LeaderboardSingleSeasonBatting
    else:
        model = LeaderboardSingleSeasonPitching

    # Get the stat column
    stat_column = getattr(model, stat, None)
    if stat_column is None:
        return {}

    # Base query
    query = model.query.filter(model.team_id != 0)

    # Apply league filter if specified
    if league_id:
        query = query.filter(model.league_id == league_id)

    # Get all available years
    years_query = db.session.query(model.year).distinct().order_by(model.year.desc())
    if league_id:
        years_query = years_query.filter(model.league_id == league_id)

    available_years = [row[0] for row in years_query.all()]

    # For each year, get top N leaders
    results = {}
    for year in available_years:
        year_query = query.filter(model.year == year).filter(stat_column.isnot(None))
        year_leaders = year_query.order_by(sort_order(stat_column)).limit(limit).all()

        results[year] = [
            {
                'player_id': leader.player_id,
                'player_name': f"{leader.first_name} {leader.last_name}",
                'team_id': leader.team_id,
                'team_abbr': leader.team_abbr,
                'year': leader.year,
                'value': getattr(leader, stat),
                'is_active': leader.is_active
            }
            for leader in year_leaders
        ]

    _set_cached(cache_key, results)
    return results