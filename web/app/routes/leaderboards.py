"""Leaderboards routes"""
from flask import Blueprint, render_template, request
from app.services import leaderboard_service

bp = Blueprint('leaderboards', __name__)


@bp.route('/')
@bp.route('/home')
def home():
    """Leaderboard home page with current year leaders and all-time records."""
    # Get current year (most recent year with data)
    years = leaderboard_service.get_available_years()
    current_year = max(years) if years else None

    # Get league options for filters
    leagues = leaderboard_service.get_top_level_leagues()

    # Get current year leaders for each major stat (top 1 across all leagues)
    current_leaders = {}
    if current_year:
        # Batting stats to show
        batting_stats = ['hr', 'avg', 'rbi', 'sb', 'h']
        for stat in batting_stats:
            leaders = leaderboard_service.get_single_season_batting_leaders(
                stat=stat, year=current_year, limit=1
            )
            if leaders['leaders']:
                current_leaders[stat] = leaders['leaders'][0]

        # Pitching stats to show
        pitching_stats = ['w', 'sv', 'so', 'era', 'whip']
        for stat in pitching_stats:
            leaders = leaderboard_service.get_single_season_pitching_leaders(
                stat=stat, year=current_year, limit=1
            )
            if leaders['leaders']:
                current_leaders[stat] = leaders['leaders'][0]

    # Get stat metadata for display names
    stat_metadata = leaderboard_service.get_stat_metadata()

    return render_template(
        'leaderboards/home.html',
        current_year=current_year,
        leagues=leagues,
        current_leaders=current_leaders,
        stat_metadata=stat_metadata
    )


@bp.route('/batting')
def batting():
    """Batting leaderboards with filtering.

    Query params:
        type: 'career', 'single-season', 'active', 'yearly' (default: 'career')
        stat: stat code like 'hr', 'avg', 'rbi' (default: 'hr')
        league: league_id for filtering (default: None = All Leagues)
        year: year filter for single-season (default: None = all-time)
    """
    # Get query parameters
    leaderboard_type = request.args.get('type', 'career')
    stat = request.args.get('stat', 'hr')
    league_id = request.args.get('league', type=int)
    year = request.args.get('year', type=int)

    # Get stat metadata
    stat_metadata = leaderboard_service.get_stat_metadata()

    # Validate stat is a batting stat
    if stat not in stat_metadata or stat_metadata[stat]['category'] not in ('batting', 'both'):
        stat = 'hr'  # Default to HR if invalid

    # Get league options for filter dropdown
    league_options = leaderboard_service.get_league_options()

    # Determine which service function to call based on type
    if leaderboard_type == 'career':
        leaders_data = leaderboard_service.get_career_batting_leaders(
            stat=stat,
            league_id=league_id,
            active_only=False,
            limit=100
        )
    elif leaderboard_type == 'active':
        leaders_data = leaderboard_service.get_career_batting_leaders(
            stat=stat,
            league_id=league_id,
            active_only=True,
            limit=100
        )
    elif leaderboard_type == 'single-season':
        leaders_data = leaderboard_service.get_single_season_batting_leaders(
            stat=stat,
            league_id=league_id,
            year=year,
            limit=100
        )
    elif leaderboard_type == 'yearly':
        # For yearly, we need year and league
        if not year:
            years = leaderboard_service.get_available_years()
            year = max(years) if years else None
        leaders_data = leaderboard_service.get_yearly_batting_leaders(
            stat=stat,
            year=year,
            league_id=league_id
        )
    else:
        # Default to career
        leaderboard_type = 'career'
        leaders_data = leaderboard_service.get_career_batting_leaders(
            stat=stat,
            league_id=league_id,
            active_only=False,
            limit=100
        )

    # Get available years for year filter
    available_years = leaderboard_service.get_available_years()

    return render_template(
        'leaderboards/leaderboard.html',
        category='batting',
        leaderboard_type=leaderboard_type,
        stat=stat,
        stat_metadata=stat_metadata,
        leaders=leaders_data['leaders'],
        league_options=league_options,
        selected_league=league_id,
        selected_year=year,
        available_years=available_years
    )


@bp.route('/pitching')
def pitching():
    """Pitching leaderboards with filtering.

    Query params:
        type: 'career', 'single-season', 'active', 'yearly' (default: 'career')
        stat: stat code like 'w', 'era', 'so' (default: 'w')
        league: league_id for filtering (default: None = All Leagues)
        year: year filter for single-season (default: None = all-time)
    """
    # Get query parameters
    leaderboard_type = request.args.get('type', 'career')
    stat = request.args.get('stat', 'w')
    league_id = request.args.get('league', type=int)
    year = request.args.get('year', type=int)

    # Get stat metadata
    stat_metadata = leaderboard_service.get_stat_metadata()

    # Validate stat is a pitching stat
    if stat not in stat_metadata or stat_metadata[stat]['category'] not in ('pitching', 'both'):
        stat = 'w'  # Default to Wins if invalid

    # Get league options for filter dropdown
    league_options = leaderboard_service.get_league_options()

    # Determine which service function to call based on type
    if leaderboard_type == 'career':
        leaders_data = leaderboard_service.get_career_pitching_leaders(
            stat=stat,
            league_id=league_id,
            active_only=False,
            limit=100
        )
    elif leaderboard_type == 'active':
        leaders_data = leaderboard_service.get_career_pitching_leaders(
            stat=stat,
            league_id=league_id,
            active_only=True,
            limit=100
        )
    elif leaderboard_type == 'single-season':
        leaders_data = leaderboard_service.get_single_season_pitching_leaders(
            stat=stat,
            league_id=league_id,
            year=year,
            limit=100
        )
    elif leaderboard_type == 'yearly':
        # For yearly, we need year and league
        if not year:
            years = leaderboard_service.get_available_years()
            year = max(years) if years else None
        leaders_data = leaderboard_service.get_yearly_pitching_leaders(
            stat=stat,
            year=year,
            league_id=league_id
        )
    else:
        # Default to career
        leaderboard_type = 'career'
        leaders_data = leaderboard_service.get_career_pitching_leaders(
            stat=stat,
            league_id=league_id,
            active_only=False,
            limit=100
        )

    # Get available years for year filter
    available_years = leaderboard_service.get_available_years()

    return render_template(
        'leaderboards/leaderboard.html',
        category='pitching',
        leaderboard_type=leaderboard_type,
        stat=stat,
        stat_metadata=stat_metadata,
        leaders=leaders_data['leaders'],
        league_options=league_options,
        selected_league=league_id,
        selected_year=year,
        available_years=available_years
    )
