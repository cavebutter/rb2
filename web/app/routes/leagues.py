"""League and year routes."""
from flask import Blueprint, render_template, abort
from app.models import League
from app.services import league_service, leaderboard_service

bp = Blueprint('leagues', __name__)


@bp.route('/<int:league_id>')
def league_home(league_id):
    """League home page showing current standings, leaders, and team stats.

    URL: /leagues/<league_id>
    """
    # Get league info
    league = League.query.get_or_404(league_id)

    # Get current standings for this league
    standings_data = league_service.get_league_standings(league_id)

    # Get team aggregate stats
    team_stats = league_service.get_league_team_stats(league_id)

    # Get top 10 batting leaders for this league
    batting_leaders = {}
    batting_stats = ['hr', 'avg', 'rbi', 'sb', 'h']
    for stat in batting_stats:
        leaders = leaderboard_service.get_yearly_batting_leaders(
            stat=stat,
            year=1997,  # Current year
            league_id=league_id,
            limit=10
        )
        batting_leaders[stat] = leaders['leaders']

    # Get top 10 pitching leaders for this league
    pitching_leaders = {}
    pitching_stats = ['w', 'sv', 'so', 'era', 'whip']
    for stat in pitching_stats:
        leaders = leaderboard_service.get_yearly_pitching_leaders(
            stat=stat,
            year=1997,  # Current year
            league_id=league_id,
            limit=10
        )
        pitching_leaders[stat] = leaders['leaders']

    # Get stat metadata
    stat_metadata = leaderboard_service.get_stat_metadata()

    return render_template(
        'leagues/home.html',
        league=league,
        standings_data=standings_data,
        team_batting_stats=team_stats['batting_stats'],
        team_pitching_stats=team_stats['pitching_stats'],
        batting_leaders=batting_leaders,
        pitching_leaders=pitching_leaders,
        batting_stats=batting_stats,
        pitching_stats=pitching_stats,
        stat_metadata=stat_metadata
    )


@bp.route('/years/<int:year>')
def year_summary(year):
    """Year summary page showing results, leaders, and awards for a season.

    URL: /years/<year>
    """
    # Get available years
    available_years = league_service.get_available_years()

    # Validate year
    if year not in available_years:
        abort(404)

    # Get standings for all leagues for this year
    year_data = league_service.get_year_standings(year)

    # Get top 10 batting leaders across all leagues
    batting_leaders = {}
    batting_stats = ['hr', 'avg', 'rbi', 'sb', 'h', 'war']
    for stat in batting_stats:
        if year_data['is_current_year']:
            leaders = leaderboard_service.get_yearly_batting_leaders(
                stat=stat,
                year=year,
                league_id=None,
                limit=10
            )
        else:
            # For historical years, use single-season leaders
            leaders = leaderboard_service.get_single_season_batting_leaders(
                stat=stat,
                year=year,
                league_id=None,
                limit=10
            )
        batting_leaders[stat] = leaders['leaders']

    # Get top 10 pitching leaders across all leagues
    pitching_leaders = {}
    pitching_stats = ['w', 'sv', 'so', 'era', 'whip', 'war']
    for stat in pitching_stats:
        if year_data['is_current_year']:
            leaders = leaderboard_service.get_yearly_pitching_leaders(
                stat=stat,
                year=year,
                league_id=None,
                limit=10
            )
        else:
            # For historical years, use single-season leaders
            leaders = leaderboard_service.get_single_season_pitching_leaders(
                stat=stat,
                year=year,
                league_id=None,
                limit=10
            )
        pitching_leaders[stat] = leaders['leaders']

    # Get stat metadata
    stat_metadata = leaderboard_service.get_stat_metadata()

    # Find previous and next years
    try:
        year_index = available_years.index(year)
        prev_year = available_years[year_index + 1] if year_index < len(available_years) - 1 else None
        next_year = available_years[year_index - 1] if year_index > 0 else None
    except ValueError:
        prev_year = None
        next_year = None

    return render_template(
        'leagues/year.html',
        year=year,
        is_current_year=year_data['is_current_year'],
        leagues_data=year_data['leagues_data'],
        batting_leaders=batting_leaders,
        pitching_leaders=pitching_leaders,
        batting_stats=batting_stats,
        pitching_stats=pitching_stats,
        stat_metadata=stat_metadata,
        prev_year=prev_year,
        next_year=next_year,
        available_years=available_years
    )
