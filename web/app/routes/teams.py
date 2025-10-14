"""Team routes"""
from flask import Blueprint, render_template, abort, send_file
from app.models import Team, TeamRecord, Player, PlayerCurrentStatus, League, Park
from app.extensions import db
import os
from app.services.team_service import (
    get_team_year_data,
    get_available_years_for_team,
    get_team_player_batting_stats,
    get_team_player_pitching_stats,
    get_team_top_players_by_war,
    get_franchise_history,
    get_franchise_top_players,
    get_franchise_year_by_year
)
from sqlalchemy.orm import joinedload

bp = Blueprint('teams', __name__)


@bp.route('/')
def teams_list():
    """List all teams"""
    # Get all MLB teams (level=1) with their records
    teams = (Team.query
            .filter_by(level=1)
            .join(TeamRecord, isouter=True)
            .order_by(Team.name)
            .all())

    return render_template('teams/list.html', teams=teams)


@bp.route('/<int:team_id>')
def team_detail(team_id):
    """Team detail page - roster, stats, schedule

    OPTIMIZATION: Use explicit query control to avoid N+1 issues
    with Player relationships. Only load what we need for display.
    """
    import time
    from sqlalchemy.orm import load_only, raiseload, joinedload, selectinload

    start_time = time.time()
    print(f"\n=== TEAM DETAIL PAGE PROFILING (team_id={team_id}) ===")

    # Get team with controlled relationship loading
    # OPTIMIZATION: Use selectinload() instead of default joinedload() to fetch
    # league, park, record, and city in separate queries (faster than huge JOIN)
    t1 = time.time()
    from app.models import City

    team = (Team.query
            .options(
                load_only(
                    Team.team_id,
                    Team.name,
                    Team.nickname,
                    Team.abbr,
                    Team.logo_file_name
                ),
                # Load city but block its nested relationships
                selectinload(Team.city).options(
                    load_only(City.city_id, City.name),
                    raiseload('*')  # Block City's nation, state, continent relationships
                ),
                # Load league but block its nested relationships
                selectinload(Team.league).options(
                    load_only(League.league_id, League.name),
                    raiseload('*')  # Block League's nation, continent relationships
                ),
                # Load park but block its nested relationships
                selectinload(Team.park).options(
                    load_only(Park.park_id, Park.name),
                    raiseload('*')  # Block Park's nation, continent relationships
                ),
                selectinload(Team.record).raiseload('*'),  # Load all columns but block back-reference to Team
                raiseload(Team.nation),     # Block nation loading
                raiseload(Team.affiliates)  # Block affiliates loading
            )
            .filter_by(team_id=team_id)
            .first_or_404())
    print(f"1. Get team: {(time.time() - t1)*1000:.1f}ms")

    # Get coaching staff for this team
    t1 = time.time()
    from app.models import Coach
    coaches = Coach.query.filter(Coach.team_id == team_id).all()
    print(f"2. Get coaches: {(time.time() - t1)*1000:.1f}ms")

    # Sort by occupation order: Owner, GM, Manager, Bench Coach, Pitching Coach, Hitting Coach, Base Coach, Scout, Trainer
    t1 = time.time()
    coaches = sorted(coaches, key=lambda c: c.occupation_sort_order)
    print(f"3. Sort coaches: {(time.time() - t1)*1000:.1f}ms")

    # Get franchise history data (US-T003)
    t1 = time.time()
    franchise_history = get_franchise_history(team_id)
    print(f"4. Get franchise history: {(time.time() - t1)*1000:.1f}ms")

    t1 = time.time()
    franchise_top_players = get_franchise_top_players(team_id, limit=24)
    print(f"5. Get franchise top players: {(time.time() - t1)*1000:.1f}ms")

    t1 = time.time()
    franchise_year_by_year = get_franchise_year_by_year(team_id)
    print(f"6. Get franchise year by year: {(time.time() - t1)*1000:.1f}ms")

    print(f"TOTAL ROUTE TIME: {(time.time() - start_time)*1000:.1f}ms")
    print("=== END PROFILING ===\n")

    # Current game year for linking to current season
    CURRENT_YEAR = 1997

    return render_template('teams/detail.html',
                          team=team,
                          coaches=coaches,
                          franchise_history=franchise_history,
                          franchise_top_players=franchise_top_players,
                          franchise_year_by_year=franchise_year_by_year,
                          current_year=CURRENT_YEAR)


@bp.route('/<int:team_id>/<int:year>')
def team_year(team_id, year):
    """Team year page - historical roster and stats for a specific season"""
    # Get team year data (handles both current and historical years)
    data = get_team_year_data(team_id, year)

    # Get available years for previous/next navigation
    available_years = get_available_years_for_team(team_id)

    # Find previous and next years
    try:
        year_index = available_years.index(year)
        prev_year = available_years[year_index + 1] if year_index < len(available_years) - 1 else None
        next_year = available_years[year_index - 1] if year_index > 0 else None
    except ValueError:
        # Year not in list
        prev_year = None
        next_year = None

    # Get individual player stats for this team-year
    player_batting_stats = get_team_player_batting_stats(team_id, year)
    player_pitching_stats = get_team_player_pitching_stats(team_id, year)

    # Get top players by WAR (for image grid)
    top_players = get_team_top_players_by_war(team_id, year, limit=12)

    return render_template('teams/year.html',
                          team=data['team'],
                          year=data['year'],
                          record=data['record'],
                          batting_stats=data['batting_stats'],
                          pitching_stats=data['pitching_stats'],
                          is_current_year=data['is_current_year'],
                          player_batting_stats=player_batting_stats,
                          player_pitching_stats=player_pitching_stats,
                          top_players=top_players,
                          prev_year=prev_year,
                          next_year=next_year)


@bp.route('/logo/<int:team_id>')
def team_logo(team_id):
    """Serve team logo from ETL data directory.

    Returns the team logo PNG file or 404 if not found.
    The filename is retrieved from the teams table logo_file_name column.
    """
    # Get team to fetch logo filename
    team = Team.query.get_or_404(team_id)

    if not team.logo_file_name:
        abort(404)

    # Path to team logos
    # From web/app/routes -> up 3 levels to rb2/ -> etl/data/images/team_logos
    logo_dir = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        '../../../etl/data/images/team_logos'
    ))

    logo_path = os.path.join(logo_dir, team.logo_file_name)

    if os.path.exists(logo_path):
        return send_file(logo_path, mimetype='image/png')
    else:
        abort(404)