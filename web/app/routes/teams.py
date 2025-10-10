"""Team routes"""
from flask import Blueprint, render_template, abort
from app.models import Team, TeamRecord, Player, PlayerCurrentStatus
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
    from sqlalchemy.orm import load_only, raiseload, joinedload

    # Get team with controlled relationship loading
    team = Team.query.get_or_404(team_id)

    # Get active roster with optimization
    # Only load essential Player columns and specific relationships needed for display
    roster = (Player.query
             .options(
                 load_only(
                     Player.player_id,
                     Player.first_name,
                     Player.last_name,
                     Player.bats,
                     Player.throws,
                     Player.date_of_birth  # Needed for age calculation
                 ),
                 joinedload(Player.city_of_birth),  # Needed for display
                 joinedload(Player.nation),  # Needed for display
                 joinedload(Player.current_status),  # Needed for position
                 raiseload(Player.batting_stats),
                 raiseload(Player.pitching_stats),
                 raiseload(Player.batting_ratings),
                 raiseload(Player.pitching_ratings),
                 raiseload(Player.fielding_ratings)
             )
             .join(PlayerCurrentStatus)
             .filter(PlayerCurrentStatus.team_id == team_id)
             .filter(PlayerCurrentStatus.retired == 0)
             .order_by(PlayerCurrentStatus.position)
             .all())

    # Get coaching staff for this team
    from app.models import Coach
    coaches = Coach.query.filter(Coach.team_id == team_id).all()

    # Sort by occupation order: Owner, GM, Manager, Bench Coach, Pitching Coach, Hitting Coach, Base Coach, Scout, Trainer
    coaches = sorted(coaches, key=lambda c: c.occupation_sort_order)

    # Get franchise history data (US-T003)
    franchise_history = get_franchise_history(team_id)
    franchise_top_players = get_franchise_top_players(team_id, limit=24)
    franchise_year_by_year = get_franchise_year_by_year(team_id)

    return render_template('teams/detail.html',
                          team=team,
                          roster=roster,
                          coaches=coaches,
                          franchise_history=franchise_history,
                          franchise_top_players=franchise_top_players,
                          franchise_year_by_year=franchise_year_by_year)


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