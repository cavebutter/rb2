"""Routes for coach pages"""
from flask import Blueprint, render_template, abort, send_file
from app.models import Coach, Player, Team, City, State, Nation
from sqlalchemy import func
from sqlalchemy.orm import joinedload, selectinload, load_only, raiseload
import os

coaches_bp = Blueprint('coaches', __name__, url_prefix='/coaches')


@coaches_bp.route('/')
def coach_list():
    """Display list of all coaches grouped by occupation"""

    # Query all coaches on teams, ordered by occupation and team
    coaches = Coach.query.filter(Coach.team_id > 0).order_by(
        Coach.occupation,
        Coach.team_id,
        Coach.last_name
    ).all()

    # Group coaches by occupation
    coaches_by_occupation = {}
    for coach in coaches:
        occupation = coach.occupation_display
        if occupation not in coaches_by_occupation:
            coaches_by_occupation[occupation] = []
        coaches_by_occupation[occupation].append(coach)

    # Get counts for display
    total_coaches = Coach.query.count()
    on_teams = Coach.query.filter(Coach.team_id > 0).count()

    return render_template(
        'coaches/list.html',
        coaches_by_occupation=coaches_by_occupation,
        total_coaches=total_coaches,
        on_teams=on_teams
    )


@coaches_bp.route('/<int:coach_id>')
def coach_detail(coach_id):
    """Display detailed information for a specific coach"""

    # Get coach or 404, eager load relationships with state for birthplace_display
    coach = Coach.query.options(
        selectinload(Coach.nation).load_only(
            Nation.nation_id,
            Nation.name,
            Nation.abbreviation
        ).raiseload('*'),
        selectinload(Coach.city_of_birth).load_only(
            City.city_id,
            City.name,
            City.state_id,
            City.nation_id
        ).selectinload(City.state).load_only(
            State.state_id,
            State.nation_id,
            State.abbreviation
        ).selectinload(State.nation).load_only(
            Nation.nation_id,
            Nation.abbreviation
        ).raiseload('*')
    ).get_or_404(coach_id)

    # Get former player data if applicable
    former_player = None
    if coach.former_player_id:
        former_player = Player.query.get(coach.former_player_id)

    # Get current team if applicable
    current_team = None
    if coach.team_id and coach.team_id > 0:
        current_team = Team.query.get(coach.team_id)

    return render_template(
        'coaches/detail.html',
        coach=coach,
        former_player=former_player,
        current_team=current_team
    )


@coaches_bp.route('/image/<int:coach_id>')
def coach_image(coach_id):
    """Serve coach image from ETL data directory.

    Coach images are stored in the same directory as player images,
    with the naming pattern: coach_{coach_id}.png

    Returns image or 404 if not found (client-side fallback handles it).
    """
    # Path to images (coaches are in the players directory)
    # From web/app/routes -> up 3 levels to rb2/ -> etl/data/images/players
    image_dir = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        '../../../etl/data/images/players'
    ))
    image_path = os.path.join(image_dir, f'coach_{coach_id}.png')

    if os.path.exists(image_path):
        return send_file(image_path, mimetype='image/png')
    else:
        abort(404)
