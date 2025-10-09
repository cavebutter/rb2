"""Team routes"""
from flask import Blueprint, render_template, abort
from app.models import Team, TeamRecord, Player, PlayerCurrentStatus
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
    """Team detail page - roster, stats, schedule"""
    # Get team with all relationships
    team = Team.query.get_or_404(team_id)

    # Get active roster
    roster = (Player.query
             .join(PlayerCurrentStatus)
             .filter(PlayerCurrentStatus.team_id == team_id)
             .filter(PlayerCurrentStatus.retired == 0)
             .order_by(PlayerCurrentStatus.position)
             .all())

    return render_template('teams/detail.html',
                          team=team,
                          roster=roster)