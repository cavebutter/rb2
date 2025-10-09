"""Leaderboards routes"""
from flask import Blueprint

bp = Blueprint('leaderboards', __name__)


@bp.route('/batting')
def batting():
    """Batting leaderboards"""
    return "<h1>Batting Leaderboards</h1><p>Coming soon...</p>"


@bp.route('/pitching')
def pitching():
    """Pitching leaderboards"""
    return "<h1>Pitching Leaderboards</h1><p>Coming soon...</p>"
