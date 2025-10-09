"""Search API routes"""
from flask import Blueprint, jsonify

bp = Blueprint('search', __name__)


@bp.route('/players')
def search_players():
    """Search players API"""
    return jsonify({'results': [], 'message': 'Coming soon'})
