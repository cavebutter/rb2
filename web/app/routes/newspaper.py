"""Newspaper routes"""
from flask import Blueprint

bp = Blueprint('newspaper', __name__)


@bp.route('/')
def index():
    """Newspaper home"""
    return "<h1>Newspaper</h1><p>Coming soon...</p>"
