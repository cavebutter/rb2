"""Main routes (Home page, etc)"""
from flask import Blueprint, render_template

bp = Blueprint('main', __name__)

@bp.route('/')
def index():
    """Home page"""
    return "<h1>RB2 Web App</h1><p>Coming soon...</p>"

@bp.route('/health')
def health():
    """Health Check endpoint"""
    return {'status': 'OK'}, 200