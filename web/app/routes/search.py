"""Search routes for global search functionality"""
from flask import Blueprint, jsonify, request, render_template
from app.services import search_service

bp = Blueprint('search', __name__, url_prefix='/search')


@bp.route('/')
def search_results():
    """Full search results page.

    Query parameter: q (search query)
    """
    query = request.args.get('q', '').strip()

    if not query or len(query) < 2:
        return render_template('search/results.html',
                             query=query,
                             results=None,
                             message='Please enter at least 2 characters to search.')

    # Get all results
    results = search_service.search_all(query, limit_per_type=50)

    return render_template('search/results.html',
                          query=query,
                          results=results,
                          message=None if results['total_results'] > 0 else 'No results found.')


@bp.route('/autocomplete')
def autocomplete():
    """AJAX autocomplete endpoint.

    Query parameter: q (search query)
    Returns: JSON with players and teams (limited to 5 each)
    """
    query = request.args.get('q', '').strip()

    if not query or len(query) < 2:
        return jsonify({'results': []})

    # Get limited results for autocomplete
    results = search_service.search_all(query, limit_per_type=5)

    # Combine into single list with type indicators
    combined = []

    # Add players first
    for player in results['players']:
        combined.append({
            'type': 'player',
            'id': player['player_id'],
            'name': player['name'],
            'subtitle': f"{player['team']} - {player['position']}" + (" (Retired)" if player['retired'] else ""),
            'url': f"/players/{player['player_id']}"
        })

    # Add teams
    for team in results['teams']:
        combined.append({
            'type': 'team',
            'id': team['team_id'],
            'name': f"{team['name']} ({team['abbr']})",
            'subtitle': team['league'],
            'url': f"/teams/{team['team_id']}"
        })

    return jsonify({'results': combined})
