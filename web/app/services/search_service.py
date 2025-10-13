"""Search service for global search functionality."""
from app.models import Player, Team, PlayerCurrentStatus
from sqlalchemy import or_, func
from app.extensions import db


def search_players(query, limit=10):
    """Search for players by name.

    Args:
        query: Search string (partial name match)
        limit: Maximum number of results to return

    Returns:
        List of player dictionaries with id, name, team, position, retired status
    """
    if not query or len(query) < 2:
        return []

    # Build search pattern for ILIKE (case-insensitive partial match)
    search_pattern = f'%{query}%'

    # Search by first name, last name, or full name
    players = (
        db.session.query(
            Player.player_id,
            Player.first_name,
            Player.last_name,
            PlayerCurrentStatus.team_id,
            PlayerCurrentStatus.position,
            PlayerCurrentStatus.retired
        )
        .outerjoin(PlayerCurrentStatus, Player.player_id == PlayerCurrentStatus.player_id)
        .filter(
            or_(
                Player.first_name.ilike(search_pattern),
                Player.last_name.ilike(search_pattern),
                func.concat(Player.first_name, ' ', Player.last_name).ilike(search_pattern)
            )
        )
        .order_by(Player.last_name, Player.first_name)
        .limit(limit)
        .all()
    )

    # Format results
    results = []
    for p in players:
        # Get team abbreviation if player has team
        team_abbr = None
        if p.team_id and p.team_id > 0:
            team = Team.query.with_entities(Team.abbr).filter_by(team_id=p.team_id).first()
            team_abbr = team.abbr if team else None

        # Format position
        position_map = {
            1: 'P', 2: 'C', 3: '1B', 4: '2B', 5: '3B',
            6: 'SS', 7: 'LF', 8: 'CF', 9: 'RF', 10: 'DH'
        }
        position_display = position_map.get(p.position, 'Unknown') if p.position else 'Unknown'

        results.append({
            'player_id': p.player_id,
            'name': f'{p.first_name} {p.last_name}',
            'team': team_abbr or 'FA',
            'position': position_display,
            'retired': bool(p.retired) if p.retired is not None else False,
            'type': 'player'
        })

    return results


def search_teams(query, limit=10):
    """Search for teams by name or abbreviation.

    Args:
        query: Search string (partial match)
        limit: Maximum number of results to return

    Returns:
        List of team dictionaries with id, name, abbreviation, league
    """
    if not query or len(query) < 2:
        return []

    # Build search pattern for ILIKE
    search_pattern = f'%{query}%'

    # Import League model and joinedload for eager loading
    from sqlalchemy.orm import joinedload
    from app.models import League

    # Search by name, nickname, or abbreviation
    # Eager-load league to avoid N+1 queries
    teams = (
        Team.query
        .options(joinedload(Team.league))
        .filter(
            or_(
                Team.name.ilike(search_pattern),
                Team.nickname.ilike(search_pattern),
                Team.abbr.ilike(search_pattern)
            )
        )
        .filter(Team.team_id > 0)  # Exclude placeholder teams
        .order_by(Team.name)
        .limit(limit)
        .all()
    )

    # Format results
    results = []
    for team in teams:
        full_name = team.name if team.name else 'Unknown Team'
        if team.nickname:
            full_name = f'{team.name} {team.nickname}'

        league_name = 'Unknown'
        if hasattr(team, 'league') and team.league:
            league_name = team.league.name

        results.append({
            'team_id': team.team_id,
            'name': full_name,
            'abbr': team.abbr if team.abbr else '???',
            'league': league_name,
            'type': 'team'
        })

    return results


def search_all(query, limit_per_type=10):
    """Search across all entity types.

    Args:
        query: Search string
        limit_per_type: Maximum results per entity type

    Returns:
        Dictionary with keys 'players', 'teams', and 'total_results'
    """
    if not query or len(query) < 2:
        return {
            'players': [],
            'teams': [],
            'total_results': 0
        }

    players = search_players(query, limit=limit_per_type)
    teams = search_teams(query, limit=limit_per_type)

    return {
        'players': players,
        'teams': teams,
        'total_results': len(players) + len(teams)
    }
