"""
Context processors for the application.

Context processors make variables available to all templates automatically.
"""
from flask import current_app
from app.models import League
from datetime import datetime


def inject_game_date():
    """
    Inject current game date into all templates.

    Fetches the game_date from the first top-level league (league_level=1).
    This represents the current in-game date for the simulation.

    Returns:
        dict: Dictionary containing current_game_date
    """
    try:
        # Get game date from first top-level league (league_level=1)
        # All leagues should have the same game_date, so we just pick one
        league = League.query.filter_by(league_level=1).first()

        if league and league.game_date:
            return {
                'current_game_date': league.game_date,
                'current_game_year': league.game_date.year if league.game_date else None
            }

        # Fallback if no league found
        return {
            'current_game_date': None,
            'current_game_year': None
        }
    except Exception as e:
        # Log error but don't break the application
        current_app.logger.error(f"Error fetching game date in context processor: {e}")
        return {
            'current_game_date': None,
            'current_game_year': None
        }
