"""Player routes"""
from flask import Blueprint, render_template, request, abort
from app.models import Player, PlayerCurrentStatus, PlayerBattingStats, PlayerPitchingStats
from sqlalchemy import desc, func, and_
from app.extensions import db
import string

bp = Blueprint('players', __name__)


@bp.route('/')
def players_list():
    """Players home page - alphabetical layout with notable players by letter.

    For each letter A-Z:
    - Letter is clickable (links to /players/letter/X)
    - Shows 10 notable players (by career WAR) whose last name starts with that letter
    """
    # OPTIMIZED: Single query to get all players with their career WAR
    # Then process in Python to group by letter and take top 10

    # Get all players with career WAR in one query
    all_players_with_war = (
        db.session.query(
            Player.player_id,
            Player.first_name,
            Player.last_name,
            func.sum(PlayerBattingStats.war).label('career_war'),
            func.substr(Player.last_name, 1, 1).label('first_letter')
        )
        .outerjoin(PlayerBattingStats, and_(
            Player.player_id == PlayerBattingStats.player_id,
            PlayerBattingStats.split_id == 1
        ))
        .group_by(Player.player_id, Player.first_name, Player.last_name)
        .order_by(desc('career_war'))
        .all()
    )

    # Group by first letter and take top 10 per letter
    players_by_letter = {}
    for row in all_players_with_war:
        letter = row.first_letter.upper() if row.first_letter else 'Z'

        if letter not in players_by_letter:
            players_by_letter[letter] = []

        # Only keep top 10 per letter
        if len(players_by_letter[letter]) < 10:
            # Create a simple object to hold player data
            player_data = type('PlayerData', (), {
                'player_id': row.player_id,
                'display_name': f"{row.first_name} {row.last_name}",
                'career_war': row.career_war
            })()
            players_by_letter[letter].append(player_data)

    return render_template('players/list.html', players_by_letter=players_by_letter)


@bp.route('/letter/<letter>')
def players_by_letter(letter):
    """Show all players whose last name starts with the given letter.

    Args:
        letter: Single letter A-Z

    Returns:
        Page with all players for that letter, showing name and years of service
    """
    # Validate letter
    letter = letter.upper()
    if len(letter) != 1 or letter not in string.ascii_uppercase:
        abort(404)

    # Get all players whose last name starts with this letter
    # Include their years of service (min/max year from stats)
    players_query = (
        db.session.query(
            Player,
            func.min(PlayerBattingStats.year).label('first_year'),
            func.max(PlayerBattingStats.year).label('last_year')
        )
        .outerjoin(PlayerBattingStats, and_(
            Player.player_id == PlayerBattingStats.player_id,
            PlayerBattingStats.split_id == 1  # Overall stats only
        ))
        .filter(Player.last_name.like(f'{letter}%'))
        .group_by(Player.player_id)
        .order_by(Player.last_name, Player.first_name)
        .all()
    )

    return render_template('players/letter.html',
                          letter=letter,
                          players=players_query)


@bp.route('/<int:player_id>')
def player_detail(player_id):
    """Player detail page - bio, stats, ratings"""
    player = Player.query.get_or_404(player_id)

    # Get career batting stats (split_id=1 for overall)
    career_batting = (PlayerBattingStats.query
                     .filter_by(player_id=player_id, split_id=1)
                     .order_by(desc(PlayerBattingStats.year))
                     .all())

    # Get career pitching stats
    career_pitching = (PlayerPitchingStats.query
                      .filter_by(player_id=player_id, split_id=1)
                      .order_by(desc(PlayerPitchingStats.year))
                      .all())

    return render_template('players/detail.html',
                          player=player,
                          career_batting=career_batting,
                          career_pitching=career_pitching)