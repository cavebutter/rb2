"""Main routes (Home page, etc)"""
from flask import Blueprint, render_template
from app.models import Team, TeamRecord, League, SubLeague, Division
from app.services.player_service import get_notable_rookies, get_featured_players, get_players_born_this_week
from sqlalchemy import and_

bp = Blueprint('main', __name__)


@bp.route('/')
def index():
    """Home page - Show current standings for all top-level leagues

    Handles flexible league structures:
    - Leagues with sub-leagues and divisions (MLB: AL/NL with divisions)
    - Leagues without sub-leagues (just divisions)
    - Leagues with neither (flat standings)
    """

    # Get all top-level leagues (league_level = 1)
    top_leagues = League.query.filter_by(league_level=1).order_by(League.name).all()

    if not top_leagues:
        return render_template('index.html', leagues_data=None)

    leagues_data = []

    for league in top_leagues:
        # Get teams in this league via team_relations
        # Use raw SQL to handle flexible structure
        from sqlalchemy import text
        from app.extensions import db as database

        query = text("""
            SELECT DISTINCT
                t.team_id,
                tr.sub_league_id,
                tr.division_id,
                t.name
            FROM teams t
            JOIN team_relations tr ON t.team_id = tr.team_id
            WHERE tr.league_id = :league_id
            ORDER BY tr.sub_league_id, tr.division_id, t.name
        """)

        result = database.session.execute(query, {'league_id': league.league_id})
        team_relations = result.fetchall()

        # Group by sub_league_id and division_id
        structure = {}
        for row in team_relations:
            sub_league_id = row[1] if row[1] else 0
            division_id = row[2] if row[2] else 0

            if sub_league_id not in structure:
                structure[sub_league_id] = {}
            if division_id not in structure[sub_league_id]:
                structure[sub_league_id][division_id] = []
            structure[sub_league_id][division_id].append(row[0])

        standings_data = []

        for sub_league_id in sorted(structure.keys()):
            # Get sub-league name if it exists
            sub_league = SubLeague.query.filter_by(
                league_id=league.league_id,
                sub_league_id=sub_league_id
            ).first() if sub_league_id > 0 else None

            sub_league_data = {
                'name': sub_league.name if sub_league else league.name,
                'divisions': []
            }

            for division_id in sorted(structure[sub_league_id].keys()):
                # Get division name (division_id can be 0, so check if it's in the structure)
                division = Division.query.filter_by(
                    league_id=league.league_id,
                    sub_league_id=sub_league_id,
                    division_id=division_id
                ).first()

                # Get teams with their records
                # OPTIMIZATION: Block cascading eager loads on Team model
                from sqlalchemy.orm import load_only, raiseload, lazyload, joinedload
                team_ids = structure[sub_league_id][division_id]
                teams = (Team.query
                        .join(TeamRecord)
                        .options(
                            # Load only fields needed for standings display
                            load_only(
                                Team.team_id,
                                Team.name,
                                Team.abbr
                            ),
                            # Load the record (needed for template)
                            joinedload(Team.record).raiseload('*'),
                            # Block all other relationship cascades
                            lazyload(Team.city),
                            lazyload(Team.park),
                            lazyload(Team.nation),
                            lazyload(Team.league),
                            raiseload('*')
                        )
                        .filter(Team.team_id.in_(team_ids))
                        .order_by(TeamRecord.pos.asc())
                        .all())

                division_data = {
                    'name': division.name if division else 'Standings',
                    'teams': teams
                }

                sub_league_data['divisions'].append(division_data)

            standings_data.append(sub_league_data)

        league_data = {
            'league': league,
            'standings': standings_data
        }

        leagues_data.append(league_data)

    # Get featured players, notable rookies, and birthdays for left column
    featured_players = get_featured_players(limit=18)
    rookies = get_notable_rookies(limit=10)
    birthdays = get_players_born_this_week(days_range=7)

    return render_template('index.html',
                          leagues_data=leagues_data,
                          featured_players=featured_players,
                          rookies=rookies,
                          birthdays=birthdays)


@bp.route('/health')
def health():
    """Health Check endpoint"""
    return {'status': 'OK'}, 200
