"""League service layer for league and year pages."""
from typing import Dict, List, Optional, Any
from sqlalchemy import text, and_
from sqlalchemy.orm import load_only, raiseload, lazyload, joinedload, selectinload
from app.extensions import db
from app.models import (
    League, SubLeague, Division, Team, TeamRecord,
    TeamHistoryRecord, TeamHistoryBattingStats, TeamHistoryPitchingStats,
    TeamBattingStats, TeamPitchingStats
)


def get_league_standings(league_id: int) -> Dict[str, Any]:
    """Get current standings for a specific league.

    Returns standings grouped by sub-league and division.

    Args:
        league_id: League ID

    Returns:
        Dict with structure:
        {
            'standings': [
                {
                    'name': 'Sub-League Name',
                    'divisions': [
                        {
                            'name': 'Division Name',
                            'teams': [Team objects with records]
                        }
                    ]
                }
            ]
        }
    """
    # Get teams in this league via team_relations
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

    result = db.session.execute(query, {'league_id': league_id})
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
        # Get sub-league name - sub_leagues can start at 0
        sub_league = SubLeague.query.filter_by(
            league_id=league_id,
            sub_league_id=sub_league_id
        ).first()

        # Get league name for fallback (when sub_league_id=0, use league name)
        if not sub_league:
            league = League.query.get(league_id)

        sub_league_data = {
            'name': sub_league.name if sub_league else (league.name if league else 'League'),
            'divisions': []
        }

        for division_id in sorted(structure[sub_league_id].keys()):
            # Get division name - divisions can start at 0
            division = Division.query.filter_by(
                league_id=league_id,
                sub_league_id=sub_league_id,
                division_id=division_id
            ).first()

            # Get teams with their records
            # OPTIMIZATION: Block cascading eager loads on Team model
            from app.models import City, Park
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
                'name': division.name if division else f'Division {division_id}',
                'teams': teams
            }

            sub_league_data['divisions'].append(division_data)

        standings_data.append(sub_league_data)

    return {
        'standings': standings_data
    }


def get_league_team_stats(league_id: int) -> Dict[str, Any]:
    """Get team aggregate batting and pitching stats for a league (current year).

    Args:
        league_id: League ID

    Returns:
        Dict with 'batting_stats' and 'pitching_stats' lists
    """
    # Get all teams in this league
    team_ids_query = text("""
        SELECT DISTINCT team_id
        FROM team_relations
        WHERE league_id = :league_id
    """)
    result = db.session.execute(team_ids_query, {'league_id': league_id})
    team_ids = [row[0] for row in result.fetchall()]

    # Get current year batting stats
    batting_stats = (TeamBattingStats.query
                    .filter(TeamBattingStats.team_id.in_(team_ids))
                    .options(
                        selectinload(TeamBattingStats.team).options(
                            load_only(Team.team_id, Team.name, Team.abbr),
                            raiseload('*')
                        ),
                        raiseload('*')
                    )
                    .all())

    # Get current year pitching stats
    pitching_stats = (TeamPitchingStats.query
                     .filter(TeamPitchingStats.team_id.in_(team_ids))
                     .options(
                         selectinload(TeamPitchingStats.team).options(
                             load_only(Team.team_id, Team.name, Team.abbr),
                             raiseload('*')
                         ),
                         raiseload('*')
                     )
                     .all())

    return {
        'batting_stats': batting_stats,
        'pitching_stats': pitching_stats
    }


def get_year_standings(year: int) -> Dict[str, Any]:
    """Get standings for all leagues for a specific year.

    Args:
        year: Season year

    Returns:
        Dict with leagues_data structure similar to front page
    """
    # Get all top-level leagues
    top_leagues = League.query.filter_by(league_level=1).order_by(League.name).all()

    if not top_leagues:
        return {'leagues_data': []}

    # For current year, use current standings
    # For historical years, use team_history_record
    # Determine current year from leagues (use max season_year from top-level leagues)
    current_year_query = db.session.query(db.func.max(League.season_year)).filter_by(league_level=1).scalar()
    is_current_year = (year == current_year_query)

    leagues_data = []

    for league in top_leagues:
        # Get teams in this league
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

        result = db.session.execute(query, {'league_id': league.league_id})
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
            # Get sub-league name
            sub_league = SubLeague.query.filter_by(
                league_id=league.league_id,
                sub_league_id=sub_league_id
            ).first() if sub_league_id > 0 else None

            sub_league_data = {
                'name': sub_league.name if sub_league else league.name,
                'divisions': []
            }

            for division_id in sorted(structure[sub_league_id].keys()):
                # Get division name
                division = Division.query.filter_by(
                    league_id=league.league_id,
                    sub_league_id=sub_league_id,
                    division_id=division_id
                ).first() if division_id > 0 else None

                team_ids = structure[sub_league_id][division_id]

                if is_current_year:
                    # Use current standings
                    teams = (Team.query
                            .join(TeamRecord)
                            .options(
                                load_only(Team.team_id, Team.name, Team.abbr),
                                joinedload(Team.record).raiseload('*'),
                                raiseload('*')
                            )
                            .filter(Team.team_id.in_(team_ids))
                            .order_by(TeamRecord.pos.asc())
                            .all())
                else:
                    # Use historical standings
                    # Get historical records for this year
                    historical_records = (TeamHistoryRecord.query
                                        .filter(
                                            TeamHistoryRecord.team_id.in_(team_ids),
                                            TeamHistoryRecord.year == year
                                        )
                                        .all())

                    # Create a map of team_id -> historical record
                    record_map = {r.team_id: r for r in historical_records}

                    # Get teams and attach historical records
                    teams = (Team.query
                            .options(
                                load_only(Team.team_id, Team.name, Team.abbr),
                                raiseload('*')
                            )
                            .filter(Team.team_id.in_(team_ids))
                            .all())

                    # Attach historical record as 'history_record' attribute (can't use 'record' - it's a relationship)
                    for team in teams:
                        team.history_record = record_map.get(team.team_id)

                    # Sort by position
                    teams = sorted(teams, key=lambda t: t.history_record.pos if t.history_record else 999)

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

    return {
        'leagues_data': leagues_data,
        'year': year,
        'is_current_year': is_current_year
    }


def get_available_years() -> List[int]:
    """Get all available years with historical data.

    Returns:
        List of years, sorted descending
    """
    # Query distinct years from team_history_record
    query = text("""
        SELECT DISTINCT year
        FROM team_history_record
        ORDER BY year DESC
    """)
    result = db.session.execute(query)
    years = [row[0] for row in result.fetchall()]

    # Add current year if not in list
    if 1997 not in years:
        years.insert(0, 1997)

    return years
