"""Service layer for team-related queries."""
from app.models import (
    Team,
    TeamHistoryRecord,
    TeamHistoryBattingStats,
    TeamHistoryPitchingStats,
    TeamBattingStats,
    TeamPitchingStats,
    TeamRecord,
    Player,
    PlayerBattingStats,
    PlayerPitchingStats,
    PlayerCurrentStatus
)
from app.extensions import db
from sqlalchemy import and_
from sqlalchemy.orm import load_only, raiseload


# Game year constant - current year in the simulation
CURRENT_GAME_YEAR = 1997


def get_team_year_data(team_id, year):
    """Get all data for a specific team-year combination.

    Args:
        team_id: Team ID
        year: Season year (1980-1997)

    Returns:
        Dictionary with:
        - team: Team object
        - year: Year
        - record: Team record for that year
        - batting_stats: Team batting stats
        - pitching_stats: Team pitching stats
        - is_current_year: Boolean
    """
    team = Team.query.get_or_404(team_id)
    is_current_year = (year == CURRENT_GAME_YEAR)

    if is_current_year:
        # Current year - use current tables
        record = TeamRecord.query.filter_by(team_id=team_id).first()
        batting_stats = TeamBattingStats.query.filter_by(
            team_id=team_id,
            year=year
        ).first()
        pitching_stats = TeamPitchingStats.query.filter_by(
            team_id=team_id,
            year=year
        ).first()
    else:
        # Historical year - use history tables
        record = TeamHistoryRecord.query.filter_by(
            team_id=team_id,
            year=year
        ).first()
        batting_stats = TeamHistoryBattingStats.query.filter_by(
            team_id=team_id,
            year=year
        ).first()
        pitching_stats = TeamHistoryPitchingStats.query.filter_by(
            team_id=team_id,
            year=year
        ).first()

    return {
        'team': team,
        'year': year,
        'record': record,
        'batting_stats': batting_stats,
        'pitching_stats': pitching_stats,
        'is_current_year': is_current_year
    }


def get_available_years_for_team(team_id):
    """Get all years for which a team has data.

    Args:
        team_id: Team ID

    Returns:
        List of years sorted descending (newest first)
    """
    from app.extensions import db

    # Get historical years
    historical_years = (
        db.session.query(TeamHistoryRecord.year)
        .filter(TeamHistoryRecord.team_id == team_id)
        .distinct()
    )

    years = [row[0] for row in historical_years.all()]

    # Add current year if team has current record
    current_year_query = (
        db.session.query(TeamRecord.team_id)
        .filter(TeamRecord.team_id == team_id)
    )

    if current_year_query.first():
        years.append(CURRENT_GAME_YEAR)

    return sorted(years, reverse=True)


def get_team_player_batting_stats(team_id, year):
    """Get individual player batting statistics for a team-year.

    Args:
        team_id: Team ID
        year: Season year

    Returns:
        List of tuples (Player, PlayerBattingStats, PlayerCurrentStatus) sorted by WAR descending

    Note: Position is currently sourced from players_current_status (workaround).
    TODO: Once fielding stats are implemented, position should come from
    players_career_fielding_stats for the specific team-year.
    """
    # Query player batting stats for this team-year
    # Join with Player to get names and PlayerCurrentStatus for position
    # Filter to split_id=1 (overall stats, not situational splits)
    # OPTIMIZATION: Only load essential Player columns and prevent all relationship loading
    stats = (
        db.session.query(Player, PlayerBattingStats, PlayerCurrentStatus)
        .options(load_only(Player.player_id, Player.first_name, Player.last_name))
        .options(raiseload('*'))
        .join(PlayerBattingStats, Player.player_id == PlayerBattingStats.player_id)
        .join(PlayerCurrentStatus, Player.player_id == PlayerCurrentStatus.player_id)
        .filter(
            and_(
                PlayerBattingStats.team_id == team_id,
                PlayerBattingStats.year == year,
                PlayerBattingStats.split_id == 1
            )
        )
        .order_by(PlayerBattingStats.war.desc().nulls_last())
        .all()
    )

    return stats


def get_team_player_pitching_stats(team_id, year):
    """Get individual player pitching statistics for a team-year.

    Args:
        team_id: Team ID
        year: Season year

    Returns:
        List of tuples (Player, PlayerPitchingStats) sorted by WAR descending
    """
    # Query player pitching stats for this team-year
    # OPTIMIZATION: Only load essential Player columns and prevent all relationship loading
    stats = (
        db.session.query(Player, PlayerPitchingStats)
        .options(load_only(Player.player_id, Player.first_name, Player.last_name))
        .options(raiseload('*'))
        .join(PlayerPitchingStats, Player.player_id == PlayerPitchingStats.player_id)
        .filter(
            and_(
                PlayerPitchingStats.team_id == team_id,
                PlayerPitchingStats.year == year,
                PlayerPitchingStats.split_id == 1
            )
        )
        .order_by(PlayerPitchingStats.war.desc().nulls_last())
        .all()
    )

    return stats


def get_team_top_players_by_war(team_id, year, limit=12):
    """Get top N players by WAR for a team-year (combined batting and pitching).

    Args:
        team_id: Team ID
        year: Season year
        limit: Number of top players to return (default 12)

    Returns:
        List of tuples (Player, WAR, stat_type) sorted by WAR descending
        stat_type is either 'batting' or 'pitching'
    """
    # OPTIMIZATION: Only load essential Player columns and disable relationship loading
    # Get batting WAR
    batting_wars = (
        db.session.query(
            Player.player_id.label('player_id'),
            Player.first_name.label('first_name'),
            Player.last_name.label('last_name'),
            PlayerBattingStats.war.label('war'),
            db.literal('batting').label('stat_type')
        )
        .join(PlayerBattingStats, Player.player_id == PlayerBattingStats.player_id)
        .filter(
            and_(
                PlayerBattingStats.team_id == team_id,
                PlayerBattingStats.year == year,
                PlayerBattingStats.split_id == 1,
                PlayerBattingStats.war.isnot(None)
            )
        )
    )

    # Get pitching WAR
    pitching_wars = (
        db.session.query(
            Player.player_id.label('player_id'),
            Player.first_name.label('first_name'),
            Player.last_name.label('last_name'),
            PlayerPitchingStats.war.label('war'),
            db.literal('pitching').label('stat_type')
        )
        .join(PlayerPitchingStats, Player.player_id == PlayerPitchingStats.player_id)
        .filter(
            and_(
                PlayerPitchingStats.team_id == team_id,
                PlayerPitchingStats.year == year,
                PlayerPitchingStats.split_id == 1,
                PlayerPitchingStats.war.isnot(None)
            )
        )
    )

    # Union both queries and order by WAR descending
    combined = batting_wars.union_all(pitching_wars).subquery()

    top_players = (
        db.session.query(
            combined.c.player_id,
            combined.c.first_name,
            combined.c.last_name,
            combined.c.war,
            combined.c.stat_type
        )
        .order_by(combined.c.war.desc())
        .limit(limit)
        .all()
    )

    return top_players


def get_franchise_history(team_id):
    """Get franchise history summary for a team.

    Aggregates data from team_history tables to provide:
    - Former team names (unique historical names)
    - Total seasons count
    - All-time W-L record
    - Playoff appearances count
    - Championships count

    Args:
        team_id: Team ID

    Returns:
        dict: {
            'former_names': [list of unique historical team names],
            'total_seasons': int,
            'all_time_wins': int,
            'all_time_losses': int,
            'all_time_pct': float,
            'playoff_appearances': int,
            'championships': int
        }
    """
    # Get current team info
    team = Team.query.get(team_id)
    if not team:
        return None

    # Get unique historical team names (excluding current name)
    historical_names_query = (
        db.session.query(TeamHistoryRecord.team_name)
        .filter(TeamHistoryRecord.team_id == team_id)
        .filter(TeamHistoryRecord.team_name != team.name)
        .distinct()
        .all()
    )
    former_names = [row[0] for row in historical_names_query if row[0]]

    # Get franchise totals
    totals_query = (
        db.session.query(
            db.func.count(TeamHistoryRecord.year).label('seasons'),
            db.func.sum(TeamHistoryRecord.w).label('wins'),
            db.func.sum(TeamHistoryRecord.l).label('losses'),
            db.func.sum(db.case((TeamHistoryRecord.playoffs == 1, 1), else_=0)).label('playoffs'),
            db.func.sum(db.case((TeamHistoryRecord.wc == 1, 1), else_=0)).label('championships')
        )
        .filter(TeamHistoryRecord.team_id == team_id)
        .first()
    )

    # Include current year if exists
    current_record = TeamRecord.query.filter_by(team_id=team_id).first()
    if current_record:
        total_seasons = (totals_query.seasons or 0) + 1
        total_wins = (totals_query.wins or 0) + (current_record.w or 0)
        total_losses = (totals_query.losses or 0) + (current_record.l or 0)
        playoff_appearances = (totals_query.playoffs or 0) + (1 if current_record.playoffs else 0)
        championships = (totals_query.championships or 0) + (1 if current_record.wc else 0)
    else:
        total_seasons = totals_query.seasons or 0
        total_wins = totals_query.wins or 0
        total_losses = totals_query.losses or 0
        playoff_appearances = totals_query.playoffs or 0
        championships = totals_query.championships or 0

    # Calculate win percentage
    total_games = total_wins + total_losses
    all_time_pct = total_wins / total_games if total_games > 0 else 0.0

    return {
        'former_names': former_names,
        'total_seasons': total_seasons,
        'all_time_wins': total_wins,
        'all_time_losses': total_losses,
        'all_time_pct': all_time_pct,
        'playoff_appearances': playoff_appearances,
        'championships': championships
    }


def get_franchise_top_players(team_id, limit=24):
    """Get top franchise players by total WAR across all seasons.

    Combines batting and pitching WAR for all players who ever played for this team.

    Args:
        team_id: Team ID
        limit: Number of top players to return (default 24)

    Returns:
        List of tuples (player_id, first_name, last_name, total_war, primary_stat_type)
        primary_stat_type is 'batting' or 'pitching' based on which contributed more WAR
    """
    # OPTIMIZATION: Only load essential Player columns and disable relationship loading
    # Get total batting WAR per player for this team
    batting_wars = (
        db.session.query(
            Player.player_id.label('player_id'),
            Player.first_name.label('first_name'),
            Player.last_name.label('last_name'),
            db.func.sum(PlayerBattingStats.war).label('batting_war'),
            db.literal(0.0).label('pitching_war')
        )
        .join(PlayerBattingStats, Player.player_id == PlayerBattingStats.player_id)
        .filter(
            and_(
                PlayerBattingStats.team_id == team_id,
                PlayerBattingStats.split_id == 1,
                PlayerBattingStats.war.isnot(None)
            )
        )
        .group_by(Player.player_id, Player.first_name, Player.last_name)
    )

    # Get total pitching WAR per player for this team
    pitching_wars = (
        db.session.query(
            Player.player_id.label('player_id'),
            Player.first_name.label('first_name'),
            Player.last_name.label('last_name'),
            db.literal(0.0).label('batting_war'),
            db.func.sum(PlayerPitchingStats.war).label('pitching_war')
        )
        .join(PlayerPitchingStats, Player.player_id == PlayerPitchingStats.player_id)
        .filter(
            and_(
                PlayerPitchingStats.team_id == team_id,
                PlayerPitchingStats.split_id == 1,
                PlayerPitchingStats.war.isnot(None)
            )
        )
        .group_by(Player.player_id, Player.first_name, Player.last_name)
    )

    # Union both queries
    combined = batting_wars.union_all(pitching_wars).subquery()

    # Aggregate by player and sum batting/pitching WAR
    top_players = (
        db.session.query(
            combined.c.player_id,
            combined.c.first_name,
            combined.c.last_name,
            db.func.sum(combined.c.batting_war).label('batting_war'),
            db.func.sum(combined.c.pitching_war).label('pitching_war'),
            (db.func.sum(combined.c.batting_war) + db.func.sum(combined.c.pitching_war)).label('total_war')
        )
        .group_by(combined.c.player_id, combined.c.first_name, combined.c.last_name)
        .order_by(db.desc('total_war'))
        .limit(limit)
        .all()
    )

    # Determine primary stat type for each player
    results = []
    for row in top_players:
        primary_type = 'batting' if row.batting_war >= row.pitching_war else 'pitching'
        results.append({
            'player_id': row.player_id,
            'first_name': row.first_name,
            'last_name': row.last_name,
            'total_war': row.total_war,
            'primary_stat_type': primary_type
        })

    return results


def get_franchise_year_by_year(team_id):
    """Get year-by-year franchise records for all seasons.

    Args:
        team_id: Team ID

    Returns:
        List of records (TeamHistoryRecord objects + current year TeamRecord)
        Sorted by year descending (most recent first)
    """
    # Get historical records
    historical_records = (
        TeamHistoryRecord.query
        .filter_by(team_id=team_id)
        .order_by(TeamHistoryRecord.year.desc())
        .all()
    )

    # Get current year record if exists
    current_record = TeamRecord.query.filter_by(team_id=team_id).first()

    # Combine them
    all_records = []
    if current_record:
        # Add current year at the top
        all_records.append({
            'year': CURRENT_GAME_YEAR,
            'team_name': current_record.team.name if current_record.team else '',
            'w': current_record.w,
            'l': current_record.l,
            'pct': current_record.pct,
            'playoffs': current_record.playoffs,
            'wc': current_record.wc,
            'is_current': True
        })

    # Add historical records
    for record in historical_records:
        all_records.append({
            'year': record.year,
            'team_name': record.team_name,
            'w': record.w,
            'l': record.l,
            'pct': record.pct,
            'playoffs': record.playoffs,
            'wc': record.wc,
            'is_current': False
        })

    return all_records
