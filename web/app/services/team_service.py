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

    OPTIMIZATION: Use load_only() to prevent loading Team relationships.
    We only need the team_id to verify it exists - no need to load city, park, league, etc.
    """
    # Check if team exists (only load team_id, no relationships)
    team = (Team.query
            .options(load_only(Team.team_id))
            .options(raiseload('*'))
            .get(team_id))
    if not team:
        return None

    # Note: TeamHistoryRecord doesn't have team_name field, uses relationship
    # Former names feature deferred - would need team name history tracking
    former_names = []

    # Get franchise totals
    # Note: TeamHistoryRecord doesn't have playoffs/championships fields
    # These would need to be added to the ETL process
    totals_query = (
        db.session.query(
            db.func.count(TeamHistoryRecord.year).label('seasons'),
            db.func.sum(TeamHistoryRecord.w).label('wins'),
            db.func.sum(TeamHistoryRecord.l).label('losses')
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
    else:
        total_seasons = totals_query.seasons or 0
        total_wins = totals_query.wins or 0
        total_losses = totals_query.losses or 0

    # Playoffs and championships data not available in current schema
    playoff_appearances = 0
    championships = 0

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
        List of dicts with player_id, first_name, last_name, total_war, primary_stat_type

    OPTIMIZATION: Uses CTE (Common Table Expression) instead of UNION ALL for better performance.
    PostgreSQL can optimize CTEs better than subqueries with UNION.
    """
    from sqlalchemy import text

    # Use raw SQL with CTE for optimal performance
    # This query aggregates batting and pitching WAR in a single pass
    query = text("""
        WITH player_wars AS (
            -- Batting WAR
            SELECT
                player_id,
                SUM(war) as batting_war,
                0.0 as pitching_war
            FROM players_career_batting_stats
            WHERE team_id = :team_id
                AND split_id = 1
                AND war IS NOT NULL
            GROUP BY player_id

            UNION ALL

            -- Pitching WAR
            SELECT
                player_id,
                0.0 as batting_war,
                SUM(war) as pitching_war
            FROM players_career_pitching_stats
            WHERE team_id = :team_id
                AND split_id = 1
                AND war IS NOT NULL
            GROUP BY player_id
        ),
        aggregated AS (
            SELECT
                player_id,
                SUM(batting_war) as batting_war,
                SUM(pitching_war) as pitching_war,
                SUM(batting_war) + SUM(pitching_war) as total_war
            FROM player_wars
            GROUP BY player_id
            ORDER BY total_war DESC
            LIMIT :limit
        )
        SELECT
            a.player_id,
            p.first_name,
            p.last_name,
            a.batting_war,
            a.pitching_war,
            a.total_war,
            CASE WHEN a.batting_war >= a.pitching_war THEN 'batting' ELSE 'pitching' END as primary_stat_type
        FROM aggregated a
        JOIN players_core p ON a.player_id = p.player_id
        ORDER BY a.total_war DESC
    """)

    result = db.session.execute(query, {'team_id': team_id, 'limit': limit})

    # Convert to list of dicts
    results = []
    for row in result:
        results.append({
            'player_id': row.player_id,
            'first_name': row.first_name,
            'last_name': row.last_name,
            'total_war': float(row.total_war) if row.total_war else 0.0,
            'primary_stat_type': row.primary_stat_type
        })

    return results


def get_franchise_year_by_year(team_id):
    """Get year-by-year franchise records for all seasons.

    Args:
        team_id: Team ID

    Returns:
        List of records (TeamHistoryRecord objects + current year TeamRecord)
        Sorted by year descending (most recent first)

    OPTIMIZATION: Removed joinedload() and use team name from single query.
    Only load the name field to avoid loading all relationships (city, park, league, etc.).
    """
    # Get team name only - avoid loading relationships
    team = (Team.query
            .options(load_only(Team.team_id, Team.name))
            .options(raiseload('*'))
            .get(team_id))
    if not team:
        return []

    # Get historical records WITHOUT joinedload (no N+1 risk)
    # All records are for the same team, so we already have the team name
    # CRITICAL: Block team and league relationships to prevent cascading eager loads
    historical_records = (
        TeamHistoryRecord.query
        .options(raiseload('*'))  # Block all relationships (team, league)
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
        # Note: playoffs/wc fields don't exist in TeamRecord model
        all_records.append({
            'year': CURRENT_GAME_YEAR,
            'team_name': team.name,
            'w': current_record.w,
            'l': current_record.l,
            'pct': current_record.pct,
            'playoffs': False,  # Not available in current schema
            'wc': False,  # Not available in current schema
            'is_current': True
        })

    # Add historical records - use team name from initial query
    for record in historical_records:
        all_records.append({
            'year': record.year,
            'team_name': team.name,  # Use team name from parent query, not relationship
            'w': record.w,
            'l': record.l,
            'pct': record.pct,
            'playoffs': False,  # Not available in current schema
            'wc': False,  # Not available in current schema
            'is_current': False
        })

    return all_records
