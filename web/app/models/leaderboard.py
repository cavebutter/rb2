"""Leaderboard models - Read-only models for materialized views.

These models map to pre-aggregated materialized views that are refreshed
after each ETL run. They provide high-performance access to leaderboard data
without expensive runtime calculations.

All leaderboard views include:
- Pre-calculated career/season statistics
- Active/retired status flags
- Proper indexes for fast sorting by any stat
"""
from ..extensions import db
from ..models.base import BaseModel, ReadOnlyMixin


class LeaderboardCareerBatting(BaseModel, ReadOnlyMixin):
    """Career batting leaderboard (materialized view).

    Maps to leaderboard_career_batting view.
    Aggregates all-time career stats with active status indicator.
    """
    __tablename__ = 'leaderboard_career_batting'

    # Primary Key
    player_id = db.Column(db.Integer, primary_key=True)

    # Player Info
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))
    seasons = db.Column(db.Integer)

    # Counting Stats
    g = db.Column(db.Integer)
    pa = db.Column(db.Integer)
    ab = db.Column(db.Integer)
    r = db.Column(db.Integer)
    h = db.Column(db.Integer)
    doubles = db.Column(db.Integer)
    triples = db.Column(db.Integer)
    hr = db.Column(db.Integer)
    rbi = db.Column(db.Integer)
    sb = db.Column(db.Integer)
    cs = db.Column(db.Integer)
    bb = db.Column(db.Integer)
    so = db.Column(db.Integer)
    ibb = db.Column(db.Integer)
    hbp = db.Column(db.Integer)
    sh = db.Column(db.Integer)
    sf = db.Column(db.Integer)
    gdp = db.Column(db.Integer)

    # Rate Stats (calculated from career totals)
    avg = db.Column(db.Numeric(4, 3))
    obp = db.Column(db.Numeric(4, 3))
    slg = db.Column(db.Numeric(4, 3))

    # Advanced Stats
    war = db.Column(db.Numeric(8, 3))

    # Status Flags
    is_active = db.Column(db.Boolean)
    retired = db.Column(db.SmallInteger)

    def __repr__(self):
        return f"<LeaderboardCareerBatting({self.player_id}: {self.first_name} {self.last_name})>"


class LeaderboardCareerPitching(BaseModel, ReadOnlyMixin):
    """Career pitching leaderboard (materialized view).

    Maps to leaderboard_career_pitching view.
    Aggregates all-time career stats with active status indicator.
    """
    __tablename__ = 'leaderboard_career_pitching'

    # Primary Key
    player_id = db.Column(db.Integer, primary_key=True)

    # Player Info
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))
    seasons = db.Column(db.Integer)

    # Counting Stats
    w = db.Column(db.Integer)
    l = db.Column(db.Integer)
    g = db.Column(db.Integer)
    gs = db.Column(db.Integer)
    cg = db.Column(db.Integer)
    sho = db.Column(db.Integer)
    sv = db.Column(db.Integer)
    ip = db.Column(db.Numeric(8, 1))
    h = db.Column(db.Integer)
    r = db.Column(db.Integer)
    er = db.Column(db.Integer)
    hr = db.Column(db.Integer)
    bb = db.Column(db.Integer)
    so = db.Column(db.Integer)
    hbp = db.Column(db.Integer)
    wp = db.Column(db.Integer)
    bk = db.Column(db.Integer)

    # Rate Stats (calculated from career totals)
    era = db.Column(db.Numeric(4, 2))
    whip = db.Column(db.Numeric(4, 2))
    k_per_9 = db.Column(db.Numeric(4, 2))
    k_bb_ratio = db.Column(db.Numeric(4, 2))

    # Advanced Stats
    war = db.Column(db.Numeric(8, 3))

    # Status Flags
    is_active = db.Column(db.Boolean)
    retired = db.Column(db.SmallInteger)

    def __repr__(self):
        return f"<LeaderboardCareerPitching({self.player_id}: {self.first_name} {self.last_name})>"


class LeaderboardSingleSeasonBatting(BaseModel, ReadOnlyMixin):
    """Single-season batting records (materialized view).

    Maps to leaderboard_single_season_batting view.
    Best individual seasons with minimum 100 PA threshold.
    """
    __tablename__ = 'leaderboard_single_season_batting'

    # Composite Primary Key
    player_id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.SmallInteger, primary_key=True)
    league_id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, primary_key=True)

    # Player Info
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))

    # Context
    league_abbr = db.Column(db.String(10))
    team_abbr = db.Column(db.String(10))

    # Counting Stats
    g = db.Column(db.Integer)
    pa = db.Column(db.Integer)
    ab = db.Column(db.Integer)
    r = db.Column(db.Integer)
    h = db.Column(db.Integer)
    doubles = db.Column(db.Integer)
    triples = db.Column(db.Integer)
    hr = db.Column(db.Integer)
    rbi = db.Column(db.Integer)
    sb = db.Column(db.Integer)
    bb = db.Column(db.Integer)
    so = db.Column(db.Integer)

    # Rate Stats
    avg = db.Column(db.Numeric(4, 3))
    obp = db.Column(db.Numeric(4, 3))
    slg = db.Column(db.Numeric(4, 3))

    # Advanced Stats
    war = db.Column(db.Numeric(8, 3))

    # Status Flag
    is_active = db.Column(db.Boolean)

    def __repr__(self):
        return f"<LeaderboardSingleSeasonBatting({self.player_id}: {self.year})>"


class LeaderboardSingleSeasonPitching(BaseModel, ReadOnlyMixin):
    """Single-season pitching records (materialized view).

    Maps to leaderboard_single_season_pitching view.
    Best individual seasons with minimum 50 IP threshold.
    """
    __tablename__ = 'leaderboard_single_season_pitching'

    # Composite Primary Key
    player_id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.SmallInteger, primary_key=True)
    league_id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, primary_key=True)

    # Player Info
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))

    # Context
    league_abbr = db.Column(db.String(10))
    team_abbr = db.Column(db.String(10))

    # Counting Stats
    w = db.Column(db.Integer)
    l = db.Column(db.Integer)
    g = db.Column(db.Integer)
    gs = db.Column(db.Integer)
    cg = db.Column(db.Integer)
    sho = db.Column(db.Integer)
    sv = db.Column(db.Integer)
    ip = db.Column(db.Numeric(8, 1))
    h = db.Column(db.Integer)
    er = db.Column(db.Integer)
    bb = db.Column(db.Integer)
    so = db.Column(db.Integer)

    # Rate Stats
    era = db.Column(db.Numeric(4, 2))
    whip = db.Column(db.Numeric(4, 2))
    k_per_9 = db.Column(db.Numeric(4, 2))

    # Advanced Stats
    war = db.Column(db.Numeric(8, 3))

    # Status Flag
    is_active = db.Column(db.Boolean)

    def __repr__(self):
        return f"<LeaderboardSingleSeasonPitching({self.player_id}: {self.year})>"


class LeaderboardYearlyBatting(BaseModel, ReadOnlyMixin):
    """Yearly batting leaders by league (materialized view).

    Maps to leaderboard_yearly_batting view.
    Top 10 players per year/league for each major stat.
    """
    __tablename__ = 'leaderboard_yearly_batting'

    # Composite Primary Key (all columns that make record unique)
    player_id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.SmallInteger, primary_key=True)
    league_id = db.Column(db.Integer, primary_key=True)

    # Player Info
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))

    # Context
    league_abbr = db.Column(db.String(10))

    # Stats
    hr = db.Column(db.Integer)
    rbi = db.Column(db.Integer)
    sb = db.Column(db.Integer)
    h = db.Column(db.Integer)
    avg = db.Column(db.Numeric(4, 3))
    war = db.Column(db.Numeric(8, 3))

    # Rankings
    hr_rank = db.Column(db.BigInteger)
    rbi_rank = db.Column(db.BigInteger)
    sb_rank = db.Column(db.BigInteger)
    h_rank = db.Column(db.BigInteger)
    avg_rank = db.Column(db.BigInteger)
    war_rank = db.Column(db.BigInteger)

    # Status Flag
    is_active = db.Column(db.Boolean)

    def __repr__(self):
        return f"<LeaderboardYearlyBatting({self.player_id}: {self.year}, {self.league_abbr})>"


class LeaderboardYearlyPitching(BaseModel, ReadOnlyMixin):
    """Yearly pitching leaders by league (materialized view).

    Maps to leaderboard_yearly_pitching view.
    Top 10 players per year/league for each major stat.
    """
    __tablename__ = 'leaderboard_yearly_pitching'

    # Composite Primary Key
    player_id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.SmallInteger, primary_key=True)
    league_id = db.Column(db.Integer, primary_key=True)

    # Player Info
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))

    # Context
    league_abbr = db.Column(db.String(10))

    # Stats
    w = db.Column(db.Integer)
    sv = db.Column(db.Integer)
    so = db.Column(db.Integer)
    era = db.Column(db.Numeric(4, 2))
    whip = db.Column(db.Numeric(4, 2))
    war = db.Column(db.Numeric(8, 3))

    # Rankings
    w_rank = db.Column(db.BigInteger)
    sv_rank = db.Column(db.BigInteger)
    so_rank = db.Column(db.BigInteger)
    era_rank = db.Column(db.BigInteger)
    whip_rank = db.Column(db.BigInteger)
    war_rank = db.Column(db.BigInteger)

    # Status Flag
    is_active = db.Column(db.Boolean)

    def __repr__(self):
        return f"<LeaderboardYearlyPitching({self.player_id}: {self.year}, {self.league_abbr})>"