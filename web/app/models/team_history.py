"""Team history models for historical stats and records by year."""
from sqlalchemy.ext.hybrid import hybrid_property
from app.extensions import db
from app.models.base import BaseModel


class TeamHistoryRecord(BaseModel):
    """Team record for a specific year (historical data, 1980-1996).

    Maps to team_history_record table. For current year, use TeamRecord instead.
    """
    __tablename__ = 'team_history_record'

    # Composite Primary Key
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)
    year = db.Column(db.Integer, primary_key=True)

    # League hierarchy
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    sub_league_id = db.Column(db.Integer)
    division_id = db.Column(db.Integer)

    # Record
    g = db.Column(db.SmallInteger)
    w = db.Column(db.SmallInteger)
    l = db.Column(db.SmallInteger)
    pct = db.Column(db.Numeric(6, 4))
    pos = db.Column(db.SmallInteger)  # Position in standings
    gb = db.Column(db.Numeric(7, 4))  # Games behind

    # Relationships
    team = db.relationship('Team', foreign_keys=[team_id])
    league = db.relationship('League', foreign_keys=[league_id])

    @hybrid_property
    def winning_percentage(self):
        """Calculate winning percentage."""
        if not self.g or self.g == 0:
            return 0.0
        return float(self.w) / float(self.g)

    @hybrid_property
    def games_back_display(self):
        """Format games back for display."""
        if self.gb is None or self.gb == 0:
            return '-'
        return f"{float(self.gb):.1f}"

    def __repr__(self):
        return f"<TeamHistoryRecord(team={self.team_id}, year={self.year}, {self.w}-{self.l})>"


class TeamHistoryBattingStats(BaseModel):
    """Team batting stats for a specific year (historical data, 1980-1996).

    Maps to team_history_batting_stats table. For current year, use TeamBattingStats.
    """
    __tablename__ = 'team_history_batting_stats'

    # Composite Primary Key
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)
    year = db.Column(db.Integer, primary_key=True)

    # League hierarchy
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    sub_league_id = db.Column(db.Integer)
    division_id = db.Column(db.Integer)
    level_id = db.Column(db.Integer)
    split_id = db.Column(db.Integer)

    # Plate Appearances and At Bats
    pa = db.Column(db.Integer)
    ab = db.Column(db.Integer)
    g = db.Column(db.Integer)
    gs = db.Column(db.Integer)

    # Hits
    h = db.Column(db.Integer)
    s = db.Column(db.Integer)  # Singles
    d = db.Column(db.Integer)  # Doubles
    t = db.Column(db.Integer)  # Triples
    hr = db.Column(db.Integer)
    tb = db.Column(db.Integer)  # Total bases
    ebh = db.Column(db.Integer)  # Extra base hits

    # Runs and RBI
    r = db.Column(db.Integer)
    rbi = db.Column(db.Integer)

    # Walks and Strikeouts
    bb = db.Column(db.Integer)
    ibb = db.Column(db.Integer)
    k = db.Column(db.Integer)

    # Other
    hp = db.Column(db.Integer)  # Hit by pitch
    sh = db.Column(db.Integer)  # Sacrifice hits
    sf = db.Column(db.Integer)  # Sacrifice flies
    sb = db.Column(db.Integer)  # Stolen bases
    cs = db.Column(db.Integer)  # Caught stealing
    gdp = db.Column(db.Integer)  # Grounded into double play
    ci = db.Column(db.Integer)  # Catcher's interference
    pitches_seen = db.Column(db.Integer)

    # Rate Stats
    avg = db.Column(db.Numeric(4, 3))
    obp = db.Column(db.Numeric(4, 3))
    slg = db.Column(db.Numeric(4, 3))
    ops = db.Column(db.Numeric(4, 3))
    iso = db.Column(db.Numeric(4, 3))
    woba = db.Column(db.Numeric(4, 3))
    sbp = db.Column(db.Numeric(4, 3))  # Stolen base percentage

    # Advanced Stats
    rc = db.Column(db.Numeric(6, 1))  # Runs created
    rc27 = db.Column(db.Numeric(5, 2))  # Runs created per 27 outs

    # Relationships
    team = db.relationship('Team', foreign_keys=[team_id])
    league = db.relationship('League', foreign_keys=[league_id])

    def __repr__(self):
        return f"<TeamHistoryBattingStats(team={self.team_id}, year={self.year})>"


class TeamHistoryPitchingStats(BaseModel):
    """Team pitching stats for a specific year (historical data, 1980-1996).

    Maps to team_history_pitching_stats table. For current year, use TeamPitchingStats.
    """
    __tablename__ = 'team_history_pitching_stats'

    # Composite Primary Key
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)
    year = db.Column(db.Integer, primary_key=True)

    # League hierarchy
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    sub_league_id = db.Column(db.Integer)
    division_id = db.Column(db.Integer)
    level_id = db.Column(db.Integer)
    split_id = db.Column(db.Integer)

    # Games
    g = db.Column(db.Integer)
    gs = db.Column(db.Integer)
    gf = db.Column(db.Integer)  # Games finished
    cg = db.Column(db.Integer)  # Complete games
    sho = db.Column(db.Integer)  # Shutouts

    # Innings and Batters Faced
    ip = db.Column(db.Integer)  # Innings pitched (as integer, e.g., 200.1 = 2001)
    ipf = db.Column(db.Integer)  # IP fractional part
    bf = db.Column(db.Integer)  # Batters faced
    pi = db.Column(db.Integer)  # Pitches
    pig = db.Column(db.Numeric(5, 2))  # Pitches per game

    # Wins and Losses
    w = db.Column(db.Integer)
    l = db.Column(db.Integer)
    s = db.Column(db.Integer)  # Saves
    svo = db.Column(db.Integer)  # Save opportunities
    bs = db.Column(db.Integer)  # Blown saves
    hld = db.Column(db.Integer)  # Holds
    qs = db.Column(db.Integer)  # Quality starts

    # Hits and Runs
    ha = db.Column(db.Integer)  # Hits allowed
    r = db.Column(db.Integer)  # Runs allowed
    er = db.Column(db.Integer)  # Earned runs
    rs = db.Column(db.Integer)  # Run support
    ra = db.Column(db.Integer)  # Runs against

    # Strikeouts and Walks
    k = db.Column(db.Integer)
    bb = db.Column(db.Integer)
    iw = db.Column(db.Integer)  # Intentional walks

    # Other
    hp = db.Column(db.Integer)  # Hit batsmen
    hra = db.Column(db.Integer)  # Home runs allowed
    wp = db.Column(db.Integer)  # Wild pitches
    bk = db.Column(db.Integer)  # Balks
    sb = db.Column(db.Integer)  # Stolen bases allowed
    cs = db.Column(db.Integer)  # Caught stealing
    dp = db.Column(db.Integer)  # Double plays
    gb = db.Column(db.Integer)  # Ground balls
    fb = db.Column(db.Integer)  # Fly balls
    ab = db.Column(db.Integer)  # At bats against
    tb = db.Column(db.Integer)  # Total bases allowed
    sh = db.Column(db.Integer)  # Sacrifice hits
    sf = db.Column(db.Integer)  # Sacrifice flies
    ta = db.Column(db.Integer)  # Total assists
    sa = db.Column(db.Integer)  # Singles allowed
    da = db.Column(db.Integer)  # Doubles allowed
    ci = db.Column(db.Integer)  # Catcher's interference

    # Rate Stats
    era = db.Column(db.Numeric(5, 2))
    avg = db.Column(db.Numeric(4, 3))  # Opponent batting average
    obp = db.Column(db.Numeric(4, 3))  # Opponent OBP
    slg = db.Column(db.Numeric(4, 3))  # Opponent SLG
    ops = db.Column(db.Numeric(4, 3))  # Opponent OPS
    whip = db.Column(db.Numeric(4, 2))
    h9 = db.Column(db.Numeric(5, 2))  # Hits per 9 innings
    hr9 = db.Column(db.Numeric(5, 2))  # Home runs per 9 innings
    bb9 = db.Column(db.Numeric(5, 2))  # Walks per 9 innings
    k9 = db.Column(db.Numeric(5, 2))  # Strikeouts per 9 innings
    r9 = db.Column(db.Numeric(5, 2))  # Runs per 9 innings
    kbb = db.Column(db.Numeric(5, 2))  # K/BB ratio
    babip = db.Column(db.Numeric(4, 3))

    # Advanced Stats
    fip = db.Column(db.Numeric(5, 2))
    cgp = db.Column(db.Numeric(4, 3))  # Complete game percentage
    qsp = db.Column(db.Numeric(4, 3))  # Quality start percentage
    winp = db.Column(db.Numeric(4, 3))  # Win percentage
    rsg = db.Column(db.Numeric(5, 2))  # Run support per game
    svp = db.Column(db.Numeric(4, 3))  # Save percentage
    bsvp = db.Column(db.Numeric(4, 3))  # Blown save percentage
    gfp = db.Column(db.Numeric(4, 3))  # Games finished percentage
    gbfbp = db.Column(db.Numeric(5, 2))  # Ground ball to fly ball ratio
    ws = db.Column(db.Integer)  # Win shares

    # Relationships
    team = db.relationship('Team', foreign_keys=[team_id])
    league = db.relationship('League', foreign_keys=[league_id])

    def __repr__(self):
        return f"<TeamHistoryPitchingStats(team={self.team_id}, year={self.year})>"


class TeamBattingStats(BaseModel):
    """Team batting stats for current year (1997).

    Maps to team_batting_stats table. For historical years, use TeamHistoryBattingStats.
    """
    __tablename__ = 'team_batting_stats'

    # Composite Primary Key
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)
    year = db.Column(db.Integer, primary_key=True)

    # Same structure as TeamHistoryBattingStats
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    level_id = db.Column(db.Integer)
    split_id = db.Column(db.Integer)

    pa = db.Column(db.Integer)
    ab = db.Column(db.Integer)
    g = db.Column(db.Integer)
    gs = db.Column(db.Integer)
    h = db.Column(db.Integer)
    s = db.Column(db.Integer)
    d = db.Column(db.Integer)
    t = db.Column(db.Integer)
    hr = db.Column(db.Integer)
    tb = db.Column(db.Integer)
    ebh = db.Column(db.Integer)
    r = db.Column(db.Integer)
    rbi = db.Column(db.Integer)
    bb = db.Column(db.Integer)
    ibb = db.Column(db.Integer)
    k = db.Column(db.Integer)
    hp = db.Column(db.Integer)
    sh = db.Column(db.Integer)
    sf = db.Column(db.Integer)
    sb = db.Column(db.Integer)
    cs = db.Column(db.Integer)
    gdp = db.Column(db.Integer)
    ci = db.Column(db.Integer)
    pitches_seen = db.Column(db.Integer)
    avg = db.Column(db.Numeric(4, 3))
    obp = db.Column(db.Numeric(4, 3))
    slg = db.Column(db.Numeric(4, 3))
    ops = db.Column(db.Numeric(4, 3))
    iso = db.Column(db.Numeric(4, 3))
    woba = db.Column(db.Numeric(4, 3))
    sbp = db.Column(db.Numeric(4, 3))
    rc = db.Column(db.Numeric(6, 1))
    rc27 = db.Column(db.Numeric(5, 2))
    ws = db.Column(db.Integer)

    # Relationships
    team = db.relationship('Team', foreign_keys=[team_id])
    league = db.relationship('League', foreign_keys=[league_id])

    def __repr__(self):
        return f"<TeamBattingStats(team={self.team_id}, year={self.year})>"


class TeamPitchingStats(BaseModel):
    """Team pitching stats for current year (1997).

    Maps to team_pitching_stats table. For historical years, use TeamHistoryPitchingStats.
    """
    __tablename__ = 'team_pitching_stats'

    # Composite Primary Key
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)
    year = db.Column(db.Integer, primary_key=True)

    # Same structure as TeamHistoryPitchingStats
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    level_id = db.Column(db.Integer)
    split_id = db.Column(db.Integer)

    g = db.Column(db.Integer)
    gs = db.Column(db.Integer)
    gf = db.Column(db.Integer)
    cg = db.Column(db.Integer)
    sho = db.Column(db.Integer)
    ip = db.Column(db.Integer)
    ipf = db.Column(db.Integer)
    bf = db.Column(db.Integer)
    pi = db.Column(db.Integer)
    pig = db.Column(db.Numeric(5, 2))
    w = db.Column(db.Integer)
    l = db.Column(db.Integer)
    s = db.Column(db.Integer)
    svo = db.Column(db.Integer)
    bs = db.Column(db.Integer)
    hld = db.Column(db.Integer)
    qs = db.Column(db.Integer)
    ha = db.Column(db.Integer)
    r = db.Column(db.Integer)
    er = db.Column(db.Integer)
    rs = db.Column(db.Integer)
    ra = db.Column(db.Integer)
    k = db.Column(db.Integer)
    bb = db.Column(db.Integer)
    iw = db.Column(db.Integer)
    hp = db.Column(db.Integer)
    hra = db.Column(db.Integer)
    wp = db.Column(db.Integer)
    bk = db.Column(db.Integer)
    sb = db.Column(db.Integer)
    cs = db.Column(db.Integer)
    dp = db.Column(db.Integer)
    gb = db.Column(db.Integer)
    fb = db.Column(db.Integer)
    ab = db.Column(db.Integer)
    tb = db.Column(db.Integer)
    sh = db.Column(db.Integer)
    sf = db.Column(db.Integer)
    ta = db.Column(db.Integer)
    sa = db.Column(db.Integer)
    da = db.Column(db.Integer)
    ci = db.Column(db.Integer)
    era = db.Column(db.Numeric(5, 2))
    avg = db.Column(db.Numeric(4, 3))
    obp = db.Column(db.Numeric(4, 3))
    slg = db.Column(db.Numeric(4, 3))
    ops = db.Column(db.Numeric(4, 3))
    whip = db.Column(db.Numeric(4, 2))
    h9 = db.Column(db.Numeric(5, 2))
    hr9 = db.Column(db.Numeric(5, 2))
    bb9 = db.Column(db.Numeric(5, 2))
    k9 = db.Column(db.Numeric(5, 2))
    r9 = db.Column(db.Numeric(5, 2))
    kbb = db.Column(db.Numeric(5, 2))
    babip = db.Column(db.Numeric(4, 3))
    fip = db.Column(db.Numeric(5, 2))
    cgp = db.Column(db.Numeric(4, 3))
    qsp = db.Column(db.Numeric(4, 3))
    winp = db.Column(db.Numeric(4, 3))
    rsg = db.Column(db.Numeric(5, 2))
    svp = db.Column(db.Numeric(4, 3))
    bsvp = db.Column(db.Numeric(4, 3))
    gfp = db.Column(db.Numeric(4, 3))
    gbfbp = db.Column(db.Numeric(5, 2))
    ws = db.Column(db.Integer)

    # Relationships
    team = db.relationship('Team', foreign_keys=[team_id])
    league = db.relationship('League', foreign_keys=[league_id])

    def __repr__(self):
        return f"<TeamPitchingStats(team={self.team_id}, year={self.year})>"
