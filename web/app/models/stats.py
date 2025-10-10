"""Player statistics models - batting, pitching, and fielding stats.

These are high-volume tables that change frequently as games are played.
Include TimestampMixin to track when stats were last calculated.
"""
from sqlalchemy.ext.hybrid import hybrid_property
from ..extensions import db
from ..models.base import BaseModel, TimestampMixin


class PlayerBattingStats(BaseModel):
    """Player batting statistics by year/team/split.

    Maps to players_career_batting_stats table. Composite primary key.
    Includes both raw stats and pre-calculated advanced metrics.

    Note: This table doesn't use TimestampMixin - it has its own
    last_updated column instead of created_at/updated_at.
    """
    __tablename__ = 'players_career_batting_stats'

    # Composite Primary Key
    player_id = db.Column(db.Integer, db.ForeignKey('players_core.player_id'), primary_key=True)
    year = db.Column(db.SmallInteger, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)
    split_id = db.Column(db.SmallInteger, primary_key=True)  # 1=overall, 2=vs LHP, 3=vs RHP, etc.
    stint = db.Column(db.SmallInteger, primary_key=True)  # For players traded mid-season

    # Foreign Keys
    game_id = db.Column(db.Integer)
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    sub_league_id = db.Column(db.Integer)
    level_id = db.Column(db.SmallInteger)

    # Position
    position = db.Column(db.SmallInteger)

    # Plate Appearance Stats
    pa = db.Column(db.SmallInteger)  # Plate appearances
    ab = db.Column(db.SmallInteger)  # At bats
    h = db.Column(db.SmallInteger)   # Hits
    d = db.Column(db.SmallInteger)   # Doubles (renamed from 2b)
    t = db.Column(db.SmallInteger)   # Triples (renamed from 3b)
    hr = db.Column(db.SmallInteger)  # Home runs
    r = db.Column(db.SmallInteger)   # Runs
    rbi = db.Column(db.SmallInteger) # RBI
    bb = db.Column(db.SmallInteger)  # Walks
    ibb = db.Column(db.SmallInteger) # Intentional walks
    k = db.Column(db.SmallInteger)   # Strikeouts
    hp = db.Column(db.SmallInteger)  # Hit by pitch
    sh = db.Column(db.SmallInteger)  # Sacrifice hits
    sf = db.Column(db.SmallInteger)  # Sacrifice flies
    ci = db.Column(db.SmallInteger)  # Catcher interference

    # Baserunning
    sb = db.Column(db.SmallInteger)  # Stolen bases
    cs = db.Column(db.SmallInteger)  # Caught stealing

    # Other
    gdp = db.Column(db.SmallInteger)  # Grounded into double play
    g = db.Column(db.SmallInteger)    # Games
    gs = db.Column(db.SmallInteger)   # Games started
    pitches_seen = db.Column(db.SmallInteger)

    # Advanced Stats (pre-calculated during ETL)
    batting_average = db.Column(db.Numeric(4, 3))      # AVG
    on_base_percentage = db.Column(db.Numeric(4, 3))   # OBP
    slugging_percentage = db.Column(db.Numeric(4, 3))  # SLG
    ops = db.Column(db.Numeric(4, 3))                   # OPS
    iso = db.Column(db.Numeric(4, 3))                   # Isolated power
    babip = db.Column(db.Numeric(4, 3))                 # Batting average on balls in play
    woba = db.Column(db.Numeric(4, 3))                  # Weighted on-base average
    wrc = db.Column(db.Integer)                         # Weighted runs created
    wrc_plus = db.Column(db.Integer)                    # WRC+ (100 = league average)
    wraa = db.Column(db.Numeric(6, 1))                  # Weighted runs above average

    # Context-dependent stats
    wpa = db.Column(db.Numeric(8, 3))  # Win probability added
    ubr = db.Column(db.Numeric(8, 3))  # Ultimate base running
    war = db.Column(db.Numeric(8, 3))  # Wins above replacement

    # Metadata
    constants_version = db.Column(db.Integer)  # Which league constants were used
    last_updated = db.Column(db.DateTime)

    # ===== RELATIONSHIPS =====

    # Many-to-One: BattingStats -> Player
    player = db.relationship(
        'Player',
        back_populates='batting_stats',
        lazy='joined'
    )

    # Many-to-One: BattingStats -> Team
    team = db.relationship(
        'Team',
        lazy='joined'
    )

    # Many-to-One: BattingStats -> League
    league = db.relationship(
        'League',
        lazy='joined'
    )

    # ===== HYBRID PROPERTIES =====

    @hybrid_property
    def position_display(self):
        """Human-readable position abbreviation."""
        position_map = {
            1: 'P', 2: 'C', 3: '1B', 4: '2B', 5: '3B',
            6: 'SS', 7: 'LF', 8: 'CF', 9: 'RF', 10: 'DH'
        }
        return position_map.get(self.position, 'Unknown')

    @hybrid_property
    def singles(self):
        """Calculate singles (hits - doubles - triples - home runs)."""
        if not self.h:
            return 0
        return self.h - (self.d or 0) - (self.t or 0) - (self.hr or 0)

    @hybrid_property
    def total_bases(self):
        """Calculate total bases."""
        if not self.h:
            return 0
        return self.singles + (2 * (self.d or 0)) + (3 * (self.t or 0)) + (4 * (self.hr or 0))

    @hybrid_property
    def is_qualified(self):
        """Check if player has enough PA to qualify for leaderboards (3.1 PA per team game)."""
        # Assuming 162 game season, qualified = 502 PA
        return self.pa and self.pa >= 502

    def __repr__(self):
        avg = f"{float(self.batting_average):.3f}" if self.batting_average else ".000"
        return f"<PlayerBattingStats({self.player_id}, {self.year}, {self.team_id}: {avg})>"


class PlayerPitchingStats(BaseModel):
    """Player pitching statistics by year/team/split.

    Maps to players_career_pitching_stats table. Composite primary key.
    Includes both raw stats and pre-calculated advanced metrics.

    Note: This table doesn't use TimestampMixin - it has its own
    last_updated column instead of created_at/updated_at.
    """
    __tablename__ = 'players_career_pitching_stats'

    # Composite Primary Key
    player_id = db.Column(db.Integer, db.ForeignKey('players_core.player_id'), primary_key=True)
    year = db.Column(db.SmallInteger, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)
    split_id = db.Column(db.SmallInteger, primary_key=True)
    stint = db.Column(db.SmallInteger, primary_key=True)

    # Foreign Keys
    game_id = db.Column(db.Integer)
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    sub_league_id = db.Column(db.SmallInteger)
    level_id = db.Column(db.SmallInteger)

    # Innings Pitched (stored as outs)
    ip = db.Column(db.SmallInteger)   # Innings pitched (whole number)
    ipf = db.Column(db.SmallInteger)  # IP fractional part (0, 1, or 2 outs)
    outs = db.Column(db.SmallInteger) # Total outs recorded

    # Batters Faced Stats
    bf = db.Column(db.SmallInteger)   # Batters faced
    ab = db.Column(db.SmallInteger)   # At bats against
    ha = db.Column(db.SmallInteger)   # Hits allowed
    sa = db.Column(db.SmallInteger)   # Singles allowed
    da = db.Column(db.SmallInteger)   # Doubles allowed
    ta = db.Column(db.SmallInteger)   # Triples allowed
    hra = db.Column(db.SmallInteger)  # Home runs allowed
    tb = db.Column(db.SmallInteger)   # Total bases allowed
    k = db.Column(db.SmallInteger)    # Strikeouts
    bb = db.Column(db.SmallInteger)   # Walks
    iw = db.Column(db.SmallInteger)   # Intentional walks
    hp = db.Column(db.SmallInteger)   # Hit batters
    sh = db.Column(db.SmallInteger)   # Sacrifice hits
    sf = db.Column(db.SmallInteger)   # Sacrifice flies

    # Runs
    r = db.Column(db.SmallInteger)    # Runs
    er = db.Column(db.SmallInteger)   # Earned runs
    rs = db.Column(db.SmallInteger)   # Run support
    ra = db.Column(db.SmallInteger)   # Runs allowed

    # Decisions
    g = db.Column(db.SmallInteger)    # Games
    gs = db.Column(db.SmallInteger)   # Games started
    gf = db.Column(db.SmallInteger)   # Games finished
    w = db.Column(db.SmallInteger)    # Wins
    l = db.Column(db.SmallInteger)    # Losses
    s = db.Column(db.SmallInteger)    # Saves
    hld = db.Column(db.SmallInteger)  # Holds
    bs = db.Column(db.SmallInteger)   # Blown saves
    svo = db.Column(db.SmallInteger)  # Save opportunities

    # Complete Games
    cg = db.Column(db.SmallInteger)   # Complete games
    sho = db.Column(db.SmallInteger)  # Shutouts
    qs = db.Column(db.SmallInteger)   # Quality starts

    # Batted Ball Types
    gb = db.Column(db.SmallInteger)   # Ground balls
    fb = db.Column(db.SmallInteger)   # Fly balls

    # Other
    pi = db.Column(db.SmallInteger)   # Pitches thrown
    wp = db.Column(db.SmallInteger)   # Wild pitches
    bk = db.Column(db.SmallInteger)   # Balks
    dp = db.Column(db.SmallInteger)   # Double plays induced
    ci = db.Column(db.SmallInteger)   # Catcher interference
    sb = db.Column(db.SmallInteger)   # Stolen bases allowed
    cs = db.Column(db.SmallInteger)   # Caught stealing

    # Inherited Runners
    ir = db.Column(db.Numeric(8, 3))  # Inherited runners
    irs = db.Column(db.Numeric(8, 3)) # Inherited runners scored

    # Advanced Stats (pre-calculated during ETL)
    era = db.Column(db.Numeric(5, 2))       # Earned run average
    whip = db.Column(db.Numeric(4, 2))      # Walks + hits per inning
    k9 = db.Column(db.Numeric(4, 1))        # Strikeouts per 9 innings
    bb9 = db.Column(db.Numeric(4, 1))       # Walks per 9 innings
    hr9 = db.Column(db.Numeric(4, 1))       # Home runs per 9 innings
    h9 = db.Column(db.Numeric(4, 1))        # Hits per 9 innings
    fip = db.Column(db.Numeric(5, 2))       # Fielding independent pitching
    xfip = db.Column(db.Numeric(5, 2))      # Expected FIP
    babip = db.Column(db.Numeric(4, 3))     # Batting average on balls in play
    era_plus = db.Column(db.Integer)        # ERA+ (100 = league average)
    era_minus = db.Column(db.Integer)       # ERA- (100 = league average, lower is better)
    fip_plus = db.Column(db.Integer)        # FIP+
    fip_minus = db.Column(db.Integer)       # FIP-

    # Context-dependent stats
    wpa = db.Column(db.Numeric(8, 3))  # Win probability added
    li = db.Column(db.Numeric(8, 3))   # Leverage index
    war = db.Column(db.Numeric(8, 3))  # Wins above replacement

    # Metadata
    constants_version = db.Column(db.Integer)
    last_updated = db.Column(db.DateTime)

    # ===== RELATIONSHIPS =====

    # Many-to-One: PitchingStats -> Player
    player = db.relationship(
        'Player',
        back_populates='pitching_stats',
        lazy='joined'
    )

    # Many-to-One: PitchingStats -> Team
    team = db.relationship(
        'Team',
        lazy='joined'
    )

    # Many-to-One: PitchingStats -> League
    league = db.relationship(
        'League',
        lazy='joined'
    )

    # ===== HYBRID PROPERTIES =====

    @hybrid_property
    def innings_pitched_display(self):
        """Format IP as X.Y (e.g., 200.1 for 200 and 1/3 innings)."""
        if self.ip is None:
            return "0.0"
        fractional = self.ipf or 0
        return f"{self.ip}.{fractional}"

    @hybrid_property
    def is_qualified(self):
        """Check if pitcher has enough IP to qualify (1 IP per team game = 162 IP)."""
        total_outs = (self.ip or 0) * 3 + (self.ipf or 0)
        innings = total_outs / 3.0
        return innings >= 162

    @hybrid_property
    def k_bb_ratio(self):
        """Calculate K/BB ratio."""
        if not self.bb or self.bb == 0:
            return None
        return float(self.k or 0) / float(self.bb)

    def __repr__(self):
        era_str = f"{float(self.era):.2f}" if self.era else "-.--"
        return f"<PlayerPitchingStats({self.player_id}, {self.year}, {self.team_id}: {era_str} ERA)>"


class PlayerFieldingStats(BaseModel):
    """Player fielding statistics by year/team/position.

    Maps to players_career_fielding_stats table. Composite primary key.
    Includes defensive metrics and zone ratings.

    Note: This table doesn't use TimestampMixin - it has its own
    last_updated column instead of created_at/updated_at.
    """
    __tablename__ = 'players_career_fielding_stats'

    # Composite Primary Key (position instead of split/stint)
    player_id = db.Column(db.Integer, db.ForeignKey('players_core.player_id'), primary_key=True)
    year = db.Column(db.SmallInteger, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)
    position = db.Column(db.SmallInteger, primary_key=True)  # 1=P, 2=C, 3=1B, etc.

    # Foreign Keys
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    level_id = db.Column(db.SmallInteger)
    split_id = db.Column(db.SmallInteger)

    # Games/Innings
    g = db.Column(db.SmallInteger)    # Games
    gs = db.Column(db.SmallInteger)   # Games started
    ip = db.Column(db.SmallInteger)   # Innings played (whole number)
    ipf = db.Column(db.SmallInteger)  # IP fractional part

    # Fielding Chances
    tc = db.Column(db.SmallInteger)   # Total chances
    po = db.Column(db.SmallInteger)   # Putouts
    a = db.Column(db.SmallInteger)    # Assists
    e = db.Column(db.SmallInteger)    # Errors
    er = db.Column(db.SmallInteger)   # Errors (reached base)

    # Double/Triple Plays
    dp = db.Column(db.SmallInteger)   # Double plays turned
    tp = db.Column(db.SmallInteger)   # Triple plays turned

    # Catcher-specific
    pb = db.Column(db.SmallInteger)   # Passed balls
    sba = db.Column(db.SmallInteger)  # Stolen bases allowed
    rto = db.Column(db.SmallInteger)  # Runners thrown out

    # Advanced Fielding
    plays = db.Column(db.SmallInteger)       # Plays made
    plays_base = db.Column(db.SmallInteger)  # Base plays
    roe = db.Column(db.SmallInteger)         # Reached on error

    # Zone Rating Opportunities (by difficulty level 0-5)
    opps_0 = db.Column(db.SmallInteger)
    opps_made_0 = db.Column(db.SmallInteger)
    opps_1 = db.Column(db.SmallInteger)
    opps_made_1 = db.Column(db.SmallInteger)
    opps_2 = db.Column(db.SmallInteger)
    opps_made_2 = db.Column(db.SmallInteger)
    opps_3 = db.Column(db.SmallInteger)
    opps_made_3 = db.Column(db.SmallInteger)
    opps_4 = db.Column(db.SmallInteger)
    opps_made_4 = db.Column(db.SmallInteger)
    opps_5 = db.Column(db.SmallInteger)
    opps_made_5 = db.Column(db.SmallInteger)

    # Pre-calculated Advanced Stats
    zr = db.Column(db.Numeric(4, 3))                # Zone rating
    fielding_percentage = db.Column(db.Numeric(4, 3))  # Fielding percentage
    range_factor = db.Column(db.Numeric(4, 2))      # Range factor
    zone_rating = db.Column(db.Numeric(4, 3))       # Zone rating

    # ===== RELATIONSHIPS =====

    # Many-to-One: FieldingStats -> Player
    player = db.relationship(
        'Player',
        lazy='joined'
    )

    # Many-to-One: FieldingStats -> Team
    team = db.relationship(
        'Team',
        lazy='joined'
    )

    # Many-to-One: FieldingStats -> League
    league = db.relationship(
        'League',
        lazy='joined'
    )

    # ===== HYBRID PROPERTIES =====

    @hybrid_property
    def position_display(self):
        """Human-readable position."""
        position_map = {
            1: 'P', 2: 'C', 3: '1B', 4: '2B', 5: '3B',
            6: 'SS', 7: 'LF', 8: 'CF', 9: 'RF', 10: 'DH'
        }
        return position_map.get(self.position, 'Unknown')

    @hybrid_property
    def total_opportunities(self):
        """Total fielding opportunities made across all zones."""
        total = 0
        for i in range(6):
            total += getattr(self, f'opps_made_{i}', 0) or 0
        return total

    def __repr__(self):
        fpct = f"{float(self.fielding_percentage):.3f}" if self.fielding_percentage else ".000"
        return f"<PlayerFieldingStats({self.player_id}, {self.year}, {self.position_display}: {fpct})>"
