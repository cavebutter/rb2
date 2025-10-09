"""Team models for teams, rosters, and standings."""
from sqlalchemy.ext.hybrid import hybrid_property
from ..extensions import db
from ..models.base import BaseModel, CacheableMixin


class Team(BaseModel, CacheableMixin):
    """Baseball team (MLB, Minor League, or historical).

    Maps to teams table. Includes organizational hierarchy
    (parent teams and affiliates), league placement, and roster.
    """
    __tablename__ = 'teams'

    # Primary Key
    team_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    name = db.Column(db.String(100), nullable=False)
    abbr = db.Column(db.String(10))
    nickname = db.Column(db.String(50))
    logo_file_name = db.Column(db.String(200))
    historical_id = db.Column(db.String(50))

    # Location
    city_id = db.Column(db.Integer, db.ForeignKey('cities.city_id'))
    park_id = db.Column(db.Integer, db.ForeignKey('parks.park_id'))
    nation_id = db.Column(db.Integer, db.ForeignKey('nations.nation_id'))

    # League Hierarchy
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    sub_league_id = db.Column(db.Integer)
    division_id = db.Column(db.Integer)

    # Organizational Hierarchy (MLB parent -> AAA/AA/A affiliates)
    parent_team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    level = db.Column(db.Integer)  # 1=MLB, 2=AAA, 3=AA, etc.

    # Team Management
    prevent_any_moves = db.Column(db.SmallInteger)
    human_team = db.Column(db.SmallInteger)
    human_id = db.Column(db.Integer)
    gender = db.Column(db.Integer)
    allstar_team = db.Column(db.SmallInteger)

    # Colors (stored as hex color IDs)
    background_color_id = db.Column(db.String(8))
    text_color_id = db.Column(db.String(8))
    ballcaps_main_color_id = db.Column(db.String(8))
    ballcaps_visor_color_id = db.Column(db.String(8))
    jersey_main_color_id = db.Column(db.String(8))
    jersey_away_color_id = db.Column(db.String(8))
    jersey_secondary_color_id = db.Column(db.String(8))
    jersey_pin_stripes_color_id = db.Column(db.String(8))

    # ===== RELATIONSHIPS =====

    # Many-to-One: Team -> City (lazy='joined' - always need location)
    city = db.relationship(
        'City',
        foreign_keys=[city_id],
        lazy='joined',
        back_populates='teams'
    )

    # Many-to-One: Team -> Park (lazy='joined' - stadium info always needed)
    park = db.relationship(
        'Park',
        foreign_keys=[park_id],
        lazy='joined',
        back_populates='teams'
    )

    # Many-to-One: Team -> Nation
    nation = db.relationship(
        'Nation',
        foreign_keys=[nation_id],
        lazy='joined'
    )

    # Many-to-One: Team -> League (lazy='joined' - always need league)
    league = db.relationship(
        'League',
        foreign_keys=[league_id],
        lazy='joined',
        back_populates='teams'
    )

    # Self-referential: MLB Parent Team -> Minor League Affiliates
    # This is the PARENT side (one MLB team has many affiliates)
    affiliates = db.relationship(
        'Team',
        backref=db.backref('parent_team', remote_side=[team_id]),
        foreign_keys=[parent_team_id],
        lazy='selectin'  # Load all affiliates in one query
    )

    # One-to-Many: Team -> Players (current roster)
    # Using lazy='dynamic' because rosters can be 40+ players
    roster = db.relationship(
        'Player',
        secondary='players_current_status',
        primaryjoin='Team.team_id == PlayerCurrentStatus.team_id',
        secondaryjoin='PlayerCurrentStatus.player_id == Player.player_id',
        lazy='dynamic',
        viewonly=True  # Read-only, managed through PlayerCurrentStatus
    )

    # One-to-One: Team -> TeamRecord (current season record)
    record = db.relationship(
        'TeamRecord',
        uselist=False,
        back_populates='team',
        lazy='joined'
    )

    # ===== HYBRID PROPERTIES =====

    @hybrid_property
    def full_name(self):
        """Full team name with city."""
        return f"{self.city.name if self.city else 'Unknown'} {self.name}"

    @hybrid_property
    def display_name(self):
        """Team name for display (just team name, no city)."""
        return self.name

    @hybrid_property
    def is_mlb(self):
        """Check if this is an MLB-level team."""
        return self.level == 1

    @hybrid_property
    def is_human_controlled(self):
        """Check if team is controlled by a human player."""
        return self.human_team == 1

    # ===== HELPER METHODS =====

    def get_active_roster(self):
        """Get active roster (non-retired, non-injured players).

        Returns query object for further filtering.
        """
        from .player import PlayerCurrentStatus  # Import here to avoid circular import
        return self.roster.join(PlayerCurrentStatus).filter(
            PlayerCurrentStatus.retired == 0
        )

    def get_roster_by_position(self, position):
        """Get roster filtered by position.

        Args:
            position: Position ID (1=P, 2=C, etc.)

        Returns:
            Query object
        """
        from .player import PlayerCurrentStatus
        return self.roster.join(PlayerCurrentStatus).filter(
            PlayerCurrentStatus.position == position
        )

    def __repr__(self):
        """Override base repr to show team name."""
        return f"<Team({self.team_id}: {self.abbr or self.name})>"


class TeamRecord(BaseModel):
    """Current season standings for a team.

    Maps to team_record table. Updated as games are played.
    """
    __tablename__ = 'team_record'

    # Primary Key (also FK to Team)
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)

    # Record Stats
    g = db.Column(db.SmallInteger)  # Games played
    w = db.Column(db.SmallInteger)  # Wins
    l = db.Column(db.SmallInteger)  # Losses
    t = db.Column(db.SmallInteger)  # Ties
    pos = db.Column(db.SmallInteger)  # Position in standings
    pct = db.Column(db.Numeric(6, 4))  # Winning percentage
    gb = db.Column(db.Numeric(7, 4))  # Games behind
    streak = db.Column(db.SmallInteger)  # Current streak (+ for wins, - for losses)
    magic_number = db.Column(db.SmallInteger)  # Playoff magic number

    # ===== RELATIONSHIPS =====

    # One-to-One: TeamRecord -> Team (reverse of team.record)
    team = db.relationship(
        'Team',
        back_populates='record'
    )

    # ===== HYBRID PROPERTIES =====

    @hybrid_property
    def winning_percentage(self):
        """Calculate winning percentage."""
        if not self.g or self.g == 0:
            return 0.0
        return float(self.w) / float(self.g)

    @hybrid_property
    def games_back_display(self):
        """Format games back for display (e.g., '5.0' or '' for leader)."""
        if self.gb is None or self.gb == 0:
            return ''
        return f"{float(self.gb):.1f}"

    @hybrid_property
    def streak_display(self):
        """Format streak for display (e.g., 'W5' or 'L3')."""
        if not self.streak:
            return ''
        prefix = 'W' if self.streak > 0 else 'L'
        return f"{prefix}{abs(self.streak)}"

    def __repr__(self):
        return f"<TeamRecord({self.team_id}: {self.w}-{self.l}, {self.pct:.3f})>"


class TeamRoster(BaseModel):
    """Team roster association (which players are on which team).

    Maps to team_roster table. Junction table between teams and players.
    """
    __tablename__ = 'team_roster'

    # Composite Primary Key
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'), primary_key=True)
    player_id = db.Column(db.Integer, db.ForeignKey('players_core.player_id'), primary_key=True)

    # Roster List ID (active, 40-man, DL, etc.)
    list_id = db.Column(db.SmallInteger)

    # ===== RELATIONSHIPS =====

    team = db.relationship('Team')
    player = db.relationship('Player')

    @hybrid_property
    def list_name(self):
        """Get human-readable list name."""
        list_map = {
            1: 'Active Roster',
            2: '40-Man Roster',
            3: 'Disabled List',
            4: 'Minor League',
            5: 'Restricted'
        }
        return list_map.get(self.list_id, 'Unknown')

    def __repr__(self):
        return f"<TeamRoster(team={self.team_id}, player={self.player_id}, list={self.list_id})>"