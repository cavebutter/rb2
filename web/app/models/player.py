"""Player models for core player data, status, ratings, and stats."""
from sqlalchemy.ext.hybrid import hybrid_property
#from app.extensions import db
#from app.models.base import BaseModel, TimestampMixin, CacheableMixin
from ..extensions import db
from ..models.base import BaseModel, TimestampMixin, CacheableMixin


class Player(BaseModel, TimestampMixin, CacheableMixin):
    """Core player biographical information.

    Maps to players_core table. This is immutable biographical data
    (birthdate, draft info, etc.) that doesn't change over time.

    Use Player.current_status for seasonally-changing data like team/position.
    """
    __tablename__ = 'players_core'

    # Primary Key
    player_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))
    nick_name = db.Column(db.String(50))
    date_of_birth = db.Column(db.Date)
    height = db.Column(db.SmallInteger)
    weight = db.Column(db.SmallInteger)
    bats = db.Column(db.SmallInteger)  # 0=R, 1=L, 2=S
    throws = db.Column(db.SmallInteger)  # 0=R, 1=L
    person_type = db.Column(db.SmallInteger)

    # Foreign Keys
    city_of_birth_id = db.Column(db.Integer, db.ForeignKey('cities.city_id'))
    nation_id = db.Column(db.Integer, db.ForeignKey('nations.nation_id'))
    second_nation_id = db.Column(db.Integer, db.ForeignKey('nations.nation_id'))

    # Language support (OOTP stores up to 2 languages)
    language_ids0 = db.Column(db.Integer)
    language_ids1 = db.Column(db.Integer)

    # Historical data
    historical_id = db.Column(db.String(50))
    historical_team_id = db.Column(db.String(50))

    # College/School
    college = db.Column(db.SmallInteger)
    school = db.Column(db.String(100))
    commit_school = db.Column(db.String(100))

    # Draft Info
    acquired = db.Column(db.String(50))
    acquired_date = db.Column(db.Date)
    draft_year = db.Column(db.SmallInteger)
    draft_round = db.Column(db.SmallInteger)
    draft_supplemental = db.Column(db.SmallInteger)
    draft_pick = db.Column(db.SmallInteger)
    draft_overall_pick = db.Column(db.SmallInteger)
    draft_eligible = db.Column(db.SmallInteger)
    draft_league_id = db.Column(db.Integer)
    draft_team_id = db.Column(db.Integer)
    hsc_status = db.Column(db.SmallInteger)
    redshirt = db.Column(db.SmallInteger)
    picked_in_draft = db.Column(db.SmallInteger)

    # Timestamps inherited from TimestampMixin
    # created_at, updated_at

    # ===== RELATIONSHIPS =====

    # Many-to-One: Player -> City (lazy='joined' because we often need it)
    city_of_birth = db.relationship(
        'City',
        foreign_keys=[city_of_birth_id],
        lazy='joined'
    )

    # Many-to-One: Player -> Nation (lazy='joined' because small reference table)
    nation = db.relationship(
        'Nation',
        foreign_keys=[nation_id],
        lazy='joined'
    )

    second_nation = db.relationship(
        'Nation',
        foreign_keys=[second_nation_id],
        lazy='joined'
    )

    # One-to-One: Player -> PlayerCurrentStatus (lazy='joined' - always need current team/position)
    current_status = db.relationship(
        'PlayerCurrentStatus',
        uselist=False,  # One-to-one relationship
        back_populates='player',
        lazy='joined'
    )

    # One-to-Many: Player -> BattingStats (lazy='dynamic' - might be 20+ years of stats)
    # Returns a query object so you can filter: player.batting_stats.filter_by(year=2023)
    # NOTE: We might move this to a service layer instead for better control
    batting_stats = db.relationship(
        'PlayerBattingStats',
        back_populates='player',
        lazy='dynamic'
    )

    # One-to-Many: Player -> PitchingStats (lazy='dynamic')
    pitching_stats = db.relationship(
        'PlayerPitchingStats',
        back_populates='player',
        lazy='dynamic'
    )

    # One-to-One: Player -> BattingRatings
    batting_ratings = db.relationship(
        'PlayerBattingRatings',
        uselist=False,
        back_populates='player',
        lazy='joined'
    )

    # One-to-One: Player -> PitchingRatings
    pitching_ratings = db.relationship(
        'PlayerPitchingRatings',
        uselist=False,
        back_populates='player',
        lazy='joined'
    )

    # One-to-One: Player -> FieldingRatings
    fielding_ratings = db.relationship(
        'PlayerFieldingRatings',
        uselist=False,
        back_populates='player',
        lazy='joined'
    )

    # ===== HYBRID PROPERTIES =====
    # These work in both Python and SQL queries

    @hybrid_property
    def full_name(self):
        """Full name with optional nickname."""
        if self.nick_name:
            return f"{self.first_name} '{self.nick_name}' {self.last_name}"
        return f"{self.first_name} {self.last_name}"

    @hybrid_property
    def display_name(self):
        """Name for display (first + last only)."""
        return f"{self.first_name} {self.last_name}"

    @hybrid_property
    def age(self):
        """Calculate current age from date of birth using game year.

        Uses the current season year from the league, not real-world date.
        Age is calculated as of June 30th (mid-season).
        """
        if not self.date_of_birth:
            return None

        from datetime import date
        from app.models.league import League

        # Get current game year from league
        league = League.query.first()
        if not league or not league.season_year:
            # Fallback to real-world date if no league data
            current_year = date.today().year
        else:
            current_year = league.season_year

        # Calculate age as of June 30th (mid-season)
        mid_season = date(current_year, 6, 30)
        return mid_season.year - self.date_of_birth.year - (
            (mid_season.month, mid_season.day) < (self.date_of_birth.month, self.date_of_birth.day)
        )

    @hybrid_property
    def bats_display(self):
        """Human-readable batting hand."""
        bats_map = {0: 'R', 1: 'L', 2: 'S'}
        return bats_map.get(self.bats, 'Unknown')

    @hybrid_property
    def throws_display(self):
        """Human-readable throwing hand."""
        throws_map = {0: 'R', 1: 'L'}
        return throws_map.get(self.throws, 'Unknown')

    @hybrid_property
    def height_display(self):
        """Convert height from centimeters to feet and inches.

        Height is stored in centimeters in the database.
        Returns formatted string like "5' 10\"" or None if height not set.
        """
        if not self.height:
            return None

        # Convert cm to inches
        total_inches = self.height / 2.54
        feet = int(total_inches // 12)
        inches = int(round(total_inches % 12))

        return f"{feet}' {inches}\""

    # ===== HELPER METHODS =====

    def get_career_stats(self, stat_type='batting'):
        """Get career totals for batting or pitching.

        Args:
            stat_type: 'batting' or 'pitching'

        Returns:
            Dictionary of career totals
        """
        # This would be implemented in a service layer for better caching
        # Just a placeholder to show the pattern
        if stat_type == 'batting':
            return self.batting_stats.filter_by(split_id=1).all()  # Career totals
        else:
            return self.pitching_stats.filter_by(split_id=1).all()

    def __repr__(self):
        """Override base repr to show name instead of just ID."""
        return f"<Player({self.player_id}: {self.display_name})>"


class PlayerCurrentStatus(BaseModel):
    """Current player status (team, position, etc.).

    Maps to players_current_status table. This data changes
    seasonally or when players are traded/signed.

    Note: This table doesn't use TimestampMixin - it has its own
    last_updated column instead of created_at/updated_at.
    """
    __tablename__ = 'players_current_status'

    # Primary Key (also FK to Player)
    player_id = db.Column(db.Integer, db.ForeignKey('players_core.player_id'), primary_key=True)

    # Current Team/League
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))
    organization_id = db.Column(db.Integer)

    # Position/Role
    position = db.Column(db.SmallInteger)
    role = db.Column(db.SmallInteger)
    uniform_number = db.Column(db.SmallInteger)

    # Status Flags
    age = db.Column(db.SmallInteger)
    retired = db.Column(db.SmallInteger)
    free_agent = db.Column(db.SmallInteger)
    hall_of_fame = db.Column(db.SmallInteger)
    inducted = db.Column(db.SmallInteger)
    turned_coach = db.Column(db.SmallInteger)
    hidden = db.Column(db.SmallInteger)

    # Last Team/League (for retired players)
    last_league_id = db.Column(db.Integer)
    last_team_id = db.Column(db.Integer)
    last_organization_id = db.Column(db.Integer)

    # Experience
    experience = db.Column(db.SmallInteger)

    # Popularity
    rust = db.Column(db.SmallInteger)
    local_pop = db.Column(db.SmallInteger)
    national_pop = db.Column(db.SmallInteger)

    # Loan/Draft
    draft_protected = db.Column(db.SmallInteger)
    on_loan = db.Column(db.SmallInteger)
    loan_league_id = db.Column(db.Integer)
    loan_team_id = db.Column(db.Integer)

    # Season tracking
    season_year = db.Column(db.Integer)
    last_updated = db.Column(db.DateTime)

    # ===== RELATIONSHIPS =====

    # One-to-One: PlayerCurrentStatus -> Player (reverse of player.current_status)
    player = db.relationship(
        'Player',
        back_populates='current_status'
    )

    # Many-to-One: Player -> Team (lazy='joined' - always need current team)
    team = db.relationship(
        'Team',
        foreign_keys=[team_id],
        lazy='joined'
    )

    # Many-to-One: Player -> League
    league = db.relationship(
        'League',
        foreign_keys=[league_id],
        lazy='joined'
    )

    @hybrid_property
    def is_active(self):
        """Check if player is currently active."""
        return self.retired == 0 and not self.turned_coach

    @hybrid_property
    def position_display(self):
        """Human-readable position."""
        # This would use a position mapping dict
        # Placeholder for now
        position_map = {
            1: 'P', 2: 'C', 3: '1B', 4: '2B', 5: '3B',
            6: 'SS', 7: 'LF', 8: 'CF', 9: 'RF', 10: 'DH'
        }
        return position_map.get(self.position, 'Unknown')

    def __repr__(self):
        team_name = self.team.abbreviation if self.team else 'FA'
        return f"<PlayerCurrentStatus({self.player_id}: {team_name})>"