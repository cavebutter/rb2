"""League models for league hierarchy (leagues, sub-leagues, divisions).

OOTP uses a 3-level hierarchy:
- League (e.g., MLB, NPB)
  - Sub-League (e.g., American League, National League)
    - Division (e.g., AL East, NL West)
"""
from sqlalchemy.ext.hybrid import hybrid_property
from ..extensions import db
from ..models.base import BaseModel, CacheableMixin


class League(BaseModel, CacheableMixin):
    """Baseball league (MLB, Minor Leagues, NPB, etc.).

    Maps to leagues table. Top level of league hierarchy.
    """
    __tablename__ = 'leagues'

    # Primary Key
    league_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    name = db.Column(db.String(100), nullable=False)
    abbr = db.Column(db.String(10))
    logo_file_name = db.Column(db.String(200))

    # Foreign Keys
    nation_id = db.Column(db.Integer, db.ForeignKey('nations.nation_id'))
    language_id = db.Column(db.Integer, db.ForeignKey('languages.language_id'))
    parent_league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))

    # League state/config
    league_state = db.Column(db.SmallInteger)
    league_level = db.Column(db.SmallInteger)  # 1=MLB, 2=AAA, etc.

    # Current game state
    season_year = db.Column(db.Integer)
    game_date = db.Column(db.Date)  # Current in-game date
    current_date_year = db.Column(db.Integer)

    # ===== RELATIONSHIPS =====

    # Many-to-One: League -> Nation
    nation = db.relationship(
        'Nation',
        lazy='joined'
    )

    # Many-to-One: League -> Language
    language = db.relationship(
        'Language',
        lazy='joined'
    )

    # Self-referential: League -> Parent League (for minor leagues)
    parent_league = db.relationship(
        'League',
        remote_side=[league_id],
        lazy='joined'
    )

    # One-to-Many: League -> Sub-Leagues
    sub_leagues = db.relationship(
        'SubLeague',
        back_populates='league',
        lazy='selectin'
    )

    # One-to-Many: League -> Teams
    teams = db.relationship(
        'Team',
        back_populates='league',
        lazy='dynamic'  # Can be many teams
    )

    @hybrid_property
    def is_mlb(self):
        """Check if this is MLB (league_level = 1)."""
        return self.league_level == 1

    def __repr__(self):
        return f"<League({self.league_id}: {self.abbr or self.name})>"


class SubLeague(BaseModel, CacheableMixin):
    """Sub-league within a league (e.g., American League, National League).

    Maps to sub_leagues table. Composite primary key.
    """
    __tablename__ = 'sub_leagues'

    # Composite Primary Key
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'), primary_key=True)
    sub_league_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    name = db.Column(db.String(50), nullable=False)
    abbr = db.Column(db.String(10))
    gender = db.Column(db.Integer)
    designated_hitter = db.Column(db.SmallInteger)

    # ===== RELATIONSHIPS =====

    # Many-to-One: SubLeague -> League
    league = db.relationship(
        'League',
        back_populates='sub_leagues',
        lazy='joined'
    )

    # One-to-Many: SubLeague -> Divisions
    divisions = db.relationship(
        'Division',
        back_populates='sub_league',
        lazy='selectin'
    )

    @hybrid_property
    def has_dh(self):
        """Check if this sub-league uses designated hitter."""
        return self.designated_hitter == 1

    def __repr__(self):
        return f"<SubLeague({self.league_id}, {self.sub_league_id}: {self.abbr or self.name})>"


class Division(BaseModel, CacheableMixin):
    """Division within a sub-league (e.g., AL East, NL West).

    Maps to divisions table. Composite primary key (3 levels).
    """
    __tablename__ = 'divisions'

    # Composite Primary Key (3 levels!)
    league_id = db.Column(db.Integer, primary_key=True)
    sub_league_id = db.Column(db.Integer, primary_key=True)
    division_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    name = db.Column(db.String(50), nullable=False)
    gender = db.Column(db.Integer)

    # Foreign key constraint to sub_leagues
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['league_id', 'sub_league_id'],
            ['sub_leagues.league_id', 'sub_leagues.sub_league_id']
        ),
    )

    # ===== RELATIONSHIPS =====

    # Many-to-One: Division -> SubLeague
    sub_league = db.relationship(
        'SubLeague',
        back_populates='divisions',
        lazy='joined'
    )

    # One-to-Many: Division -> Teams
    # Teams reference division via team_relations table, so this is view-only
    teams = db.relationship(
        'Team',
        foreign_keys='[Team.league_id, Team.sub_league_id, Team.division_id]',
        primaryjoin='and_(Division.league_id == foreign(Team.league_id), '
                    'Division.sub_league_id == foreign(Team.sub_league_id), '
                    'Division.division_id == foreign(Team.division_id))',
        lazy='dynamic',
        viewonly=True
    )

    def __repr__(self):
        return f"<Division({self.league_id}, {self.sub_league_id}, {self.division_id}: {self.name})>"