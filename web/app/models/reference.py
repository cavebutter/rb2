"""Reference data models - continents, nations, states, cities, languages, parks.

These are mostly static lookup tables with simple relationships.
No timestamps needed since data rarely changes.
"""
from sqlalchemy.ext.hybrid import hybrid_property
from ..extensions import db
from ..models.base import BaseModel, CacheableMixin


class Continent(BaseModel, CacheableMixin):
    """Continents (top of geographic hierarchy).

    Maps to continents table. Static reference data.
    """
    __tablename__ = 'continents'

    # Primary Key
    continent_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    name = db.Column(db.String(50), nullable=False)
    abbreviation = db.Column(db.String(10))
    demonym = db.Column(db.String(30))
    population = db.Column(db.BigInteger)
    main_language_id = db.Column(db.Integer)

    # ===== RELATIONSHIPS =====

    # One-to-Many: Continent -> Nations
    nations = db.relationship(
        'Nation',
        back_populates='continent',
        lazy='selectin'
    )

    def __repr__(self):
        return f"<Continent({self.continent_id}: {self.name})>"


class Nation(BaseModel, CacheableMixin):
    """Nations/Countries.

    Maps to nations table. Used for player birthplace, team locations, etc.
    """
    __tablename__ = 'nations'

    # Primary Key
    nation_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    name = db.Column(db.String(50), nullable=False)
    short_name = db.Column(db.String(50))
    abbreviation = db.Column(db.String(10))
    demonym = db.Column(db.String(50))
    population = db.Column(db.Integer)
    gender = db.Column(db.Integer)

    # Baseball-specific
    baseball_quality = db.Column(db.Integer)
    quality_total = db.Column(db.Integer)

    # Foreign Keys
    continent_id = db.Column(db.Integer, db.ForeignKey('continents.continent_id'))
    capital_id = db.Column(db.Integer)  # References cities, but can't FK (circular)
    main_language_id = db.Column(db.Integer)

    # Flags
    use_hardcoded_ml_player_origins = db.Column(db.SmallInteger)
    this_is_the_usa = db.Column(db.SmallInteger)

    # ===== RELATIONSHIPS =====

    # Many-to-One: Nation -> Continent
    continent = db.relationship(
        'Continent',
        back_populates='nations',
        lazy='joined'
    )

    # One-to-Many: Nation -> States
    states = db.relationship(
        'State',
        back_populates='nation',
        lazy='selectin'
    )

    # One-to-Many: Nation -> Cities
    cities = db.relationship(
        'City',
        foreign_keys='City.nation_id',
        back_populates='nation',
        lazy='selectin'
    )

    # One-to-Many: Nation -> Teams
    teams = db.relationship(
        'Team',
        foreign_keys='Team.nation_id',
        lazy='dynamic'
    )

    @hybrid_property
    def is_usa(self):
        """Check if this is the USA."""
        return self.this_is_the_usa == 1

    def __repr__(self):
        return f"<Nation({self.nation_id}: {self.abbreviation or self.name})>"


class State(BaseModel, CacheableMixin):
    """States/Provinces within nations.

    Maps to states table. Composite primary key (state_id, nation_id).
    """
    __tablename__ = 'states'

    # Composite Primary Key
    state_id = db.Column(db.Integer, primary_key=True)
    nation_id = db.Column(db.Integer, db.ForeignKey('nations.nation_id'), primary_key=True)

    # Basic Info
    name = db.Column(db.String(50), nullable=False)
    abbreviation = db.Column(db.String(10))
    population = db.Column(db.Integer)
    main_language_id = db.Column(db.Integer)

    # ===== RELATIONSHIPS =====

    # Many-to-One: State -> Nation
    nation = db.relationship(
        'Nation',
        back_populates='states',
        lazy='joined'
    )

    # One-to-Many: State -> Cities
    cities = db.relationship(
        'City',
        foreign_keys='[City.state_id, City.nation_id]',
        primaryjoin='and_(State.state_id == foreign(City.state_id), State.nation_id == foreign(City.nation_id))',
        lazy='selectin',
        viewonly=True
    )

    def __repr__(self):
        return f"<State({self.state_id}, {self.nation_id}: {self.abbreviation or self.name})>"


class City(BaseModel, CacheableMixin):
    """Cities.

    Maps to cities table. Used for player birthplace, team locations, etc.
    """
    __tablename__ = 'cities'

    # Primary Key
    city_id = db.Column(db.Integer, primary_key=True)

    # Foreign Keys
    nation_id = db.Column(db.Integer, db.ForeignKey('nations.nation_id'), nullable=False)
    state_id = db.Column(db.Integer)  # FK to states, but composite key is tricky

    # Basic Info
    name = db.Column(db.String(100), nullable=False)
    abbreviation = db.Column(db.String(50))
    population = db.Column(db.Integer)
    main_language_id = db.Column(db.Integer)

    # ===== RELATIONSHIPS =====

    # Many-to-One: City -> Nation
    nation = db.relationship(
        'Nation',
        back_populates='cities',
        lazy='joined'
    )

    # Many-to-One: City -> State
    # Note: Can't use proper FK because states has composite PK (state_id, nation_id)
    # So we define relationship manually with foreign_keys and primaryjoin
    state = db.relationship(
        'State',
        foreign_keys=[state_id, nation_id],
        primaryjoin='and_(City.state_id == State.state_id, City.nation_id == State.nation_id)',
        lazy='joined',
        viewonly=True  # Can't easily enforce FK constraint
    )

    # One-to-Many: City -> Teams
    teams = db.relationship(
        'Team',
        foreign_keys='Team.city_id',
        back_populates='city',
        lazy='dynamic'
    )

    @hybrid_property
    def full_name(self):
        """Full name with state/nation (e.g., 'Boston, MA, USA')."""
        parts = [self.name]
        if self.state:
            parts.append(self.state.abbreviation or self.state.name)
        if self.nation:
            parts.append(self.nation.abbreviation or self.nation.name)
        return ', '.join(parts)

    def __repr__(self):
        return f"<City({self.city_id}: {self.name})>"


class Language(BaseModel, CacheableMixin):
    """Languages.

    Maps to languages table. Simple lookup table.
    """
    __tablename__ = 'languages'

    # Primary Key
    language_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    name = db.Column(db.String(50), nullable=False)

    def __repr__(self):
        return f"<Language({self.language_id}: {self.name})>"


class Park(BaseModel, CacheableMixin):
    """Ballparks/Stadiums.

    Maps to parks table. Includes dimensions and park factors.
    """
    __tablename__ = 'parks'

    # Primary Key
    park_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    name = db.Column(db.String(100), nullable=False)
    nation_id = db.Column(db.Integer, db.ForeignKey('nations.nation_id'))
    capacity = db.Column(db.Integer)

    # Physical characteristics
    type = db.Column(db.SmallInteger)  # Indoor/outdoor/retractable
    foul_ground = db.Column(db.SmallInteger)
    turf = db.Column(db.SmallInteger)  # Grass/turf

    # Outfield distances (7 measurement points)
    distances0 = db.Column(db.SmallInteger)  # Left field line
    distances1 = db.Column(db.SmallInteger)
    distances2 = db.Column(db.SmallInteger)
    distances3 = db.Column(db.SmallInteger)  # Center field
    distances4 = db.Column(db.SmallInteger)
    distances5 = db.Column(db.SmallInteger)
    distances6 = db.Column(db.SmallInteger)  # Right field line

    # Wall heights (7 measurement points)
    wall_heights0 = db.Column(db.SmallInteger)
    wall_heights1 = db.Column(db.SmallInteger)
    wall_heights2 = db.Column(db.SmallInteger)
    wall_heights3 = db.Column(db.SmallInteger)
    wall_heights4 = db.Column(db.SmallInteger)
    wall_heights5 = db.Column(db.SmallInteger)
    wall_heights6 = db.Column(db.SmallInteger)

    # Park factors (1.000 = neutral, >1.000 = hitter-friendly, <1.000 = pitcher-friendly)
    avg = db.Column(db.Numeric(6, 4))  # Batting average factor
    d = db.Column(db.Numeric(6, 4))    # Doubles factor
    t = db.Column(db.Numeric(6, 4))    # Triples factor
    hr = db.Column(db.Numeric(6, 4))   # Home run factor

    # ===== RELATIONSHIPS =====

    # Many-to-One: Park -> Nation
    nation = db.relationship(
        'Nation',
        lazy='joined'
    )

    # One-to-Many: Park -> Teams (teams that play here)
    teams = db.relationship(
        'Team',
        foreign_keys='Team.park_id',
        back_populates='park',
        lazy='selectin'
    )

    @hybrid_property
    def left_field_distance(self):
        """Left field line distance."""
        return self.distances0

    @hybrid_property
    def center_field_distance(self):
        """Center field distance."""
        return self.distances3

    @hybrid_property
    def right_field_distance(self):
        """Right field line distance."""
        return self.distances6

    @hybrid_property
    def is_hitter_friendly(self):
        """Check if park favors hitters (HR factor > 1.05)."""
        return self.hr and self.hr > 1.05

    def __repr__(self):
        return f"<Park({self.park_id}: {self.name})>"