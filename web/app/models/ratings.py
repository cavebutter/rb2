"""Player ratings/abilities models - batting, pitching, and fielding ratings.

These represent player skills and potential (not performance stats).
One-to-one with Player - each player has one set of current ratings.
"""
from sqlalchemy.ext.hybrid import hybrid_property
from ..extensions import db
from ..models.base import BaseModel, CacheableMixin


class PlayerBattingRatings(BaseModel, CacheableMixin):
    """Player batting ratings/abilities.

    Maps to players_batting table. One-to-one with Player.
    Ratings on 0-250 scale (20-80 scout scale * ~3).
    """
    __tablename__ = 'players_batting'

    # Primary Key (also FK to Player)
    player_id = db.Column(db.Integer, db.ForeignKey('players_core.player_id'), primary_key=True)

    # Foreign Keys
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))

    # Position/Role
    position = db.Column(db.SmallInteger)
    role = db.Column(db.SmallInteger)

    # ===== OVERALL RATINGS (current ability) =====
    batting_ratings_overall_contact = db.Column(db.SmallInteger)      # Contact ability
    batting_ratings_overall_gap = db.Column(db.SmallInteger)          # Gap power (doubles/triples)
    batting_ratings_overall_power = db.Column(db.SmallInteger)        # Home run power
    batting_ratings_overall_eye = db.Column(db.SmallInteger)          # Plate discipline
    batting_ratings_overall_strikeouts = db.Column(db.SmallInteger)   # K avoidance (higher = fewer Ks)
    batting_ratings_overall_hp = db.Column(db.SmallInteger)           # Hit by pitch tendency
    batting_ratings_overall_babip = db.Column(db.SmallInteger)        # BABIP talent

    # ===== VS RIGHTIES (vsr) =====
    batting_ratings_vsr_contact = db.Column(db.SmallInteger)
    batting_ratings_vsr_gap = db.Column(db.SmallInteger)
    batting_ratings_vsr_power = db.Column(db.SmallInteger)
    batting_ratings_vsr_eye = db.Column(db.SmallInteger)
    batting_ratings_vsr_strikeouts = db.Column(db.SmallInteger)
    batting_ratings_vsr_hp = db.Column(db.SmallInteger)
    batting_ratings_vsr_babip = db.Column(db.SmallInteger)

    # ===== VS LEFTIES (vsl) =====
    batting_ratings_vsl_contact = db.Column(db.SmallInteger)
    batting_ratings_vsl_gap = db.Column(db.SmallInteger)
    batting_ratings_vsl_power = db.Column(db.SmallInteger)
    batting_ratings_vsl_eye = db.Column(db.SmallInteger)
    batting_ratings_vsl_strikeouts = db.Column(db.SmallInteger)
    batting_ratings_vsl_hp = db.Column(db.SmallInteger)
    batting_ratings_vsl_babip = db.Column(db.SmallInteger)

    # ===== TALENT (potential/true talent) =====
    batting_ratings_talent_contact = db.Column(db.SmallInteger)
    batting_ratings_talent_gap = db.Column(db.SmallInteger)
    batting_ratings_talent_power = db.Column(db.SmallInteger)
    batting_ratings_talent_eye = db.Column(db.SmallInteger)
    batting_ratings_talent_strikeouts = db.Column(db.SmallInteger)
    batting_ratings_talent_hp = db.Column(db.SmallInteger)
    batting_ratings_talent_babip = db.Column(db.SmallInteger)

    # ===== MISC RATINGS =====
    batting_ratings_misc_bunt = db.Column(db.SmallInteger)              # Bunting ability
    batting_ratings_misc_bunt_for_hit = db.Column(db.SmallInteger)      # Bunt for hit
    batting_ratings_misc_gb_hitter_type = db.Column(db.SmallInteger)    # Ground ball tendency
    batting_ratings_misc_fb_hitter_type = db.Column(db.SmallInteger)    # Fly ball tendency
    batting_ratings_misc_groundball_pct = db.Column(db.SmallInteger)    # GB%

    # ===== RELATIONSHIPS =====

    # One-to-One: BattingRatings -> Player
    player = db.relationship(
        'Player',
        back_populates='batting_ratings',
        lazy='joined'
    )

    # ===== HYBRID PROPERTIES =====

    @hybrid_property
    def contact_grade(self):
        """Convert contact rating to scout grade (20-80 scale)."""
        if not self.batting_ratings_overall_contact:
            return None
        return round(self.batting_ratings_overall_contact / 3)

    @hybrid_property
    def power_grade(self):
        """Convert power rating to scout grade."""
        if not self.batting_ratings_overall_power:
            return None
        return round(self.batting_ratings_overall_power / 3)

    @hybrid_property
    def eye_grade(self):
        """Convert eye rating to scout grade."""
        if not self.batting_ratings_overall_eye:
            return None
        return round(self.batting_ratings_overall_eye / 3)

    @hybrid_property
    def has_platoon_split(self):
        """Check if player has significant platoon split (>15 point difference)."""
        if not (self.batting_ratings_vsr_contact and self.batting_ratings_vsl_contact):
            return False
        diff = abs(self.batting_ratings_vsr_contact - self.batting_ratings_vsl_contact)
        return diff > 15

    def __repr__(self):
        return f"<PlayerBattingRatings({self.player_id}: {self.contact_grade}/{self.power_grade})>"


class PlayerPitchingRatings(BaseModel, CacheableMixin):
    """Player pitching ratings/abilities.

    Maps to players_pitching table. One-to-one with Player.
    Includes pitch ratings (fastball, slider, etc.) on 0-250 scale.
    """
    __tablename__ = 'players_pitching'

    # Primary Key (also FK to Player)
    player_id = db.Column(db.Integer, db.ForeignKey('players_core.player_id'), primary_key=True)

    # Foreign Keys
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))

    # Position/Role
    position = db.Column(db.SmallInteger)
    role = db.Column(db.SmallInteger)  # Starter/reliever

    # ===== OVERALL RATINGS (current ability) =====
    pitching_ratings_overall_stuff = db.Column(db.SmallInteger)         # Pitch quality
    pitching_ratings_overall_control = db.Column(db.SmallInteger)       # Command/control
    pitching_ratings_overall_movement = db.Column(db.SmallInteger)      # Pitch movement
    pitching_ratings_overall_balk = db.Column(db.SmallInteger)          # Balk tendency (lower = more balks)
    pitching_ratings_overall_hp = db.Column(db.SmallInteger)            # Hit batter tendency
    pitching_ratings_overall_wild_pitch = db.Column(db.SmallInteger)    # Wild pitch tendency

    # ===== VS RIGHTIES (vsr) =====
    pitching_ratings_vsr_stuff = db.Column(db.SmallInteger)
    pitching_ratings_vsr_control = db.Column(db.SmallInteger)
    pitching_ratings_vsr_movement = db.Column(db.SmallInteger)
    pitching_ratings_vsr_balk = db.Column(db.SmallInteger)
    pitching_ratings_vsr_hp = db.Column(db.SmallInteger)
    pitching_ratings_vsr_wild_pitch = db.Column(db.SmallInteger)

    # ===== VS LEFTIES (vsl) =====
    pitching_ratings_vsl_stuff = db.Column(db.SmallInteger)
    pitching_ratings_vsl_control = db.Column(db.SmallInteger)
    pitching_ratings_vsl_movement = db.Column(db.SmallInteger)
    pitching_ratings_vsl_balk = db.Column(db.SmallInteger)
    pitching_ratings_vsl_hp = db.Column(db.SmallInteger)
    pitching_ratings_vsl_wild_pitch = db.Column(db.SmallInteger)

    # ===== TALENT (potential) =====
    pitching_ratings_talent_stuff = db.Column(db.SmallInteger)
    pitching_ratings_talent_control = db.Column(db.SmallInteger)
    pitching_ratings_talent_movement = db.Column(db.SmallInteger)
    pitching_ratings_talent_balk = db.Column(db.SmallInteger)
    pitching_ratings_talent_hp = db.Column(db.SmallInteger)
    pitching_ratings_talent_wild_pitch = db.Column(db.SmallInteger)

    # ===== PITCH RATINGS (current) =====
    pitching_ratings_pitches_fastball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_slider = db.Column(db.SmallInteger)
    pitching_ratings_pitches_curveball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_changeup = db.Column(db.SmallInteger)
    pitching_ratings_pitches_splitter = db.Column(db.SmallInteger)
    pitching_ratings_pitches_sinker = db.Column(db.SmallInteger)
    pitching_ratings_pitches_cutter = db.Column(db.SmallInteger)
    pitching_ratings_pitches_forkball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_screwball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_knuckleball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_circlechange = db.Column(db.SmallInteger)
    pitching_ratings_pitches_knucklecurve = db.Column(db.SmallInteger)

    # ===== PITCH RATINGS (potential) =====
    pitching_ratings_pitches_talent_fastball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_slider = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_curveball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_changeup = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_splitter = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_sinker = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_cutter = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_forkball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_screwball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_knuckleball = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_circlechange = db.Column(db.SmallInteger)
    pitching_ratings_pitches_talent_knucklecurve = db.Column(db.SmallInteger)

    # ===== MISC RATINGS =====
    pitching_ratings_misc_velocity = db.Column(db.SmallInteger)      # Fastball velocity
    pitching_ratings_misc_arm_slot = db.Column(db.SmallInteger)      # Arm slot/delivery
    pitching_ratings_misc_stamina = db.Column(db.SmallInteger)       # Stamina (SP vs RP)
    pitching_ratings_misc_ground_fly = db.Column(db.SmallInteger)    # GB/FB tendency
    pitching_ratings_misc_hold = db.Column(db.SmallInteger)          # Holding runners

    # ===== RELATIONSHIPS =====

    # One-to-One: PitchingRatings -> Player
    player = db.relationship(
        'Player',
        back_populates='pitching_ratings',
        lazy='joined'
    )

    # ===== HELPER METHODS =====

    def get_pitch_arsenal(self, min_rating=100):
        """Get list of pitches above minimum rating.

        Args:
            min_rating: Minimum pitch rating (default 100 = ~33 grade)

        Returns:
            List of (pitch_name, rating) tuples
        """
        pitches = [
            ('Fastball', self.pitching_ratings_pitches_fastball),
            ('Slider', self.pitching_ratings_pitches_slider),
            ('Curveball', self.pitching_ratings_pitches_curveball),
            ('Changeup', self.pitching_ratings_pitches_changeup),
            ('Splitter', self.pitching_ratings_pitches_splitter),
            ('Sinker', self.pitching_ratings_pitches_sinker),
            ('Cutter', self.pitching_ratings_pitches_cutter),
            ('Forkball', self.pitching_ratings_pitches_forkball),
            ('Screwball', self.pitching_ratings_pitches_screwball),
            ('Knuckleball', self.pitching_ratings_pitches_knuckleball),
            ('Circle Change', self.pitching_ratings_pitches_circlechange),
            ('Knuckle Curve', self.pitching_ratings_pitches_knucklecurve),
        ]
        return [(name, rating) for name, rating in pitches if rating and rating >= min_rating]

    # ===== HYBRID PROPERTIES =====

    @hybrid_property
    def stuff_grade(self):
        """Convert stuff rating to scout grade."""
        if not self.pitching_ratings_overall_stuff:
            return None
        return round(self.pitching_ratings_overall_stuff / 3)

    @hybrid_property
    def control_grade(self):
        """Convert control rating to scout grade."""
        if not self.pitching_ratings_overall_control:
            return None
        return round(self.pitching_ratings_overall_control / 3)

    @hybrid_property
    def movement_grade(self):
        """Convert movement rating to scout grade."""
        if not self.pitching_ratings_overall_movement:
            return None
        return round(self.pitching_ratings_overall_movement / 3)

    @hybrid_property
    def is_starter(self):
        """Check if pitcher has starter stamina (>100)."""
        return self.pitching_ratings_misc_stamina and self.pitching_ratings_misc_stamina > 100

    def __repr__(self):
        return f"<PlayerPitchingRatings({self.player_id}: {self.stuff_grade}/{self.control_grade}/{self.movement_grade})>"


class PlayerFieldingRatings(BaseModel, CacheableMixin):
    """Player fielding ratings/abilities.

    Maps to players_fielding table. One-to-one with Player.
    Includes defensive abilities and positional experience.
    """
    __tablename__ = 'players_fielding'

    # Primary Key (also FK to Player)
    player_id = db.Column(db.Integer, db.ForeignKey('players_core.player_id'), primary_key=True)

    # Foreign Keys
    team_id = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    league_id = db.Column(db.Integer, db.ForeignKey('leagues.league_id'))

    # Position/Role
    position = db.Column(db.SmallInteger)
    role = db.Column(db.SmallInteger)

    # ===== DEFENSIVE RATINGS =====
    # Infield
    fielding_ratings_infield_range = db.Column(db.SmallInteger)     # Infield range
    fielding_ratings_infield_arm = db.Column(db.SmallInteger)       # Infield arm strength
    fielding_ratings_infield_error = db.Column(db.SmallInteger)     # Error avoidance (higher = fewer errors)
    fielding_ratings_turn_doubleplay = db.Column(db.SmallInteger)   # DP turning ability

    # Outfield
    fielding_ratings_outfield_range = db.Column(db.SmallInteger)    # Outfield range
    fielding_ratings_outfield_arm = db.Column(db.SmallInteger)      # OF arm strength
    fielding_ratings_outfield_error = db.Column(db.SmallInteger)    # Error avoidance

    # Catcher
    fielding_ratings_catcher_arm = db.Column(db.SmallInteger)       # Catching/throwing
    fielding_ratings_catcher_ability = db.Column(db.SmallInteger)   # Overall catching skill

    # ===== POSITIONAL EXPERIENCE (games played) =====
    fielding_experience_0 = db.Column(db.SmallInteger)  # Pitcher
    fielding_experience_1 = db.Column(db.SmallInteger)  # Catcher
    fielding_experience_2 = db.Column(db.SmallInteger)  # 1B
    fielding_experience_3 = db.Column(db.SmallInteger)  # 2B
    fielding_experience_4 = db.Column(db.SmallInteger)  # 3B
    fielding_experience_5 = db.Column(db.SmallInteger)  # SS
    fielding_experience_6 = db.Column(db.SmallInteger)  # LF
    fielding_experience_7 = db.Column(db.SmallInteger)  # CF
    fielding_experience_8 = db.Column(db.SmallInteger)  # RF
    fielding_experience_9 = db.Column(db.SmallInteger)  # DH

    # ===== POSITIONAL RATINGS (ability at each position) =====
    fielding_rating_pos_1 = db.Column(db.SmallInteger)  # C
    fielding_rating_pos_2 = db.Column(db.SmallInteger)  # 1B
    fielding_rating_pos_3 = db.Column(db.SmallInteger)  # 2B
    fielding_rating_pos_4 = db.Column(db.SmallInteger)  # 3B
    fielding_rating_pos_5 = db.Column(db.SmallInteger)  # SS
    fielding_rating_pos_6 = db.Column(db.SmallInteger)  # LF
    fielding_rating_pos_7 = db.Column(db.SmallInteger)  # CF
    fielding_rating_pos_8 = db.Column(db.SmallInteger)  # RF
    fielding_rating_pos_9 = db.Column(db.SmallInteger)  # DH

    # ===== RELATIONSHIPS =====

    # One-to-One: FieldingRatings -> Player
    player = db.relationship(
        'Player',
        back_populates='fielding_ratings',
        lazy='joined'
    )

    # ===== HELPER METHODS =====

    def get_positions_played(self, min_games=10):
        """Get list of positions with significant experience.

        Args:
            min_games: Minimum games to include position

        Returns:
            List of (position_name, games) tuples
        """
        position_map = {
            0: ('P', self.fielding_experience_0),
            1: ('C', self.fielding_experience_1),
            2: ('1B', self.fielding_experience_2),
            3: ('2B', self.fielding_experience_3),
            4: ('3B', self.fielding_experience_4),
            5: ('SS', self.fielding_experience_5),
            6: ('LF', self.fielding_experience_6),
            7: ('CF', self.fielding_experience_7),
            8: ('RF', self.fielding_experience_8),
            9: ('DH', self.fielding_experience_9),
        }
        return [(name, games) for name, games in position_map.values() if games and games >= min_games]

    def get_position_rating(self, position_id):
        """Get fielding rating for a specific position.

        Args:
            position_id: Position ID (1-9)

        Returns:
            Rating (0-250 scale) or None
        """
        if position_id < 1 or position_id > 9:
            return None
        return getattr(self, f'fielding_rating_pos_{position_id}', None)

    # ===== HYBRID PROPERTIES =====

    @hybrid_property
    def primary_position(self):
        """Get primary position (most experience)."""
        positions = self.get_positions_played(min_games=1)
        if not positions:
            return None
        return max(positions, key=lambda x: x[1])[0]

    @hybrid_property
    def is_versatile(self):
        """Check if player can play 3+ positions adequately."""
        positions = self.get_positions_played(min_games=20)
        return len(positions) >= 3

    def __repr__(self):
        return f"<PlayerFieldingRatings({self.player_id}: {self.primary_position})>"