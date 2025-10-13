"""Coach model for coaching staff and front office personnel"""
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
from app.models.base import BaseModel, db


class Coach(BaseModel):
    """Coach model mapping to coaches table

    Represents coaching staff, scouts, trainers, and front office personnel.
    Links to players via former_player_id for coaches who were formerly players.
    """
    __tablename__ = 'coaches'

    # Primary Key
    coach_id = db.Column(db.Integer, primary_key=True)

    # Personal Information
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))
    nick_name = db.Column(db.String(50))
    age = db.Column(db.SmallInteger)
    date_of_birth = db.Column(db.Date)
    city_of_birth_id = db.Column(db.Integer, ForeignKey('cities.city_id'))
    nation_id = db.Column(db.Integer, ForeignKey('nations.nation_id'))
    weight = db.Column(db.SmallInteger)  # in kg
    height = db.Column(db.SmallInteger)  # in cm

    # Role Information
    position = db.Column(db.SmallInteger)  # Likely unused for coaches
    experience = db.Column(db.SmallInteger)
    occupation = db.Column(db.SmallInteger)  # 1=GM, 2=Manager, 3=BC, 4=PC, 5=HC, 6=Scout, 12=Trainer, 13=Owner
    team_id = db.Column(db.Integer, ForeignKey('teams.team_id'))
    former_player_id = db.Column(db.Integer, ForeignKey('players_core.player_id'))

    # Contract Information
    quick_left = db.Column(db.Integer)
    contract_salary = db.Column(db.Integer)
    contract_years = db.Column(db.Integer)
    contract_extension_salary = db.Column(db.Integer)
    contract_extension_years = db.Column(db.Integer)

    # Scouting Abilities
    scout_major = db.Column(db.Integer)
    scout_minor = db.Column(db.Integer)
    scout_international = db.Column(db.Integer)
    scout_amateur = db.Column(db.Integer)
    scout_amateur_preference = db.Column(db.Integer)

    # Teaching Abilities
    teach_hitting = db.Column(db.Integer)
    teach_pitching = db.Column(db.Integer)
    teach_fielding = db.Column(db.Integer)
    teach_running = db.Column(db.Integer)

    # People Skills
    handle_veterans = db.Column(db.Integer)
    handle_rookies = db.Column(db.Integer)
    handle_players = db.Column(db.Integer)
    strategy_knowledge = db.Column(db.Integer)

    # Medical Abilities
    heal_legs = db.Column(db.Integer)
    heal_arms = db.Column(db.Integer)
    heal_back = db.Column(db.Integer)
    heal_other = db.Column(db.Integer)
    heal_rest = db.Column(db.Integer)
    prevent_legs = db.Column(db.Integer)
    prevent_arms = db.Column(db.Integer)
    prevent_back = db.Column(db.Integer)
    prevent_other = db.Column(db.Integer)

    # Personality & Style
    management_style = db.Column(db.Integer)
    personality = db.Column(db.Integer)
    hitting_focus = db.Column(db.Integer)
    pitching_focus = db.Column(db.Integer)
    training_focus = db.Column(db.Integer)

    # Manager Preferences (-5 to +5 scale)
    stealing = db.Column(db.Integer)
    running = db.Column(db.Integer)
    pinchrun = db.Column(db.Integer)
    pinchhit_pos = db.Column(db.Integer)
    pinchhit_pitch = db.Column(db.Integer)
    hook_start = db.Column(db.Integer)
    hook_relief = db.Column(db.Integer)
    closer = db.Column(db.Integer)
    lr_matchup = db.Column(db.Integer)
    bunt_hit = db.Column(db.Integer)
    bunt = db.Column(db.Integer)
    hit_run = db.Column(db.Integer)
    run_hit = db.Column(db.Integer)
    squeeze = db.Column(db.Integer)
    pitch_around = db.Column(db.Integer)
    intentional_walk = db.Column(db.Integer)
    hold_runner = db.Column(db.Integer)
    guard_lines = db.Column(db.Integer)
    infield_in = db.Column(db.Integer)
    outfield_in = db.Column(db.Integer)
    corners_in = db.Column(db.Integer)
    shift_if = db.Column(db.Integer)
    shift_of = db.Column(db.Integer)
    opener = db.Column(db.Integer)
    num_pitchers = db.Column(db.Integer)
    num_hitters = db.Column(db.Integer)

    # GM/Front Office Preferences
    favor_speed_to_power = db.Column(db.Integer)
    favor_avg_to_obp = db.Column(db.Integer)
    favor_defense_to_offense = db.Column(db.Integer)
    favor_pitching_to_hitting = db.Column(db.Integer)
    favor_veterans_to_prospects = db.Column(db.Integer)
    trade_aggressiveness = db.Column(db.Integer)
    player_loyalty = db.Column(db.Integer)
    trade_frequency = db.Column(db.Integer)
    trade_preference = db.Column(db.Integer)
    value_stats = db.Column(db.Integer)
    value_this_year = db.Column(db.Integer)
    value_last_year = db.Column(db.Integer)
    value_two_years = db.Column(db.Integer)
    draft_value = db.Column(db.Integer)
    intl_fa_value = db.Column(db.Integer)
    develop_value = db.Column(db.Integer)
    ratings_value = db.Column(db.Integer)

    # Staff Ratings (out of 200)
    manager_value = db.Column(db.Integer)
    pitching_coach_value = db.Column(db.Integer)
    hitting_coach_value = db.Column(db.Integer)
    scout_value = db.Column(db.Integer)
    doctor_value = db.Column(db.Integer)

    # Status
    busy = db.Column(db.Integer)
    type = db.Column(db.Integer)
    data = db.Column(db.Integer)
    days_left = db.Column(db.Integer)

    # Relationships
    team = relationship('Team', foreign_keys=[team_id], backref='coaches')
    former_player = relationship('Player', foreign_keys=[former_player_id])
    city_of_birth = relationship('City', foreign_keys=[city_of_birth_id])
    nation = relationship('Nation', foreign_keys=[nation_id])

    @hybrid_property
    def full_name(self):
        """Get coach's full name"""
        if self.nick_name:
            return f"{self.first_name} '{self.nick_name}' {self.last_name}"
        return f"{self.first_name} {self.last_name}"

    @hybrid_property
    def occupation_display(self):
        """Get human-readable occupation name"""
        occupation_map = {
            1: "General Manager",
            2: "Manager",
            3: "Bench Coach",
            4: "Pitching Coach",
            5: "Hitting Coach",
            6: "Scout",
            7: "Base Coach",
            8: "Base Coach",
            12: "Trainer",
            13: "Owner"
        }
        return occupation_map.get(self.occupation, "Staff")

    @hybrid_property
    def occupation_short(self):
        """Get abbreviated occupation name"""
        occupation_map = {
            1: "GM",
            2: "Manager",
            3: "Bench Coach",
            4: "Pitching Coach",
            5: "Hitting Coach",
            6: "Scout",
            7: "Base Coach",
            8: "Base Coach",
            12: "Trainer",
            13: "Owner"
        }
        return occupation_map.get(self.occupation, "Staff")

    @hybrid_property
    def occupation_sort_order(self):
        """Get sort order for occupation (lower = higher priority)

        Order: Owner, GM, Manager, Bench Coach, Pitching Coach, Hitting Coach,
               Base Coach, Base Coach, Scout, Trainer, Other Staff
        """
        sort_order_map = {
            13: 1,  # Owner
            1: 2,   # General Manager
            2: 3,   # Manager
            3: 4,   # Bench Coach
            4: 5,   # Pitching Coach
            5: 6,   # Hitting Coach
            7: 7,   # Base Coach
            8: 8,   # Base Coach
            6: 9,   # Scout
            12: 10, # Trainer
        }
        return sort_order_map.get(self.occupation, 99)  # Other staff at the end

    @hybrid_property
    def height_display(self):
        """Convert height from cm to feet and inches"""
        if not self.height:
            return None

        total_inches = self.height / 2.54
        feet = int(total_inches // 12)
        inches = int(total_inches % 12)
        return f"{feet}'{inches}\""

    @hybrid_property
    def weight_display(self):
        """Display weight (already in lbs)"""
        if not self.weight:
            return None

        return f"{self.weight} lbs"

    @hybrid_property
    def birthplace_display(self):
        """Format birthplace with city, state/province (for US/Canada), and country.

        Returns formatted string like:
        - "Toronto, ON, CAN" (Canadian with state)
        - "New York, NY, USA" (US with state)
        - "Tokyo, JPN" (other countries without state)
        - "Unknown" if no location data
        """
        parts = []

        # Add city name
        if self.city_of_birth:
            parts.append(self.city_of_birth.name)

            # Add state/province for US and Canada
            if self.city_of_birth.state:
                state_nation_abbr = self.city_of_birth.state.nation.abbreviation if self.city_of_birth.state.nation else None
                if state_nation_abbr in ('USA', 'CAN'):
                    parts.append(self.city_of_birth.state.abbreviation)

        # Add nation
        if self.nation:
            parts.append(self.nation.abbreviation)

        return ', '.join(parts) if parts else 'Unknown'

    def __repr__(self):
        return f"<Coach {self.coach_id}: {self.full_name} - {self.occupation_display}>"