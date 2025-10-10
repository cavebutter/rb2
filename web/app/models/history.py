"""Player history models - trades, transactions, awards, etc."""
from sqlalchemy.ext.hybrid import hybrid_property
from ..extensions import db
from ..models.base import BaseModel


class TradeHistory(BaseModel):
    """Trade history records.

    Maps to trade_history table. Records trades between two teams,
    including players, draft picks, and cash exchanged.

    Structure:
    - Team 0 trades → Team 1
    - Each team can trade up to 10 players
    - Each team can trade up to 5 draft picks
    - Summary field contains human-readable trade description with embedded tags
    """
    __tablename__ = 'trade_history'

    # Primary Key
    trade_id = db.Column(db.Integer, primary_key=True)

    # Basic Info
    message_id = db.Column(db.Integer)
    date = db.Column(db.Date)
    summary = db.Column(db.Text)

    # Team 0 (trading away)
    team_id_0 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))

    # Team 0 Players (up to 10)
    player_id_0_0 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_0_1 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_0_2 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_0_3 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_0_4 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_0_5 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_0_6 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_0_7 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_0_8 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_0_9 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))

    # Team 0 Draft Picks (up to 5)
    draft_round_0_0 = db.Column(db.Integer)
    draft_team_0_0 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    draft_round_0_1 = db.Column(db.Integer)
    draft_team_0_1 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    draft_round_0_2 = db.Column(db.Integer)
    draft_team_0_2 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    draft_round_0_3 = db.Column(db.Integer)
    draft_team_0_3 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    draft_round_0_4 = db.Column(db.Integer)
    draft_team_0_4 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))

    # Team 0 Cash/Cap
    cash_0 = db.Column(db.Integer)
    iafa_cap_0 = db.Column(db.Integer)

    # Team 1 (receiving)
    team_id_1 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))

    # Team 1 Players (up to 10)
    player_id_1_0 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1_1 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1_2 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1_3 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1_4 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1_5 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1_6 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1_7 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1_8 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1_9 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))

    # Team 1 Draft Picks (up to 5)
    draft_round_1_0 = db.Column(db.Integer)
    draft_team_1_0 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    draft_round_1_1 = db.Column(db.Integer)
    draft_team_1_1 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    draft_round_1_2 = db.Column(db.Integer)
    draft_team_1_2 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    draft_round_1_3 = db.Column(db.Integer)
    draft_team_1_3 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    draft_round_1_4 = db.Column(db.Integer)
    draft_team_1_4 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))

    # Team 1 Cash/Cap
    cash_1 = db.Column(db.Integer)
    iafa_cap_1 = db.Column(db.Integer)

    # ===== RELATIONSHIPS =====

    # Team 0
    team_0 = db.relationship(
        'Team',
        foreign_keys=[team_id_0],
        lazy='joined'
    )

    # Team 1
    team_1 = db.relationship(
        'Team',
        foreign_keys=[team_id_1],
        lazy='joined'
    )

    # Note: Player relationships not defined to avoid complexity
    # Players can be accessed directly via query if needed
    # The summary field contains human-readable trade description

    @hybrid_property
    def year(self):
        """Extract year from trade date."""
        if self.date:
            return self.date.year
        return None

    def involves_player(self, player_id):
        """Check if this trade involves a specific player.

        Args:
            player_id: Player ID to check

        Returns:
            bool: True if player was traded
        """
        player_fields = [
            self.player_id_0_0, self.player_id_0_1, self.player_id_0_2,
            self.player_id_0_3, self.player_id_0_4, self.player_id_0_5,
            self.player_id_0_6, self.player_id_0_7, self.player_id_0_8,
            self.player_id_0_9, self.player_id_1_0, self.player_id_1_1,
            self.player_id_1_2, self.player_id_1_3, self.player_id_1_4,
            self.player_id_1_5, self.player_id_1_6, self.player_id_1_7,
            self.player_id_1_8, self.player_id_1_9
        ]
        return player_id in player_fields

    def get_destination_team(self, player_id):
        """Get the team this player was traded to.

        Args:
            player_id: Player ID

        Returns:
            int: team_id the player was traded to, or None
        """
        # Check if player was on team 0 (traded to team 1)
        team_0_players = [
            self.player_id_0_0, self.player_id_0_1, self.player_id_0_2,
            self.player_id_0_3, self.player_id_0_4, self.player_id_0_5,
            self.player_id_0_6, self.player_id_0_7, self.player_id_0_8,
            self.player_id_0_9
        ]
        if player_id in team_0_players:
            return self.team_id_1

        # Check if player was on team 1 (traded to team 0)
        team_1_players = [
            self.player_id_1_0, self.player_id_1_1, self.player_id_1_2,
            self.player_id_1_3, self.player_id_1_4, self.player_id_1_5,
            self.player_id_1_6, self.player_id_1_7, self.player_id_1_8,
            self.player_id_1_9
        ]
        if player_id in team_1_players:
            return self.team_id_0

        return None

    def __repr__(self):
        return f"<TradeHistory({self.trade_id}: {self.date})>"
