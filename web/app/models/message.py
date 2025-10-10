"""News message models - player news, awards, injuries, contracts, highlights."""
from sqlalchemy.ext.hybrid import hybrid_property
from ..extensions import db
from ..models.base import BaseModel


class Message(BaseModel):
    """News messages from the game.

    Maps to messages table. Contains news stories about players, teams, and league events.
    Focuses on player-relevant news: contracts, injuries, awards, highlights, and retirements.

    Relevant Message Types (for player pages):
    - 2: Contract signings
    - 3: Retirements & suspensions
    - 4: Performance highlights (shutouts, hitting for cycle, etc.)
    - 7: Awards (Player of the Week, etc.)
    - 8: Injuries

    Excluded Types:
    - 0: General announcements (not player-specific)
    - 1: Trades (covered by trade_history table)
    - 6: General
    - 11: Trade rumors (speculation, not news)

    Sender Types:
    - 0: Commissioner/League Office
    - 3: League
    - 5: Team
    """
    __tablename__ = 'messages'

    # Primary Key
    message_id = db.Column(db.Integer, primary_key=True)

    # Content
    subject = db.Column(db.String(255))
    body = db.Column(db.Text, nullable=False)

    # Players (up to 10 can be referenced per message)
    player_id_0 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_1 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_2 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_3 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_4 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_5 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_6 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_7 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_8 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))
    player_id_9 = db.Column(db.Integer, db.ForeignKey('players_core.player_id'))

    # Teams (up to 5)
    team_id_0 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    team_id_1 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    team_id_2 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    team_id_3 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))
    team_id_4 = db.Column(db.Integer, db.ForeignKey('teams.team_id'))

    # Leagues (up to 2)
    league_id_0 = db.Column(db.Integer)
    league_id_1 = db.Column(db.Integer)

    # Metadata
    importance = db.Column(db.SmallInteger)
    message_type = db.Column(db.SmallInteger)
    hype = db.Column(db.SmallInteger)
    sender_type = db.Column(db.SmallInteger)
    sender_id = db.Column(db.Integer)
    recipient_id = db.Column(db.Integer)
    trade_id = db.Column(db.Integer, db.ForeignKey('trade_history.trade_id'))
    date = db.Column(db.Date)
    deleted = db.Column(db.SmallInteger, default=0)
    notify = db.Column(db.SmallInteger, default=1)
    ongoing_story_id = db.Column(db.Integer, default=-1)
    text_is_modified = db.Column(db.SmallInteger, default=0)

    # ===== RELATIONSHIPS =====

    # Note: Trade relationship available but not needed for player news
    # (trades are shown separately via TradeHistory model)

    @hybrid_property
    def category(self):
        """Get human-readable category based on message_type."""
        categories = {
            2: 'Contract',
            3: 'Career',
            4: 'Highlight',
            7: 'Award',
            8: 'Injury'
        }
        return categories.get(self.message_type, 'News')

    @hybrid_property
    def icon(self):
        """Get emoji icon for display."""
        icons = {
            2: '✍️',   # Contract
            3: '👋',  # Retirement/Suspension (Career milestone)
            4: '⭐',  # Highlight
            7: '🏆',  # Award
            8: '🏥'   # Injury
        }
        return icons.get(self.message_type, '📰')

    @hybrid_property
    def color_class(self):
        """Get Tailwind color class for category badge."""
        colors = {
            2: 'bg-green-100 text-green-800',      # Contract (green for money)
            3: 'bg-purple-100 text-purple-800',    # Career (purple for milestone)
            4: 'bg-yellow-100 text-yellow-800',    # Highlight (yellow for star)
            7: 'bg-blue-100 text-blue-800',        # Award (blue for trophy)
            8: 'bg-red-100 text-red-800'           # Injury (red for medical)
        }
        return colors.get(self.message_type, 'bg-gray-100 text-gray-800')

    def involves_player(self, player_id):
        """Check if this message involves a specific player.

        Args:
            player_id: Player ID to check

        Returns:
            bool: True if player is referenced in this message
        """
        player_fields = [
            self.player_id_0, self.player_id_1, self.player_id_2,
            self.player_id_3, self.player_id_4, self.player_id_5,
            self.player_id_6, self.player_id_7, self.player_id_8,
            self.player_id_9
        ]
        return player_id in player_fields

    @hybrid_property
    def year(self):
        """Extract year from message date."""
        if self.date:
            return self.date.year
        return None

    def __repr__(self):
        return f"<Message({self.message_id}: {self.category} - {self.subject[:30]}...)>"
