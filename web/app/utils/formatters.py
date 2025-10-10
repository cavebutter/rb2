"""Custom Jinja2 template filters"""
import re

def register_filters(app):
    """Register custom template filters with Flask app"""

    @app.template_filter('format_stat')
    def format_stat(value, stat_type='default'):
        """Format statistical values for display

        Args:
            value: The numeric value to format
            stat_type: Type of stat ('avg', 'era', 'pct', etc.)

        Returns:
            Formatted string for display
        """
        if value is None:
            return '-'

        if stat_type == 'avg':
            # Batting average, OBP, SLG, OPS: Show .XXX if < 1, else 1.000
            if value < 1:
                return f".{int(value * 1000):03d}"
            else:
                return "1.000"
        elif stat_type == 'era':
            return f"{value:.2f}"
        elif stat_type == 'pct':
            return f"{int(value * 1000):03d}"
        else:
            return str(value)

    @app.template_filter('active_indicator')
    def active_indicator(is_active):
        """Add asterisk for active players"""
        return '*' if is_active else ''

    @app.template_filter('clean_trade_summary')
    def clean_trade_summary(summary):
        """Convert trade summary tags to HTML links and clean up formatting.

        Converts OOTP tags to clickable links:
            '<Team Name:team#123>' -> link to /teams/123
            '<Player Name:player#456>' -> link to /players/456

        Also strips trailing newlines and excessive whitespace.

        Args:
            summary: Raw trade summary string with OOTP tags

        Returns:
            HTML string with links
        """
        if not summary:
            return ''

        # Replace literal \n strings (escaped newlines from database)
        cleaned = summary.replace('\\n', ' ')

        # Also handle actual newline characters
        cleaned = cleaned.replace('\n', ' ')

        # Strip all trailing/leading whitespace
        cleaned = cleaned.strip()

        # Convert team tags to links
        # Pattern: <Team Name:team#ID>
        def replace_team(match):
            full_match = match.group(0)
            team_name = match.group(1)
            team_id = match.group(2)
            return f'<a href="/teams/{team_id}" class="font-semibold text-blue-600 hover:text-blue-800">{team_name}</a>'

        cleaned = re.sub(r'<([^:>]+):team#(\d+)>', replace_team, cleaned)

        # Convert player tags to links
        # Pattern: <Player Name:player#ID>
        def replace_player(match):
            full_match = match.group(0)
            player_name = match.group(1)
            player_id = match.group(2)
            return f'<a href="/players/{player_id}" class="font-semibold text-gray-900 hover:text-blue-600">{player_name}</a>'

        cleaned = re.sub(r'<([^:>]+):player#(\d+)>', replace_player, cleaned)

        # Clean up multiple spaces (after all replacements)
        cleaned = re.sub(r'\s{2,}', ' ', cleaned)

        return cleaned.strip()