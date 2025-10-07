"""Custom Jinja2 template filters"""

def register_filters(app):
    """Register custom template filters with Flask app"""

    @app.template_filter('format_stat')
    def format_stat(value, stat_type='default'):
        """Format statistical values for display"""
        if value is None:
            return '-'
        if stat_type == 'avg':
            return f".{int(value * 1000):03d}" if value < 1 else "1.000"
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