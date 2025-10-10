"""Flask application factory"""
from flask import Flask
from loguru import logger
import sys

def create_app(config_name='development'):
    """Application factory for creating Flask app instances"""
    app = Flask(__name__)

    # Load configuration
    from .config import config
    app.config.from_object(config[config_name])

    # Configure logging
    configure_logging(app.config['DEBUG'])

    # Initialize extensions
    from .extensions import db, cache
    db.init_app(app)
    cache.init_app(app)

    # Register blueprints
    from .routes import main, players, coaches, teams, leaderboards, newspaper, search
    app.register_blueprint(main.bp)
    app.register_blueprint(players.bp, url_prefix='/players')
    app.register_blueprint(coaches.coaches_bp, url_prefix='/coaches')
    app.register_blueprint(teams.bp, url_prefix='/teams')
    app.register_blueprint(leaderboards.bp, url_prefix='/leaderboards')
    app.register_blueprint(newspaper.bp, url_prefix='/newspaper')
    app.register_blueprint(search.bp, url_prefix='/api/search')

    # Register template filters
    from .utils.formatters import register_filters
    register_filters(app)

    logger.info(f"Flask app created with config: {config_name}")

    return app


def configure_logging(debug=False):
    """Configure loguru for the web application"""
    # Remove default handler
    logger.remove()

    # Console handler (always)
    logger.add(
        sys.stderr,
        level="DEBUG" if debug else "INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>"
    )

    # File handler (production)
    if not debug:
        logger.add(
            "logs/rb2_web_{time:YYYY-MM-DD}.log",
            rotation="00:00",  # New file at midnight
            retention="30 days",
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}"
        )

    logger.info("Logging configured")