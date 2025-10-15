"""Flask Configuration Classes"""
import os
from pathlib import Path

class Config:
    """Base configuration -shared across all environments"""
    # Secret key
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-prod'

    # Database - reuse ETL settings
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'postgresql://ootp_etl:d0ghouse@192.168.10.94:5432/ootp_dev'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False

    # SQLAlchemy engine options
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_size': 10,
        'pool_recycle': 3600,
        'pool_pre_ping': True,
    }

    # Caching
    CACHE_TYPE = 'SimpleCache' # Override in production
    CACHE_DEFAULT_TIMEOUT = 3600

    # Static Files
    STATIC_FOLDER = 'static'
    PLAYER_IMAGES_PATH = Path('mnt/hdd/PycharmProjects/rb2/etl/data/images/players')

    # Pagination
    ITEMS_PER_PAGE = 50

    # Application settings
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024

    # Debug Mode
    DEBUG = False


class DevelopmentConfig(Config):
    """Development specific configuration - Local Redis on dev machine"""
    DEBUG = True
    SQLALCHEMY_ECHO = True # Log all SQL queries
    TEMPLATES_AUTO_RELOAD = True

    # Redis caching (local instance on dev machine)
    CACHE_TYPE = 'RedisCache'
    CACHE_REDIS_URL = os.environ.get('REDIS_URL') or 'redis://localhost:6379/0'
    CACHE_DEFAULT_TIMEOUT = 300  # 5 minutes default
    CACHE_KEY_PREFIX = 'rb2_dev:'


class StagingConfig(Config):
    """Staging environment - Centralized Redis on DB server"""
    DEBUG = False
    SQLALCHEMY_ECHO = False

    # Redis caching (centralized on DB server, separate DB namespace)
    CACHE_TYPE = 'RedisCache'
    CACHE_REDIS_URL = os.environ.get('REDIS_URL') or 'redis://192.168.10.94:6379/1'
    CACHE_DEFAULT_TIMEOUT = 300  # 5 minutes default
    CACHE_KEY_PREFIX = 'rb2_staging:'

    # Security Headers
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'


class ProductionConfig(Config):
    """Production specific configuration - Centralized Redis on DB server"""
    DEBUG = False

    # Redis caching (centralized on DB server, separate DB namespace)
    CACHE_TYPE = 'RedisCache'
    CACHE_REDIS_URL = os.environ.get('REDIS_URL') or 'redis://192.168.10.94:6379/2'
    CACHE_DEFAULT_TIMEOUT = 600  # 10 minutes default for production
    CACHE_KEY_PREFIX = 'rb2_prod:'

    # Security Headers
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'


class TestingConfig(Config):
    """Testing specific configuration"""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'postgresql://ootp_etl:d0ghouse@192.168.10.94:5432/ootp_test'
    CACHE_TYPE = 'NullCache'
    WTF_CSRF_ENABLED = False


config = {
    'development': DevelopmentConfig,
    'staging': StagingConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}