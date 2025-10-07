"""Flask extensions - initialized here, attached to app in factory"""
from flask_sqlalchemy import SQLAlchemy
from flask_caching import Cache

db = SQLAlchemy()
cache = Cache()

