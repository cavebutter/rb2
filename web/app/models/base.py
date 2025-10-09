"""Base model classes and mixins for all database models."""
from datetime import datetime, timezone
from app.extensions import db, cache


class TimestampMixin:
    """Mixin for models that need created_at/updated_at timestamps.

    Use this for tables whose data changes over time (stats, records, player status).
    Don't use for static reference data (nations, languages, parks).
    """
    created_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class CacheableMixin:
    """Mixin providing caching functionality for models.

    Uses Flask-Caching with aggressive TTL for read-heavy baseball stats.
    """

    @classmethod
    def get_cache_key(cls, **kwargs):
        """Generate cache key from model name and kwargs."""
        key_parts = [cls.__name__]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        return ":".join(key_parts)

    @classmethod
    def get_cached(cls, timeout=3600, **kwargs):
        """Get instance from cache or database.

        Args:
            timeout: Cache TTL in seconds (default 1 hour)
            **kwargs: Query filters (e.g., player_id=123)

        Returns:
            Model instance or None
        """
        cache_key = cls.get_cache_key(**kwargs)

        # Try cache first
        result = cache.get(cache_key)
        if result is not None:
            return result

        # Query database
        result = cls.query.filter_by(**kwargs).first()

        # Store in cache
        if result:
            cache.set(cache_key, result, timeout=timeout)

        return result

    def invalidate_cache(self):
        """Invalidate this instance's cache entries."""
        # Get primary key values
        pk_dict = {}
        for pk_col in self.__table__.primary_key.columns:
            pk_dict[pk_col.name] = getattr(self, pk_col.name)

        cache_key = self.get_cache_key(**pk_dict)
        cache.delete(cache_key)


class BaseModel(db.Model):
    """Abstract base model providing common functionality.

    All models should inherit from this to get:
    - Automatic serialization (to_dict)
    - Common query helpers
    - Consistent __repr__
    """
    __abstract__ = True  # Don't create a table for this class

    def to_dict(self, exclude=None):
        """Convert model instance to dictionary.

        Args:
            exclude: List of column names to exclude from output

        Returns:
            Dictionary of column_name: value
        """
        exclude = exclude or []
        result = {}

        for column in self.__table__.columns:
            if column.name not in exclude:
                value = getattr(self, column.name)
                # Convert datetime to ISO format string
                if isinstance(value, datetime):
                    value = value.isoformat()
                result[column.name] = value

        return result

    def __repr__(self):
        """Default repr showing primary key(s)."""
        pk_values = []
        for pk_col in self.__table__.primary_key.columns:
            pk_values.append(f"{pk_col.name}={getattr(self, pk_col.name)}")

        return f"<{self.__class__.__name__}({', '.join(pk_values)})>"


class ReadOnlyMixin:
    """Mixin for read-only models (materialized views, calculated tables).

    Prevents accidental inserts/updates/deletes on views.
    """

    def __init__(self, *args, **kwargs):
        raise RuntimeError(f"{self.__class__.__name__} is read-only (materialized view)")

    @classmethod
    def create(cls, *args, **kwargs):
        raise RuntimeError(f"{cls.__name__} is read-only (materialized view)")

    def save(self):
        raise RuntimeError(f"{self.__class__.__name__} is read-only (materialized view)")

    def delete(self):
        raise RuntimeError(f"{self.__class__.__name__} is read-only (materialized view)")
