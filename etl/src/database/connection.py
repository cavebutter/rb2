"""Database connection management"""
import os
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

class DatabaseConnection:
    """Manager PostgreSQL database connections"""
    def __init__(self, environment='dev'):
        self.environment = environment
        self.engine = None
        self.SessionLocal = None
        self._init_connection()


    def _init_connection(self):
        """Initialize database connection"""
        # Get credentials from environment variables
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", "5432")
        user = os.getenv("DB_USER_ETL")
        password = os.getenv("OOTP_ETL_PASSWORD")

        # Select database based on environment
        if self.environment == 'dev':
            database = os.getenv("DB_NAME_DEV")
        elif self.environment == 'staging':
            database = os.getenv("DB_NAME_STAGING")
        else:
            raise ValueError(f"Unknown environment: {self.environment}")

        # Build connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        logger.info(f"Connecting to database at {host}:{port}, database: {database}")

        try:
            self.engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping= True,
                echo=False
            )
            self.SessionLocal = sessionmaker(bind=self.engine)
            logger.success("Database connection established")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise


    @contextmanager
    def get_session(self):
        """Get a database session"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session rollback due to error: {e}")
            raise
        finally:
            session.close()
            logger.debug("Database session closed")


    def execute_sql(self, sql, params=None):
        """Execute raw SQL query"""
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            conn.commit()
            return result


# Global connection instance
db = DatabaseConnection()


def test_connection():
    """Test Database connection"""
    try:
        with db.get_session() as session:
            result = session.execute(text("SELECT 1"))
            return result.scalar() == 1
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False
