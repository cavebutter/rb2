"""Database Schema Management"""
from pathlib import Path
from loguru import logger
from .connection import db
from sqlalchemy import text

class SchemaManager:
    """Manage database schema creation and updates"""

    def __init__(self):
        self.sql_dir = Path(__file__).parent.parent.parent / "sql" / "tables"
        self.indexes_dir = Path(__file__).parent.parent.parent / "sql" / "indexes"


    def execute_sql_file(self, filename, directory=None):
        """Execute a SQL file from specified directory or default tables directory"""
        sql_dir = directory if directory else self.sql_dir
        sql_path = sql_dir / filename

        if not sql_path.exists():
            logger.error(f"SQL file not found: {sql_path}")
            return False

        logger.info(f"Executing SQL file: {filename}")

        try:
            with open(sql_path, 'r') as f:
                sql_content = f.read()

            # Split by semicolons but be careful with functions
            statements = self._split_sql_statements(sql_content)

            for i, statement in enumerate(statements):
                if statement.strip():
                    # Show first 100 characters of the statement for debugging
                    stmt_preview = statement.strip()[:100].replace('\n', ' ')
                    logger.debug(f"Executing statement {i+1}/{len(statements)}: {stmt_preview}...")
                    try:
                        db.execute_sql(text(statement))
                    except Exception as e:
                        logger.error(f"Failed on statement {i+1}: {stmt_preview} \nError: {e}")
                        raise

            logger.success(f"Successfully executed {filename}")
            return True

        except Exception as e:
            logger.error(f"Error executing {filename}: {e}")
            return False


    def _split_sql_statements(self, sql_content: str):
        """Split SQL content into individual statements"""
        lines = []
        in_function = False

        for line in sql_content.split('\n'):
            # Skip comments
            stripped = line.strip()
            if stripped.startswith('--') and '$$' not in line:
                continue

            # Track if we're inside a function definition
            if '$$' in line:
                in_function = not in_function
            lines.append(line)

        # Now split on semicolons
        statements = []
        current = []
        in_function = False

        for line in lines:
            if '$$' in line:
                in_function = not in_function
            current.append(line)

            # Split on semicolon if not inside a function and line ends with semicolon
            if line.rstrip().endswith(';') and not in_function:
                statement = '\n'.join(current).strip()
                # Only add non-empty statements
                if statement and not statement.startswith('--'):
                    statements.append(statement)
                current = []

        # Add any remaining lines as a statement
        if current:
            statement = '\n'.join(current).strip()
            if statement and not statement.startswith('--'):
                statements.append(statement)
        return statements



    def create_metadata_tables(self):
        """Create metadata tracking tables"""
        return self.execute_sql_file('00_etl_metadata.sql')

    def create_all_tables(self):
        """Create all tables in order"""
        sql_files = sorted(self.sql_dir.glob('*.sql'))
        for sql_file in sql_files:
            if not self.execute_sql_file(sql_file.name):
                logger.error(f"Failed to execute {sql_file.name}, stopping.")
                return False

        # After creating tables, create indexes
        logger.info("Creating performance indexes...")
        if not self.create_indexes():
            logger.warning("Some indexes failed to create, but continuing...")

        return True

    def create_indexes(self):
        """Create all indexes from the indexes directory"""
        if not self.indexes_dir.exists():
            logger.warning(f"Indexes directory not found: {self.indexes_dir}")
            return True  # Not a critical error

        index_files = sorted(self.indexes_dir.glob('*.sql'))
        if not index_files:
            logger.info("No index files found in indexes directory")
            return True

        success = True
        for index_file in index_files:
            logger.info(f"Creating indexes from {index_file.name}")
            if not self.execute_sql_file(index_file.name, directory=self.indexes_dir):
                logger.error(f"Failed to execute {index_file.name}")
                success = False

        return success