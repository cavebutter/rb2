#! /usr/bin/env python3
"""
OOTP ETL Pipeline Entry Point
"""

import click
from loguru import logger
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Configure logger
logger.remove()
logger.add(
    "logs/etl_{time: YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    level="DEBUG",
    format="{time: YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
)
logger.add(sys.stderr, level="INFO")

# Load environment variables from .env file
load_dotenv()

@click.group()
@click.option('--debug/--no-debug', default=False, help='Enable debug logging')
def cli(debug):
    """OOTP ETIL Pipeline"""
    if debug:
        logger.add(sys.stderr, level="DEBUG")
        logger.info("Starting OOTP ETL Pipeline")


@cli.command()
@click.option('--dry-run', is_flag=True, help='Show what would be done without executing')
def fetch_data(dry_run):
    """Fetch latest data from OOTP game machine"""
    from src.utils.fetch import fetch_game_data
    logger.info(f"Fetching game data (dry_run={dry_run})")
    fetch_game_data(dry_run=dry_run)


@cli.command()
@click.option('--tables', '-t', multiple=True, help='Specific tables to load')
@click.option('--full/--incremental', default=False, help='Full reload vs incremental')
def load_data(tables, full):
    """Load CSV data into PostreSQL"""
    logger.info(f"Loading data - Tables: {tables or 'all'}, Mode: {'full' if full else 'incremental'}")
    # TODO Implement data loading
    click.echo("Data loading not yet implemented.")


@cli.command()
def check_status():
    """Check ETL pipeline status"""
    from src.database.connection import test_connection
    logger.info('Checking ETL pipeline status')

    # Test database connection
    if test_connection():
        click.echo("✓ Database connection successful")
    else:
        click.echo("✗ Database connection failed")

    # TODO Add more status checks (e.g., last successful run, data freshness)


@cli.command('init-db')
@click.option('--metadata-only', is_flag=True, help='Only create metadata tables')
def init_database(metadata_only):
    """Initializ database schema"""
    from src.database.schema import SchemaManager

    schema_manager = SchemaManager()

    if metadata_only:
        logger.info('Creating ETL metadata tables only')
        if schema_manager.create_metadata_tables():
            click.echo("Metadata table created")
        else:
            click.echo("Failed to create metadata tables")
    else:
        logger.info('Creating all database tables')
        if schema_manager.create_all_tables():
            click.echo("✓ All tables created successfully")
        else:
            click.echo("✗ Failed to create all tables")


@cli.command('load-reference')
@click.option('--file', '-f', help="Specific CSV file to load")
@click.option('--force', is_flag=True, help="Force reload even if unchanged")
def load_reference_data(file, force):
    """Load reference data tables"""
    from src.loaders.reference_loader import ReferenceLoader
    from src.database.connection import db
    from pathlib import Path
    import uuid
    from sqlalchemy import text

    # Start a batch
    batch_id = str(uuid.uuid4())
    batch_sql = text("""
        INSERT INTO etl_batch_runs (batch_id, batch_type, triggered_by, environment, status)
        VALUES (:batch_id, :batch_type, :triggered_by, :environment, :status)
        """)

    db.execute_sql(batch_sql, {
        'batch_id': batch_id,
        'batch_type': 'incremental',
        'triggered_by': 'manual',
        'environment': 'dev',
        'status': 'running'
    })
    # Get data directory
    data_dir = Path(__file__).parent / "data" / "incoming" / "csv"

    if file:
        # Load specific file
        csv_files = [file] if file.endswith('.csv') else [f"{file}.csv"]

    else:
        # Load all reference files in order
        csv_files = ReferenceLoader.get_load_order()

    logger.info(f"Loading reference tables: {csv_files}")

    for csv_file in csv_files:
        csv_path = data_dir / csv_file

        if not csv_path.exists():
            logger.warning(f"File {csv_path} not found.")
            continue

        try:
            loader = ReferenceLoader(csv_path.name, batch_id)
            if force:
                # Temporarily override load strategy to full
                loader.get_load_strategy = lambda: 'full'

            success = loader.load_csv(csv_path)

            if success:
                click.echo(f"Successfully loaded {csv_path}")
            else:
                click.echo(f"Failed to load {csv_path}")

        except Exception as e:
            logger.error(f"Error loading {csv_path}: {e}")
            click.echo(f"Error loading {csv_path}: {e}")


if __name__ == "__main__":
    cli()