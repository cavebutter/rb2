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


if __name__ == "__main__":
    cli()