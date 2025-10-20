#! /usr/bin/env python3
"""
OOTP ETL Pipeline Entry Point
"""
import uuid

import click
from loguru import logger
from dotenv import load_dotenv
import sys
from pathlib import Path
from src.utils.batch import generate_batch_id
from src.database.schema import db
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


@cli.command('refresh-views')
def refresh_materialized_views():
    """Refresh all materialized views (run after loading stats)"""
    from pathlib import Path
    from sqlalchemy import text

    logger.info('Refreshing materialized views...')

    sql_path = Path(__file__).parent / "sql" / "maintenance" / "refresh_materialized_views.sql"

    if not sql_path.exists():
        logger.error(f"SQL file not found: {sql_path}")
        click.echo("✗ Refresh script not found")
        return

    try:
        with open(sql_path, 'r') as f:
            sql_content = f.read()

        db.execute_sql(text(sql_content))

        logger.success("All materialized views refreshed successfully!")
        click.echo("✓ Materialized views refreshed")

    except Exception as e:
        logger.error(f"Error refreshing views: {e}")
        click.echo(f"✗ Failed to refresh views: {e}")


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


@cli.command('load-stats')
@click.option('--force-all-constants', is_flag=True, help="Recalculate constants for all years")
def load_stats(force_all_constants):
  """Load all player statistics"""
  from src.loaders.players_loader import PlayersLoader
  from src.loaders.batting_stats_loader import BattingStatsLoader
  from src.loaders.pitching_stats_loader import PitchingStatsLoader
  from src.transformers.league_constants_transformer import LeagueConstantsTransformer
  from sqlalchemy import text

  batch_id = generate_batch_id()

  # Phase 1 - Load raw data
  logger.info('Loading players...')
  players_loader = PlayersLoader(batch_id)
  players_loader.load_csv(Path("data/incoming/csv/players.csv"))

  logger.info('Loading batting stats...')
  batting_loader = BattingStatsLoader(batch_id=generate_batch_id())
  batting_loader.load_csv(Path("data/incoming/csv/players_career_batting_stats.csv"))

  logger.info('Loading pitching stats...')
  pitching_loader = PitchingStatsLoader(batch_id=generate_batch_id())
  pitching_loader.load_csv(Path("data/incoming/csv/players_career_pitching_stats.csv"))

  # Load game-level stats for newspaper article generation
  logger.info('Loading game batting stats...')
  from src.loaders.game_stats_loader import GameBattingStatsLoader, GamePitchingStatsLoader
  game_batting_loader = GameBattingStatsLoader(batch_id=generate_batch_id())
  game_batting_loader.load_csv(Path("data/incoming/csv/players_game_batting.csv"))
  click.echo("✓ Game batting stats loaded")

  logger.info('Loading game pitching stats...')
  game_pitching_loader = GamePitchingStatsLoader(batch_id=generate_batch_id())
  game_pitching_loader.load_csv(Path("data/incoming/csv/players_game_pitching_stats.csv"))
  click.echo("✓ Game pitching stats loaded")

  # Phase 2 - Calculate league constants
  logger.info('Calculating league constants...')
  constants_transformer = LeagueConstantsTransformer(
      batch_id=batch_id,
      force_all=force_all_constants
  )
  if not constants_transformer.transform_constants():
      logger.error("Constants calculation failed")
      return

  logger.info("Stats loading complete!")

  # Load history tables with player FKs (must be loaded after players)
  logger.info('Loading league history...')
  try:
      from src.loaders.reference_loader import ReferenceLoader
      league_history_loader = ReferenceLoader('league_history.csv', batch_id)
      league_history_loader.load_csv(Path("data/incoming/csv/league_history.csv"))
      click.echo("✓ League history loaded successfully")
  except Exception as e:
      logger.error(f"Error loading league history: {e}")
      click.echo(f"Error loading league history: {e}")

  logger.info('Loading team history...')
  try:
      team_history_loader = ReferenceLoader('team_history.csv', batch_id)
      team_history_loader.load_csv(Path("data/incoming/csv/team_history.csv"))
      click.echo("✓ Team history loaded successfully")
  except Exception as e:
      logger.error(f"Error loading team history: {e}")
      click.echo(f"Error loading team history: {e}")

  # Load coaches and rosters after players are loaded
  logger.info('Loading coaches...')
  try:
      coaches_loader = ReferenceLoader('coaches.csv', batch_id)
      coaches_loader.load_csv(Path("data/incoming/csv/coaches.csv"))
      click.echo("✓ Coaches loaded successfully")
  except Exception as e:
      logger.error(f"Error loading coaches: {e}")
      click.echo(f"Error loading coaches: {e}")

  logger.info('Loading team rosters...')
  try:
      roster_loader = ReferenceLoader('team_roster.csv', batch_id)
      roster_loader.load_csv(Path("data/incoming/csv/team_roster.csv"))
      click.echo("✓ Rosters loaded successfully")
  except Exception as e:
      logger.error(f"Error loading rosters: {e}")
      click.echo(f"Error loading rosters: {e}")

  logger.info('Loading team roster staff...')
  try:
      staff_loader = ReferenceLoader('team_roster_staff.csv', batch_id)
      staff_loader.load_csv(Path("data/incoming/csv/team_roster_staff.csv"))
      click.echo("✓ Roster staff loaded successfully")
  except Exception as e:
      logger.error(f"Error loading staff: {e}")
      click.echo(f"Error loading staff: {e}")

  # Phase 3 - Refresh materialized views for web performance
  logger.info('Refreshing materialized views...')
  try:
      sql_path = Path(__file__).parent / "sql" / "maintenance" / "refresh_materialized_views.sql"
      with open(sql_path, 'r') as f:
          sql_content = f.read()
      db.execute_sql(text(sql_content))
      logger.success("Materialized views refreshed successfully!")
      click.echo("✓ Materialized views refreshed")
  except Exception as e:
      logger.error(f"Error refreshing views: {e}")
      click.echo(f"⚠ Warning: Failed to refresh materialized views - leaderboards may be stale")

  logger.info('All stats, coaches, and rosters loaded!')


@cli.command('generate-articles')
@click.option('--date-range', help='Date range YYYY-MM-DD:YYYY-MM-DD')
@click.option('--force', is_flag=True, help='Regenerate existing articles')
@click.option('--priority', multiple=True, default=['MUST_GENERATE', 'SHOULD_GENERATE'],
              help='Priority tiers to generate (MUST_GENERATE, SHOULD_GENERATE, COULD_GENERATE)')
def generate_newspaper_articles(date_range, force, priority):
    """Generate newspaper articles for Branch family performances"""
    from src.newspaper.pipeline import generate_branch_articles_pipeline
    from datetime import datetime

    logger.info("=" * 80)
    logger.info("NEWSPAPER ARTICLE GENERATION")
    logger.info("=" * 80)

    # Parse date range if provided
    date_range_tuple = None
    if date_range:
        try:
            start_str, end_str = date_range.split(':')
            start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
            date_range_tuple = (start_date, end_date)
            logger.info(f"Date range: {start_date} to {end_date}")
        except ValueError as e:
            click.echo(f"✗ Invalid date range format. Use YYYY-MM-DD:YYYY-MM-DD")
            return 1

    # Parse priority filter
    priority_filter = list(priority) if priority else None
    logger.info(f"Priority filter: {priority_filter}")
    logger.info(f"Force regenerate: {force}")

    try:
        results = generate_branch_articles_pipeline(
            date_range=date_range_tuple,
            force_regenerate=force,
            priority_filter=priority_filter
        )

        # Display results
        click.echo("\n" + "=" * 80)
        click.echo("NEWSPAPER GENERATION RESULTS")
        click.echo("=" * 80)
        click.echo(f"Detected:  {results['detected']:>3} Branch game performances")
        click.echo(f"Generated: {results['generated']:>3} articles")
        click.echo(f"Failed:    {results['failed']:>3} articles")
        click.echo(f"Skipped:   {results['skipped']:>3} articles (already exist)")

        if results['errors']:
            click.echo(f"\nErrors ({len(results['errors'])}):")
            for error in results['errors'][:5]:
                click.echo(f"  - {error}")
            if len(results['errors']) > 5:
                click.echo(f"  ... and {len(results['errors']) - 5} more errors")

        click.echo("=" * 80)

        if results['generated'] > 0:
            click.echo(f"\n✓ Successfully generated {results['generated']} articles")
            return 0
        elif results['failed'] > 0:
            click.echo(f"\n⚠ {results['failed']} articles failed to generate")
            return 1
        else:
            click.echo("\n⚠ No articles generated (this may be normal if no high-priority games found)")
            return 0

    except Exception as e:
        logger.error(f"Article generation failed: {e}")
        click.echo(f"\n✗ Article generation failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


# def _is_initial_load() -> bool:
#   """Check if this is the first time loading data"""
#   return constants.is_initial_load()

if __name__ == "__main__":
    cli()