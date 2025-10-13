"""Profile player detail page performance with detailed breakdown."""
import sys
import time
from sqlalchemy import event
from sqlalchemy.engine import Engine

# Track queries with details
queries = []

@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())

@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    # Get first 100 chars of statement
    stmt_preview = statement[:100].replace('\n', ' ')
    queries.append((total, stmt_preview))

# Now import app (after event listeners are set up)
sys.path.insert(0, '/mnt/hdd/PycharmProjects/rb2/web')
from app import create_app

app = create_app()

# Test with a real player ID
player_id = 16747

print(f"\n{'='*80}")
print(f"DETAILED PROFILING: Player Detail Page (player_id={player_id})")
print(f"{'='*80}\n")

with app.app_context():
    from app.routes.players import player_detail
    from flask import Flask

    # Reset counters
    queries.clear()

    # Time the entire request
    start_time = time.time()

    # Simulate the route call
    from app.models import Player
    from app.services import player_service
    from sqlalchemy.orm import load_only, selectinload, raiseload, lazyload
    from app.models import PlayerCurrentStatus, City, Nation, Team
    from app.models import PlayerBattingRatings, PlayerPitchingRatings, PlayerFieldingRatings

    # Mark where player query starts
    print(">>> FETCHING PLAYER...")
    query_start = len(queries)

    player = (Player.query
              .options(
                  load_only(
                      Player.player_id,
                      Player.first_name,
                      Player.last_name,
                      Player.nick_name,
                      Player.date_of_birth,
                      Player.height,
                      Player.weight,
                      Player.bats,
                      Player.throws,
                      Player.city_of_birth_id,
                      Player.nation_id,
                      Player.second_nation_id
                  ),
                  selectinload(Player.city_of_birth).load_only(
                      City.city_id,
                      City.name
                  ).raiseload('*'),
                  selectinload(Player.nation).load_only(
                      Nation.nation_id,
                      Nation.name,
                      Nation.abbreviation
                  ).raiseload('*'),
                  selectinload(Player.second_nation).load_only(
                      Nation.nation_id,
                      Nation.name,
                      Nation.abbreviation
                  ).raiseload('*'),
                  selectinload(Player.current_status).load_only(
                      PlayerCurrentStatus.player_id,
                      PlayerCurrentStatus.team_id,
                      PlayerCurrentStatus.position,
                      PlayerCurrentStatus.retired
                  ).selectinload(PlayerCurrentStatus.team).load_only(
                      Team.team_id,
                      Team.name,
                      Team.abbr
                  ).raiseload('*'),
                  lazyload(Player.batting_ratings),
                  lazyload(Player.pitching_ratings),
                  lazyload(Player.fielding_ratings),
                  raiseload('*')
              )
              .filter_by(player_id=player_id)
              .first())

    player_queries = len(queries) - query_start
    print(f"    Player query: {player_queries} queries")

    print("\n>>> FETCHING BATTING STATS...")
    query_start = len(queries)
    batting_data = player_service.get_player_career_batting_stats(player_id)
    batting_queries = len(queries) - query_start
    print(f"    Batting stats: {batting_queries} queries")

    print("\n>>> FETCHING PITCHING STATS...")
    query_start = len(queries)
    pitching_data = player_service.get_player_career_pitching_stats(player_id)
    pitching_queries = len(queries) - query_start
    print(f"    Pitching stats: {pitching_queries} queries")

    print("\n>>> FETCHING TRADE HISTORY...")
    query_start = len(queries)
    trade_history = player_service.get_player_trade_history(player_id)
    trade_queries = len(queries) - query_start
    print(f"    Trade history: {trade_queries} queries")

    print("\n>>> FETCHING PLAYER NEWS...")
    query_start = len(queries)
    player_news = player_service.get_player_news(player_id)
    news_queries = len(queries) - query_start
    print(f"    Player news: {news_queries} queries")

    end_time = time.time()
    total_time = (end_time - start_time) * 1000

print(f"\n{'='*80}")
print(f"SUMMARY:")
print(f"{'='*80}")
print(f"Total Time:      {total_time:.1f}ms")
print(f"Total Queries:   {len(queries)}")
print(f"  - Player:      {player_queries}")
print(f"  - Batting:     {batting_queries}")
print(f"  - Pitching:    {pitching_queries}")
print(f"  - Trades:      {trade_queries}")
print(f"  - News:        {news_queries}")
print(f"{'='*80}\n")
