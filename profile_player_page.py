"""Profile player detail page performance."""
import sys
import time
from sqlalchemy import event
from sqlalchemy.engine import Engine

# Track queries
query_count = 0
query_times = []

@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())

@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    global query_count, query_times
    total = time.time() - conn.info['query_start_time'].pop(-1)
    query_count += 1
    query_times.append(total)

# Now import app (after event listeners are set up)
sys.path.insert(0, '/mnt/hdd/PycharmProjects/rb2/web')
from app import create_app

app = create_app()

# Test with a real player ID
player_id = 16747  # From previous sessions

print(f"\n{'='*80}")
print(f"PROFILING: Player Detail Page (player_id={player_id})")
print(f"{'='*80}\n")

with app.app_context():
    from app.routes.players import player_detail
    from flask import Flask

    # Reset counters
    query_count = 0
    query_times = []

    # Time the entire request
    start_time = time.time()

    # Simulate the route call
    from app.models import Player
    from app.services import player_service
    from sqlalchemy.orm import joinedload, raiseload
    from app.models import PlayerCurrentStatus

    # This is the exact code from player_detail route
    player = (Player.query
              .options(
                  joinedload(Player.city_of_birth),
                  joinedload(Player.nation),
                  joinedload(Player.second_nation),
                  joinedload(Player.current_status).joinedload(PlayerCurrentStatus.team, innerjoin=False),
                  raiseload(Player.batting_stats),
                  raiseload(Player.pitching_stats)
              )
              .filter_by(player_id=player_id)
              .first())

    batting_data = player_service.get_player_career_batting_stats(player_id)
    pitching_data = player_service.get_player_career_pitching_stats(player_id)
    trade_history = player_service.get_player_trade_history(player_id)
    player_news = player_service.get_player_news(player_id)

    end_time = time.time()
    total_time = (end_time - start_time) * 1000  # Convert to ms

print(f"\n{'='*80}")
print(f"RESULTS:")
print(f"{'='*80}")
print(f"Total Time:    {total_time:.1f}ms")
print(f"Query Count:   {query_count}")
print(f"Avg Query:     {sum(query_times)/len(query_times)*1000:.1f}ms" if query_times else "N/A")
print(f"Slowest Query: {max(query_times)*1000:.1f}ms" if query_times else "N/A")
print(f"\n{'='*80}")
print(f"BREAKDOWN:")
print(f"{'='*80}")
print(f"Batting stats:  {len(batting_data['yearly_stats'])} years")
print(f"Pitching stats: {len(pitching_data['yearly_stats'])} years")
print(f"Trade history:  {len(trade_history)} trades")
print(f"Player news:    {len(player_news)} messages")
print(f"{'='*80}\n")

# Show query time distribution
if query_times:
    query_times_sorted = sorted(query_times, reverse=True)
    print("\nTop 5 Slowest Queries:")
    for i, qt in enumerate(query_times_sorted[:5], 1):
        print(f"  {i}. {qt*1000:.1f}ms")
    print()
