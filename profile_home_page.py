"""Profile home page performance."""
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

print(f"\n{'='*80}")
print(f"PROFILING: Home Page")
print(f"{'='*80}\n")

with app.app_context():
    # Reset counters
    query_count = 0
    query_times = []

    # Time the entire request
    start_time = time.time()

    # Simulate the route call (import index function)
    from app.routes.main import index

    # Call the index function (this simulates a request)
    try:
        result = index()
        end_time = time.time()
        total_time = (end_time - start_time) * 1000  # Convert to ms
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        total_time = 0

print(f"\n{'='*80}")
print(f"RESULTS:")
print(f"{'='*80}")
print(f"Total Time:    {total_time:.1f}ms")
print(f"Query Count:   {query_count}")
print(f"Avg Query:     {sum(query_times)/len(query_times)*1000:.1f}ms" if query_times else "N/A")
print(f"Slowest Query: {max(query_times)*1000:.1f}ms" if query_times else "N/A")
print(f"{'='*80}\n")

# Show query time distribution
if query_times:
    query_times_sorted = sorted(query_times, reverse=True)
    print("\nTop 10 Slowest Queries:")
    for i, qt in enumerate(query_times_sorted[:10], 1):
        print(f"  {i}. {qt*1000:.1f}ms")
    print()
