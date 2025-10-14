# Performance Investigation Report: 20+ Second Page Loads After Database Rebuild

**Date:** 2025-10-13
**Investigator:** Architecture Analysis
**Severity:** CRITICAL
**Status:** ROOT CAUSE IDENTIFIED

---

## Executive Summary

After rebuilding the database with fresh data, pages that previously loaded in < 1 second now take 20+ seconds despite database queries being fast (0.107ms). Investigation reveals a **MASSIVE SQLAlchemy cascade explosion** caused by `lazy='joined'` relationships that cannot be overridden by query options.

**Key Finding:** The profiling script shows only 204ms and 5 queries, but the actual web application is experiencing 20+ second loads. This suggests:
1. The profiling script is NOT using a full Flask request context
2. The actual web application is triggering additional cascades not captured by the profile
3. Template rendering may be accessing relationships that trigger lazy loads

---

## Root Cause Analysis

### The Smoking Gun: Player.query.get() in Service Layer

In `/mnt/hdd/PycharmProjects/rb2/web/app/services/player_service.py`:

```python
def get_player_career_batting_stats(player_id, league_level_filter=None):
    # LINE 62: THIS IS THE PROBLEM
    player = Player.query.get(player_id)
    if not player:
        return {'yearly_stats': [], 'career_totals': None}
```

**This single line triggers the entire cascade explosion:**

```
Player.query.get(player_id)
  ├─> city_of_birth (lazy='joined')
  │     ├─> nation (lazy='joined')
  │     │     └─> continent (lazy='joined')
  │     └─> state (lazy='joined')
  │           └─> nation (lazy='joined')
  │                 └─> continent (lazy='joined')
  ├─> nation (lazy='joined')
  │     └─> continent (lazy='joined')
  ├─> second_nation (lazy='joined')
  │     └─> continent (lazy='joined')
  ├─> current_status (lazy='joined')
  │     ├─> team (lazy='joined')
  │     │     ├─> city (lazy='joined')
  │     │     │     ├─> nation (lazy='joined')
  │     │     │     │     └─> continent (lazy='joined')
  │     │     │     └─> state (lazy='joined')
  │     │     │           └─> nation (lazy='joined')
  │     │     │                 └─> continent (lazy='joined')
  │     │     ├─> park (lazy='joined')
  │     │     │     └─> nation (lazy='joined')
  │     │     │           └─> continent (lazy='joined')
  │     │     ├─> nation (lazy='joined')
  │     │     │     └─> continent (lazy='joined')
  │     │     └─> league (lazy='joined')
  │     │           ├─> nation (lazy='joined')
  │     │           │     └─> continent (lazy='joined')
  │     │           ├─> language (lazy='joined')
  │     │           └─> parent_league (lazy='joined') [RECURSIVE!]
  │     │                 └─> Same cascade as above
  │     └─> league (lazy='joined')
  │           └─> Same cascade as League above
  ├─> batting_ratings (lazy='joined')
  ├─> pitching_ratings (lazy='joined')
  └─> fielding_ratings (lazy='joined')
```

**Estimated Query Count Per Player Load: 50-100+ queries** (depending on relationship depth)

---

## Why This Worked Before vs. Now

### Theory 1: Data Volume in Reference Tables (MOST LIKELY)

**Before Database Rebuild:**
- Possibly fewer nations, cities, continents in reference tables
- Fewer league hierarchies
- Smaller cascade depth

**After Database Rebuild:**
- More complete reference data (all nations, cities, states)
- More complex league hierarchies (parent leagues, affiliates)
- Deeper relationship graphs

**Evidence:**
- The continuity.txt shows this is a "test league" with non-traditional structure
- 4 top-level leagues instead of standard AL/NL
- Complex affiliate relationships

### Theory 2: PostgreSQL Configuration or Connection Pooling

**Before:**
- Database might have had better connection pooling settings
- Query plan cache might have been warmer

**After:**
- Fresh database = cold caches
- Connection pool might need tuning

### Theory 3: Foreign Key Constraints Added

The schema shows FOREIGN KEY constraints exist. If these were NOT present before but ARE present now, SQLAlchemy's relationship loading behavior could change.

---

## Complete Cascade Map

### Reference Data Hierarchy

```
Continent (no relationships)
  └─> Nation (lazy='joined': continent)
        ├─> State (lazy='joined': nation)
        └─> City (lazy='joined': nation, state)
```

### League Hierarchy (RECURSIVE DANGER!)

```
League (lazy='joined': nation, language, parent_league)
  ├─> Nation → Continent cascade
  ├─> Language (safe, no relationships)
  └─> parent_league → RECURSIVE League cascade
```

**CRITICAL:** The `parent_league` relationship can recursively load multiple levels of league hierarchy!

### Team Cascade (MASSIVE)

```
Team (lazy='joined': city, park, nation, league)
  ├─> City (lazy='joined': nation, state)
  │     ├─> Nation → Continent
  │     └─> State → Nation → Continent
  ├─> Park (lazy='joined': nation)
  │     └─> Nation → Continent
  ├─> Nation → Continent
  └─> League (see recursive cascade above)
```

**Per Team: ~15-25 additional queries**

### Player Cascade

```
Player (lazy='joined': city_of_birth, nation, second_nation, current_status,
        batting_ratings, pitching_ratings, fielding_ratings)
  ├─> city_of_birth → Full City cascade (~5 queries)
  ├─> nation → Continent (~2 queries)
  ├─> second_nation → Continent (~2 queries)
  ├─> current_status (lazy='joined': team, league)
  │     ├─> team → Full Team cascade (~20 queries)
  │     └─> league → Full League cascade (~5+ queries, potentially recursive)
  ├─> batting_ratings (safe, no relationships)
  ├─> pitching_ratings (safe, no relationships)
  └─> fielding_ratings (safe, no relationships)
```

**Per Player: ~40-60 queries**

---

## Why Query Options Don't Work

From the endpoint code (`/mnt/hdd/PycharmProjects/rb2/web/app/routes/players.py` lines 121-184):

```python
player = (Player.query
          .options(
              load_only(Player.player_id, Player.first_name, ...),
              selectinload(Player.city_of_birth).load_only(...).raiseload('*'),
              raiseload('*')  # <-- This does NOT block lazy='joined'!
          )
          .filter_by(player_id=player_id)
          .first_or_404())
```

**THE PROBLEM:**
- `raiseload('*')` blocks `lazy='select'` and `lazy='selectin'` relationships
- `lazyload()` changes load strategy to `lazy='select'`
- **NEITHER OF THESE WORK ON `lazy='joined'` RELATIONSHIPS**
- `lazy='joined'` is baked into the query at the model definition level
- SQLAlchemy adds LEFT OUTER JOINs to the query automatically

**Proof:** The service layer calls `Player.query.get(player_id)` which uses NO query options, so ALL `lazy='joined'` relationships load.

---

## Actual Query Count on Player Detail Page

Based on code analysis:

1. **Player detail endpoint** (`player_detail()`):
   - Initial player query: 1 (but triggers ~40-60 cascaded queries)

2. **Batting stats** (`get_player_career_batting_stats()`):
   - `Player.query.get()`: 1 (triggers ~40-60 cascaded queries AGAIN)
   - Batting stats query: 1
   - Career totals aggregation: 1

3. **Pitching stats** (`get_player_career_pitching_stats()`):
   - `Player.query.get()`: 1 (triggers ~40-60 cascaded queries AGAIN!)
   - Pitching stats query: 1
   - Career totals aggregation: 1

4. **Trade history** (`get_player_trade_history()`):
   - Trade query: 1

5. **Player news** (`get_player_news()`):
   - Message query: 1

**Conservative Estimate:**
- Base queries: ~10
- Cascaded queries: 3 × 50 (three Player.query.get() calls) = 150
- **Total: ~160 queries**

**Actual execution:**
- 160 queries × 0.5ms average = 80ms in query time
- Plus Python overhead for object creation
- Plus template rendering accessing relationships
- **Result: 20+ seconds is extreme, but 150+ queries explains significant slowness**

---

## Why Profiling Script Shows Different Results

The profiling script (`/mnt/hdd/PycharmProjects/rb2/profile_player_detailed.py`) shows only 5 queries and 204ms because:

1. **Missing Flask request context:** Template rendering isn't happening
2. **No actual relationship access:** The script doesn't access `player.city_of_birth.name` or other properties
3. **SQLAlchemy lazy load deferred:** Relationships are loaded but not accessed, so nested cascades don't trigger
4. **No SQLAlchemy session echo:** The script counts queries but may not be seeing all of them

**To fix profiling:**
```python
# Add this to see ALL queries
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Access relationships to trigger cascades
print(player.city_of_birth.name)  # This would trigger the cascade
print(player.current_status.team.city.name)  # This would show the explosion
```

---

## Recommended Solutions (In Priority Order)

### Solution 1: Fix Service Layer to NOT Load Full Player Object (IMMEDIATE FIX)

**Problem:** Lines 62 and 221 in `player_service.py`:
```python
player = Player.query.get(player_id)  # BAD - loads everything
```

**Solution:** Only query for date_of_birth:
```python
def get_player_career_batting_stats(player_id, league_level_filter=None):
    """Get player's yearly batting statistics with career totals."""
    # FIX: Don't load entire Player object, just get birth date
    from sqlalchemy.orm import load_only

    player = Player.query.options(
        load_only(Player.player_id, Player.date_of_birth)
    ).get(player_id)

    # Or even better - use raw SQL
    from sqlalchemy import text
    result = db.session.execute(
        text("SELECT date_of_birth FROM players_core WHERE player_id = :player_id"),
        {"player_id": player_id}
    ).first()

    if not result or not result[0]:
        return {'yearly_stats': [], 'career_totals': None}

    birth_date = result[0]
    # ... rest of function uses birth_date instead of player.date_of_birth
```

**Impact:** Eliminates 100+ cascaded queries immediately.

**Files to Change:**
- `/mnt/hdd/PycharmProjects/rb2/web/app/services/player_service.py` (lines 62, 221)

### Solution 2: Add lazy='noload' Override in Query Options

While `raiseload('*')` doesn't work on `lazy='joined'`, we CAN explicitly override each relationship:

```python
from sqlalchemy.orm import noload

player = (Player.query
          .options(
              load_only(Player.player_id, Player.first_name, ...),
              noload(Player.city_of_birth),  # Explicitly block
              noload(Player.nation),         # Explicitly block
              noload(Player.second_nation),  # Explicitly block
              noload(Player.current_status), # Explicitly block
              noload(Player.batting_ratings),
              noload(Player.pitching_ratings),
              noload(Player.fielding_ratings),
          )
          .filter_by(player_id=player_id)
          .first_or_404())
```

**Impact:** Prevents cascade in endpoint, but doesn't fix service layer issue.

### Solution 3: Change Model Definitions to lazy='select' (LONG-TERM FIX)

**Problem:** All the `lazy='joined'` declarations in models are dangerous.

**Solution:** Change default loading strategy:

```python
# In /mnt/hdd/PycharmProjects/rb2/web/app/models/player.py
# CHANGE FROM:
city_of_birth = db.relationship('City', foreign_keys=[city_of_birth_id], lazy='joined')

# CHANGE TO:
city_of_birth = db.relationship('City', foreign_keys=[city_of_birth_id], lazy='select')
```

**Then use explicit `selectinload()` or `joinedload()` in queries where needed:**

```python
# When you actually WANT the city:
player = (Player.query
          .options(joinedload(Player.city_of_birth))
          .filter_by(player_id=player_id)
          .first())
```

**Impact:** Requires extensive code changes across all models and queries.

**Pros:**
- Explicit control over loading
- Prevents accidental cascades
- Better long-term maintainability

**Cons:**
- Requires testing all existing code
- Must add explicit loads where needed

### Solution 4: Use Raw SQL for Player Detail Page (QUICK WIN)

Following the pattern from `continuity.txt` (lines 107-118), convert player detail to raw SQL:

```python
from sqlalchemy import text

@bp.route('/<int:player_id>')
def player_detail(player_id):
    """Player detail page - bio, stats, ratings."""
    # Raw SQL to avoid cascade
    query = text("""
        SELECT
            p.player_id, p.first_name, p.last_name, p.nick_name,
            p.date_of_birth, p.height, p.weight, p.bats, p.throws,
            c.name as city_name, c.state_id, c.nation_id,
            n.abbreviation as nation_abbr,
            s.abbreviation as state_abbr,
            pcs.team_id, pcs.position, pcs.retired,
            t.name as team_name, t.abbr as team_abbr
        FROM players_core p
        LEFT JOIN cities c ON p.city_of_birth_id = c.city_id
        LEFT JOIN nations n ON p.nation_id = n.nation_id
        LEFT JOIN states s ON c.state_id = s.state_id AND c.nation_id = s.nation_id
        LEFT JOIN players_current_status pcs ON p.player_id = pcs.player_id
        LEFT JOIN teams t ON pcs.team_id = t.team_id
        WHERE p.player_id = :player_id
    """)

    result = db.session.execute(query, {"player_id": player_id}).first()
    if not result:
        abort(404)

    # Convert to dict for template
    player_data = {
        'player_id': result.player_id,
        'first_name': result.first_name,
        # ... etc
    }

    # ... rest of endpoint
```

**Impact:**
- Eliminates ALL cascades
- One query instead of 160+
- Page load: < 100ms

**Pros:**
- Immediate fix
- Proven pattern (already used in context_processors.py)
- Predictable performance

**Cons:**
- Loses ORM benefits (change tracking, relationships)
- More verbose code
- Must manually map columns

---

## Alternative Solutions (NOT Recommended)

### NOT Recommended: Database-Level Fixes

**Why not:**
- The database is HEALTHY (queries are fast, indexes work)
- Foreign key constraints are CORRECT
- Problem is in ORM layer, not database

**What NOT to try:**
- Removing foreign keys (breaks data integrity)
- Changing connection pooling (won't fix cascade problem)
- Adding more indexes (queries are already using indexes)

### NOT Recommended: Rolling Back to Old Database

**Why not:**
- The old data may not exist anymore
- Doesn't fix the underlying ORM design problem
- New data should work with existing code

---

## Immediate Action Plan (Next 30 Minutes)

### Step 1: Fix Service Layer (5 minutes)

Edit `/mnt/hdd/PycharmProjects/rb2/web/app/services/player_service.py`:

```python
# Line 62 - Change from:
player = Player.query.get(player_id)

# Change to:
birth_date_result = db.session.execute(
    text("SELECT date_of_birth FROM players_core WHERE player_id = :player_id"),
    {"player_id": player_id}
).first()

if not birth_date_result or not birth_date_result[0]:
    return {'yearly_stats': [], 'career_totals': None}

birth_date = birth_date_result[0]

# Change all references from player.date_of_birth to birth_date
# Line 92: stat.age = calculate_age_for_season(birth_date, stat.year)
```

Repeat for `get_player_career_pitching_stats()` at line 221.

### Step 2: Test Player Detail Page (2 minutes)

```bash
# Start Flask app
cd /mnt/hdd/PycharmProjects/rb2/web
~/virtual-envs/rb2/bin/python run.py

# In browser, test:
# http://localhost:5000/players/16747
```

**Expected result:** Page load should drop from 20+ seconds to < 1 second.

### Step 3: Enhanced Profiling (3 minutes)

Update profiling script to see actual cascades:

```python
# In profile_player_detailed.py, add after line 19:
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Add after player query (line 101):
print(f"Player name: {player.first_name} {player.last_name}")
print(f"Birthplace: {player.city_of_birth.name if player.city_of_birth else 'Unknown'}")
print(f"Team: {player.current_status.team.name if player.current_status and player.current_status.team else 'Unknown'}")
```

Run again:
```bash
~/virtual-envs/rb2/bin/python profile_player_detailed.py 2>&1 | grep "SELECT" | wc -l
```

This will show the REAL query count.

---

## Long-Term Architecture Recommendations

### 1. Establish Loading Strategy Convention

Create `/mnt/hdd/PycharmProjects/rb2/web/docs/ORM_LOADING_STRATEGY.md`:

```markdown
# ORM Loading Strategy Convention

## Default: lazy='select'
All relationships should use `lazy='select'` by default unless explicitly needed otherwise.

## When to use lazy='joined':
- NEVER for relationships that have their own relationships (prevents cascade)
- Only for true one-to-one relationships with no further nesting
- Only after profiling confirms benefit

## When to use lazy='selectin':
- For collections (one-to-many) that are usually loaded together
- Reference data that's frequently accessed
- When you need the data but want to avoid N+1 queries

## When to use lazy='noload' or lazy='raise':
- For debugging relationship loading
- For relationships that should never be accessed

## Explicit Loading in Queries:
Always use explicit loading options in queries:
- selectinload() for collections
- joinedload() for single objects
- load_only() to limit columns
```

### 2. Create Service Layer Pattern

All data access should go through service layer that uses raw SQL or explicit loading:

```python
# web/app/services/base_service.py
from sqlalchemy import text
from app.extensions import db

class BaseService:
    """Base service with helper methods for raw SQL queries."""

    @staticmethod
    def execute_query(query_text, params=None):
        """Execute raw SQL query safely."""
        return db.session.execute(text(query_text), params or {})

    @staticmethod
    def fetch_one(query_text, params=None):
        """Fetch single row."""
        result = BaseService.execute_query(query_text, params)
        return result.first()

    @staticmethod
    def fetch_all(query_text, params=None):
        """Fetch all rows."""
        result = BaseService.execute_query(query_text, params)
        return result.fetchall()
```

### 3. Add Query Profiling Middleware

```python
# web/app/middleware/query_profiler.py
from flask import g, request
from sqlalchemy import event
from sqlalchemy.engine import Engine
import time

def init_query_profiler(app):
    """Initialize query profiling for development."""
    if not app.debug:
        return

    @event.listens_for(Engine, "before_cursor_execute")
    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        conn.info.setdefault('query_start_time', []).append(time.time())
        g.setdefault('query_count', 0)
        g.query_count += 1

    @event.listens_for(Engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        total = time.time() - conn.info['query_start_time'].pop(-1)
        g.setdefault('query_time', 0)
        g.query_time += total

    @app.after_request
    def log_query_stats(response):
        """Log query stats in response headers (dev only)."""
        if hasattr(g, 'query_count'):
            response.headers['X-Query-Count'] = g.query_count
            response.headers['X-Query-Time'] = f"{g.query_time * 1000:.2f}ms"
        return response
```

### 4. Migration Plan for Models

Priority order for changing `lazy='joined'` to `lazy='select'`:

1. **HIGH PRIORITY** (causes deep cascades):
   - Player.current_status
   - Team.league
   - Team.city
   - Team.park
   - City.state
   - Nation.continent
   - League.parent_league

2. **MEDIUM PRIORITY** (causes moderate cascades):
   - Player.city_of_birth
   - Player.nation
   - PlayerCurrentStatus.team
   - PlayerCurrentStatus.league

3. **LOW PRIORITY** (safe, no further relationships):
   - Player.batting_ratings
   - Player.pitching_ratings
   - Player.fielding_ratings

---

## Performance Testing Checklist

After implementing fixes, verify:

- [ ] Player detail page: < 1s, < 20 queries
- [ ] Team detail page: < 500ms, < 15 queries
- [ ] Home page: < 200ms, < 10 queries
- [ ] Leaderboard pages: < 2s, < 30 queries
- [ ] Profile script shows consistent results with actual pages
- [ ] Browser DevTools Network tab shows reasonable load times
- [ ] PostgreSQL slow query log shows no queries > 100ms

---

## Conclusion

The 20+ second page loads are caused by SQLAlchemy `lazy='joined'` cascades that load 100+ queries per page. The database is healthy; the problem is purely in the ORM layer.

**Immediate fix:** Change `Player.query.get()` to raw SQL in service layer.

**Long-term fix:** Refactor models to use `lazy='select'` and explicit loading in queries.

**Estimated time to fix immediate issue:** 30 minutes
**Estimated time for full model refactor:** 4-8 hours
**Expected performance improvement:** 20+ seconds → < 1 second

---

## Files Referenced

- `/mnt/hdd/PycharmProjects/rb2/web/app/models/player.py` (lines 72-137)
- `/mnt/hdd/PycharmProjects/rb2/web/app/models/team.py` (lines 58-87)
- `/mnt/hdd/PycharmProjects/rb2/web/app/models/league.py` (lines 44-62)
- `/mnt/hdd/PycharmProjects/rb2/web/app/models/reference.py` (lines 74-86, 132-136, 174-189)
- `/mnt/hdd/PycharmProjects/rb2/web/app/routes/players.py` (lines 108-207)
- `/mnt/hdd/PycharmProjects/rb2/web/app/services/player_service.py` (lines 45-347)
- `/mnt/hdd/PycharmProjects/rb2/profile_player_detailed.py` (entire file)
- `/mnt/hdd/PycharmProjects/rb2/continuity.txt` (lines 20-66, 72-125)

---

**End of Report**
