-- Phase 1 Performance Optimization Indexes
-- Created: 2025-10-14
-- Purpose: Add critical missing indexes for web application performance
-- Expected Impact: 50-70% improvement in page load times
--
-- These indexes address the most common query patterns identified in the
-- optimization strategy (docs/optimization-strategy.md) and will persist
-- across environment refreshes when init-db is run.

-- =============================================================================
-- BATCH 1: Critical Foreign Key Indexes
-- =============================================================================
-- Impact: Player detail pages, coach pages, team roster queries
-- Expected improvement: 30-50% on affected pages

-- Player status indexes (heavily used on player detail pages)
CREATE INDEX IF NOT EXISTS idx_player_status_player_id
ON players_current_status(player_id);

CREATE INDEX IF NOT EXISTS idx_player_status_team_id
ON players_current_status(team_id);

-- Partial index for active players (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_player_status_retired
ON players_current_status(retired)
WHERE retired = 0;

-- Coach indexes (used on coach pages and team pages)
CREATE INDEX IF NOT EXISTS idx_coaches_team
ON coaches(team_id, occupation);


-- =============================================================================
-- BATCH 2: Player Page Optimization Indexes
-- =============================================================================
-- Impact: Player detail pages showing career statistics
-- Expected improvement: 20-30% on player detail pages

-- Composite indexes for player stats with year ordering and split filtering
-- Most queries filter by player_id and split_id=1 (regular season), order by year DESC
CREATE INDEX IF NOT EXISTS idx_batting_stats_composite
ON players_career_batting_stats(player_id, year DESC, split_id)
WHERE split_id = 1;

CREATE INDEX IF NOT EXISTS idx_pitching_stats_composite
ON players_career_pitching_stats(player_id, year DESC, split_id)
WHERE split_id = 1;

-- Trade history optimization (displayed on player detail pages)
CREATE INDEX IF NOT EXISTS idx_trade_history_player
ON trade_history(player_id, trade_date DESC);

-- Messages/news optimization for player pages
CREATE INDEX IF NOT EXISTS idx_messages_player
ON messages(player_id, message_date DESC)
WHERE player_id IS NOT NULL;


-- =============================================================================
-- BATCH 3: Team Page Optimization Indexes
-- =============================================================================
-- Impact: Team pages, franchise history, team rosters
-- Expected improvement: 20-30% on team pages

-- Team relations composite index (used for league/division navigation)
CREATE INDEX IF NOT EXISTS idx_team_relations_composite
ON team_relations(league_id, sub_league_id, division_id, team_id);

-- Team record with included columns for standings queries
-- INCLUDE clause brings commonly-accessed columns into the index
CREATE INDEX IF NOT EXISTS idx_team_record_composite
ON team_record(team_id) INCLUDE (w, l, pct, gb);

-- Team historical data indexes
CREATE INDEX IF NOT EXISTS idx_team_history_composite
ON team_history(team_id, year DESC);

CREATE INDEX IF NOT EXISTS idx_team_season_batting
ON team_season_batting_stats(team_id, year);

CREATE INDEX IF NOT EXISTS idx_team_season_pitching
ON team_season_pitching_stats(team_id, year);


-- =============================================================================
-- BATCH 4: Home Page / Standings Optimization Indexes
-- =============================================================================
-- Impact: Front page standings display
-- Expected improvement: 15-25% on home page

-- Filter top-level teams efficiently (level=1 are MLB teams)
CREATE INDEX IF NOT EXISTS idx_teams_league_level
ON teams(level)
WHERE level = 1;

-- Position-based sorting for standings display
CREATE INDEX IF NOT EXISTS idx_team_record_position
ON team_record(pos);

-- League structure navigation indexes
CREATE INDEX IF NOT EXISTS idx_sub_leagues_composite
ON sub_leagues(league_id, sub_league_id);

CREATE INDEX IF NOT EXISTS idx_divisions_composite
ON divisions(league_id, sub_league_id, division_id);
