-- Phase 2: Remove Unused Indexes (Index Cleanup)
-- Created: 2025-10-14
-- Purpose: Drop indexes from Phase 1 that showed 0 scans in pg_stat_user_indexes
-- Expected Impact: 1-3% improvement by reducing index maintenance overhead
--
-- Analysis based on pg_stat_user_indexes query after Phase 1 testing showed
-- these indexes were never used by the query planner, adding overhead without benefit.

-- =============================================================================
-- UNUSED INDEXES TO DROP (0 scans in pg_stat_user_indexes)
-- =============================================================================

-- Player status indexes - team_id and retired partial index not used
DROP INDEX IF EXISTS idx_player_status_team_id;
DROP INDEX IF EXISTS idx_player_status_retired;

-- Team relations and structure - application doesn't query these directly
DROP INDEX IF EXISTS idx_team_relations_composite;
DROP INDEX IF EXISTS idx_teams_league_level;
DROP INDEX IF EXISTS idx_sub_leagues_composite;
DROP INDEX IF EXISTS idx_divisions_composite;

-- Team history - not used in current query patterns
DROP INDEX IF EXISTS idx_team_history_composite;

-- Team record position - not used for sorting in actual queries
DROP INDEX IF EXISTS idx_team_record_position;

-- =============================================================================
-- INDEXES KEPT (Heavily Used in Phase 1 Testing)
-- =============================================================================
-- idx_player_status_player_id      - 3,007 scans ⭐⭐⭐
-- idx_batting_stats_composite       - 276 scans ⭐⭐
-- idx_pitching_stats_composite      - 354 scans ⭐⭐
-- idx_team_record_composite         - 72 scans ⭐
-- idx_coaches_team                  - 27 scans ⭐
-- idx_trade_history_player          - Kept for player detail pages
-- idx_messages_player               - Kept for player detail pages
-- idx_team_season_batting           - Kept for team pages
-- idx_team_season_pitching          - Kept for team pages
