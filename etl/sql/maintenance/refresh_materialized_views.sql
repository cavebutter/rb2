-- ============================================================================
-- Refresh Materialized Views
-- ============================================================================
-- This script refreshes all materialized views used for leaderboard performance.
-- Run this after loading stats data to populate the views.
--
-- Usage:
--   psql -h 192.168.10.94 -U ootp_etl -d ootp_dev -f refresh_materialized_views.sql
--
-- Or via Python:
--   python main.py refresh-views
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE 'Refreshing materialized views...';

    -- Refresh career leaderboards
    RAISE NOTICE 'Refreshing leaderboard_career_batting...';
    REFRESH MATERIALIZED VIEW leaderboard_career_batting;

    RAISE NOTICE 'Refreshing leaderboard_career_pitching...';
    REFRESH MATERIALIZED VIEW leaderboard_career_pitching;

    -- Refresh single-season leaderboards
    RAISE NOTICE 'Refreshing leaderboard_single_season_batting...';
    REFRESH MATERIALIZED VIEW leaderboard_single_season_batting;

    RAISE NOTICE 'Refreshing leaderboard_single_season_pitching...';
    REFRESH MATERIALIZED VIEW leaderboard_single_season_pitching;

    -- Refresh yearly leaderboards
    RAISE NOTICE 'Refreshing leaderboard_yearly_batting...';
    REFRESH MATERIALIZED VIEW leaderboard_yearly_batting;

    RAISE NOTICE 'Refreshing leaderboard_yearly_pitching...';
    REFRESH MATERIALIZED VIEW leaderboard_yearly_pitching;

    RAISE NOTICE 'All materialized views refreshed successfully!';
END $$;
