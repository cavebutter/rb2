-- =====================================================
-- Migration: 003_create_staging_tables.sql
-- Task: 2.1 - Create staging tables for volatile CSV data
-- Date: 2025-10-20
-- Description: Creates staging tables for per-game batting and pitching stats
--              from volatile CSV exports (players_game_batting.csv, players_game_pitching_stats.csv)
-- =====================================================

-- Staging table for per-game batting stats from players_game_batting.csv
CREATE TABLE IF NOT EXISTS staging_branch_game_batting (
    player_id INTEGER NOT NULL,
    year SMALLINT,
    team_id INTEGER,
    game_id INTEGER NOT NULL,
    league_id INTEGER,
    level_id SMALLINT,
    split_id SMALLINT,
    position SMALLINT,
    ab SMALLINT,
    h SMALLINT,
    k SMALLINT,
    pa SMALLINT,
    g SMALLINT,
    d SMALLINT,  -- doubles
    t SMALLINT,  -- triples
    hr SMALLINT,
    r SMALLINT,
    rbi SMALLINT,
    sb SMALLINT,
    bb SMALLINT,
    wpa NUMERIC,
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (player_id, game_id)
);

-- Staging table for per-game pitching stats from players_game_pitching_stats.csv
CREATE TABLE IF NOT EXISTS staging_branch_game_pitching (
    player_id INTEGER NOT NULL,
    year SMALLINT,
    team_id INTEGER,
    game_id INTEGER NOT NULL,
    league_id INTEGER,
    level_id SMALLINT,
    split_id SMALLINT,
    g SMALLINT,
    gs SMALLINT,
    ip NUMERIC,
    h SMALLINT,
    r SMALLINT,
    er SMALLINT,
    hr SMALLINT,
    bb SMALLINT,
    k SMALLINT,
    w SMALLINT,
    l SMALLINT,
    sv SMALLINT,
    hld SMALLINT,
    wpa NUMERIC,
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (player_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_staging_batting_game ON staging_branch_game_batting(game_id);
CREATE INDEX IF NOT EXISTS idx_staging_pitching_game ON staging_branch_game_pitching(game_id);
CREATE INDEX IF NOT EXISTS idx_staging_batting_year ON staging_branch_game_batting(year);
CREATE INDEX IF NOT EXISTS idx_staging_pitching_year ON staging_branch_game_pitching(year);

COMMENT ON TABLE staging_branch_game_batting IS 'Temporary staging for Branch player batting stats from players_game_batting.csv - truncated after each article generation run';
COMMENT ON TABLE staging_branch_game_pitching IS 'Temporary staging for Branch player pitching stats from players_game_pitching_stats.csv - truncated after each article generation run';
COMMENT ON COLUMN staging_branch_game_batting.loaded_at IS 'Timestamp when data was loaded from CSV';
COMMENT ON COLUMN staging_branch_game_pitching.loaded_at IS 'Timestamp when data was loaded from CSV';

-- =====================================================
-- Migration complete
-- =====================================================
-- Verify with:
-- SELECT COUNT(*) FROM staging_branch_game_batting;
-- SELECT COUNT(*) FROM staging_branch_game_pitching;
-- =====================================================