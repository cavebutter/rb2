-- ETL Metadata tracking tables
-- These tables track the ETL process, file changes, and data lineage

-- Drop existing tables if needed (for development)
DROP TABLE IF EXISTS etl_change_log CASCADE;
DROP TABLE IF EXISTS etl_calculation_queue CASCADE;
DROP TABLE IF EXISTS etl_watermarks CASCADE;
DROP TABLE IF EXISTS etl_batch_runs CASCADE;
DROP TABLE IF EXISTS etl_file_metadata CASCADE;
DROP TABLE IF EXISTS etl_table_config CASCADE;
DROP TABLE IF EXISTS etl_performance_metrics CASCADE;

-- Drop existing indexes if needed (for development)
DROP INDEX IF EXISTS idx_batch_runs_status_date;
DROP INDEX IF EXISTS idx_etl_metadata_status;
DROP INDEX IF EXISTS idx_etl_metadata_strategy;
DROP INDEX IF EXISTS idx_change_log_table_operation;
DROP INDEX IF EXISTS idx_change_log_game;
DROP INDEX IF EXISTS idx_change_log_batch;
DROP INDEX IF EXISTS idx_calc_queue_status_priority;
DROP INDEX IF EXISTS idx_calc_queue_player;
DROP INDEX IF EXISTS idx_calc_queue_batch;
DROP INDEX IF EXISTS idx_perf_metrics_batch;
DROP INDEX IF EXISTS idx_perf_metrics_type_table;

-- Drop existing views if needed (for development)
DROP VIEW IF EXISTS v_etl_recent_runs;
DROP VIEW IF EXISTS v_etl_table_status;

-- Drop functions if needed (for development)
DROP FUNCTION IF EXISTS start_etl_batch CASCADE;
DROP FUNCTION IF EXISTS complete_etl_batch CASCADE;

-- ETL Batch Runs (parent table for tracking ETL executions)
CREATE TABLE IF NOT EXISTS etl_batch_runs (
    batch_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_type VARCHAR(50) NOT NULL CHECK (batch_type IN ('full', 'incremental', 'fetch_only')),
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    environment VARCHAR(20) not null check (environment in ('dev', 'staging', 'prod')),
    triggered_by VARCHAR(100),
    error_message TEXT,
    stats JSONB
    );

CREATE INDEX idx_batch_runs_status_date2 ON etl_batch_runs(status, started_at DESC);

-- File metadata tracking
CREATE TABLE IF NOT EXISTS etl_file_metadata (
    filename VARCHAR(100) PRIMARY KEY,
    file_path VARCHAR(500),
    last_modified TIMESTAMP,
    file_size BIGINT,
    row_count INTEGER,
    checksum VARCHAR(64),
    load_strategy VARCHAR(20) NOT NULL DEFAULT 'full' CHECK (load_strategy IN ('full', 'incremental', 'skip', 'append')),
    last_processed TIMESTAMP,
    last_batch_id UUID REFERENCES etl_batch_runs(batch_id),
    last_status VARCHAR(20) CHECK (last_status IN ('success', 'failed', 'skipped', 'in_progress')),
    processing_time_seconds INTEGER,
    rows_processed INTEGER,
    rows_updated INTEGER,
    rows_deleted INTEGER,
    error_message TEXT,
    metadata JSONB
);
CREATE INDEX idx_etl_metadata_status ON etl_file_metadata(last_status, last_processed);
CREATE INDEX idx_etl_metadata_strategy ON etl_file_metadata(load_strategy);

-- Change log for data lineage
CREATE TABLE IF NOT EXISTS etl_change_log (
    change_id SERIAL PRIMARY KEY,
    batch_id UUID REFERENCES etl_batch_runs(batch_id),
    table_name VARCHAR(50) NOT NULL,
    primary_key_values TEXT NOT NULL,
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    changed_fields TEXT[], -- Array of changed field names
    old_values JSONB,     -- JSONB of old values for updated fields
    new_values JSONB,     -- JSONB of new values for updated fields
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    season_year INTEGER,
    game_id INTEGER
);
  CREATE INDEX idx_change_log_table_operation ON etl_change_log(table_name, operation, changed_at);
  CREATE INDEX idx_change_log_game ON etl_change_log(game_id) WHERE game_id IS NOT NULL;
  CREATE INDEX idx_change_log_batch ON etl_change_log(batch_id);

--Watermark tracking for Append-Only tables
CREATE TABLE IF NOT EXISTS etl_watermarks (
    table_name VARCHAR(50) PRIMARY KEY,
    watermark_column VARCHAR(50) NOT NULL,
    watermark_value TEXT NOT NULL,
    watermark_type VARCHAR(20) NOT NULL CHECK (watermark_type IN ('integer', 'timestamp', 'date')),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_batch_id UUID REFERENCES etl_batch_runs(batch_id)
);

-- Calculation queue for Stats Processing
CREATE TABLE IF NOT EXISTS etl_calculation_queue (
    queue_id SERIAL PRIMARY KEY,
    batch_id UUID REFERENCES etl_batch_runs(batch_id),
    table_name VARCHAR(50) NOT NULL,
    player_id INTEGER,
    year INTEGER,
    team_id INTEGER,
    calculation_type VARCHAR(50) NOT NULL, -- woba, war, wrc_plus, etc.
    dependencies TEXT[], -- Other calculations that must be completed first
    priority INTEGER DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    error_message TEXT
);
  CREATE INDEX idx_calc_queue_status_priority ON etl_calculation_queue(status, priority DESC, created_at);
  CREATE INDEX idx_calc_queue_player ON etl_calculation_queue(player_id, year) WHERE player_id IS NOT NULL;
  CREATE INDEX idx_calc_queue_batch ON etl_calculation_queue(batch_id);

-- Table Load Strategies Configuration
CREATE TABLE IF NOT EXISTS etl_table_config (
    table_name VARCHAR(50) PRIMARY KEY,
    load_strategy VARCHAR(20) NOT NULL CHECK (load_strategy IN ('skip', 'full', 'incremental', 'append')),
    primary_key_columns TEXT[] NOT NULL, -- Array of primary key column names
    comparison_columns TEXT[], -- Columns to compare for changes (for incremental loads)
    exclude_columns TEXT[], -- Columns to exclude from comparison (e.g. last_modified)
    depends_on_tables TEXT[], -- Other tables that must be loaded first
    triggers_calculations BOOLEAN DEFAULT FALSE, -- Whether loading this table should trigger calculations
    active BOOLEAN DEFAULT TRUE,
    notes TEXT
);

-- Performance metrics
CREATE TABLE IF NOT EXISTS etl_performance_metrics (
    metric_id SERIAL PRIMARY KEY,
    batch_id UUID REFERENCES etl_batch_runs(batch_id),
    metric_type VARCHAR(50) NOT NULL,
    table_name VARCHAR(50),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_seconds NUMERIC(10,3) GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (end_time - start_time))) STORED,
    rows_processed INTEGER,
    rows_per_second NUMERIC(10,2) GENERATED ALWAYS AS (CASE WHEN EXTRACT(EPOCH FROM (end_time - start_time)) > 0 THEN rows_processed / EXTRACT(EPOCH FROM (end_time - start_time)) ELSE NULL END) STORED,
    memory_usage_mb INTEGER,
    notes TEXT
);
CREATE INDEX idx_perf_metrics_batch ON etl_performance_metrics(batch_id);
CREATE INDEX idx_perf_metrics_type_table ON etl_performance_metrics(metric_type, table_name);

-- Helper views for Monitoring
CREATE OR REPLACE VIEW v_etl_recent_runs AS
    SELECT
    b.batch_id,
    b.batch_type,
    b.started_at,
    b.completed_at,
    b.status,
    b.environment,
    EXTRACT(EPOCH FROM (COALESCE(b.completed_at, CURRENT_TIMESTAMP) - b.started_at))::INT AS duration_seconds,
    COUNT(DISTINCT f.filename) AS files_processed,
    SUM(f.rows_processed) AS total_rows_inserted,
    SUM(f.rows_updated) AS total_rows_updated,
    SUM(f.rows_deleted) AS total_rows_deleted
FROM etl_batch_runs b
LEFT JOIN etl_file_metadata f ON f.last_batch_id = b.batch_id
GROUP BY b.batch_id
ORDER BY b.started_at DESC
LIMIT 20;

CREATE OR REPLACE VIEW v_etl_table_status AS
SELECT
    tc.table_name,
    tc.load_strategy,
    f.last_processed,
    f.last_status,
    f.row_count,
    f.processing_time_seconds,
    w.watermark_value,
    w.last_updated AS watermark_last_updated
FROM etl_table_config tc
LEFT JOIN etl_file_metadata f ON f.filename = tc.table_name || '.csv'
LEFT JOIN etl_watermarks w ON w.table_name = tc.table_name
WHERE tc.active = TRUE
ORDER BY tc.table_name;


-- Insert default table configurations
  INSERT INTO etl_table_config (table_name, load_strategy, primary_key_columns, triggers_calculations)
  VALUES
  -- Static reference data
  ('continents', 'skip', ARRAY['continent_id'], FALSE),
  ('nations', 'skip', ARRAY['nation_id'], FALSE),
  ('states', 'skip', ARRAY['state_id', 'nation_id'], FALSE),
  ('cities', 'skip', ARRAY['city_id'], FALSE),
  ('languages', 'skip', ARRAY['language_id'], FALSE),
  ('parks', 'skip', ARRAY['park_id'], FALSE),

  -- Slowly changing dimensions
  ('leagues', 'incremental', ARRAY['league_id'], FALSE),
  ('sub_leagues', 'incremental', ARRAY['league_id', 'sub_league_id'], FALSE),
  ('divisions', 'incremental', ARRAY['league_id', 'sub_league_id', 'division_id'], FALSE),
  ('teams', 'incremental', ARRAY['team_id'], FALSE),
  ('players', 'incremental', ARRAY['player_id'], FALSE),
  ('coaches', 'incremental', ARRAY['coach_id'], FALSE),

  -- Frequently updated stats
  ('players_career_batting_stats', 'incremental',
   ARRAY['player_id', 'year', 'team_id', 'split_id', 'stint'], TRUE),
  ('players_career_pitching_stats', 'incremental',
   ARRAY['player_id', 'year', 'team_id', 'split_id', 'stint'], TRUE),
  ('players_career_fielding_stats', 'incremental',
   ARRAY['player_id', 'year', 'team_id', 'position'], TRUE),

  -- Append-only tables
  ('games', 'append', ARRAY['game_id', 'date'], FALSE),
  ('trade_history', 'append', ARRAY['trade_id'], FALSE)
  ON CONFLICT (table_name) DO UPDATE SET
      load_strategy = EXCLUDED.load_strategy,
      primary_key_columns = EXCLUDED.primary_key_columns,
      triggers_calculations = EXCLUDED.triggers_calculations;

  -- Utility functions
  CREATE OR REPLACE FUNCTION start_etl_batch(
      p_batch_type VARCHAR(50),
      p_triggered_by VARCHAR(100) DEFAULT 'manual',
      p_environment VARCHAR(20) DEFAULT 'dev'
  ) RETURNS UUID AS $$
  DECLARE
      v_batch_id UUID;
  BEGIN
      INSERT INTO etl_batch_runs (batch_type, triggered_by, environment)
      VALUES (p_batch_type, p_triggered_by, p_environment)
      RETURNING batch_id INTO v_batch_id;

      RETURN v_batch_id;
  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION complete_etl_batch(
      p_batch_id UUID,
      p_status VARCHAR(20) DEFAULT 'completed',
      p_error_message TEXT DEFAULT NULL
  ) RETURNS VOID AS $$
  BEGIN
      UPDATE etl_batch_runs
      SET completed_at = CURRENT_TIMESTAMP,
          status = p_status,
          error_message = p_error_message
      WHERE batch_id = p_batch_id;
  END;
  $$ LANGUAGE plpgsql;

