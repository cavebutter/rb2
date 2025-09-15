-- ETL Metadata tracking tables
-- These tables track the ETL process, file changes, and data lineage

-- Drop existing tables if needed (for development)
-- DROP TABLE IF EXISTS etl_change_log CASCADE;
-- DROP TABLE IF EXISTS etl_calculation_queue CASCADE;
-- DROP TABLE IF EXISTS etl_watermarks CASCADE;
-- DROP TABLE IF EXISTS etl_batch_runs CASCADE;
-- DROP TABLE IF EXISTS etl_file_metadata CASCADE;

-- ETL Batch Runs (parent table for tracking ETL executions)
CREATE TABLE IF NOT EXISTS etl_batch_runs (
    batch_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_type VARCHAR(50) NOT NULL CHECK (batch_type IN ('full', 'incremental', 'fetch_only')),
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'canceled')),
    environment VARCHAR(20) not null check (envrironment in ('dev', 'staging', 'prod')),
    triggered_by VARCHAR(100),
    error_message TEXT,
    stats JSONB,
    );

CREATE INDEX idx_batch_runs_status_date ON etl(status, started_at DESC);

-- File metadata tracking
CREATE TABLE IF NOT EXISTS etl_file_metadata (
    filename VARCHAR(100) PRIMARY KEY,
    file_path VARCHAR(500),
    last_modified TIMESTAMP,
    file_size BIGINT,
    row_count INTEGER,
    checksum VARCHAR(64),
    load_strategy VARCHAR(20) NOT NULL DEFAULT 'full' CHECK (load_strategy IN ('full', 'incremental', 'skip', 'append')),
    last_proessed TIMESTAMP,
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


