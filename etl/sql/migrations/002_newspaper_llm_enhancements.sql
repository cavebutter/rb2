-- =====================================================
-- Migration: 002_newspaper_llm_enhancements.sql
-- Task: 1.2 - Database Schema Creation for LLM Article Generation
-- Date: 2025-10-20
-- Description: Adds LLM article generation fields to newspaper_articles
--              and creates branch_game_moments table
-- =====================================================

-- Add new columns to newspaper_articles table
-- These support the LLM article generation workflow

DO $$
BEGIN
    -- game_id column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='game_id') THEN
        ALTER TABLE newspaper_articles ADD COLUMN game_id INTEGER;
        COMMENT ON COLUMN newspaper_articles.game_id IS 'Direct reference to games table (no FK yet as games may not exist)';
    END IF;

    -- generation_method column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='generation_method') THEN
        ALTER TABLE newspaper_articles ADD COLUMN generation_method VARCHAR(50);
        COMMENT ON COLUMN newspaper_articles.generation_method IS 'How article was created: user, ai_generated, message_reprint';
    END IF;

    -- model_used column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='model_used') THEN
        ALTER TABLE newspaper_articles ADD COLUMN model_used VARCHAR(50);
        COMMENT ON COLUMN newspaper_articles.model_used IS 'Ollama model name (e.g., qwen2.5:14b, llama3.1:8b)';
    END IF;

    -- newsworthiness_score column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='newsworthiness_score') THEN
        ALTER TABLE newspaper_articles ADD COLUMN newsworthiness_score INTEGER;
        ALTER TABLE newspaper_articles ADD CONSTRAINT valid_newsworthiness_score
            CHECK (newsworthiness_score IS NULL OR (newsworthiness_score >= 0 AND newsworthiness_score <= 100));
        COMMENT ON COLUMN newspaper_articles.newsworthiness_score IS '0-100 scoring for prioritization and model selection';
    END IF;

    -- status column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='status') THEN
        ALTER TABLE newspaper_articles ADD COLUMN status VARCHAR(20) DEFAULT 'draft';
        ALTER TABLE newspaper_articles ADD CONSTRAINT valid_status
            CHECK (status IN ('draft', 'published', 'rejected'));
        COMMENT ON COLUMN newspaper_articles.status IS 'Editorial workflow status: draft, published, rejected';
    END IF;

    -- reviewed_by column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='reviewed_by') THEN
        ALTER TABLE newspaper_articles ADD COLUMN reviewed_by VARCHAR(100);
        COMMENT ON COLUMN newspaper_articles.reviewed_by IS 'Username/identifier of reviewer';
    END IF;

    -- reviewed_at column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='reviewed_at') THEN
        ALTER TABLE newspaper_articles ADD COLUMN reviewed_at TIMESTAMP;
        COMMENT ON COLUMN newspaper_articles.reviewed_at IS 'Timestamp when article was reviewed';
    END IF;

    -- generation_count column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='generation_count') THEN
        ALTER TABLE newspaper_articles ADD COLUMN generation_count INTEGER DEFAULT 1;
        COMMENT ON COLUMN newspaper_articles.generation_count IS 'Number of times article has been regenerated';
    END IF;

    -- previous_version_id column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='previous_version_id') THEN
        ALTER TABLE newspaper_articles ADD COLUMN previous_version_id INTEGER;
        ALTER TABLE newspaper_articles ADD CONSTRAINT fk_previous_version
            FOREIGN KEY (previous_version_id) REFERENCES newspaper_articles(article_id) ON DELETE SET NULL;
        COMMENT ON COLUMN newspaper_articles.previous_version_id IS 'Links to previous version when article is regenerated';
    END IF;

    -- source_message_id column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='newspaper_articles' AND column_name='source_message_id') THEN
        ALTER TABLE newspaper_articles ADD COLUMN source_message_id INTEGER;
        ALTER TABLE newspaper_articles ADD CONSTRAINT fk_source_message
            FOREIGN KEY (source_message_id) REFERENCES messages(message_id) ON DELETE SET NULL;
        COMMENT ON COLUMN newspaper_articles.source_message_id IS 'Links to messages table for message reprints';
    END IF;

END $$;

-- Create indexes for new columns
CREATE INDEX IF NOT EXISTS idx_articles_game_id ON newspaper_articles(game_id);
CREATE INDEX IF NOT EXISTS idx_articles_status ON newspaper_articles(status);
CREATE INDEX IF NOT EXISTS idx_articles_status_published ON newspaper_articles(game_date DESC) WHERE status = 'published';
CREATE INDEX IF NOT EXISTS idx_articles_newsworthiness ON newspaper_articles(newsworthiness_score DESC) WHERE newsworthiness_score IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_articles_source_message ON newspaper_articles(source_message_id) WHERE source_message_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_articles_generation_method ON newspaper_articles(generation_method);

-- =====================================================
-- Create branch_game_moments table
-- =====================================================
-- Stores extracted play-by-play data for Branch family
-- player performances. Used as a cache for article
-- generation without re-parsing game_logs.csv.
-- =====================================================

CREATE TABLE IF NOT EXISTS branch_game_moments (
    moment_id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    inning INTEGER,
    inning_half VARCHAR(10),
    moment_type VARCHAR(50),  -- 'at_bat', 'pitching_inning', 'defensive_play'
    play_sequence JSONB,  -- Array of play-by-play lines with context
    outcome VARCHAR(200),  -- Summary of what happened
    exit_velocity DECIMAL(5,1),  -- Exit velocity if available
    hit_location VARCHAR(20),  -- Hit location if available
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT valid_inning_half CHECK (inning_half IN ('top', 'bottom', NULL)),
    FOREIGN KEY (player_id) REFERENCES players_core(player_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_moments_game_player ON branch_game_moments(game_id, player_id);
CREATE INDEX IF NOT EXISTS idx_moments_player ON branch_game_moments(player_id);
CREATE INDEX IF NOT EXISTS idx_moments_game ON branch_game_moments(game_id);
CREATE INDEX IF NOT EXISTS idx_moments_type ON branch_game_moments(moment_type);

COMMENT ON TABLE branch_game_moments IS 'Cached play-by-play moments for Branch family player performances';
COMMENT ON COLUMN branch_game_moments.play_sequence IS 'JSONB array of play-by-play text lines with surrounding context';
COMMENT ON COLUMN branch_game_moments.moment_type IS 'Type of moment: at_bat, pitching_inning, or defensive_play';
COMMENT ON COLUMN branch_game_moments.outcome IS 'Human-readable summary of the play outcome';

-- =====================================================
-- Migration complete
-- =====================================================
-- Verify with:
-- SELECT column_name, data_type FROM information_schema.columns
-- WHERE table_name='newspaper_articles' AND column_name IN
-- ('game_id', 'generation_method', 'model_used', 'newsworthiness_score',
--  'status', 'reviewed_by', 'reviewed_at', 'generation_count',
--  'previous_version_id', 'source_message_id');
--
-- SELECT COUNT(*) FROM branch_game_moments;
-- =====================================================