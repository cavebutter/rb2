-- ============================================================================
-- Database Reset Script
-- ============================================================================
-- This script completely cleans the ootp_dev database by dropping all:
--   - Materialized Views
--   - Views
--   - Tables
--   - Functions/Procedures
--   - Sequences
--   - Custom Types
--
-- Usage:
--   Run as ootp_etl user:
--   psql -h 192.168.10.94 -U ootp_etl -d ootp_dev -f reset_database.sql
--
-- After running this script, you must run: python main.py init-db
-- ============================================================================

DO $$
DECLARE
    r RECORD;
BEGIN
    RAISE NOTICE 'Starting database cleanup...';

    -- Drop all materialized views first (they may depend on tables)
    RAISE NOTICE 'Dropping materialized views...';
    FOR r IN (SELECT matviewname FROM pg_matviews WHERE schemaname = 'public')
    LOOP
        RAISE NOTICE 'Dropping materialized view: %', r.matviewname;
        EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS ' || quote_ident(r.matviewname) || ' CASCADE';
    END LOOP;

    -- Drop all views
    RAISE NOTICE 'Dropping views...';
    FOR r IN (SELECT viewname FROM pg_views WHERE schemaname = 'public')
    LOOP
        RAISE NOTICE 'Dropping view: %', r.viewname;
        EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident(r.viewname) || ' CASCADE';
    END LOOP;

    -- Drop all tables
    RAISE NOTICE 'Dropping tables...';
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
    LOOP
        RAISE NOTICE 'Dropping table: %', r.tablename;
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;

    -- Drop all functions
    RAISE NOTICE 'Dropping functions...';
    FOR r IN (SELECT proname, oidvectortypes(proargtypes) as argtypes
              FROM pg_proc
              INNER JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
              WHERE pg_namespace.nspname = 'public')
    LOOP
        RAISE NOTICE 'Dropping function: %(%)' , r.proname, r.argtypes;
        EXECUTE 'DROP FUNCTION IF EXISTS ' || quote_ident(r.proname) || '(' || r.argtypes || ') CASCADE';
    END LOOP;

    -- Drop all sequences
    RAISE NOTICE 'Dropping sequences...';
    FOR r IN (SELECT sequencename FROM pg_sequences WHERE schemaname = 'public')
    LOOP
        RAISE NOTICE 'Dropping sequence: %', r.sequencename;
        EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r.sequencename) || ' CASCADE';
    END LOOP;

    -- Drop all custom types
    RAISE NOTICE 'Dropping custom types...';
    FOR r IN (SELECT typname FROM pg_type
              WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
              AND typtype = 'c')
    LOOP
        RAISE NOTICE 'Dropping type: %', r.typname;
        EXECUTE 'DROP TYPE IF EXISTS ' || quote_ident(r.typname) || ' CASCADE';
    END LOOP;

    RAISE NOTICE 'Database cleanup complete!';
    RAISE NOTICE 'Next step: Run "python main.py init-db" to recreate all tables';
END $$;
