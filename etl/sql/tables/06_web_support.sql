  -- =====================================================
  -- Web Support Tables
  -- =====================================================
  -- These tables support web application functionality
  -- including logo file management for teams and leagues.
  --
  -- Note: Current game date is stored in leagues.game_date
  -- =====================================================

  -- Team Logos
  -- Manages team logo file associations
  CREATE TABLE IF NOT EXISTS team_logos (
      team_id INTEGER PRIMARY KEY REFERENCES teams(team_id) ON DELETE CASCADE,
      logo_filename VARCHAR(255) NOT NULL,
      logo_path VARCHAR(500),
      upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );

  CREATE INDEX IF NOT EXISTS idx_team_logos_filename ON team_logos(logo_filename);

  COMMENT ON TABLE team_logos IS 'Stores team logo file references for web display';
  COMMENT ON COLUMN team_logos.logo_filename IS 'Name of the logo file';
  COMMENT ON COLUMN team_logos.logo_path IS 'Full or relative path to logo file';

  -- League Logos
  -- Manages league logo file associations
  CREATE TABLE IF NOT EXISTS league_logos (
      league_id INTEGER PRIMARY KEY REFERENCES leagues(league_id) ON DELETE CASCADE,
      logo_filename VARCHAR(255) NOT NULL,
      logo_path VARCHAR(500),
      upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );

  CREATE INDEX IF NOT EXISTS idx_league_logos_filename ON league_logos(logo_filename);

  COMMENT ON TABLE league_logos IS 'Stores league logo file references for web display';
  COMMENT ON COLUMN league_logos.logo_filename IS 'Name of the logo file';
  COMMENT ON COLUMN league_logos.logo_path IS 'Full or relative path to logo file';
