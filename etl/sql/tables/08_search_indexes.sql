  -- =====================================================
  -- Full-Text Search Indexes
  -- =====================================================
  -- Supports autocomplete and search functionality
  -- for the web application using PostgreSQL's
  -- full-text search capabilities.
  -- =====================================================

  -- Player Name Search
  -- Supports searching by first name, last name, or full name
  CREATE INDEX IF NOT EXISTS idx_player_name_search
  ON players_core
  USING GIN(to_tsvector('english',
      COALESCE(first_name, '') || ' ' ||
      COALESCE(last_name, '') || ' ' ||
      COALESCE(nick_name, '')
  ));

  -- Additional index for simple prefix matching (faster for autocomplete)
  CREATE INDEX IF NOT EXISTS idx_player_last_name_prefix
  ON players_core(last_name text_pattern_ops);

  CREATE INDEX IF NOT EXISTS idx_player_first_name_prefix
  ON players_core(first_name text_pattern_ops);

  COMMENT ON INDEX idx_player_name_search IS 'Full-text search index for player names';
  COMMENT ON INDEX idx_player_last_name_prefix IS 'Prefix search index for autocomplete on last name';
  COMMENT ON INDEX idx_player_first_name_prefix IS 'Prefix search index for autocomplete on first name';

  -- Team Search
  -- Supports searching by team name, nickname, or abbreviation
  CREATE INDEX IF NOT EXISTS idx_team_search
  ON teams
  USING GIN(to_tsvector('english',
      COALESCE(name, '') || ' ' ||
      COALESCE(nickname, '') || ' ' ||
      COALESCE(abbr, '')
  ));

  -- Additional index for abbreviation exact match
  CREATE INDEX IF NOT EXISTS idx_team_abbr
  ON teams(UPPER(abbr));

  CREATE INDEX IF NOT EXISTS idx_team_name_prefix
  ON teams(name text_pattern_ops);

  COMMENT ON INDEX idx_team_search IS 'Full-text search index for team names, nicknames, and abbreviations';
  COMMENT ON INDEX idx_team_abbr IS 'Case-insensitive abbreviation lookup';
  COMMENT ON INDEX idx_team_name_prefix IS 'Prefix search index for autocomplete on team name';

  -- Article Search (for future newspaper search functionality)
  CREATE INDEX IF NOT EXISTS idx_article_title_search
  ON newspaper_articles
  USING GIN(to_tsvector('english', title));

  CREATE INDEX IF NOT EXISTS idx_article_content_search
  ON newspaper_articles
  USING GIN(to_tsvector('english', content));

  -- Combined title + content search (weighted: title is more important)
  CREATE INDEX IF NOT EXISTS idx_article_combined_search
  ON newspaper_articles
  USING GIN((
      setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
      setweight(to_tsvector('english', COALESCE(excerpt, '')), 'B') ||
      setweight(to_tsvector('english', COALESCE(content, '')), 'C')
  ));

  COMMENT ON INDEX idx_article_title_search IS 'Full-text search on article titles';
  COMMENT ON INDEX idx_article_content_search IS 'Full-text search on article content';
  COMMENT ON INDEX idx_article_combined_search IS 'Weighted full-text search (title > excerpt > content)';

  -- Common Query Pattern Indexes
  -- These support frequent web application queries

  -- Player queries by position and birth date
  -- Note: position is in players_current_status, not players_core
  -- CREATE INDEX IF NOT EXISTS idx_player_position
  -- ON players_core(position);

  CREATE INDEX IF NOT EXISTS idx_player_dob
  ON players_core(date_of_birth)
  WHERE date_of_birth IS NOT NULL;

  -- Extract month/day for "born this week" feature
  CREATE INDEX IF NOT EXISTS idx_player_birth_month_day
  ON players_core(
      EXTRACT(MONTH FROM date_of_birth),
      EXTRACT(DAY FROM date_of_birth)
  ) WHERE date_of_birth IS NOT NULL;

  -- COMMENT ON INDEX idx_player_position IS 'Filter players by position';
  COMMENT ON INDEX idx_player_dob IS 'Player birth date queries';
  COMMENT ON INDEX idx_player_birth_month_day IS 'Supports "born this week" feature (ignores year)';

  -- Team queries by league/division
  CREATE INDEX IF NOT EXISTS idx_team_league
  ON teams(league_id)
  WHERE league_id IS NOT NULL;

  CREATE INDEX IF NOT EXISTS idx_team_division
  ON teams(league_id, sub_league_id, division_id);

  -- League game_date index for current date queries
  CREATE INDEX IF NOT EXISTS idx_league_game_date
  ON leagues(game_date DESC)
  WHERE game_date IS NOT NULL;

  COMMENT ON INDEX idx_league_game_date IS 'Quick lookup of current game date';

  -- Analyze tables to update statistics for query planner
  ANALYZE players_core;
  ANALYZE teams;
  ANALYZE leagues;
  ANALYZE newspaper_articles;
