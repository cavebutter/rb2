  -- =====================================================
  -- Newspaper & Content Management Tables
  -- =====================================================
  -- These tables support the newspaper/journal section
  -- of the website, including articles, categories, and
  -- tagging relationships to players, teams, and games.
  -- =====================================================

  -- Drop existing tables (in reverse dependency order)
  DROP TABLE IF EXISTS messages CASCADE;
  DROP TABLE IF EXISTS trade_history CASCADE;
  DROP TABLE IF EXISTS article_game_tags CASCADE;
  DROP TABLE IF EXISTS article_team_tags CASCADE;
  DROP TABLE IF EXISTS article_player_tags CASCADE;
  DROP TABLE IF EXISTS newspaper_articles CASCADE;
  DROP TABLE IF EXISTS article_categories CASCADE;

  -- Article Categories
  -- Pre-defined categories for organizing articles
  CREATE TABLE IF NOT EXISTS article_categories (
      category_id SERIAL PRIMARY KEY,
      name VARCHAR(50) NOT NULL UNIQUE,
      slug VARCHAR(50) UNIQUE NOT NULL,
      description TEXT,
      display_order INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );

  CREATE INDEX IF NOT EXISTS idx_article_categories_slug ON article_categories(slug);

  COMMENT ON TABLE article_categories IS 'Categories for organizing newspaper articles';
  COMMENT ON COLUMN article_categories.slug IS 'URL-friendly version of category name';
  COMMENT ON COLUMN article_categories.display_order IS 'Order for display in navigation (lower = first)';

  -- Newspaper Articles
  -- Main content table for all articles (user-written and AI-generated)
  CREATE TABLE IF NOT EXISTS newspaper_articles (
      article_id SERIAL PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      slug VARCHAR(255) UNIQUE NOT NULL,
      content TEXT NOT NULL,
      excerpt TEXT,
      category_id INTEGER REFERENCES article_categories(category_id) ON DELETE SET NULL,
      author_type VARCHAR(20) DEFAULT 'user' CHECK (author_type IN ('user', 'ai')),
      game_date DATE,  -- In-game date associated with article
      publish_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      is_published BOOLEAN DEFAULT FALSE,
      is_featured BOOLEAN DEFAULT FALSE,
      view_count INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );

  CREATE INDEX IF NOT EXISTS idx_articles_slug ON newspaper_articles(slug);
  CREATE INDEX IF NOT EXISTS idx_articles_category ON newspaper_articles(category_id);
  CREATE INDEX IF NOT EXISTS idx_articles_published ON newspaper_articles(is_published) WHERE is_published = TRUE;
  CREATE INDEX IF NOT EXISTS idx_articles_featured ON newspaper_articles(is_featured) WHERE is_featured = TRUE;
  CREATE INDEX IF NOT EXISTS idx_articles_game_date ON newspaper_articles(game_date);
  CREATE INDEX IF NOT EXISTS idx_articles_publish_date ON newspaper_articles(publish_date DESC);
  CREATE INDEX IF NOT EXISTS idx_articles_author_type ON newspaper_articles(author_type);

  COMMENT ON TABLE newspaper_articles IS 'All newspaper articles (user-written and AI-generated)';
  COMMENT ON COLUMN newspaper_articles.slug IS 'URL-friendly unique identifier for article';
  COMMENT ON COLUMN newspaper_articles.excerpt IS 'Short summary/teaser (auto-generated from first 200 chars if not provided)';
  COMMENT ON COLUMN newspaper_articles.author_type IS 'Type of author: user or ai';
  COMMENT ON COLUMN newspaper_articles.game_date IS 'In-game date associated with article events';

  -- Article-Player Tags
  -- Junction table linking articles to mentioned players
  CREATE TABLE IF NOT EXISTS article_player_tags (
      article_id INTEGER REFERENCES newspaper_articles(article_id) ON DELETE CASCADE,
      player_id INTEGER REFERENCES players_core(player_id) ON DELETE CASCADE,
      is_primary BOOLEAN DEFAULT FALSE,  -- Mark primary subject of article
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (article_id, player_id)
  );

  CREATE INDEX IF NOT EXISTS idx_article_player_tags_player ON article_player_tags(player_id);
  CREATE INDEX IF NOT EXISTS idx_article_player_tags_primary ON article_player_tags(is_primary) WHERE is_primary = TRUE;

  COMMENT ON TABLE article_player_tags IS 'Links articles to players mentioned in the content';
  COMMENT ON COLUMN article_player_tags.is_primary IS 'TRUE if player is primary subject of article';

  -- Article-Team Tags
  -- Junction table linking articles to mentioned teams
  CREATE TABLE IF NOT EXISTS article_team_tags (
      article_id INTEGER REFERENCES newspaper_articles(article_id) ON DELETE CASCADE,
      team_id INTEGER REFERENCES teams(team_id) ON DELETE CASCADE,
      is_primary BOOLEAN DEFAULT FALSE,  -- Mark primary subject of article
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (article_id, team_id)
  );

  CREATE INDEX IF NOT EXISTS idx_article_team_tags_team ON article_team_tags(team_id);
  CREATE INDEX IF NOT EXISTS idx_article_team_tags_primary ON article_team_tags(is_primary) WHERE is_primary = TRUE;

  COMMENT ON TABLE article_team_tags IS 'Links articles to teams mentioned in the content';
  COMMENT ON COLUMN article_team_tags.is_primary IS 'TRUE if team is primary subject of article';

  -- Article-Game Tags
  -- Junction table linking articles to specific games
  CREATE TABLE IF NOT EXISTS article_game_tags (
      article_id INTEGER REFERENCES newspaper_articles(article_id) ON DELETE CASCADE,
      game_id INTEGER,  -- Note: No FK constraint as games table may not exist yet
      is_recap BOOLEAN DEFAULT FALSE,  -- TRUE if article is a game recap
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (article_id, game_id)
  );

  CREATE INDEX IF NOT EXISTS idx_article_game_tags_game ON article_game_tags(game_id);
  CREATE INDEX IF NOT EXISTS idx_article_game_tags_recap ON article_game_tags(is_recap) WHERE is_recap = TRUE;

  COMMENT ON TABLE article_game_tags IS 'Links articles to specific games';
  COMMENT ON COLUMN article_game_tags.is_recap IS 'TRUE if article is a game recap';

  -- Insert default categories
  INSERT INTO article_categories (name, slug, description, display_order)
  VALUES
      ('Game Recap', 'game-recap', 'Recaps and summaries of completed games', 1),
      ('Feature Story', 'feature-story', 'In-depth feature articles about players, teams, or league events', 2),
      ('Branch Family Journal', 'branch-family-journal', 'Personal stories and journal entries from Branch family members', 3),
      ('League News', 'league-news', 'General news and announcements about the league', 4),
      ('Player Profile', 'player-profile', 'Detailed profiles and career retrospectives of notable players', 5),
      ('Season Preview', 'season-preview', 'Previews and predictions for upcoming seasons', 6),
      ('Season Recap', 'season-recap', 'End-of-season reviews and analysis', 7),
      ('Transaction Wire', 'transaction-wire', 'Player signings, trades, and roster moves', 8)
  ON CONFLICT (slug) DO NOTHING;

  -- Trigger to auto-update updated_at timestamp on articles
  CREATE OR REPLACE FUNCTION update_article_timestamp()
  RETURNS TRIGGER AS $$
  BEGIN
      NEW.updated_at = CURRENT_TIMESTAMP;
      RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;

  DROP TRIGGER IF EXISTS trigger_update_article_timestamp ON newspaper_articles;
  CREATE TRIGGER trigger_update_article_timestamp
      BEFORE UPDATE ON newspaper_articles
      FOR EACH ROW
      EXECUTE FUNCTION update_article_timestamp();

  COMMENT ON FUNCTION update_article_timestamp() IS 'Auto-updates the updated_at timestamp on article modifications';

-- =====================================================
-- Transaction and News Tables
-- =====================================================
-- These tables store game-generated transactions and
-- news messages for use in newspaper content generation
-- =====================================================

-- Trade History
-- Transactions and player movements between teams
CREATE TABLE IF NOT EXISTS trade_history (
    trade_id SERIAL PRIMARY KEY,
    message_id INTEGER,
    date DATE NOT NULL,
    summary TEXT NOT NULL,
    team_id_0 INTEGER,
    player_id_0_0 INTEGER,
    player_id_0_1 INTEGER,
    player_id_0_2 INTEGER,
    player_id_0_3 INTEGER,
    player_id_0_4 INTEGER,
    player_id_0_5 INTEGER,
    player_id_0_6 INTEGER,
    player_id_0_7 INTEGER,
    player_id_0_8 INTEGER,
    player_id_0_9 INTEGER,
    draft_round_0_0 INTEGER,
    draft_team_0_0 INTEGER,
    draft_round_0_1 INTEGER,
    draft_team_0_1 INTEGER,
    draft_round_0_2 INTEGER,
    draft_team_0_2 INTEGER,
    draft_round_0_3 INTEGER,
    draft_team_0_3 INTEGER,
    draft_round_0_4 INTEGER,
    draft_team_0_4 INTEGER,
    cash_0 INTEGER,
    iafa_cap_0 INTEGER,
    team_id_1 INTEGER,
    player_id_1_0 INTEGER,
    player_id_1_1 INTEGER,
    player_id_1_2 INTEGER,
    player_id_1_3 INTEGER,
    player_id_1_4 INTEGER,
    player_id_1_5 INTEGER,
    player_id_1_6 INTEGER,
    player_id_1_7 INTEGER,
    player_id_1_8 INTEGER,
    player_id_1_9 INTEGER,
    draft_round_1_0 INTEGER,
    draft_team_1_0 INTEGER,
    draft_round_1_1 INTEGER,
    draft_team_1_1 INTEGER,
    draft_round_1_2 INTEGER,
    draft_team_1_2 INTEGER,
    draft_round_1_3 INTEGER,
    draft_team_1_3 INTEGER,
    draft_round_1_4 INTEGER,
    draft_team_1_4 INTEGER,
    cash_1 INTEGER,
    iafa_cap_1 INTEGER,
    FOREIGN KEY (team_id_0) REFERENCES teams(team_id),
    FOREIGN KEY (team_id_1) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_trade_history_date ON trade_history(date DESC);
CREATE INDEX IF NOT EXISTS idx_trade_history_team_0 ON trade_history(team_id_0);
CREATE INDEX IF NOT EXISTS idx_trade_history_team_1 ON trade_history(team_id_1);

COMMENT ON TABLE trade_history IS 'Transaction records with embedded player and draft pick details';
COMMENT ON COLUMN trade_history.summary IS 'Human-readable transaction summary with embedded references';
COMMENT ON COLUMN trade_history.player_id_0_0 IS 'First player involved for team 0 (0 = no player)';
COMMENT ON COLUMN trade_history.cash_0 IS 'Cash involved in transaction for team 0';

-- Game-Generated Messages/News
-- News items generated by the game engine with embedded references
CREATE TABLE IF NOT EXISTS messages (
    message_id INTEGER PRIMARY KEY,
    subject VARCHAR(255),
    player_id_0 INTEGER,
    player_id_1 INTEGER,
    player_id_2 INTEGER,
    player_id_3 INTEGER,
    player_id_4 INTEGER,
    player_id_5 INTEGER,
    player_id_6 INTEGER,
    player_id_7 INTEGER,
    player_id_8 INTEGER,
    player_id_9 INTEGER,
    team_id_0 INTEGER,
    team_id_1 INTEGER,
    team_id_2 INTEGER,
    team_id_3 INTEGER,
    team_id_4 INTEGER,
    league_id_0 INTEGER,
    league_id_1 INTEGER,
    importance SMALLINT,
    message_type SMALLINT,
    hype SMALLINT,
    sender_type SMALLINT,
    sender_id INTEGER,
    recipient_id INTEGER,
    trade_id INTEGER,
    date DATE,
    deleted SMALLINT DEFAULT 0,
    notify SMALLINT DEFAULT 1,
    ongoing_story_id INTEGER DEFAULT -1,
    text_is_modified SMALLINT DEFAULT 0,
    body TEXT NOT NULL,
    FOREIGN KEY (trade_id) REFERENCES trade_history(trade_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date DESC);
CREATE INDEX IF NOT EXISTS idx_messages_type ON messages(message_type);
CREATE INDEX IF NOT EXISTS idx_messages_importance ON messages(importance);
CREATE INDEX IF NOT EXISTS idx_messages_trade ON messages(trade_id);
CREATE INDEX IF NOT EXISTS idx_messages_deleted ON messages(deleted) WHERE deleted = 0;

COMMENT ON TABLE messages IS 'Game-generated news messages with embedded player/team references';
COMMENT ON COLUMN messages.body IS 'Message text with embedded references like <Player Name:player#ID> and <Team Name:team#ID>';
COMMENT ON COLUMN messages.importance IS 'Message importance level (higher = more important)';
COMMENT ON COLUMN messages.message_type IS 'Type of message (mapped to game engine message types)';
COMMENT ON COLUMN messages.ongoing_story_id IS 'Links related messages in a story arc (-1 = standalone)';
COMMENT ON COLUMN messages.text_is_modified IS 'Flag indicating if body text has been manually edited';
