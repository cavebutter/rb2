-- Migration: Add article images support
-- Date: 2025-10-21
-- Purpose: Allow articles to include player pictures, team logos, and uploaded images

CREATE TABLE article_images (
    image_id SERIAL PRIMARY KEY,
    article_id INTEGER NOT NULL REFERENCES newspaper_articles(article_id) ON DELETE CASCADE,
    image_type VARCHAR(20) NOT NULL, -- 'player', 'team_logo', 'uploaded'

    -- For player images (references player_id, path constructed as: etl/data/images/players/player_{id}.png)
    player_id INTEGER REFERENCES players_core(player_id),

    -- For team logo images (path constructed as: etl/data/images/team_logos/{team_name}_{size}.png)
    team_id INTEGER,
    logo_size VARCHAR(10) DEFAULT 'default', -- 'default', '16', '25', '40', '50', '110'

    -- For uploaded images (stored in web/app/static/uploads/articles/)
    uploaded_filename VARCHAR(255),
    uploaded_path VARCHAR(500),
    file_size INTEGER, -- bytes
    mime_type VARCHAR(100),

    -- Common fields
    caption TEXT,
    alt_text VARCHAR(255),
    display_order SMALLINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Validation constraints
    CONSTRAINT valid_image_type CHECK (image_type IN ('player', 'team_logo', 'uploaded')),
    CONSTRAINT player_image_check CHECK (
        (image_type = 'player' AND player_id IS NOT NULL) OR image_type != 'player'
    ),
    CONSTRAINT team_image_check CHECK (
        (image_type = 'team_logo' AND team_id IS NOT NULL) OR image_type != 'team_logo'
    ),
    CONSTRAINT uploaded_image_check CHECK (
        (image_type = 'uploaded' AND uploaded_filename IS NOT NULL AND uploaded_path IS NOT NULL)
        OR image_type != 'uploaded'
    )
);

-- Indexes
CREATE INDEX idx_article_images_article ON article_images(article_id);
CREATE INDEX idx_article_images_order ON article_images(article_id, display_order);
CREATE INDEX idx_article_images_player ON article_images(player_id) WHERE player_id IS NOT NULL;
CREATE INDEX idx_article_images_team ON article_images(team_id) WHERE team_id IS NOT NULL;

-- Comments
COMMENT ON TABLE article_images IS 'Images associated with newspaper articles';
COMMENT ON COLUMN article_images.image_type IS 'Type: player (person picture), team_logo (team logo), uploaded (user upload)';
COMMENT ON COLUMN article_images.player_id IS 'For player images - path: etl/data/images/players/player_{id}.png';
COMMENT ON COLUMN article_images.team_id IS 'For team logos - requires team name lookup for path construction';
COMMENT ON COLUMN article_images.logo_size IS 'Logo size variant: default, 16, 25, 40, 50, 110 pixels';
COMMENT ON COLUMN article_images.display_order IS 'Order for displaying multiple images (1 = first)';
