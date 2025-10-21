"""Newspaper models"""
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import BaseModel


class ArticleCategory(BaseModel):
    """Article categories for organizing newspaper content"""
    __tablename__ = 'article_categories'

    category_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    slug = Column(String(100), nullable=False, unique=True)
    description = Column(Text)
    display_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    articles = relationship('Article', back_populates='category')

    def __repr__(self):
        return f"<ArticleCategory {self.name}>"


class Article(BaseModel):
    """Newspaper articles - AI-generated and user-written"""
    __tablename__ = 'newspaper_articles'

    article_id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    slug = Column(String(255), nullable=False, unique=True)
    content = Column(Text, nullable=False)
    excerpt = Column(Text)

    # Categorization
    category_id = Column(Integer, ForeignKey('article_categories.category_id'))

    # Metadata
    author_type = Column(String(50), default='user')  # 'user', 'ai', 'message_reprint'
    game_date = Column(Date)
    publish_date = Column(DateTime, default=datetime.utcnow)

    # Status flags
    is_published = Column(Boolean, default=False)
    is_featured = Column(Boolean, default=False)
    view_count = Column(Integer, default=0)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Game context
    game_id = Column(Integer)  # FK to games.game_id (Game model not yet defined)

    # AI generation metadata
    generation_method = Column(String(50))  # 'ollama', 'claude', etc.
    model_used = Column(String(50))  # Model name (e.g., 'qwen2.5:14b')
    newsworthiness_score = Column(Integer)

    # Editorial workflow
    status = Column(String(20), default='draft')  # 'draft', 'published', 'rejected'
    reviewed_by = Column(String(100))
    reviewed_at = Column(DateTime)

    # Regeneration tracking
    generation_count = Column(Integer, default=1)
    previous_version_id = Column(Integer, ForeignKey('newspaper_articles.article_id'))

    # Message reprint source
    source_message_id = Column(Integer, ForeignKey('messages.message_id'))

    # Relationships
    category = relationship('ArticleCategory', back_populates='articles')
    player_tags = relationship('ArticlePlayerTag', back_populates='article', cascade='all, delete-orphan')
    team_tags = relationship('ArticleTeamTag', back_populates='article', cascade='all, delete-orphan')
    game_tags = relationship('ArticleGameTag', back_populates='article', cascade='all, delete-orphan')
    images = relationship('ArticleImage', back_populates='article', cascade='all, delete-orphan', order_by='ArticleImage.display_order')
    previous_version = relationship('Article', remote_side=[article_id], uselist=False)

    def __repr__(self):
        return f"<Article {self.article_id}: {self.title[:50]}>"

    @property
    def is_draft(self):
        """Check if article is in draft status"""
        return self.status == 'draft'

    @property
    def primary_players(self):
        """Get primary tagged players"""
        return [tag.player for tag in self.player_tags if tag.is_primary]

    @property
    def all_players(self):
        """Get all tagged players"""
        return [tag.player for tag in self.player_tags]

    @property
    def primary_teams(self):
        """Get primary tagged teams"""
        return [tag.team for tag in self.team_tags if tag.is_primary]

    @property
    def all_teams(self):
        """Get all tagged teams"""
        return [tag.team for tag in self.team_tags]


class ArticlePlayerTag(BaseModel):
    """Junction table linking articles to featured players"""
    __tablename__ = 'article_player_tags'

    article_id = Column(Integer, ForeignKey('newspaper_articles.article_id'), primary_key=True)
    player_id = Column(Integer, ForeignKey('players_core.player_id'), primary_key=True)
    is_primary = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    article = relationship('Article', back_populates='player_tags')
    player = relationship('Player', foreign_keys=[player_id])

    def __repr__(self):
        return f"<ArticlePlayerTag article_id={self.article_id} player_id={self.player_id}>"


class ArticleTeamTag(BaseModel):
    """Junction table linking articles to featured teams"""
    __tablename__ = 'article_team_tags'

    article_id = Column(Integer, ForeignKey('newspaper_articles.article_id'), primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.team_id'), primary_key=True)
    is_primary = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    article = relationship('Article', back_populates='team_tags')
    team = relationship('Team', foreign_keys=[team_id])

    def __repr__(self):
        return f"<ArticleTeamTag article_id={self.article_id} team_id={self.team_id}>"


class ArticleGameTag(BaseModel):
    """Junction table linking articles to games (for game recaps)"""
    __tablename__ = 'article_game_tags'

    article_id = Column(Integer, ForeignKey('newspaper_articles.article_id'), primary_key=True)
    game_id = Column(Integer, primary_key=True)  # FK to games.game_id (Game model not yet defined)
    is_recap = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    article = relationship('Article', back_populates='game_tags')
    # game = relationship('Game', foreign_keys=[game_id])  # Uncomment when Game model exists

    def __repr__(self):
        return f"<ArticleGameTag article_id={self.article_id} game_id={self.game_id}>"


class ArticleImage(BaseModel):
    """Images associated with newspaper articles"""
    __tablename__ = 'article_images'

    image_id = Column(Integer, primary_key=True)
    article_id = Column(Integer, ForeignKey('newspaper_articles.article_id'), nullable=False)
    image_type = Column(String(20), nullable=False)  # 'player', 'team_logo', 'uploaded'

    # For player images (path: etl/data/images/players/player_{id}.png)
    player_id = Column(Integer, ForeignKey('players_core.player_id'))

    # For team logo images (path: etl/data/images/team_logos/{team_name}_{size}.png)
    team_id = Column(Integer, ForeignKey('teams.team_id'))
    logo_size = Column(String(10), default='default')  # 'default', '16', '25', '40', '50', '110'

    # For uploaded images (stored in web/app/static/uploads/articles/)
    uploaded_filename = Column(String(255))
    uploaded_path = Column(String(500))
    file_size = Column(Integer)  # bytes
    mime_type = Column(String(100))

    # Common fields
    caption = Column(Text)
    alt_text = Column(String(255))
    display_order = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    article = relationship('Article', back_populates='images')
    player = relationship('Player', foreign_keys=[player_id])
    team = relationship('Team', foreign_keys=[team_id])

    def __repr__(self):
        return f"<ArticleImage {self.image_id}: {self.image_type}>"

    def get_image_url(self):
        """Generate the URL for this image"""
        from flask import url_for

        if self.image_type == 'player':
            # Player images: /etl-images/players/player_{id}.png
            return url_for('main.serve_etl_image', image_path=f'players/player_{self.player_id}.png')

        elif self.image_type == 'team_logo':
            # Team logos: /etl-images/team_logos/{team_name}_{nickname}_{size}.png
            if self.team:
                # Format: name_nickname (e.g., mesquite_smokies)
                team_name = self.team.name.lower().replace(' ', '_')
                team_nickname = self.team.nickname.lower().replace(' ', '_')
                base_name = f'{team_name}_{team_nickname}'

                if self.logo_size and self.logo_size != 'default':
                    return url_for('main.serve_etl_image', image_path=f'team_logos/{base_name}_{self.logo_size}.png')
                else:
                    return url_for('main.serve_etl_image', image_path=f'team_logos/{base_name}.png')

        elif self.image_type == 'uploaded':
            # Uploaded images: /static/uploads/articles/{filename}
            return url_for('static', filename=f'uploads/articles/{self.uploaded_filename}')

        return None
