"""
Article Processor Module

Processes LLM-generated articles:
- Parses headline and body from raw LLM output
- Validates article quality
- Saves to newspaper_articles table
- Supports article regeneration

Expected article format from LLM:
    HEADLINE: [Article headline in caps]

    [Article body paragraphs]
"""

import re
import psycopg2
from datetime import datetime
from typing import Dict, Optional, Tuple
from loguru import logger
from slugify import slugify


class ArticleProcessor:
    """Process and store LLM-generated newspaper articles."""

    def __init__(self, db_config: Dict):
        """
        Initialize article processor with database connection.

        Args:
            db_config: Database configuration dict with host, port, database, user, password
        """
        self.db_config = db_config
        self.conn = None
        self._connect()

    def _connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info(f"Connected to database: {self.db_config['database']}")
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def _ensure_connection(self):
        """Ensure database connection is alive."""
        if self.conn is None or self.conn.closed:
            logger.warning("Database connection lost, reconnecting...")
            self._connect()

    def close(self):
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")

    def parse_article(self, raw_text: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Parse headline and body from LLM output.

        Expected format:
            HEADLINE: Branch Powers Pilgrims Victory

            Article body text here...

        Args:
            raw_text: Raw text from LLM generation

        Returns:
            Tuple of (headline, body). Returns (None, None) if parsing fails.
        """
        if not raw_text or not raw_text.strip():
            logger.error("Empty article text provided")
            return None, None

        lines = raw_text.strip().split('\n')
        headline = None
        body_lines = []
        found_headline = False

        for line in lines:
            line_stripped = line.strip()

            # Look for headline marker
            if line_stripped.startswith('HEADLINE:'):
                headline = line_stripped.replace('HEADLINE:', '').strip()
                found_headline = True
                logger.debug(f"Found headline: {headline}")
                continue

            # Skip empty lines immediately after headline
            if found_headline and not line_stripped:
                continue

            # Collect body lines after headline
            if found_headline and line_stripped:
                body_lines.append(line_stripped)

        # Fallback: If no headline marker found, use first non-empty line
        if not headline:
            logger.warning("No HEADLINE: marker found, using first line as headline")
            if lines:
                headline = lines[0].strip()
                body_lines = [line.strip() for line in lines[1:] if line.strip()]
            else:
                return None, None

        # Assemble body
        body = '\n\n'.join(body_lines) if body_lines else None

        if not headline or not body:
            logger.error(f"Parse failed - headline: {bool(headline)}, body: {bool(body)}")
            return None, None

        logger.info(f"Article parsed successfully - headline: {len(headline)} chars, body: {len(body)} chars")
        return headline, body

    def validate_article(
        self,
        headline: str,
        body: str,
        min_headline_length: int = 10,
        max_headline_length: int = 200,
        min_body_length: int = 100,
        max_body_length: int = 5000,
        min_word_count: int = 50,
        max_word_count: int = 1000
    ) -> Tuple[bool, list]:
        """
        Validate article quality.

        Args:
            headline: Article headline
            body: Article body text
            min_headline_length: Minimum headline characters
            max_headline_length: Maximum headline characters
            min_body_length: Minimum body characters
            max_body_length: Maximum body characters
            min_word_count: Minimum word count for body
            max_word_count: Maximum word count for body

        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []

        # Headline validations
        if len(headline) < min_headline_length:
            errors.append(f"Headline too short: {len(headline)} chars (min: {min_headline_length})")

        if len(headline) > max_headline_length:
            errors.append(f"Headline too long: {len(headline)} chars (max: {max_headline_length})")

        # Body validations
        if len(body) < min_body_length:
            errors.append(f"Body too short: {len(body)} chars (min: {min_body_length})")

        if len(body) > max_body_length:
            errors.append(f"Body too long: {len(body)} chars (max: {max_body_length})")

        # Word count validation
        word_count = len(body.split())
        if word_count < min_word_count:
            errors.append(f"Body word count too low: {word_count} words (min: {min_word_count})")

        if word_count > max_word_count:
            errors.append(f"Body word count too high: {word_count} words (max: {max_word_count})")

        # Content quality checks
        if headline.isupper() or headline.islower():
            logger.warning("Headline is all caps or all lowercase")

        # Check for placeholder text (common in failed generations)
        placeholders = ['[insert', 'TODO', 'TBD', 'PLACEHOLDER']
        for placeholder in placeholders:
            if placeholder.lower() in headline.lower() or placeholder.lower() in body.lower():
                errors.append(f"Article contains placeholder text: {placeholder}")

        is_valid = len(errors) == 0

        if is_valid:
            logger.info(f"Article validation passed - {word_count} words")
        else:
            logger.warning(f"Article validation failed: {errors}")

        return is_valid, errors

    def generate_slug(self, headline: str, game_date: Optional[datetime.date] = None) -> str:
        """
        Generate URL-friendly slug from headline.

        Args:
            headline: Article headline
            game_date: Optional game date to ensure uniqueness

        Returns:
            URL-friendly slug
        """
        base_slug = slugify(headline, max_length=200)

        # Add date prefix if provided
        if game_date:
            date_prefix = game_date.strftime('%Y%m%d')
            slug = f"{date_prefix}-{base_slug}"
        else:
            slug = base_slug

        # Ensure uniqueness by checking database
        self._ensure_connection()
        cursor = self.conn.cursor()

        original_slug = slug
        counter = 1

        while True:
            cursor.execute(
                "SELECT COUNT(*) FROM newspaper_articles WHERE slug = %s",
                (slug,)
            )
            count = cursor.fetchone()[0]

            if count == 0:
                break

            # Slug exists, append counter
            slug = f"{original_slug}-{counter}"
            counter += 1

        cursor.close()
        logger.debug(f"Generated slug: {slug}")

        return slug

    def save_article(
        self,
        headline: str,
        body: str,
        game_context: Dict,
        generation_metadata: Dict,
        newsworthiness_score: Optional[int] = None,
        category_name: str = 'Game Recap',
        player_ids: Optional[list] = None,
        team_ids: Optional[list] = None
    ) -> int:
        """
        Save article to database.

        Args:
            headline: Article headline (will be used as title)
            body: Article body text (stored in content)
            game_context: Game context dict with game_id, date, teams, etc.
            generation_metadata: Metadata from LLM generation (model_used, attempts, etc.)
            newsworthiness_score: Optional newsworthiness score (0-100)
            category_name: Category name (default: 'Game Recap')
            player_ids: Optional list of player IDs to tag
            team_ids: Optional list of team IDs to tag

        Returns:
            article_id of saved article

        Raises:
            psycopg2.Error: On database errors
        """
        self._ensure_connection()

        # Generate slug
        game_date = game_context.get('date')
        slug = self.generate_slug(headline, game_date)

        # Get category_id
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT category_id FROM article_categories WHERE name = %s",
            (category_name,)
        )
        result = cursor.fetchone()
        category_id = result[0] if result else None

        if not category_id:
            logger.warning(f"Category '{category_name}' not found, article will have no category")

        # Generate excerpt (first 200 chars of body)
        excerpt = body[:200] + '...' if len(body) > 200 else body

        # Insert article
        insert_query = """
            INSERT INTO newspaper_articles (
                title,
                slug,
                content,
                excerpt,
                category_id,
                author_type,
                game_date,
                is_published,
                game_id,
                generation_method,
                model_used,
                newsworthiness_score,
                status,
                generation_count
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING article_id
        """

        cursor.execute(insert_query, (
            headline,
            slug,
            body,
            excerpt,
            category_id,
            'ai',  # author_type
            game_date,
            False,  # is_published (default to draft)
            game_context.get('game_id'),
            'ai_generated',  # generation_method
            generation_metadata.get('model_used'),
            newsworthiness_score,
            'draft',  # status
            1  # generation_count
        ))

        article_id = cursor.fetchone()[0]

        # Tag players if provided
        if player_ids:
            for i, player_id in enumerate(player_ids):
                is_primary = (i == 0)  # First player is primary
                cursor.execute(
                    """
                    INSERT INTO article_player_tags (article_id, player_id, is_primary)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (article_id, player_id, is_primary)
                )

        # Tag teams if provided
        if team_ids:
            for i, team_id in enumerate(team_ids):
                is_primary = (i == 0)  # First team is primary
                cursor.execute(
                    """
                    INSERT INTO article_team_tags (article_id, team_id, is_primary)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (article_id, team_id, is_primary)
                )

        # Tag game
        if game_context.get('game_id'):
            cursor.execute(
                """
                INSERT INTO article_game_tags (article_id, game_id, is_recap)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (article_id, game_context['game_id'], True)  # is_recap=True
            )

        self.conn.commit()
        cursor.close()

        logger.info(f"Article saved successfully: article_id={article_id}, slug={slug}")
        return article_id

    def regenerate_article(
        self,
        original_article_id: int,
        new_headline: str,
        new_body: str,
        generation_metadata: Dict
    ) -> int:
        """
        Regenerate an existing article (creates new version with link to original).

        Args:
            original_article_id: ID of article being regenerated
            new_headline: New headline
            new_body: New body text
            generation_metadata: Metadata from new generation

        Returns:
            article_id of new article version
        """
        self._ensure_connection()
        cursor = self.conn.cursor()

        # Get original article details
        cursor.execute(
            """
            SELECT game_date, game_id, category_id, newsworthiness_score, generation_count
            FROM newspaper_articles
            WHERE article_id = %s
            """,
            (original_article_id,)
        )

        result = cursor.fetchone()
        if not result:
            raise ValueError(f"Original article {original_article_id} not found")

        game_date, game_id, category_id, newsworthiness_score, old_gen_count = result

        # Generate new slug
        slug = self.generate_slug(new_headline, game_date)

        # Generate excerpt
        excerpt = new_body[:200] + '...' if len(new_body) > 200 else new_body

        # Insert new version
        insert_query = """
            INSERT INTO newspaper_articles (
                title,
                slug,
                content,
                excerpt,
                category_id,
                author_type,
                game_date,
                is_published,
                game_id,
                generation_method,
                model_used,
                newsworthiness_score,
                status,
                generation_count,
                previous_version_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING article_id
        """

        cursor.execute(insert_query, (
            new_headline,
            slug,
            new_body,
            excerpt,
            category_id,
            'ai',
            game_date,
            False,  # is_published
            game_id,
            'ai_generated',
            generation_metadata.get('model_used'),
            newsworthiness_score,
            'draft',
            old_gen_count + 1,  # Increment generation count
            original_article_id  # Link to previous version
        ))

        new_article_id = cursor.fetchone()[0]

        # Copy player tags from original
        cursor.execute(
            """
            INSERT INTO article_player_tags (article_id, player_id, is_primary)
            SELECT %s, player_id, is_primary
            FROM article_player_tags
            WHERE article_id = %s
            """,
            (new_article_id, original_article_id)
        )

        # Copy team tags from original
        cursor.execute(
            """
            INSERT INTO article_team_tags (article_id, team_id, is_primary)
            SELECT %s, team_id, is_primary
            FROM article_team_tags
            WHERE article_id = %s
            """,
            (new_article_id, original_article_id)
        )

        # Copy game tag from original
        cursor.execute(
            """
            INSERT INTO article_game_tags (article_id, game_id, is_recap)
            SELECT %s, game_id, is_recap
            FROM article_game_tags
            WHERE article_id = %s
            """,
            (new_article_id, original_article_id)
        )

        self.conn.commit()
        cursor.close()

        logger.info(f"Article regenerated: new_id={new_article_id}, original_id={original_article_id}, gen_count={old_gen_count + 1}")
        return new_article_id

    def get_article(self, article_id: int) -> Optional[Dict]:
        """
        Retrieve article by ID.

        Args:
            article_id: Article ID

        Returns:
            Article dict or None if not found
        """
        self._ensure_connection()
        cursor = self.conn.cursor()

        cursor.execute(
            """
            SELECT
                article_id,
                title,
                slug,
                content,
                excerpt,
                category_id,
                author_type,
                game_date,
                is_published,
                game_id,
                generation_method,
                model_used,
                newsworthiness_score,
                status,
                generation_count,
                previous_version_id,
                created_at,
                updated_at
            FROM newspaper_articles
            WHERE article_id = %s
            """,
            (article_id,)
        )

        result = cursor.fetchone()
        cursor.close()

        if not result:
            return None

        return {
            'article_id': result[0],
            'title': result[1],
            'slug': result[2],
            'content': result[3],
            'excerpt': result[4],
            'category_id': result[5],
            'author_type': result[6],
            'game_date': result[7],
            'is_published': result[8],
            'game_id': result[9],
            'generation_method': result[10],
            'model_used': result[11],
            'newsworthiness_score': result[12],
            'status': result[13],
            'generation_count': result[14],
            'previous_version_id': result[15],
            'created_at': result[16],
            'updated_at': result[17]
        }

    def process_and_save(
        self,
        raw_article_text: str,
        game_context: Dict,
        generation_metadata: Dict,
        newsworthiness_score: Optional[int] = None,
        category_name: str = 'Game Recap',
        player_ids: Optional[list] = None,
        team_ids: Optional[list] = None,
        validate: bool = True
    ) -> Tuple[Optional[int], Optional[Dict]]:
        """
        Complete workflow: parse, validate, and save article.

        Args:
            raw_article_text: Raw LLM output
            game_context: Game context dict
            generation_metadata: LLM generation metadata
            newsworthiness_score: Optional newsworthiness score
            category_name: Category name
            player_ids: Optional list of player IDs to tag
            team_ids: Optional list of team IDs to tag
            validate: Whether to validate article (default: True)

        Returns:
            Tuple of (article_id, result_dict) where result_dict contains:
                - success: bool
                - article_id: int (if successful)
                - headline: str (if parsed)
                - body: str (if parsed)
                - word_count: int (if parsed)
                - validation_errors: list (if validation failed)
                - error: str (if failed)
        """
        result = {'success': False}

        # Step 1: Parse
        headline, body = self.parse_article(raw_article_text)

        if not headline or not body:
            result['error'] = 'Failed to parse headline and body from article text'
            return None, result

        result['headline'] = headline
        result['body'] = body
        result['word_count'] = len(body.split())

        # Step 2: Validate
        if validate:
            is_valid, validation_errors = self.validate_article(headline, body)

            if not is_valid:
                result['validation_errors'] = validation_errors
                result['error'] = f'Article validation failed: {"; ".join(validation_errors)}'
                return None, result

        # Step 3: Save
        try:
            article_id = self.save_article(
                headline=headline,
                body=body,
                game_context=game_context,
                generation_metadata=generation_metadata,
                newsworthiness_score=newsworthiness_score,
                category_name=category_name,
                player_ids=player_ids,
                team_ids=team_ids
            )

            result['success'] = True
            result['article_id'] = article_id

            logger.info(f"Article processed and saved successfully: article_id={article_id}")
            return article_id, result

        except Exception as e:
            logger.error(f"Failed to save article: {e}")
            result['error'] = f'Database save failed: {str(e)}'
            return None, result


def create_processor(db_config: Dict) -> ArticleProcessor:
    """
    Factory function to create ArticleProcessor instance.

    Args:
        db_config: Database configuration dict

    Returns:
        ArticleProcessor instance
    """
    return ArticleProcessor(db_config)