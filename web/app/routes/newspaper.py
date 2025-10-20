"""Newspaper routes - public facing"""
from flask import Blueprint, render_template, abort
from app.models import Article
from app.extensions import db
from app.utils.article_links import process_article_for_display
from sqlalchemy import desc

bp = Blueprint('newspaper', __name__)


@bp.route('/')
def index():
    """Newspaper homepage - Task 4.1"""
    # Get published articles
    articles = (
        db.session.query(Article)
        .filter(Article.is_published == True)
        .order_by(desc(Article.game_date), desc(Article.newsworthiness_score))
        .limit(20)
        .all()
    )

    # Get hero article (highest newsworthiness from last week)
    hero_article = None
    if articles:
        hero_article = articles[0]

    return render_template(
        'newspaper/index.html',
        hero_article=hero_article,
        articles=articles[1:] if hero_article else articles
    )


@bp.route('/article/<slug>')
def article_detail(slug):
    """Individual article page - Task 4.2"""
    article = db.session.query(Article).filter_by(slug=slug).first_or_404()

    # Increment view count
    article.view_count += 1
    db.session.commit()

    # Process article content to auto-link player/team names
    processed_content = process_article_for_display(article)

    # Get related articles (same players or same game)
    related_articles = []
    if article.player_tags:
        from app.models import ArticlePlayerTag
        player_ids = [tag.player_id for tag in article.player_tags]

        # Find articles that share at least one player
        related_articles = (
            db.session.query(Article)
            .join(ArticlePlayerTag)
            .filter(
                Article.article_id != article.article_id,
                Article.is_published == True,
                ArticlePlayerTag.player_id.in_(player_ids)
            )
            .order_by(desc(Article.game_date))
            .limit(5)
            .all()
        )

    return render_template(
        'newspaper/article.html',
        article=article,
        processed_content=processed_content,
        related_articles=related_articles
    )
