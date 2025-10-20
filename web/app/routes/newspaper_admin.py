"""Newspaper admin routes for editorial workflow"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, abort
from app.models import Article, ArticleCategory, Player, Team, ArticlePlayerTag, ArticleTeamTag, ArticleGameTag
from app.extensions import db
from sqlalchemy import desc, or_, func
from datetime import datetime
from loguru import logger

bp = Blueprint('newspaper_admin', __name__)


# Authentication decorator - currently open for development
def admin_required(f):
    """
    Decorator to require admin access.

    DEVELOPMENT: Currently passes through without authentication.

    PRODUCTION TODO: Implement Flask-Login authentication:
    - Install flask-login package
    - Create User model with hashed passwords
    - Add login/logout routes
    - Replace this decorator with @login_required
    - Add role-based access control (admin role)

    For staging/production deployment, see:
    - Flask-Login docs: https://flask-login.readthedocs.io/
    - Implement in web/app/auth.py
    - Add User model to web/app/models/user.py
    """
    from functools import wraps

    @wraps(f)
    def decorated_function(*args, **kwargs):
        # TODO: Replace with Flask-Login @login_required decorator
        # if not current_user.is_authenticated or not current_user.is_admin:
        #     abort(403)
        return f(*args, **kwargs)

    return decorated_function


@bp.route('/drafts')
@admin_required
def draft_list():
    """
    Show all draft articles, sorted by newsworthiness and date.

    Task 3.1 - Draft Article List
    """
    # Get all draft articles with player/team tags
    drafts = (
        db.session.query(Article)
        .filter(Article.status == 'draft')
        .order_by(
            desc(Article.newsworthiness_score),
            desc(Article.game_date),
            desc(Article.created_at)
        )
        .all()
    )

    return render_template('newspaper/admin/drafts.html', drafts=drafts)


@bp.route('/review/<int:article_id>')
@admin_required
def review_article(article_id):
    """
    Full article display with editorial controls.

    Task 3.1 - Article Review Page
    """
    article = db.session.query(Article).get_or_404(article_id)

    # Get game info if available
    game_info = None
    if article.game_id:
        # TODO: Fetch game details when Game model is available
        pass

    return render_template(
        'newspaper/admin/review.html',
        article=article,
        game_info=game_info
    )


@bp.route('/publish/<int:article_id>', methods=['POST'])
@admin_required
def publish_article(article_id):
    """
    Publish a draft article.

    Task 3.1 - Publish Handler
    """
    article = db.session.query(Article).get_or_404(article_id)

    if article.status != 'draft':
        flash('Article is not in draft status', 'error')
        return redirect(url_for('newspaper_admin.review_article', article_id=article_id))

    try:
        article.status = 'published'
        article.is_published = True
        article.publish_date = datetime.utcnow()
        article.reviewed_by = 'Admin'  # TODO: Get actual user name
        article.reviewed_at = datetime.utcnow()
        article.updated_at = datetime.utcnow()

        db.session.commit()

        flash(f'Article "{article.title}" published successfully!', 'success')
        logger.info(f'Article {article_id} published: {article.title}')

        # Redirect to the public article page
        return redirect(url_for('newspaper.article_detail', slug=article.slug))

    except Exception as e:
        db.session.rollback()
        logger.error(f'Error publishing article {article_id}: {str(e)}')
        flash(f'Error publishing article: {str(e)}', 'error')
        return redirect(url_for('newspaper_admin.review_article', article_id=article_id))


@bp.route('/reject/<int:article_id>', methods=['POST'])
@admin_required
def reject_article(article_id):
    """
    Reject a draft article.
    """
    article = db.session.query(Article).get_or_404(article_id)

    try:
        article.status = 'rejected'
        article.reviewed_by = 'Admin'  # TODO: Get actual user name
        article.reviewed_at = datetime.utcnow()
        article.updated_at = datetime.utcnow()

        db.session.commit()

        flash(f'Article "{article.title}" rejected', 'info')
        logger.info(f'Article {article_id} rejected: {article.title}')

        return redirect(url_for('newspaper_admin.draft_list'))

    except Exception as e:
        db.session.rollback()
        logger.error(f'Error rejecting article {article_id}: {str(e)}')
        flash(f'Error rejecting article: {str(e)}', 'error')
        return redirect(url_for('newspaper_admin.review_article', article_id=article_id))


@bp.route('/regenerate/<int:article_id>', methods=['GET', 'POST'])
@admin_required
def regenerate_article(article_id):
    """
    Regenerate article with optional feedback.

    Task 3.1 - Regenerate Handler

    NOTE: This calls the ETL pipeline's article generation with regeneration.
    The actual LLM generation happens in etl/src/newspaper/
    """
    article = db.session.query(Article).get_or_404(article_id)

    if request.method == 'POST':
        feedback = request.form.get('feedback', '')
        model_override = request.form.get('model_override')
        temperature = request.form.get('temperature', '0.7')

        try:
            # Import the regeneration function from ETL pipeline
            import sys
            sys.path.insert(0, '/mnt/hdd/PycharmProjects/rb2/etl')

            # TODO: When the full pipeline is ready, call it here
            # For now, log the request and show info message
            # from src.newspaper.pipeline import regenerate_article_with_feedback
            # new_article_id = regenerate_article_with_feedback(
            #     article_id=article_id,
            #     feedback=feedback,
            #     model_override=model_override,
            #     temperature=float(temperature)
            # )

            flash(
                'Article regeneration requested. This will be integrated with the full AI pipeline '
                'when Phase 2 Task 2.4 (Ollama Client) and Task 2.6 (Pipeline) are complete. '
                'The backend regenerate_article() function is ready in article_processor.py.',
                'info'
            )
            logger.info(
                f'Regeneration requested for article {article_id}: '
                f'feedback="{feedback}", model={model_override}, temp={temperature}'
            )

        except Exception as e:
            logger.error(f'Error during regeneration request: {str(e)}')
            flash(f'Error requesting regeneration: {str(e)}', 'error')

        return redirect(url_for('newspaper_admin.review_article', article_id=article_id))

    # GET request - show regeneration form
    return render_template('newspaper/admin/regenerate.html', article=article)


@bp.route('/delete/<int:article_id>', methods=['POST'])
@admin_required
def delete_article(article_id):
    """
    Delete a draft article.
    """
    article = db.session.query(Article).get_or_404(article_id)

    if article.status == 'published':
        flash('Cannot delete published articles', 'error')
        return redirect(url_for('newspaper_admin.review_article', article_id=article_id))

    try:
        title = article.title
        db.session.delete(article)
        db.session.commit()

        flash(f'Article "{title}" deleted successfully', 'success')
        logger.info(f'Article {article_id} deleted: {title}')

        return redirect(url_for('newspaper_admin.draft_list'))

    except Exception as e:
        db.session.rollback()
        logger.error(f'Error deleting article {article_id}: {str(e)}')
        flash(f'Error deleting article: {str(e)}', 'error')
        return redirect(url_for('newspaper_admin.review_article', article_id=article_id))


@bp.route('/create', methods=['GET', 'POST'])
@admin_required
def create_article():
    """
    Manual article creation form for user-written content.

    Task 3.2 - User Content Creation Interface
    """
    if request.method == 'POST':
        try:
            # Get form data
            title = request.form.get('title', '').strip()
            content = request.form.get('content', '').strip()
            excerpt = request.form.get('excerpt', '').strip()
            author_type = request.form.get('author_type', 'user')
            game_date_str = request.form.get('game_date')
            category_id = request.form.get('category_id')

            # Player and team IDs from autocomplete
            player_ids = request.form.getlist('player_ids[]')
            team_ids = request.form.getlist('team_ids[]')
            game_id = request.form.get('game_id')

            # Validation
            if not title or not content:
                flash('Title and content are required', 'error')
                return redirect(url_for('newspaper_admin.create_article'))

            # Generate slug from title
            slug = title.lower().replace(' ', '-').replace("'", '').replace('"', '')
            # Add date prefix if game_date provided
            if game_date_str:
                game_date = datetime.strptime(game_date_str, '%Y-%m-%d').date()
                slug = f"{game_date.strftime('%Y-%m-%d')}-{slug}"

            # Ensure slug is unique
            base_slug = slug
            counter = 1
            while db.session.query(Article).filter_by(slug=slug).first():
                slug = f"{base_slug}-{counter}"
                counter += 1

            # Create article
            article = Article(
                title=title,
                slug=slug,
                content=content,
                excerpt=excerpt or content[:200] + '...',
                author_type=author_type,
                game_date=game_date if game_date_str else None,
                category_id=int(category_id) if category_id else None,
                game_id=int(game_id) if game_id else None,
                status='published',  # User content auto-publishes
                is_published=True,
                publish_date=datetime.utcnow(),
                reviewed_by='User',
                reviewed_at=datetime.utcnow()
            )

            db.session.add(article)
            db.session.flush()  # Get article_id

            # Add player tags
            for idx, player_id in enumerate(player_ids):
                if player_id:
                    tag = ArticlePlayerTag(
                        article_id=article.article_id,
                        player_id=int(player_id),
                        is_primary=(idx == 0)  # First player is primary
                    )
                    db.session.add(tag)

            # Add team tags
            for idx, team_id in enumerate(team_ids):
                if team_id:
                    tag = ArticleTeamTag(
                        article_id=article.article_id,
                        team_id=int(team_id),
                        is_primary=(idx == 0)  # First team is primary
                    )
                    db.session.add(tag)

            # Add game tag if game_id provided
            if game_id:
                game_tag = ArticleGameTag(
                    article_id=article.article_id,
                    game_id=int(game_id),
                    is_recap=False  # User articles aren't game recaps
                )
                db.session.add(game_tag)

            db.session.commit()

            flash(f'Article "{title}" created successfully!', 'success')
            logger.info(f'User article created: {article.article_id} - {title}')

            return redirect(url_for('newspaper.article_detail', slug=article.slug))

        except Exception as e:
            db.session.rollback()
            logger.error(f'Error creating article: {str(e)}')
            flash(f'Error creating article: {str(e)}', 'error')
            return redirect(url_for('newspaper_admin.create_article'))

    # GET request - show form
    categories = db.session.query(ArticleCategory).order_by(ArticleCategory.display_order).all()
    return render_template('newspaper/admin/create.html', categories=categories)


# API Endpoints for autocomplete

@bp.route('/api/players/search')
@admin_required
def search_players():
    """
    Search players by name for autocomplete.

    Task 3.2 - Player Search API
    """
    query = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 10))

    if not query or len(query) < 2:
        return jsonify([])

    # Search by first or last name
    players = (
        db.session.query(Player)
        .filter(
            or_(
                Player.first_name.ilike(f'%{query}%'),
                Player.last_name.ilike(f'%{query}%'),
                func.concat(Player.first_name, ' ', Player.last_name).ilike(f'%{query}%')
            )
        )
        .order_by(Player.last_name, Player.first_name)
        .limit(limit)
        .all()
    )

    results = [
        {
            'player_id': p.player_id,
            'name': f"{p.first_name} {p.last_name}",
            'position': p.position_display if hasattr(p, 'position_display') else '',
        }
        for p in players
    ]

    return jsonify(results)


@bp.route('/api/teams/search')
@admin_required
def search_teams():
    """
    Search teams by name or abbreviation for autocomplete.

    Task 3.2 - Team Search API
    """
    query = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 10))

    if not query or len(query) < 2:
        return jsonify([])

    # Search by name, nickname, or abbreviation
    teams = (
        db.session.query(Team)
        .filter(
            or_(
                Team.name.ilike(f'%{query}%'),
                Team.nickname.ilike(f'%{query}%'),
                Team.abbr.ilike(f'%{query}%')
            )
        )
        .order_by(Team.name)
        .limit(limit)
        .all()
    )

    results = [
        {
            'team_id': t.team_id,
            'name': t.name,
            'nickname': t.nickname,
            'abbr': t.abbr,
            'display': f"{t.name} {t.nickname} ({t.abbr})"
        }
        for t in teams
    ]

    return jsonify(results)
