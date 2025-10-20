"""
Utility functions for auto-linking player and team names in article content.

Based on newspaper-implementation-plan.md Task 3.2 - Player/Team Linking
"""
import re
from flask import url_for


def auto_link_content(content, player_tags, team_tags):
    """
    Automatically convert player and team names to hyperlinks in article content.

    Strategy:
    1. Get player/team names from tags
    2. Find name mentions in content (case-insensitive)
    3. Replace with <a> tags linking to player/team detail pages
    4. Process longest names first to avoid partial matches

    Args:
        content: Article body text (plain text or HTML)
        player_tags: List of ArticlePlayerTag objects
        team_tags: List of ArticleTeamTag objects

    Returns:
        HTML string with auto-linked player and team names
    """
    if not content:
        return content

    linked_content = content
    replacements = []

    # Build replacement list for players
    for tag in player_tags:
        player = tag.player
        full_name = f"{player.first_name} {player.last_name}"
        last_name = player.last_name

        # Create link HTML
        link_html = f'<a href="{url_for("players.player_detail", player_id=player.player_id)}" class="text-forest hover:text-vintage-gold font-medium">{full_name}</a>'

        # Add both full name and last name for matching
        replacements.append({
            'name': full_name,
            'link': link_html,
            'length': len(full_name)
        })

        # Also match last name only (but with lower priority)
        last_link_html = f'<a href="{url_for("players.player_detail", player_id=player.player_id)}" class="text-forest hover:text-vintage-gold font-medium">{last_name}</a>'
        replacements.append({
            'name': last_name,
            'link': last_link_html,
            'length': len(last_name)
        })

    # Build replacement list for teams
    for tag in team_tags:
        team = tag.team
        full_name = f"{team.name} {team.nickname}"  # e.g., "Boston Red Sox"
        nickname = team.nickname  # e.g., "Red Sox"

        # Create link HTML
        link_html = f'<a href="{url_for("teams.team_detail", team_id=team.team_id)}" class="text-forest hover:text-vintage-gold font-medium">{full_name}</a>'

        replacements.append({
            'name': full_name,
            'link': link_html,
            'length': len(full_name)
        })

        # Also match nickname only
        nickname_link_html = f'<a href="{url_for("teams.team_detail", team_id=team.team_id)}" class="text-forest hover:text-vintage-gold font-medium">{nickname}</a>'
        replacements.append({
            'name': nickname,
            'link': nickname_link_html,
            'length': len(nickname)
        })

    # Sort by length (longest first) to avoid partial replacements
    # e.g., "Mike Branch" before "Branch", "Red Sox" before "Sox"
    replacements.sort(key=lambda x: x['length'], reverse=True)

    # Track what we've already replaced to avoid double-linking
    already_replaced = set()

    # Perform replacements
    for replacement in replacements:
        name = replacement['name']
        link = replacement['link']

        # Skip if we've already replaced this name
        if name.lower() in already_replaced:
            continue

        # Use word boundaries to avoid partial matches
        # \b ensures we match whole words only
        pattern = re.compile(r'\b' + re.escape(name) + r'\b', re.IGNORECASE)

        # Only replace if NOT already inside an <a> tag
        # This regex checks that the match is not within <a>...</a>
        def replace_if_not_in_link(match):
            # Get position of match
            start = match.start()

            # Check if this position is inside an existing <a> tag
            # Look backwards for <a and forwards for </a>
            before = linked_content[:start]
            after = linked_content[start:]

            # Count opening and closing tags before this position
            open_tags = before.count('<a ')
            close_tags = before.count('</a>')

            # If we have more open tags than close tags, we're inside a link
            if open_tags > close_tags:
                return match.group(0)  # Don't replace

            # Also check if the matched text itself contains <a
            if '<a ' in match.group(0):
                return match.group(0)  # Don't replace

            # Safe to replace
            already_replaced.add(name.lower())
            return link

        linked_content = pattern.sub(replace_if_not_in_link, linked_content)

    return linked_content


def process_article_for_display(article):
    """
    Process an article's content for display by auto-linking player/team names.

    This is meant to be called when rendering articles on the public site.

    Args:
        article: Article model instance with player_tags and team_tags loaded

    Returns:
        Processed content with auto-links
    """
    return auto_link_content(
        article.content,
        article.player_tags,
        article.team_tags
    )
