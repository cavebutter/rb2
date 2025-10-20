


"""
Prompt Builder Module

Constructs detailed prompts for LLM-based article generation.
Provides structured context about games, players, and play-by-play details
with explicit journalistic style instructions.

Features era-appropriate style suggestions spanning from 1920s to present day,
adapting vocabulary, tone, and formatting conventions to match the historical period.

Prompt Types:
- Single Branch player performance articles
- Multi-Branch player family angle articles
- Article regeneration with editorial feedback
"""

from typing import Dict, List, Optional, Tuple
from datetime import date
from loguru import logger


def get_era_from_date(game_date) -> Tuple[str, int]:
    """
    Determine journalistic era and decade from game date.

    Args:
        game_date: date object or string

    Returns:
        Tuple of (era_name, year)

    Eras:
        - 1920s: "Roaring Twenties" / "Jazz Age"
        - 1930s-1940s: "Golden Age"
        - 1950s-1960s: "Post-War Era"
        - 1970s-1980s: "Modern Era"
        - 1990s-2000s: "Contemporary Era"
        - 2010s+: "Digital Era"
    """
    if isinstance(game_date, str):
        # Try to extract year from string
        try:
            year = int(game_date[:4])
        except (ValueError, IndexError):
            logger.warning(f"Could not parse year from date string: {game_date}, defaulting to 1960s")
            return ("Post-War Era", 1960)
    elif isinstance(game_date, date):
        year = game_date.year
    else:
        logger.warning(f"Unexpected date type: {type(game_date)}, defaulting to 1960s")
        return ("Post-War Era", 1960)

    # Map year to era
    if year < 1930:
        return ("Roaring Twenties", year)
    elif year < 1950:
        return ("Golden Age", year)
    elif year < 1970:
        return ("Post-War Era", year)
    elif year < 1990:
        return ("Modern Era", year)
    elif year < 2010:
        return ("Contemporary Era", year)
    else:
        return ("Digital Era", year)


def get_era_style_guidelines(era_name: str, year: int) -> List[str]:
    """
    Get era-specific writing style guidelines for LLM prompts.

    Args:
        era_name: Era name from get_era_from_date()
        year: Specific year

    Returns:
        List of style guideline strings
    """
    base_guidelines = [
        f"- Write in the style of a {year}s baseball newspaper",
        "- Maintain factual accuracy - only report what's in the provided data",
        "- Use proper baseball terminology",
        "- Start with a compelling lead paragraph",
    ]

    # Era-specific guidelines
    if era_name == "Roaring Twenties":
        era_guidelines = [
            "- Use 1920s journalistic style: formal, flowery prose with longer sentences",
            "- Language: colorful nicknames welcome, dramatic verbs ('smashed', 'walloped', 'crushed')",
            "- Tone: enthusiastic and celebratory, baseball as entertainment",
            "- Stats mentioned but narrative-focused rather than statistics-heavy",
            "- Common phrases: 'horsehide', 'pellet', 'circuit clout' (home run), 'hurler' (pitcher)",
        ]
    elif era_name == "Golden Age":
        era_guidelines = [
            "- Use 1930s-40s journalistic style: formal but slightly more concise than 1920s",
            "- Language: colorful but professional, vivid action verbs",
            "- Tone: reverent toward the game, players treated as heroes",
            "- Stats integrated into narrative flow",
            "- Common phrases: 'the national pastime', 'horsehide', 'circuit blow', 'backstop' (catcher)",
            "- WWII-era (1941-1945): may reference military service or wartime context if appropriate",
        ]
    elif era_name == "Post-War Era":
        era_guidelines = [
            "- Use 1950s-60s journalistic style: formal, objective, classic 'wire service' clarity",
            "- Language: precise and professional, avoid slang",
            "- Tone: objective and authoritative, 'just the facts' approach",
            "- Stats prominently featured alongside narrative",
            "- Common phrases: 'four-bagger' (HR), 'junior circuit' (AL), 'senior circuit' (NL)",
            "- 1960s: avoid any modern technology references (no computers, advanced metrics)",
        ]
    elif era_name == "Modern Era":
        era_guidelines = [
            "- Use 1970s-80s journalistic style: professional but more casual than previous eras",
            "- Language: straightforward, some personality allowed",
            "- Tone: balanced between narrative and statistical analysis",
            "- Stats more emphasized, beginning of sabermetric awareness (1980s)",
            "- Common phrases: 'round-tripper' (HR), 'free pass' (walk), 'whiff' (strikeout)",
            "- Can reference modern concepts like 'momentum' and 'clutch performance'",
        ]
    elif era_name == "Contemporary Era":
        era_guidelines = [
            "- Use 1990s-2000s journalistic style: concise, statistics-integrated",
            "- Language: professional but conversational, analytical",
            "- Tone: fact-driven with context, advanced stats begin appearing",
            "- Stats heavily featured: batting average, OPS, ERA becoming standard",
            "- Common phrases: 'yard' (HR), 'K' (strikeout), references to analytics",
            "- Can mention pitch counts, specialized roles (setup man, closer)",
        ]
    else:  # Digital Era (2010+)
        era_guidelines = [
            "- Use 2010s+ journalistic style: data-driven, concise, analytical",
            "- Language: modern, sabermetric-aware, efficient prose",
            "- Tone: analytical and contextual, stats tell the story",
            "- Stats: advanced metrics expected (exit velocity, launch angle, WAR if available)",
            "- Common phrases: 'barrels', 'exit velo', 'launch angle', sabermetric terms",
            "- Can reference replay review, pitch tracking, advanced defensive metrics",
        ]

    return base_guidelines + era_guidelines


def build_era_specific_instructions(game_date) -> Tuple[str, List[str]]:
    """
    Build era-specific writing instructions for prompts.

    Args:
        game_date: Game date (date object or string)

    Returns:
        Tuple of (era_description, style_guidelines_list)
    """
    era_name, year = get_era_from_date(game_date)
    guidelines = get_era_style_guidelines(era_name, year)

    decade = (year // 10) * 10
    era_description = f"{decade}s-era baseball newspaper"

    logger.debug(f"Era detection: {year} -> {era_name} ({decade}s)")

    return (era_description, guidelines)


def format_date(game_date) -> str:
    """
    Format game date for article context.

    Args:
        game_date: date object or string

    Returns:
        Formatted date string (e.g., "June 15, 1969")
    """
    if isinstance(game_date, str):
        return game_date

    if isinstance(game_date, date):
        return game_date.strftime("%B %d, %Y")

    return str(game_date)


def format_batting_line(stats: Dict) -> str:
    """
    Format batting statistics as readable line.

    Example: "3-for-4 with 2 home runs and 5 RBI"

    Args:
        stats: Dict with keys: ab, h, hr, rbi, bb, k, etc.

    Returns:
        Formatted batting line string
    """
    ab = stats.get('ab', 0)
    h = stats.get('h', 0)
    hr = stats.get('hr', 0)
    rbi = stats.get('rbi', 0)
    bb = stats.get('bb', 0)
    k = stats.get('k', 0)
    r = stats.get('r', 0)

    parts = [f"{h}-for-{ab}"]

    if hr > 0:
        hr_text = "home run" if hr == 1 else "home runs"
        parts.append(f"{hr} {hr_text}")

    if rbi > 0:
        parts.append(f"{rbi} RBI")

    if r > 0:
        parts.append(f"{r} {'run' if r == 1 else 'runs'} scored")

    if bb > 0:
        parts.append(f"{bb} {'walk' if bb == 1 else 'walks'}")

    if k > 0:
        parts.append(f"{k} {'strikeout' if k == 1 else 'strikeouts'}")

    return " with ".join(parts[:2]) + (f", {', '.join(parts[2:])}" if len(parts) > 2 else "")


def format_pitching_line(stats: Dict) -> str:
    """
    Format pitching statistics as readable line.

    Example: "7.0 innings, allowing 3 hits and 1 earned run with 9 strikeouts"

    Args:
        stats: Dict with keys: ip, h, r, er, bb, k, w, l, sv

    Returns:
        Formatted pitching line string
    """
    ip = stats.get('ip', 0.0)
    h = stats.get('h', 0)
    er = stats.get('er', 0)
    bb = stats.get('bb', 0)
    k = stats.get('k', 0)
    w = stats.get('w', 0)
    sv = stats.get('sv', 0)

    parts = [f"{ip} innings"]

    parts.append(f"allowing {h} {'hit' if h == 1 else 'hits'}")

    if er > 0:
        parts.append(f"{er} earned {'run' if er == 1 else 'runs'}")
    else:
        parts.append("no earned runs")

    if bb > 0:
        parts.append(f"{bb} {'walk' if bb == 1 else 'walks'}")

    if k > 0:
        parts.append(f"{k} {'strikeout' if k == 1 else 'strikeouts'}")

    result = ", ".join(parts)

    if w > 0:
        result = f"Win: {result}"
    elif sv > 0:
        result = f"Save: {result}"

    return result


def format_play_sequence(at_bat: Dict) -> str:
    """
    Format play-by-play sequence into readable narrative.

    Args:
        at_bat: Dict from game_log_parser with keys: sequence, outcome, exit_velocity, etc.

    Returns:
        Narrative description of the at-bat
    """
    outcome = at_bat.get('outcome', 'other')
    inning = at_bat.get('inning')
    inning_half = at_bat.get('inning_half', 'top')
    ev = at_bat.get('exit_velocity')
    location = at_bat.get('hit_location')

    # Build inning context
    inning_text = f"{inning_half} of the {inning}{'st' if inning == 1 else 'nd' if inning == 2 else 'rd' if inning == 3 else 'th'}"

    # Format outcome
    outcome_map = {
        'home_run': 'homered',
        'triple': 'tripled',
        'double': 'doubled',
        'single': 'singled',
        'walk': 'walked',
        'strikeout': 'struck out',
        'ground_out': 'grounded out',
        'fly_out': 'flied out',
        'line_out': 'lined out',
        'popup': 'popped out',
    }

    outcome_text = outcome_map.get(outcome, outcome.replace('_', ' '))

    # Add exit velocity if available
    ev_text = f" (exit velocity: {ev} MPH)" if ev else ""

    return f"In the {inning_text}, {outcome_text}{ev_text}"


def build_article_prompt(
    game_context: Dict,
    player_details: Dict,
    branch_at_bats: Optional[List[Dict]] = None
) -> str:
    """
    Construct comprehensive prompt for single Branch player article.

    Args:
        game_context: Game metadata from game_context.py
        player_details: Player bio and stats from game_context.py
        branch_at_bats: Optional play-by-play details from game_log_parser.py

    Returns:
        Formatted prompt string for LLM
    """
    # Extract game info
    game_date = format_date(game_context.get('date'))
    home_team = game_context.get('home_team', {})
    away_team = game_context.get('away_team', {})
    score = game_context.get('score', {})
    attendance = game_context.get('attendance')

    # Extract player info
    player_name = player_details.get('full_name', 'Unknown Player')
    team_name = player_details.get('team', {}).get('name', 'Unknown Team')
    game_stats = player_details.get('game_stats', {})

    # Determine if batting or pitching performance
    batting = game_stats.get('batting')
    pitching = game_stats.get('pitching')

    # Get era-specific instructions
    era_description, era_guidelines = build_era_specific_instructions(game_context.get('date'))

    # Build prompt
    prompt_parts = [
        f"You are a sports journalist writing for a {era_description}.",
        "",
        "GAME CONTEXT:",
        f"Date: {game_date}",
        f"Teams: {away_team.get('name')} ({away_team.get('abbr')}) at {home_team.get('name')} ({home_team.get('abbr')})",
        f"Final Score: {away_team.get('name')} {score.get('away')}, {home_team.get('name')} {score.get('home')}",
    ]

    if attendance:
        prompt_parts.append(f"Attendance: {attendance:,}")

    prompt_parts.append("")
    prompt_parts.append(f"FEATURED PLAYER: {player_name} ({team_name})")
    prompt_parts.append("")

    # Add performance stats
    if batting:
        batting_line = format_batting_line(batting)
        prompt_parts.append(f"BATTING PERFORMANCE: {batting_line}")

    if pitching:
        pitching_line = format_pitching_line(pitching)
        prompt_parts.append(f"PITCHING PERFORMANCE: {pitching_line}")

    # Add play-by-play if available
    if branch_at_bats:
        prompt_parts.append("")
        prompt_parts.append("PLAY-BY-PLAY DETAILS:")
        for i, at_bat in enumerate(branch_at_bats, 1):
            play_desc = format_play_sequence(at_bat)
            prompt_parts.append(f"{i}. {play_desc}")

    # Add era-specific writing instructions
    prompt_parts.append("")
    prompt_parts.append("WRITING INSTRUCTIONS:")
    prompt_parts.append("- Write a newspaper article about this game, focusing on the featured player's performance")
    prompt_parts.append("- Target length: 200-250 words")

    # Add all era-specific guidelines
    for guideline in era_guidelines:
        prompt_parts.append(guideline)

    prompt_parts.extend([
        "- Include specific details from the statistics and play-by-play",
        "- End with context about team standings or player's season performance if relevant",
        "",
        "CRITICAL ACCURACY RULES:",
        "- ONLY use information explicitly provided in this prompt",
        "- DO NOT invent player nicknames, positions, or biographical details",
        "- DO NOT add specific pitch types, pitch sequences, or fielding details not provided",
        "- DO NOT invent stadium names, specific pitchers faced, or game situations",
        "- DO NOT add contextual details about team records or player statistics not provided",
        "- Stick to the facts: teams, scores, stats, and play-by-play details given above",
        "",
        "OUTPUT FORMAT:",
        "HEADLINE: [Write a compelling headline in ALL CAPS, 8-12 words]",
        "",
        "[Article body text, 200-250 words, written in journalistic inverted pyramid style]",
        "",
        "Generate the article now:"
    ])

    prompt = "\n".join(prompt_parts)

    logger.debug(f"Built article prompt for {player_name}, length: {len(prompt)} characters")
    return prompt


def build_multi_branch_prompt(
    game_context: Dict,
    branch_players: List[Dict],
    at_bats_dict: Optional[Dict[int, List[Dict]]] = None
) -> str:
    """
    Construct prompt for games featuring multiple Branch family members.

    Emphasizes the family angle and comparative performances.

    Args:
        game_context: Game metadata from game_context.py
        branch_players: List of player detail dicts
        at_bats_dict: Optional dict mapping player_id -> at-bats

    Returns:
        Formatted prompt string for LLM
    """
    # Extract game info
    game_date = format_date(game_context.get('date'))
    home_team = game_context.get('home_team', {})
    away_team = game_context.get('away_team', {})
    score = game_context.get('score', {})
    attendance = game_context.get('attendance')

    # Get era-specific instructions
    era_description, era_guidelines = build_era_specific_instructions(game_context.get('date'))

    # Build prompt
    prompt_parts = [
        f"You are a sports journalist writing for a {era_description}.",
        "",
        "GAME CONTEXT:",
        f"Date: {game_date}",
        f"Teams: {away_team.get('name')} ({away_team.get('abbr')}) at {home_team.get('name')} ({home_team.get('abbr')})",
        f"Final Score: {away_team.get('name')} {score.get('away')}, {home_team.get('name')} {score.get('home')}",
    ]

    if attendance:
        prompt_parts.append(f"Attendance: {attendance:,}")

    prompt_parts.append("")
    prompt_parts.append(f"FEATURED: BRANCH FAMILY MEMBERS ({len(branch_players)} players in this game)")
    prompt_parts.append("")

    # Add each player's performance
    for i, player in enumerate(branch_players, 1):
        player_name = player.get('full_name', 'Unknown Player')
        team_name = player.get('team', {}).get('name', 'Unknown Team')
        game_stats = player.get('game_stats', {})

        prompt_parts.append(f"PLAYER {i}: {player_name} ({team_name})")

        batting = game_stats.get('batting')
        pitching = game_stats.get('pitching')

        if batting:
            batting_line = format_batting_line(batting)
            prompt_parts.append(f"  Batting: {batting_line}")

        if pitching:
            pitching_line = format_pitching_line(pitching)
            prompt_parts.append(f"  Pitching: {pitching_line}")

        # Add play-by-play if available
        if at_bats_dict and player['player_id'] in at_bats_dict:
            at_bats = at_bats_dict[player['player_id']]
            if at_bats:
                prompt_parts.append(f"  Key moments:")
                for at_bat in at_bats[:3]:  # Limit to 3 key moments
                    play_desc = format_play_sequence(at_bat)
                    prompt_parts.append(f"    - {play_desc}")

        prompt_parts.append("")

    # Add era-specific writing instructions with family angle emphasis
    prompt_parts.append("")
    prompt_parts.append("WRITING INSTRUCTIONS:")
    prompt_parts.append("- Write a newspaper article about this game, focusing on the Branch family's involvement")
    prompt_parts.append("- IMPORTANT: Emphasize the family angle - multiple Branch family members playing in the same game is noteworthy")
    prompt_parts.append("- Compare and contrast their performances")
    prompt_parts.append("- Target length: 250-300 words (slightly longer due to multiple players)")

    # Add all era-specific guidelines
    for guideline in era_guidelines:
        prompt_parts.append(guideline)

    prompt_parts.extend([
        "- Include specific details from each player's performance",
        "- Start with a compelling lead highlighting the family connection",
        "",
        "CRITICAL ACCURACY RULES:",
        "- ONLY use information explicitly provided in this prompt",
        "- DO NOT invent player nicknames, positions, or biographical details",
        "- DO NOT add specific pitch types, pitch sequences, or fielding details not provided",
        "- DO NOT invent stadium names, specific pitchers faced, or game situations",
        "- DO NOT add contextual details about team records or player statistics not provided",
        "- Use player full names as provided - do NOT shorten or create nicknames",
        "- Stick to the facts: teams, scores, stats given above",
        "",
        "OUTPUT FORMAT:",
        "HEADLINE: [Write a compelling headline mentioning the Branch family, ALL CAPS, 8-12 words]",
        "",
        "[Article body text, 250-300 words, emphasizing the family angle]",
        "",
        "Generate the article now:"
    ])

    prompt = "\n".join(prompt_parts)

    logger.debug(f"Built multi-Branch prompt for {len(branch_players)} players, length: {len(prompt)} characters")
    return prompt


def build_regeneration_prompt(
    original_article: Dict,
    feedback: str,
    game_context: Optional[Dict] = None,
    player_details: Optional[Dict] = None
) -> str:
    """
    Construct prompt for article regeneration based on editorial feedback.

    Args:
        original_article: Dict with keys: headline, body, newsworthiness_score
        feedback: Editorial feedback text
        game_context: Optional game metadata for reference
        player_details: Optional player details for reference

    Returns:
        Formatted prompt string for LLM
    """
    headline = original_article.get('headline', '')
    body = original_article.get('body', '')

    prompt_parts = [
        "You are a sports journalist revising an article based on editorial feedback.",
        "",
        "ORIGINAL ARTICLE:",
        f"HEADLINE: {headline}",
        "",
        body,
        "",
        "EDITORIAL FEEDBACK:",
        feedback,
        "",
        "REVISION INSTRUCTIONS:",
        "- Revise the article addressing the editorial feedback",
        "- Maintain the original 1960s-era journalistic style",
        "- Keep factual accuracy - don't add information not in the original",
        "- You may improve phrasing, clarity, and structure",
        "- Target length: 200-250 words",
        "",
        "OUTPUT FORMAT:",
        "HEADLINE: [Revised headline in ALL CAPS]",
        "",
        "[Revised article body text]",
        "",
        "Generate the revised article now:"
    ]

    # Add game context if available for reference
    if game_context:
        prompt_parts.insert(3, "")
        prompt_parts.insert(4, "REFERENCE - GAME CONTEXT:")
        prompt_parts.insert(5, f"Teams: {game_context.get('away_team', {}).get('name')} at {game_context.get('home_team', {}).get('name')}")
        prompt_parts.insert(6, f"Score: {game_context.get('score', {}).get('away')}-{game_context.get('score', {}).get('home')}")
        prompt_parts.insert(7, "")

    prompt = "\n".join(prompt_parts)

    logger.debug(f"Built regeneration prompt, feedback length: {len(feedback)} characters")
    return prompt


def build_headline_only_prompt(article_body: str) -> str:
    """
    Generate a headline for an existing article body.

    Args:
        article_body: Complete article text

    Returns:
        Prompt for headline generation
    """
    prompt_parts = [
        "You are a sports journalist writing headlines for a 1960s-era baseball newspaper.",
        "",
        "ARTICLE TEXT:",
        article_body,
        "",
        "INSTRUCTIONS:",
        "- Write a compelling headline for this article",
        "- Use 1960s-era style (formal, no modern slang)",
        "- Length: 8-12 words",
        "- All caps format",
        "- Focus on the key story element",
        "",
        "OUTPUT FORMAT:",
        "HEADLINE: [Your headline here in ALL CAPS]",
        "",
        "Generate the headline now:"
    ]

    return "\n".join(prompt_parts)


def estimate_token_count(prompt: str) -> int:
    """
    Rough estimate of token count for a prompt.

    Uses simple heuristic: ~4 characters per token (conservative).

    Args:
        prompt: Prompt string

    Returns:
        Estimated token count
    """
    return len(prompt) // 4


def validate_prompt_length(prompt: str, max_tokens: int = 2000) -> bool:
    """
    Validate that prompt isn't too long for model context window.

    Args:
        prompt: Prompt string
        max_tokens: Maximum allowed tokens (default 2000 for safety)

    Returns:
        True if prompt is acceptable length
    """
    estimated = estimate_token_count(prompt)

    if estimated > max_tokens:
        logger.warning(f"Prompt may be too long: ~{estimated} tokens (max {max_tokens})")
        return False

    return True


def get_model_for_priority(priority: str) -> str:
    """
    Select appropriate Ollama model based on article priority.

    Args:
        priority: Priority tier ('MUST_GENERATE', 'SHOULD_GENERATE', 'COULD_GENERATE')

    Returns:
        Model name string
    """
    model_map = {
        'MUST_GENERATE': 'qwen2.5:14b',      # Best model for exceptional games
        'SHOULD_GENERATE': 'llama3.1:8b',    # Good model for solid performances
        'COULD_GENERATE': 'llama3.1:8b',     # Fast model for routine games
    }

    return model_map.get(priority, 'llama3.1:8b')  # Default to llama3.1:8b


def get_temperature_for_priority(priority: str) -> float:
    """
    Select appropriate temperature based on article priority.

    Higher priority = lower temperature (more conservative/accurate).

    Args:
        priority: Priority tier

    Returns:
        Temperature float (0.0-1.0)
    """
    temp_map = {
        'MUST_GENERATE': 0.6,      # More conservative for important games
        'SHOULD_GENERATE': 0.7,    # Balanced
        'COULD_GENERATE': 0.75,    # Slightly more creative for routine games
    }

    return temp_map.get(priority, 0.7)
