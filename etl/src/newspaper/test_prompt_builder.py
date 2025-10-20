"""
Test script for prompt_builder.py

Generates sample prompts using realistic game data.
"""

import sys
from pathlib import Path

# Add etl/src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from newspaper.prompt_builder import (
    build_article_prompt,
    build_multi_branch_prompt,
    build_regeneration_prompt,
    format_batting_line,
    format_pitching_line,
    estimate_token_count,
    get_era_from_date,
    get_era_style_guidelines
)
from datetime import date
from loguru import logger


def test_era_detection():
    """Test era detection and guidelines."""
    logger.info("Testing era detection...")

    test_dates = [
        (date(1925, 7, 4), "Roaring Twenties", 1920),
        (date(1942, 8, 15), "Golden Age", 1940),
        (date(1969, 6, 15), "Post-War Era", 1960),
        (date(1985, 9, 20), "Modern Era", 1980),
        (date(2003, 10, 25), "Contemporary Era", 2000),
        (date(2022, 4, 7), "Digital Era", 2020),
    ]

    for test_date, expected_era, expected_decade in test_dates:
        era_name, year = get_era_from_date(test_date)
        assert era_name == expected_era, f"Expected {expected_era}, got {era_name}"
        decade = (year // 10) * 10
        assert decade == expected_decade, f"Expected {expected_decade}s, got {decade}s"
        logger.info(f"✓ Era detection: {test_date.year} -> {era_name} ({decade}s)")

    # Test guidelines
    guidelines = get_era_style_guidelines("Roaring Twenties", 1925)
    assert any("flowery prose" in g for g in guidelines), "Expected 1920s style guidance"
    logger.info(f"✓ Era guidelines: 1920s has {len(guidelines)} guidelines")


def test_formatting_functions():
    """Test stat formatting functions."""
    logger.info("\nTesting formatting functions...")

    # Test batting line
    batting_stats = {
        'ab': 4,
        'h': 3,
        'hr': 2,
        'rbi': 5,
        'r': 2,
        'bb': 1,
        'k': 1
    }
    batting_line = format_batting_line(batting_stats)
    logger.info(f"✓ Batting line: {batting_line}")

    # Test pitching line
    pitching_stats = {
        'ip': 7.0,
        'h': 3,
        'er': 1,
        'bb': 2,
        'k': 9,
        'w': 1
    }
    pitching_line = format_pitching_line(pitching_stats)
    logger.info(f"✓ Pitching line: {pitching_line}")


def test_single_player_prompt():
    """Test single Branch player article prompt."""
    logger.info("\nTesting single player prompt...")

    # Sample game context
    game_context = {
        'game_id': 1,
        'date': date(1969, 6, 15),
        'home_team': {
            'name': 'Boston Pilgrims',
            'abbr': 'BOS',
            'nickname': 'Pilgrims'
        },
        'away_team': {
            'name': 'Cleveland Roosters',
            'abbr': 'CLE',
            'nickname': 'Roosters'
        },
        'score': {
            'home': 5,
            'away': 3
        },
        'attendance': 24567
    }

    # Sample player details
    player_details = {
        'player_id': 1001,
        'full_name': 'Donovan Branch',
        'team': {
            'name': 'Boston Pilgrims',
            'abbr': 'BOS'
        },
        'game_stats': {
            'batting': {
                'ab': 4,
                'h': 3,
                'hr': 2,
                'rbi': 5,
                'r': 2,
                'bb': 0,
                'k': 1
            }
        }
    }

    # Sample at-bats
    at_bats = [
        {
            'inning': 1,
            'inning_half': 'bottom',
            'outcome': 'single',
            'exit_velocity': 95.3
        },
        {
            'inning': 4,
            'inning_half': 'bottom',
            'outcome': 'home_run',
            'exit_velocity': 108.7
        },
        {
            'inning': 7,
            'inning_half': 'bottom',
            'outcome': 'home_run',
            'exit_velocity': 106.2
        }
    ]

    prompt = build_article_prompt(game_context, player_details, at_bats)

    logger.info(f"✓ Generated single player prompt")
    logger.info(f"  Length: {len(prompt)} characters")
    logger.info(f"  Estimated tokens: {estimate_token_count(prompt)}")

    return prompt


def test_multi_player_prompt():
    """Test multi-Branch family article prompt."""
    logger.info("\nTesting multi-player prompt...")

    # Sample game context
    game_context = {
        'game_id': 2,
        'date': date(1969, 7, 4),
        'home_team': {
            'name': 'New York Monarchs',
            'abbr': 'NYM'
        },
        'away_team': {
            'name': 'Chicago Railyards',
            'abbr': 'CHI'
        },
        'score': {
            'home': 4,
            'away': 3
        },
        'attendance': 35821
    }

    # Sample players
    branch_players = [
        {
            'player_id': 1001,
            'full_name': 'Donovan Branch',
            'team': {'name': 'New York Monarchs'},
            'game_stats': {
                'batting': {
                    'ab': 5,
                    'h': 2,
                    'hr': 1,
                    'rbi': 2,
                    'r': 1,
                    'bb': 0,
                    'k': 2
                }
            }
        },
        {
            'player_id': 1002,
            'full_name': 'Randall Branch',
            'team': {'name': 'Chicago Railyards'},
            'game_stats': {
                'pitching': {
                    'ip': 6.0,
                    'h': 7,
                    'er': 4,
                    'bb': 2,
                    'k': 5,
                    'l': 1
                }
            }
        }
    ]

    prompt = build_multi_branch_prompt(game_context, branch_players)

    logger.info(f"✓ Generated multi-player prompt")
    logger.info(f"  Length: {len(prompt)} characters")
    logger.info(f"  Estimated tokens: {estimate_token_count(prompt)}")

    return prompt


def test_regeneration_prompt():
    """Test article regeneration prompt."""
    logger.info("\nTesting regeneration prompt...")

    original_article = {
        'headline': 'BRANCH SMASHES TWO HOMERS IN PILGRIMS VICTORY',
        'body': '''Donovan Branch powered the Boston Pilgrims to a 5-3 victory over the Cleveland Roosters on Sunday with a spectacular two-home run performance at Fenway Park.

Branch went 3-for-4 with five runs batted in, connecting for solo shots in both the fourth and seventh innings. His offensive explosion helped the Pilgrims overcome an early deficit.

The 24-year-old outfielder's performance was particularly timely as Boston seeks to climb back into contention in the American League standings.'''
    }

    feedback = "Great article! Could you emphasize the attendance figure and add more detail about the game situation when Branch hit his home runs?"

    prompt = build_regeneration_prompt(original_article, feedback)

    logger.info(f"✓ Generated regeneration prompt")
    logger.info(f"  Length: {len(prompt)} characters")

    return prompt


def save_sample_prompts(prompts: dict):
    """Save sample prompts to documentation file."""
    output_path = Path(__file__).parent.parent.parent.parent / 'docs' / 'newspaper' / 'sample-prompts.md'

    content_parts = [
        "# Sample Prompts for Article Generation",
        "",
        "Auto-generated sample prompts demonstrating the prompt builder module.",
        "",
        "## Single Player Performance Article",
        "",
        "```",
        prompts['single_player'],
        "```",
        "",
        "---",
        "",
        "## Multi-Branch Family Article",
        "",
        "```",
        prompts['multi_player'],
        "```",
        "",
        "---",
        "",
        "## Article Regeneration with Feedback",
        "",
        "```",
        prompts['regeneration'],
        "```",
    ]

    content = "\n".join(content_parts)

    with open(output_path, 'w') as f:
        f.write(content)

    logger.info(f"\n✓ Saved sample prompts to {output_path}")


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("Prompt Builder Test Suite")
    logger.info("=" * 60)

    try:
        test_era_detection()
        test_formatting_functions()

        prompts = {
            'single_player': test_single_player_prompt(),
            'multi_player': test_multi_player_prompt(),
            'regeneration': test_regeneration_prompt()
        }

        save_sample_prompts(prompts)

        logger.info("\n" + "=" * 60)
        logger.info("✓ All tests passed!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)