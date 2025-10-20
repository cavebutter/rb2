"""
Quick test script for game_log_parser.py

Tests parsing of game_logs.csv with sample data.
"""

import sys
import os
from pathlib import Path

# Add etl/src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from newspaper.game_log_parser import (
    extract_player_id_from_text,
    parse_inning_from_header,
    extract_exit_velocity,
    extract_hit_location,
    classify_outcome,
    load_game_log_for_game,
    extract_branch_plays_from_game_log
)
from loguru import logger


def test_helper_functions():
    """Test parsing helper functions."""
    logger.info("Testing helper functions...")

    # Test player ID extraction
    text = 'Batting: RHB <a href="../players/player_2496.html">Tim Korman</a>'
    player_id = extract_player_id_from_text(text)
    assert player_id == 2496, f"Expected 2496, got {player_id}"
    logger.info(f"✓ Player ID extraction: {player_id}")

    # Test inning parsing
    text = "Top of the 3rd - Boston Pilgrims batting"
    inning, half = parse_inning_from_header(text)
    assert inning == 3 and half == 'top', f"Expected (3, 'top'), got ({inning}, '{half}')"
    logger.info(f"✓ Inning parsing: {half} {inning}")

    # Test exit velocity
    text = "Ground out 6-3 (Groundball, 4MD, EV 97.5 MPH)"
    ev = extract_exit_velocity(text)
    assert ev == 97.5, f"Expected 97.5, got {ev}"
    logger.info(f"✓ Exit velocity: {ev}")

    # Test hit location
    text = "(Flyball, 89XD, EV 106.9 MPH)"
    loc = extract_hit_location(text)
    assert loc == "89XD", f"Expected '89XD', got '{loc}'"
    logger.info(f"✓ Hit location: {loc}")

    # Test outcome classification
    outcomes = [
        ('<b>HOME RUN</b>', 'home_run'),
        ('<b>TRIPLE</b>', 'triple'),
        ('Strikes out looking', 'strikeout'),
        ('Base on Balls', 'walk'),
        ('Ground out 4-3', 'ground_out'),
    ]

    for text, expected in outcomes:
        result = classify_outcome(text)
        assert result == expected, f"Expected '{expected}', got '{result}' for '{text}'"
        logger.info(f"✓ Outcome classification: '{text}' -> {result}")


def test_load_game_log():
    """Test loading game log for specific game."""
    logger.info("\nTesting load_game_log_for_game...")

    csv_path = '/mnt/hdd/PycharmProjects/rb2/etl/data/incoming/csv/game_logs.csv'

    # Load game 1
    entries = load_game_log_for_game(csv_path, 1)
    logger.info(f"✓ Loaded {len(entries)} entries for game_id=1")

    # Display first few entries
    for i, entry in enumerate(entries[:5]):
        logger.info(f"  Line {entry['line']}: Type {entry['type']} - {entry['text'][:60]}...")


def test_extract_branch_plays():
    """Test extracting Branch player plays from game log."""
    logger.info("\nTesting extract_branch_plays_from_game_log...")

    csv_path = '/mnt/hdd/PycharmProjects/rb2/etl/data/incoming/csv/game_logs.csv'

    # For testing, let's use some player IDs from the first game
    # We'll extract a few player IDs and pretend they're Branch players
    test_player_ids = [2496, 1860, 1863]  # From first game

    branch_plays = extract_branch_plays_from_game_log(csv_path, 1, test_player_ids)

    logger.info(f"✓ Extracted plays for {len(branch_plays)} players")

    for player_id, plays in branch_plays.items():
        logger.info(f"\n  Player {player_id}: {len(plays)} at-bats")
        for i, play in enumerate(plays[:2]):  # Show first 2 at-bats
            logger.info(f"    At-bat {i+1}: {play['inning_half']} {play['inning']}")
            logger.info(f"      Outcome: {play['outcome']}")
            if play.get('exit_velocity'):
                logger.info(f"      Exit velocity: {play['exit_velocity']} MPH")
            logger.info(f"      Pitches: {len(play['sequence'])}")


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("Game Log Parser Test Suite")
    logger.info("=" * 60)

    try:
        test_helper_functions()
        test_load_game_log()
        test_extract_branch_plays()

        logger.info("\n" + "=" * 60)
        logger.info("✓ All tests passed!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)