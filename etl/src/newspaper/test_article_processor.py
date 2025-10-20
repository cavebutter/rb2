"""
Test script for article_processor.py

Tests article parsing, validation, and database storage.
"""

import sys
from pathlib import Path
from datetime import date

# Add etl to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.newspaper.article_processor import ArticleProcessor, create_processor
from config.etl_config import DB_CONFIG
from loguru import logger


def test_parse_article():
    """Test article parsing from LLM output."""
    logger.info("=" * 60)
    logger.info("Test 1: Article Parsing")
    logger.info("=" * 60)

    processor = create_processor(DB_CONFIG['dev'])

    # Test case 1: Standard format with HEADLINE: marker
    test_article_1 = """HEADLINE: BRANCH POWERS PILGRIMS TO VICTORY

In a thrilling contest at Fenway Park, Donovan Branch led the Boston Pilgrims to a 5-3 victory over the Cleveland Roosters yesterday afternoon.

Branch went 3-for-4 with two home runs and five runs batted in, powering the Pilgrims' offense throughout the game."""

    headline, body = processor.parse_article(test_article_1)

    if headline and body:
        logger.info("✓ Test case 1 passed (standard format)")
        logger.info(f"  Headline: {headline}")
        logger.info(f"  Body length: {len(body)} characters")
    else:
        logger.error("✗ Test case 1 failed")
        return False

    # Test case 2: No HEADLINE: marker (fallback mode)
    test_article_2 = """Branch Powers Pilgrims to Victory

Donovan Branch went 3-for-4 with two home runs yesterday."""

    headline, body = processor.parse_article(test_article_2)

    if headline and body:
        logger.info("✓ Test case 2 passed (no marker, fallback mode)")
        logger.info(f"  Headline: {headline}")
    else:
        logger.error("✗ Test case 2 failed")
        return False

    # Test case 3: Empty input
    headline, body = processor.parse_article("")

    if headline is None and body is None:
        logger.info("✓ Test case 3 passed (empty input handled)")
    else:
        logger.error("✗ Test case 3 failed")
        return False

    processor.close()
    return True


def test_validate_article():
    """Test article validation."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 2: Article Validation")
    logger.info("=" * 60)

    processor = create_processor(DB_CONFIG['dev'])

    # Test case 1: Valid article
    valid_headline = "Branch Powers Pilgrims to Victory"
    valid_body = "Donovan Branch went 3-for-4 with two home runs and five RBI in the Pilgrims' 5-3 victory over the Cleveland Roosters yesterday afternoon at Fenway Park. Branch's first home run came in the fourth inning with a man on base, giving Boston a 3-1 lead. His second blast in the seventh inning sealed the victory for the Pilgrims."

    is_valid, errors = processor.validate_article(valid_headline, valid_body)

    if is_valid:
        logger.info("✓ Test case 1 passed (valid article)")
    else:
        logger.error(f"✗ Test case 1 failed: {errors}")
        processor.close()
        return False

    # Test case 2: Headline too short
    is_valid, errors = processor.validate_article("Short", valid_body)

    if not is_valid and "Headline too short" in str(errors):
        logger.info("✓ Test case 2 passed (headline too short detected)")
    else:
        logger.error("✗ Test case 2 failed")
        processor.close()
        return False

    # Test case 3: Body too short
    is_valid, errors = processor.validate_article(valid_headline, "Too short")

    if not is_valid and "too short" in str(errors):
        logger.info("✓ Test case 3 passed (body too short detected)")
    else:
        logger.error("✗ Test case 3 failed")
        processor.close()
        return False

    # Test case 4: Placeholder text detection
    placeholder_body = valid_body + " [INSERT PLAYER NAME HERE]"
    is_valid, errors = processor.validate_article(valid_headline, placeholder_body)

    if not is_valid and "placeholder" in str(errors):
        logger.info("✓ Test case 4 passed (placeholder text detected)")
    else:
        logger.error("✗ Test case 4 failed")
        processor.close()
        return False

    processor.close()
    return True


def test_slug_generation():
    """Test slug generation."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 3: Slug Generation")
    logger.info("=" * 60)

    processor = create_processor(DB_CONFIG['dev'])

    # Test case 1: Basic slug
    headline = "Branch Powers Pilgrims to Victory"
    slug = processor.generate_slug(headline)

    if slug == "branch-powers-pilgrims-to-victory":
        logger.info(f"✓ Test case 1 passed: {slug}")
    else:
        logger.error(f"✗ Test case 1 failed: {slug}")
        processor.close()
        return False

    # Test case 2: Slug with date
    game_date = date(1969, 6, 15)
    slug = processor.generate_slug(headline, game_date)

    if slug.startswith("19690615-"):
        logger.info(f"✓ Test case 2 passed (with date): {slug}")
    else:
        logger.error(f"✗ Test case 2 failed: {slug}")
        processor.close()
        return False

    # Test case 3: Special characters handling
    headline_special = "Branch's 2 HR Power Pilgrims: 5-3 Victory!"
    slug = processor.generate_slug(headline_special)

    if "branch-s-2-hr-power-pilgrims" in slug:
        logger.info(f"✓ Test case 3 passed (special chars): {slug}")
    else:
        logger.error(f"✗ Test case 3 failed: {slug}")
        processor.close()
        return False

    processor.close()
    return True


def test_save_and_retrieve():
    """Test saving and retrieving an article."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 4: Save and Retrieve Article")
    logger.info("=" * 60)

    processor = create_processor(DB_CONFIG['dev'])

    # Create test article
    headline = "Test Article - Branch Powers Victory"
    body = "This is a test article body. " * 20  # Make it long enough

    game_context = {
        'game_id': 99999,  # Test game ID
        'date': date(1969, 6, 15)
    }

    generation_metadata = {
        'model_used': 'qwen2.5:14b',
        'attempts': 1,
        'total_time': 5.2
    }

    try:
        # Save article
        article_id = processor.save_article(
            headline=headline,
            body=body,
            game_context=game_context,
            generation_metadata=generation_metadata,
            newsworthiness_score=75,
            player_ids=[1001],  # Test player ID
            team_ids=[100, 101]  # Test team IDs
        )

        logger.info(f"✓ Article saved with ID: {article_id}")

        # Retrieve article
        article = processor.get_article(article_id)

        if article:
            logger.info("✓ Article retrieved successfully")
            logger.info(f"  Title: {article['title']}")
            logger.info(f"  Slug: {article['slug']}")
            logger.info(f"  Status: {article['status']}")
            logger.info(f"  Model: {article['model_used']}")
            logger.info(f"  Newsworthiness: {article['newsworthiness_score']}")

            # Clean up test article
            cursor = processor.conn.cursor()
            cursor.execute("DELETE FROM newspaper_articles WHERE article_id = %s", (article_id,))
            processor.conn.commit()
            cursor.close()
            logger.info(f"✓ Test article {article_id} cleaned up")

            processor.close()
            return True
        else:
            logger.error("✗ Failed to retrieve article")
            processor.close()
            return False

    except Exception as e:
        logger.error(f"✗ Save/retrieve test failed: {e}")
        import traceback
        traceback.print_exc()
        processor.close()
        return False


def test_regeneration():
    """Test article regeneration."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 5: Article Regeneration")
    logger.info("=" * 60)

    processor = create_processor(DB_CONFIG['dev'])

    # Create original article
    original_headline = "Original Headline - Test Regen"
    original_body = "This is the original article body. " * 20

    game_context = {
        'game_id': 99999,
        'date': date(1969, 6, 15)
    }

    generation_metadata = {
        'model_used': 'qwen2.5:14b',
        'attempts': 1,
        'total_time': 5.2
    }

    try:
        # Save original
        original_id = processor.save_article(
            headline=original_headline,
            body=original_body,
            game_context=game_context,
            generation_metadata=generation_metadata,
            player_ids=[1001]
        )

        logger.info(f"✓ Original article saved: {original_id}")

        # Regenerate with new content
        new_headline = "Regenerated Headline - Test Regen"
        new_body = "This is the regenerated article body. " * 20

        new_metadata = {
            'model_used': 'qwen2.5:7b',
            'attempts': 2,
            'total_time': 4.8
        }

        new_id = processor.regenerate_article(
            original_article_id=original_id,
            new_headline=new_headline,
            new_body=new_body,
            generation_metadata=new_metadata
        )

        logger.info(f"✓ Article regenerated: {new_id}")

        # Verify regenerated article
        new_article = processor.get_article(new_id)

        if new_article:
            if new_article['previous_version_id'] == original_id:
                logger.info("✓ Regeneration link verified")
            else:
                logger.error("✗ Regeneration link incorrect")
                processor.close()
                return False

            if new_article['generation_count'] == 2:
                logger.info("✓ Generation count incremented")
            else:
                logger.error(f"✗ Generation count incorrect: {new_article['generation_count']}")
                processor.close()
                return False

            # Clean up
            cursor = processor.conn.cursor()
            cursor.execute("DELETE FROM newspaper_articles WHERE article_id IN (%s, %s)", (original_id, new_id))
            processor.conn.commit()
            cursor.close()
            logger.info("✓ Test articles cleaned up")

            processor.close()
            return True
        else:
            logger.error("✗ Failed to retrieve regenerated article")
            processor.close()
            return False

    except Exception as e:
        logger.error(f"✗ Regeneration test failed: {e}")
        import traceback
        traceback.print_exc()
        processor.close()
        return False


def test_process_and_save():
    """Test complete workflow: parse, validate, and save."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 6: Complete Workflow (process_and_save)")
    logger.info("=" * 60)

    processor = create_processor(DB_CONFIG['dev'])

    # Raw article from LLM
    raw_article = """HEADLINE: BRANCH POWERS PILGRIMS TO VICTORY IN FENWAY THRILLER

In a dramatic contest at Fenway Park yesterday afternoon, Donovan Branch led the Boston Pilgrims to a hard-fought 5-3 victory over the Cleveland Roosters. Branch went 3-for-4 at the plate with two home runs and five runs batted in, powering the Pilgrims' offense throughout the game.

Branch's first home run came in the fourth inning with a runner on base, giving Boston a 3-1 lead. His second blast in the seventh inning sealed the victory for the home team. The Pilgrims improved their record with the win."""

    game_context = {
        'game_id': 99999,
        'date': date(1969, 6, 15)
    }

    generation_metadata = {
        'model_used': 'qwen2.5:14b',
        'attempts': 1,
        'total_time': 6.2
    }

    try:
        article_id, result = processor.process_and_save(
            raw_article_text=raw_article,
            game_context=game_context,
            generation_metadata=generation_metadata,
            newsworthiness_score=85,
            player_ids=[1001],
            team_ids=[100, 101]
        )

        if result['success']:
            logger.info(f"✓ Article processed and saved: {article_id}")
            logger.info(f"  Headline: {result['headline']}")
            logger.info(f"  Word count: {result['word_count']}")

            # Clean up
            cursor = processor.conn.cursor()
            cursor.execute("DELETE FROM newspaper_articles WHERE article_id = %s", (article_id,))
            processor.conn.commit()
            cursor.close()
            logger.info("✓ Test article cleaned up")

            processor.close()
            return True
        else:
            logger.error(f"✗ Process and save failed: {result.get('error')}")
            processor.close()
            return False

    except Exception as e:
        logger.error(f"✗ Process and save test failed: {e}")
        import traceback
        traceback.print_exc()
        processor.close()
        return False


def main():
    """Run all tests."""
    logger.info("\n" + "=" * 80)
    logger.info("ARTICLE PROCESSOR TEST SUITE")
    logger.info("=" * 80)

    tests = [
        ("Article Parsing", test_parse_article),
        ("Article Validation", test_validate_article),
        ("Slug Generation", test_slug_generation),
        ("Save and Retrieve", test_save_and_retrieve),
        ("Article Regeneration", test_regeneration),
        ("Complete Workflow", test_process_and_save),
    ]

    results = []

    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            logger.error(f"\n✗ {test_name} crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for test_name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        logger.info(f"  {status}: {test_name}")

    logger.info(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        logger.info("\n✓ All tests passed!")
        return 0
    else:
        logger.error(f"\n✗ {total - passed} test(s) failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
