"""
End-to-End Test: Game Log Extraction → Prompt Building → Article Generation

Tests the complete pipeline from parsing game logs to generating finished articles.
"""

import sys
from pathlib import Path
from datetime import date

# Add etl to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.newspaper.prompt_builder import build_article_prompt, build_multi_branch_prompt
from src.newspaper.ollama_client import OllamaClient
from src.newspaper.article_processor import create_processor
from config.etl_config import OLLAMA_CONFIG, DB_CONFIG
from loguru import logger


def test_single_player_pipeline():
    """
    Test complete pipeline for a single Branch player performance.

    Flow:
    1. Parse game log data
    2. Build prompt with game context and play-by-play
    3. Generate article with Ollama
    4. Display results
    """
    logger.info("=" * 80)
    logger.info("END-TO-END TEST: Single Player Article Generation")
    logger.info("=" * 80)

    # Step 1: Set up game context (normally from database)
    logger.info("\n[Step 1] Setting up game context...")

    game_context = {
        'game_id': 12345,
        'date': date(1969, 6, 15),
        'home_team': {
            'team_id': 100,
            'name': 'Boston Pilgrims',
            'abbr': 'BOS',
            'nickname': 'Pilgrims'
        },
        'away_team': {
            'team_id': 101,
            'name': 'Cleveland Roosters',
            'abbr': 'CLE',
            'nickname': 'Roosters'
        },
        'score': {
            'home': 5,
            'away': 3
        },
        'attendance': 24567,
        'park': 'Fenway Park'
    }

    player_details = {
        'player_id': 1001,
        'full_name': 'Donovan Branch',
        'first_name': 'Donovan',
        'last_name': 'Branch',
        'position': 'CF',
        'team': {
            'team_id': 100,
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
                'k': 1,
                'd': 0,
                't': 0
            }
        }
    }

    logger.info(f"  Game: {game_context['away_team']['name']} @ {game_context['home_team']['name']}")
    logger.info(f"  Date: {game_context['date']}")
    logger.info(f"  Featured Player: {player_details['full_name']}")
    logger.info(f"  Performance: {player_details['game_stats']['batting']['h']}-for-{player_details['game_stats']['batting']['ab']}, {player_details['game_stats']['batting']['hr']} HR, {player_details['game_stats']['batting']['rbi']} RBI")

    # Step 2: Simulate play-by-play extraction (normally from game_log_parser)
    logger.info("\n[Step 2] Simulating play-by-play extraction...")

    # Simulated at-bats (this would come from game_log_parser.py)
    branch_at_bats = [
        {
            'inning': 1,
            'inning_half': 'bottom',
            'outcome': 'single',
            'exit_velocity': 95.3,
            'hit_location': 'left field',
            'sequence': ['Ball', 'Strike', 'Foul', 'In play']
        },
        {
            'inning': 4,
            'inning_half': 'bottom',
            'outcome': 'home_run',
            'exit_velocity': 108.7,
            'hit_location': 'left-center',
            'sequence': ['Strike', 'Ball', 'Ball', 'In play']
        },
        {
            'inning': 7,
            'inning_half': 'bottom',
            'outcome': 'home_run',
            'exit_velocity': 106.2,
            'hit_location': 'right field',
            'sequence': ['Ball', 'Strike', 'In play']
        },
        {
            'inning': 9,
            'inning_half': 'bottom',
            'outcome': 'strikeout',
            'exit_velocity': None,
            'hit_location': None,
            'sequence': ['Strike', 'Foul', 'Foul', 'Strike']
        }
    ]

    logger.info(f"  Extracted {len(branch_at_bats)} at-bats from game log")
    for i, ab in enumerate(branch_at_bats, 1):
        logger.info(f"    {i}. Inning {ab['inning']}: {ab['outcome']}")

    # Step 3: Build prompt
    logger.info("\n[Step 3] Building article prompt...")

    prompt = build_article_prompt(
        game_context=game_context,
        player_details=player_details,
        branch_at_bats=branch_at_bats
    )

    logger.info(f"  Prompt length: {len(prompt)} characters")
    logger.info(f"  Estimated tokens: ~{len(prompt) // 4}")

    # Show a snippet of the prompt
    logger.info("\n  Prompt preview (first 500 chars):")
    logger.info("  " + "-" * 76)
    for line in prompt[:500].split('\n'):
        logger.info(f"  {line}")
    logger.info("  " + "-" * 76)
    logger.info("  [...rest of prompt...]")

    # Step 4: Generate article with Ollama
    logger.info("\n[Step 4] Generating article with Ollama...")

    client = OllamaClient(
        base_url=OLLAMA_CONFIG['base_url'],
        default_model='qwen2.5:7b',
        timeout=OLLAMA_CONFIG['timeout']
    )

    # Check if Ollama is available
    if not client.health_check():
        logger.error("\n✗ Ollama service is not available!")
        logger.error("  Please start Ollama: ollama serve")
        return False

    # Check if model is available - try qwen2.5:14b first, then llama3.1:8b
    available_models = client.list_available_models()

    if 'qwen2.5:14b' in available_models:
        model = 'qwen2.5:14b'
    elif 'llama3.1:8b' in available_models:
        model = 'llama3.1:8b'
    elif available_models:
        model = available_models[0]
        logger.info(f"  Using first available model: {model}")
    else:
        logger.error("\n✗ No models available!")
        logger.error("  Please pull a model: ollama pull qwen2.5:14b")
        return False

    logger.info(f"  Using model: {model}")
    logger.info("  Generating (this may take 10-30 seconds)...")

    try:
        article_text, metadata = client.generate_with_retry(
            prompt=prompt,
            model=model,
            temperature=0.7,
            max_tokens=400,
            max_retries=3
        )

        # Step 5: Display results
        logger.info("\n[Step 5] Article Generation Complete!")
        logger.info(f"  Generation time: {metadata['total_time']:.2f}s")
        logger.info(f"  Attempts: {metadata['attempts']}")
        logger.info(f"  Article length: {len(article_text)} characters")

        logger.info("\n" + "=" * 80)
        logger.info("GENERATED ARTICLE")
        logger.info("=" * 80)
        print(article_text)  # Use print for clean output
        logger.info("=" * 80)

        # Step 6: Process and save article to database
        logger.info("\n[Step 6] Processing and saving article to database...")

        processor = create_processor(DB_CONFIG['dev'])

        try:
            article_id, result = processor.process_and_save(
                raw_article_text=article_text,
                game_context=game_context,
                generation_metadata=metadata,
                newsworthiness_score=85,  # High score for this exceptional performance
                category_name='Game Recap',
                player_ids=[player_details['player_id']],
                team_ids=[
                    game_context['home_team']['team_id'],
                    game_context['away_team']['team_id']
                ]
            )

            if result['success']:
                logger.info(f"  ✓ Article saved to database: article_id={article_id}")
                logger.info(f"  ✓ Headline: {result['headline']}")
                logger.info(f"  ✓ Word count: {result['word_count']} words")

                # Retrieve and display saved article
                saved_article = processor.get_article(article_id)
                if saved_article:
                    logger.info(f"  ✓ Article retrieved from database")
                    logger.info(f"     - Slug: {saved_article['slug']}")
                    logger.info(f"     - Status: {saved_article['status']}")
                    logger.info(f"     - Model: {saved_article['model_used']}")
                    logger.info(f"     - Newsworthiness: {saved_article['newsworthiness_score']}")

                # Clean up test article
                cursor = processor.conn.cursor()
                cursor.execute("DELETE FROM newspaper_articles WHERE article_id = %s", (article_id,))
                processor.conn.commit()
                cursor.close()
                logger.info(f"  ✓ Test article cleaned up")

                processor.close()
                logger.info("\n✓ END-TO-END TEST COMPLETED SUCCESSFULLY!")
                return True
            else:
                logger.error(f"  ✗ Article processing failed: {result.get('error')}")
                processor.close()
                return False

        except Exception as e:
            logger.error(f"  ✗ Database save failed: {e}")
            import traceback
            traceback.print_exc()
            processor.close()
            return False

    except Exception as e:
        logger.error(f"\n✗ Article generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multi_player_pipeline():
    """
    Test pipeline for multi-Branch family game.
    """
    logger.info("\n\n" + "=" * 80)
    logger.info("END-TO-END TEST: Multi-Player Article Generation")
    logger.info("=" * 80)

    # Step 1: Game context
    logger.info("\n[Step 1] Setting up game context (Branch family matchup)...")

    game_context = {
        'game_id': 12346,
        'date': date(1969, 7, 4),
        'home_team': {
            'team_id': 102,
            'name': 'New York Monarchs',
            'abbr': 'NYM',
        },
        'away_team': {
            'team_id': 103,
            'name': 'Chicago Railyards',
            'abbr': 'CHI',
        },
        'score': {
            'home': 4,
            'away': 3
        },
        'attendance': 35821
    }

    branch_players = [
        {
            'player_id': 1001,
            'full_name': 'Donovan Branch',
            'team': {'name': 'New York Monarchs', 'abbr': 'NYM'},
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
            'team': {'name': 'Chicago Railyards', 'abbr': 'CHI'},
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

    logger.info(f"  Game: {game_context['away_team']['name']} @ {game_context['home_team']['name']}")
    logger.info(f"  Branch Family Members: {len(branch_players)}")
    for player in branch_players:
        logger.info(f"    - {player['full_name']} ({player['team']['name']})")

    # Step 2: Build multi-player prompt
    logger.info("\n[Step 2] Building multi-player article prompt...")

    prompt = build_multi_branch_prompt(
        game_context=game_context,
        branch_players=branch_players
    )

    logger.info(f"  Prompt length: {len(prompt)} characters")

    # Step 3: Generate article
    logger.info("\n[Step 3] Generating article with Ollama...")

    client = OllamaClient(
        base_url=OLLAMA_CONFIG['base_url'],
        default_model='qwen2.5:7b',
        timeout=OLLAMA_CONFIG['timeout']
    )

    if not client.health_check():
        logger.error("✗ Ollama service not available")
        return False

    # Use same model selection logic
    available_models = client.list_available_models()

    if 'qwen2.5:14b' in available_models:
        model = 'qwen2.5:14b'
    elif 'llama3.1:8b' in available_models:
        model = 'llama3.1:8b'
    elif available_models:
        model = available_models[0]
    else:
        logger.error("✗ No models available!")
        return False

    logger.info("  Generating...")

    try:
        article_text, metadata = client.generate_with_retry(
            prompt=prompt,
            model=model,
            temperature=0.7,
            max_tokens=450,  # Slightly more for multi-player
            max_retries=3
        )

        logger.info(f"\n  ✓ Generation complete: {metadata['total_time']:.2f}s")

        logger.info("\n" + "=" * 80)
        logger.info("GENERATED ARTICLE (MULTI-BRANCH FAMILY)")
        logger.info("=" * 80)
        print(article_text)
        logger.info("=" * 80)

        logger.info("\n✓ MULTI-PLAYER TEST COMPLETED SUCCESSFULLY!")
        return True

    except Exception as e:
        logger.error(f"\n✗ Generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run end-to-end tests."""
    logger.info("Starting End-to-End Pipeline Tests\n")

    results = []

    # Test 1: Single player
    try:
        success = test_single_player_pipeline()
        results.append(("Single Player Pipeline", success))
    except Exception as e:
        logger.error(f"Single player test crashed: {e}")
        results.append(("Single Player Pipeline", False))

    # Test 2: Multi-player
    try:
        success = test_multi_player_pipeline()
        results.append(("Multi-Player Pipeline", success))
    except Exception as e:
        logger.error(f"Multi-player test crashed: {e}")
        results.append(("Multi-Player Pipeline", False))

    # Summary
    logger.info("\n\n" + "=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)

    for test_name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        logger.info(f"  {status}: {test_name}")

    passed = sum(1 for _, success in results if success)
    total = len(results)

    if passed == total:
        logger.info(f"\n✓ All {total} tests passed!")
        return 0
    else:
        logger.error(f"\n✗ {total - passed} test(s) failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
