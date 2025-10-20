"""
Test script for ollama_client.py

Tests Ollama client functionality including:
- Service health check
- Model availability
- Article generation
- Retry logic
- Benchmarking
"""

import sys
from pathlib import Path

# Add etl to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.newspaper.ollama_client import OllamaClient, get_fallback_model
from src.newspaper.prompt_builder import build_article_prompt
from config.etl_config import OLLAMA_CONFIG
from datetime import date
from loguru import logger


def test_service_health():
    """Test if Ollama service is running."""
    logger.info("=" * 60)
    logger.info("Test 1: Ollama Service Health Check")
    logger.info("=" * 60)

    client = OllamaClient(
        base_url=OLLAMA_CONFIG['base_url'],
        timeout=OLLAMA_CONFIG['timeout']
    )

    is_healthy = client.health_check()

    if is_healthy:
        logger.info("✓ Ollama service is running and accessible")
    else:
        logger.error("✗ Ollama service is not accessible")
        logger.error("  Make sure Ollama is running: ollama serve")
        return False

    return True


def test_model_availability():
    """Test model availability checking."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 2: Model Availability")
    logger.info("=" * 60)

    client = OllamaClient(base_url=OLLAMA_CONFIG['base_url'])

    # List all available models
    available_models = client.list_available_models()
    logger.info(f"\nAvailable models ({len(available_models)}):")
    for model in available_models:
        logger.info(f"  - {model}")

    # Check specific models
    test_models = [
        'qwen2.5:14b',
        'qwen2.5:7b',
        'qwen2.5:3b',
        'llama3.1:8b',
    ]

    logger.info("\nChecking configured models:")
    for model in test_models:
        is_available = client.check_model_availability(model)
        status = "✓" if is_available else "✗"
        logger.info(f"  {status} {model}")

    return True


def test_simple_generation():
    """Test basic article generation."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 3: Simple Article Generation")
    logger.info("=" * 60)

    client = OllamaClient(
        base_url=OLLAMA_CONFIG['base_url'],
        default_model='qwen2.5:7b',
        timeout=OLLAMA_CONFIG['timeout']
    )

    # Simple test prompt
    test_prompt = """You are a sports journalist writing for a 1960s-era baseball newspaper.

GAME CONTEXT:
Date: June 15, 1969
Teams: Cleveland Roosters at Boston Pilgrims
Final Score: Cleveland Roosters 3, Boston Pilgrims 5

FEATURED PLAYER: Donovan Branch (Boston Pilgrims)
BATTING PERFORMANCE: 3-for-4 with 2 home runs and 5 RBI

WRITING INSTRUCTIONS:
- Write a brief 150-word newspaper article about this performance
- Use 1960s-era journalistic style
- Be factual and concise

OUTPUT FORMAT:
HEADLINE: [Write headline in ALL CAPS]

[Article body text]

Generate the article now:"""

    logger.info("\nGenerating test article...")

    try:
        article = client.generate_article(
            prompt=test_prompt,
            model='qwen2.5:7b',
            temperature=0.7,
            max_tokens=300
        )

        logger.info("\n" + "-" * 60)
        logger.info("Generated Article:")
        logger.info("-" * 60)
        logger.info(article)
        logger.info("-" * 60)

        logger.info(f"\n✓ Article generated successfully ({len(article)} characters)")
        return True

    except Exception as e:
        logger.error(f"\n✗ Generation failed: {e}")
        return False


def test_retry_logic():
    """Test retry logic with a potentially flaky connection."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 4: Retry Logic")
    logger.info("=" * 60)

    client = OllamaClient(
        base_url=OLLAMA_CONFIG['base_url'],
        default_model='qwen2.5:7b'
    )

    # Use a simple prompt
    test_prompt = "Write a single sentence about baseball."

    logger.info("\nTesting retry logic (should succeed on first attempt if service is healthy)...")

    try:
        article, metadata = client.generate_with_retry(
            prompt=test_prompt,
            model='qwen2.5:7b',
            temperature=0.7,
            max_tokens=50,
            max_retries=3
        )

        logger.info(f"\n✓ Generation succeeded:")
        logger.info(f"  Attempts: {metadata['attempts']}")
        logger.info(f"  Total time: {metadata['total_time']:.2f}s")
        logger.info(f"  Model used: {metadata['model_used']}")
        logger.info(f"  Output: {article[:100]}...")

        return True

    except Exception as e:
        logger.error(f"\n✗ Retry test failed: {e}")
        return False


def test_with_sample_prompt():
    """Test with a real prompt from prompt_builder."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 5: Full Prompt Integration")
    logger.info("=" * 60)

    # Build a sample prompt using prompt_builder
    game_context = {
        'game_id': 1,
        'date': date(1969, 6, 15),
        'home_team': {
            'name': 'Boston Pilgrims',
            'abbr': 'BOS',
        },
        'away_team': {
            'name': 'Cleveland Roosters',
            'abbr': 'CLE',
        },
        'score': {
            'home': 5,
            'away': 3
        },
        'attendance': 24567
    }

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

    prompt = build_article_prompt(game_context, player_details)

    logger.info(f"\nPrompt length: {len(prompt)} characters")
    logger.info("Generating article with full prompt...")

    client = OllamaClient(
        base_url=OLLAMA_CONFIG['base_url'],
        default_model='qwen2.5:7b',
        timeout=OLLAMA_CONFIG['timeout']
    )

    try:
        article, metadata = client.generate_with_retry(
            prompt=prompt,
            model='qwen2.5:7b',
            temperature=0.7,
            max_tokens=400,
            max_retries=3
        )

        logger.info("\n" + "=" * 60)
        logger.info("GENERATED ARTICLE")
        logger.info("=" * 60)
        logger.info(article)
        logger.info("=" * 60)

        logger.info(f"\n✓ Full integration test successful")
        logger.info(f"  Generation time: {metadata['total_time']:.2f}s")
        logger.info(f"  Article length: {len(article)} characters")

        return True

    except Exception as e:
        logger.error(f"\n✗ Full integration test failed: {e}")
        return False


def test_fallback_models():
    """Test fallback model selection."""
    logger.info("\n" + "=" * 60)
    logger.info("Test 6: Fallback Model Selection")
    logger.info("=" * 60)

    client = OllamaClient(base_url=OLLAMA_CONFIG['base_url'])

    test_cases = [
        'qwen2.5:14b',
        'qwen2.5:7b',
        'llama3.1:70b',
        'nonexistent:model',
    ]

    for preferred in test_cases:
        fallback = get_fallback_model(preferred, client)
        if fallback == preferred:
            logger.info(f"  {preferred} -> Using preferred model ✓")
        else:
            logger.info(f"  {preferred} -> Fallback to {fallback}")

    return True


def test_benchmark(model_name: str = 'qwen2.5:7b', iterations: int = 2):
    """Run a quick benchmark test."""
    logger.info("\n" + "=" * 60)
    logger.info(f"Test 7: Benchmark {model_name}")
    logger.info("=" * 60)

    client = OllamaClient(base_url=OLLAMA_CONFIG['base_url'])

    if not client.check_model_availability(model_name):
        logger.warning(f"Model {model_name} not available, skipping benchmark")
        return True

    test_prompt = "Write a 100-word newspaper article about a baseball player hitting a home run."

    logger.info(f"\nBenchmarking {model_name} with {iterations} iterations...")

    try:
        results = client.benchmark_model(model_name, test_prompt, iterations=iterations)

        if 'error' in results:
            logger.error(f"✗ Benchmark failed: {results['error']}")
            return False

        logger.info(f"\n✓ Benchmark Results:")
        logger.info(f"  Model: {results['model']}")
        logger.info(f"  Iterations: {results['iterations']}")
        logger.info(f"  Average time: {results['avg_time']:.2f}s")
        logger.info(f"  Min/Max time: {results['min_time']:.2f}s / {results['max_time']:.2f}s")
        logger.info(f"  Average output length: {results['avg_length']:.0f} characters")

        if results.get('sample_output'):
            logger.info(f"\n  Sample output preview:")
            logger.info(f"  {results['sample_output'][:200]}...")

        return True

    except Exception as e:
        logger.error(f"\n✗ Benchmark failed: {e}")
        return False


def main():
    """Run all tests."""
    logger.info("\n" + "=" * 80)
    logger.info("OLLAMA CLIENT TEST SUITE")
    logger.info("=" * 80)

    tests = [
        ("Service Health Check", test_service_health, []),
        ("Model Availability", test_model_availability, []),
        ("Simple Generation", test_simple_generation, []),
        ("Retry Logic", test_retry_logic, []),
        ("Full Prompt Integration", test_with_sample_prompt, []),
        ("Fallback Models", test_fallback_models, []),
        ("Benchmark (optional)", lambda: test_benchmark('qwen2.5:7b', 2), []),
    ]

    results = []
    for test_name, test_func, args in tests:
        try:
            success = test_func(*args)
            results.append((test_name, success))
        except Exception as e:
            logger.error(f"\n✗ {test_name} crashed: {e}")
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
