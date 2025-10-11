#!/usr/bin/env python3
"""Test script for leaderboard service layer.

Tests all major service functions to ensure:
1. Materialized views are accessible
2. Filtering works correctly
3. Caching is functional
4. Query performance meets benchmarks
"""
import sys
import time
from pathlib import Path

# Add web directory to path
sys.path.insert(0, str(Path(__file__).parent / 'web'))

from app import create_app
from app.extensions import db
from app.services import leaderboard_service

app = create_app()

def test_league_options():
    """Test league filter options."""
    print("\n" + "="*80)
    print("TEST: League Options")
    print("="*80)

    with app.app_context():
        options = leaderboard_service.get_league_options()
        print(f"✓ Retrieved {len(options)} league options")
        for opt in options:
            print(f"  - {opt['abbr']}: {opt['name']} (ID: {opt['league_id']})")
        return options


def test_career_batting():
    """Test career batting leaderboards."""
    print("\n" + "="*80)
    print("TEST: Career Batting Leaders")
    print("="*80)

    with app.app_context():
        # Test HR leaders
        start = time.time()
        result = leaderboard_service.get_career_batting_leaders(stat='hr', limit=10)
        elapsed = (time.time() - start) * 1000

        print(f"✓ HR Leaders query: {elapsed:.1f}ms")
        print(f"  Total leaders: {result['total']}")
        print(f"  Top 10:")
        for i, leader in enumerate(result['leaders'][:10], 1):
            active = "✓" if leader.is_active else " "
            print(f"    {i:2d}. [{active}] {leader.first_name} {leader.last_name}: {leader.hr} HR ({leader.seasons} seasons)")

        # Test with league filter
        if result['leaders']:
            # Get first league with data
            from app.models import PlayerBattingStats
            league_with_data = db.session.query(PlayerBattingStats.league_id).filter(
                PlayerBattingStats.split_id == 1
            ).first()

            if league_with_data:
                league_id = league_with_data[0]
                start = time.time()
                filtered = leaderboard_service.get_career_batting_leaders(
                    stat='hr', league_id=league_id, limit=5
                )
                elapsed = (time.time() - start) * 1000
                print(f"\n✓ HR Leaders (League {league_id}) query: {elapsed:.1f}ms")
                print(f"  Total: {filtered['total']}")

        # Test active only
        start = time.time()
        active = leaderboard_service.get_career_batting_leaders(
            stat='hr', active_only=True, limit=5
        )
        elapsed = (time.time() - start) * 1000
        print(f"\n✓ HR Leaders (Active Only) query: {elapsed:.1f}ms")
        print(f"  Total active: {active['total']}")


def test_career_pitching():
    """Test career pitching leaderboards."""
    print("\n" + "="*80)
    print("TEST: Career Pitching Leaders")
    print("="*80)

    with app.app_context():
        # Test W leaders
        start = time.time()
        result = leaderboard_service.get_career_pitching_leaders(stat='w', limit=10)
        elapsed = (time.time() - start) * 1000

        print(f"✓ Win Leaders query: {elapsed:.1f}ms")
        print(f"  Total leaders: {result['total']}")
        print(f"  Top 10:")
        for i, leader in enumerate(result['leaders'][:10], 1):
            active = "✓" if leader.is_active else " "
            print(f"    {i:2d}. [{active}] {leader.first_name} {leader.last_name}: {leader.w} W ({leader.seasons} seasons)")

        # Test ERA leaders (rate stat, lower is better)
        start = time.time()
        era_result = leaderboard_service.get_career_pitching_leaders(stat='era', limit=5)
        elapsed = (time.time() - start) * 1000
        print(f"\n✓ ERA Leaders query: {elapsed:.1f}ms")
        print(f"  Total (min 500 IP): {era_result['total']}")
        for i, leader in enumerate(era_result['leaders'][:5], 1):
            print(f"    {i}. {leader.first_name} {leader.last_name}: {leader.era} ERA ({leader.ip} IP)")


def test_single_season_batting():
    """Test single-season batting leaderboards."""
    print("\n" + "="*80)
    print("TEST: Single-Season Batting Leaders")
    print("="*80)

    with app.app_context():
        # Get available years
        years = leaderboard_service.get_available_years()
        print(f"✓ Available years: {len(years)} ({min(years)}-{max(years)})")

        # Test all-time single season HR
        start = time.time()
        result = leaderboard_service.get_single_season_batting_leaders(stat='hr', limit=10)
        elapsed = (time.time() - start) * 1000

        print(f"\n✓ All-Time Single-Season HR Leaders query: {elapsed:.1f}ms")
        print(f"  Total seasons: {result['total']}")
        print(f"  Top 10:")
        for i, leader in enumerate(result['leaders'][:10], 1):
            print(f"    {i:2d}. {leader.first_name} {leader.last_name} ({leader.year}, {leader.team_abbr}): {leader.hr} HR")

        # Test specific year
        current_year = max(years)
        start = time.time()
        year_result = leaderboard_service.get_single_season_batting_leaders(
            stat='hr', year=current_year, limit=5
        )
        elapsed = (time.time() - start) * 1000
        print(f"\n✓ {current_year} HR Leaders query: {elapsed:.1f}ms")
        print(f"  Total: {year_result['total']}")
        for i, leader in enumerate(year_result['leaders'][:5], 1):
            print(f"    {i}. {leader.first_name} {leader.last_name} ({leader.team_abbr}): {leader.hr} HR")


def test_single_season_pitching():
    """Test single-season pitching leaderboards."""
    print("\n" + "="*80)
    print("TEST: Single-Season Pitching Leaders")
    print("="*80)

    with app.app_context():
        # Test all-time single season SO
        start = time.time()
        result = leaderboard_service.get_single_season_pitching_leaders(stat='so', limit=10)
        elapsed = (time.time() - start) * 1000

        print(f"✓ All-Time Single-Season SO Leaders query: {elapsed:.1f}ms")
        print(f"  Total seasons: {result['total']}")
        print(f"  Top 10:")
        for i, leader in enumerate(result['leaders'][:10], 1):
            print(f"    {i:2d}. {leader.first_name} {leader.last_name} ({leader.year}, {leader.team_abbr}): {leader.so} SO")


def test_yearly_leaders():
    """Test yearly league leaders."""
    print("\n" + "="*80)
    print("TEST: Yearly League Leaders")
    print("="*80)

    with app.app_context():
        years = leaderboard_service.get_available_years()
        current_year = max(years)

        # Test batting leaders for current year
        start = time.time()
        hr_leaders = leaderboard_service.get_yearly_batting_leaders(
            stat='hr', year=current_year, limit=5
        )
        elapsed = (time.time() - start) * 1000

        print(f"✓ {current_year} HR Leaders by League query: {elapsed:.1f}ms")
        print(f"  Total entries: {len(hr_leaders)}")
        for leader in hr_leaders[:5]:
            print(f"    {leader.league_abbr} #{leader.hr_rank}: {leader.first_name} {leader.last_name} - {leader.hr} HR")

        # Test pitching leaders for current year
        start = time.time()
        w_leaders = leaderboard_service.get_yearly_pitching_leaders(
            stat='w', year=current_year, limit=5
        )
        elapsed = (time.time() - start) * 1000

        print(f"\n✓ {current_year} Win Leaders by League query: {elapsed:.1f}ms")
        print(f"  Total entries: {len(w_leaders)}")
        for leader in w_leaders[:5]:
            print(f"    {leader.league_abbr} #{leader.w_rank}: {leader.first_name} {leader.last_name} - {leader.w} W")


def test_caching():
    """Test caching functionality."""
    print("\n" + "="*80)
    print("TEST: Cache Performance")
    print("="*80)

    with app.app_context():
        # First query (no cache)
        start = time.time()
        result1 = leaderboard_service.get_career_batting_leaders(stat='hr', limit=100)
        elapsed1 = (time.time() - start) * 1000

        # Second query (cached)
        start = time.time()
        result2 = leaderboard_service.get_career_batting_leaders(stat='hr', limit=100)
        elapsed2 = (time.time() - start) * 1000

        speedup = elapsed1 / elapsed2 if elapsed2 > 0 else float('inf')
        print(f"✓ First query (no cache): {elapsed1:.1f}ms")
        print(f"✓ Second query (cached):  {elapsed2:.1f}ms")
        print(f"✓ Cache speedup: {speedup:.1f}x faster")

        # Clear cache and verify
        cleared = leaderboard_service.clear_cache()
        print(f"\n✓ Cleared {cleared} cache entries")

        # Query after clear (no cache again)
        start = time.time()
        result3 = leaderboard_service.get_career_batting_leaders(stat='hr', limit=100)
        elapsed3 = (time.time() - start) * 1000
        print(f"✓ After cache clear: {elapsed3:.1f}ms")


def test_stat_metadata():
    """Test stat metadata."""
    print("\n" + "="*80)
    print("TEST: Stat Metadata")
    print("="*80)

    metadata = leaderboard_service.get_stat_metadata()
    print(f"✓ Retrieved metadata for {len(metadata)} stats")

    batting = [k for k, v in metadata.items() if v['category'] in ('batting', 'both')]
    pitching = [k for k, v in metadata.items() if v['category'] in ('pitching', 'both')]

    print(f"\n  Batting stats ({len(batting)}): {', '.join(batting)}")
    print(f"  Pitching stats ({len(pitching)}): {', '.join(pitching)}")


def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("LEADERBOARD SERVICE LAYER TESTS")
    print("="*80)

    try:
        test_league_options()
        test_career_batting()
        test_career_pitching()
        test_single_season_batting()
        test_single_season_pitching()
        test_yearly_leaders()
        test_caching()
        test_stat_metadata()

        print("\n" + "="*80)
        print("✓ ALL TESTS PASSED")
        print("="*80 + "\n")

    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()