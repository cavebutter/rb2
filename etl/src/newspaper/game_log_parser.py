"""
Game Log Parser Module

Extracts play-by-play details for Branch family members from game_logs.csv.
Parses batting sequences, pitching appearances, and defensive plays to provide
narrative context for article generation.

CSV Format (game_logs.csv):
- game_id: Game identifier
- type: Event type (1=inning header, 2=batter/pitcher change, 3=play, 4=inning summary)
- line: Sequential line number within game
- text: HTML-formatted play description with embedded player links

Example:
    game_id,type,line,text
    1,2,3,"Batting: RHB <a href=\"../players/player_2496.html\">Tim Korman</a>"
    1,3,4,"0-0: Ground out 6-3 (Groundball, 4MD, EV 97.5 MPH)"
"""

import csv
import re
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from loguru import logger


# Event type constants from game_logs.csv
EVENT_TYPE_INNING_HEADER = 1
EVENT_TYPE_PLAYER_CHANGE = 2
EVENT_TYPE_PLAY = 3
EVENT_TYPE_INNING_SUMMARY = 4


def extract_player_id_from_text(text: str) -> Optional[int]:
    """
    Extract player_id from HTML link in game log text.

    Example:
        Input: 'Batting: RHB <a href="../players/player_2496.html">Tim Korman</a>'
        Output: 2496

    Args:
        text: Game log text with potential player link

    Returns:
        Player ID integer, or None if no link found
    """
    match = re.search(r'player_(\d+)\.html', text)
    if match:
        return int(match.group(1))
    return None


def parse_inning_from_header(text: str) -> Optional[Tuple[int, str]]:
    """
    Parse inning number and half from inning header text.

    Examples:
        "Top of the 1st - Cleveland batting" -> (1, 'top')
        "Bottom of the 3rd - Boston batting" -> (3, 'bottom')

    Args:
        text: Inning header text

    Returns:
        Tuple of (inning_number, inning_half), or None if parse fails
    """
    match = re.search(r'(Top|Bottom) of the (\d+)', text, re.IGNORECASE)
    if match:
        half = match.group(1).lower()
        inning = int(match.group(2))
        return (inning, half)
    return None


def extract_exit_velocity(text: str) -> Optional[float]:
    """
    Extract exit velocity from play description.

    Example:
        "Ground out 6-3 (Groundball, 4MD, EV 97.5 MPH)" -> 97.5

    Args:
        text: Play description text

    Returns:
        Exit velocity in MPH, or None if not found
    """
    match = re.search(r'EV\s+([\d.]+)\s*MPH', text, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None


def extract_hit_location(text: str) -> Optional[str]:
    """
    Extract hit location code from play description.

    Examples:
        "(Flyball, 89XD, EV 106.9 MPH)" -> "89XD"
        "(Groundball, 4MD, EV 97.5 MPH)" -> "4MD"

    Args:
        text: Play description text

    Returns:
        Hit location code, or None if not found
    """
    # Look for location codes after ball type (Flyball, Groundball, etc.)
    match = re.search(r'\((Flyball|Groundball|Line Drive|Popup|Bunt),\s*([^,]+)', text, re.IGNORECASE)
    if match:
        return match.group(2).strip()
    return None


def parse_pitch_count(text: str) -> Optional[Tuple[int, int]]:
    """
    Parse balls-strikes count from pitch description.

    Example:
        "3-2: Ball" -> (3, 2)

    Args:
        text: Pitch description text

    Returns:
        Tuple of (balls, strikes), or None if no count found
    """
    match = re.search(r'^(\d+)-(\d+):', text)
    if match:
        return (int(match.group(1)), int(match.group(2)))
    return None


def classify_outcome(text: str) -> str:
    """
    Classify play outcome from description text.

    Categories:
        - 'single', 'double', 'triple', 'home_run'
        - 'walk', 'strikeout'
        - 'ground_out', 'fly_out', 'line_out', 'popup'
        - 'error', 'fielders_choice'
        - 'stolen_base', 'caught_stealing'
        - 'other'

    Args:
        text: Play description text

    Returns:
        Outcome classification string
    """
    text_lower = text.lower()

    # Check for hits
    if '<b>home run</b>' in text_lower or '<b>hr</b>' in text_lower:
        return 'home_run'
    if '<b>triple</b>' in text_lower:
        return 'triple'
    if '<b>double</b>' in text_lower:
        return 'double'
    if '<b>single</b>' in text_lower:
        return 'single'

    # Check for walks/strikeouts
    if 'base on balls' in text_lower or 'intentional walk' in text_lower:
        return 'walk'
    if 'strikes out' in text_lower or 'strikeout' in text_lower:
        return 'strikeout'

    # Check for outs
    if 'ground out' in text_lower or 'grounds out' in text_lower:
        return 'ground_out'
    if 'fly out' in text_lower or 'flies out' in text_lower:
        return 'fly_out'
    if 'line out' in text_lower or 'lines out' in text_lower:
        return 'line_out'
    if 'pop out' in text_lower or 'pops out' in text_lower or 'popup' in text_lower:
        return 'popup'

    # Check for baserunning
    if 'stolen base' in text_lower or 'steals' in text_lower:
        return 'stolen_base'
    if 'caught stealing' in text_lower:
        return 'caught_stealing'

    # Check for errors/fielders choice
    if 'error' in text_lower:
        return 'error'
    if "fielder's choice" in text_lower or 'fielders choice' in text_lower:
        return 'fielders_choice'

    return 'other'


def load_game_log_for_game(csv_path: str, game_id: int) -> List[Dict]:
    """
    Load all log entries for a specific game from game_logs.csv.

    Args:
        csv_path: Path to game_logs.csv file
        game_id: Game ID to filter

    Returns:
        List of log entry dicts with keys: game_id, type, line, text
    """
    entries = []

    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)

            for row in reader:
                if int(row['game_id']) == game_id:
                    entries.append({
                        'game_id': int(row['game_id']),
                        'type': int(row['type']),
                        'line': int(row['line']),
                        'text': row['text']
                    })

    except FileNotFoundError:
        logger.error(f"Game log CSV not found: {csv_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading game log for game_id={game_id}: {e}")
        raise

    logger.debug(f"Loaded {len(entries)} log entries for game_id={game_id}")
    return entries


def extract_branch_plays_from_game_log(
    csv_path: str,
    game_id: int,
    branch_player_ids: List[int]
) -> Dict[int, List[Dict]]:
    """
    Parse game_logs.csv for specific game and extract plays involving Branch players.

    Tracks current game state (inning, batter, pitcher) and captures full play sequences
    when Branch family members are involved.

    Args:
        csv_path: Path to game_logs.csv file
        game_id: Game ID to parse
        branch_player_ids: List of Branch family player IDs to detect

    Returns:
        Dict mapping player_id -> list of play dicts
        Each play dict contains:
            - player_id: Branch player ID
            - role: 'batter' or 'pitcher'
            - inning: Inning number
            - inning_half: 'top' or 'bottom'
            - sequence: List of line texts (pitch-by-pitch)
            - outcome: Classified outcome (single, strikeout, etc.)
            - exit_velocity: Exit velocity if available
            - hit_location: Hit location code if available
    """
    # Load all entries for this game
    entries = load_game_log_for_game(csv_path, game_id)

    if not entries:
        logger.warning(f"No game log entries found for game_id={game_id}")
        return {}

    branch_ids_set = set(branch_player_ids)
    branch_plays = defaultdict(list)

    # State tracking
    current_inning = None
    current_half = None
    current_batter = None
    current_pitcher = None
    current_sequence = []

    for entry in entries:
        event_type = entry['type']
        text = entry['text']

        # Update inning context
        if event_type == EVENT_TYPE_INNING_HEADER:
            parsed_inning = parse_inning_from_header(text)
            if parsed_inning:
                current_inning, current_half = parsed_inning
                logger.debug(f"Inning context: {current_half} {current_inning}")

        # Track player changes
        elif event_type == EVENT_TYPE_PLAYER_CHANGE:
            player_id = extract_player_id_from_text(text)

            if 'Batting:' in text:
                # New batter - save previous at-bat if it was Branch player
                if current_batter in branch_ids_set and current_sequence:
                    play_data = _finalize_at_bat(
                        current_batter,
                        current_inning,
                        current_half,
                        current_sequence
                    )
                    branch_plays[current_batter].append(play_data)

                # Reset for new at-bat
                current_batter = player_id
                current_sequence = []

            elif 'Pitching:' in text:
                current_pitcher = player_id

        # Track plays
        elif event_type == EVENT_TYPE_PLAY:
            # Add to current sequence if Branch player is involved
            if current_batter in branch_ids_set:
                current_sequence.append(text)

            # Check if this is a pitching play for Branch pitcher
            # (for now we focus on batting, but could extend to pitching)
            if current_pitcher in branch_ids_set:
                # Could track pitching stats here in future
                pass

    # Finalize last at-bat if needed
    if current_batter in branch_ids_set and current_sequence:
        play_data = _finalize_at_bat(
            current_batter,
            current_inning,
            current_half,
            current_sequence
        )
        branch_plays[current_batter].append(play_data)

    # Log summary
    total_plays = sum(len(plays) for plays in branch_plays.values())
    logger.info(f"Extracted {total_plays} Branch plays for {len(branch_plays)} players in game_id={game_id}")

    return dict(branch_plays)


def _finalize_at_bat(
    player_id: int,
    inning: Optional[int],
    inning_half: Optional[str],
    sequence: List[str]
) -> Dict:
    """
    Convert raw play sequence into structured at-bat dict.

    Args:
        player_id: Branch player ID
        inning: Inning number
        inning_half: 'top' or 'bottom'
        sequence: List of play text lines

    Returns:
        Structured at-bat dict
    """
    # Get outcome from last line
    outcome_text = sequence[-1] if sequence else ""
    outcome = classify_outcome(outcome_text)

    # Extract metadata
    exit_velocity = None
    hit_location = None

    for line in sequence:
        if not exit_velocity:
            exit_velocity = extract_exit_velocity(line)
        if not hit_location:
            hit_location = extract_hit_location(line)

    return {
        'player_id': player_id,
        'role': 'batter',
        'inning': inning,
        'inning_half': inning_half,
        'sequence': sequence,
        'outcome': outcome,
        'exit_velocity': exit_velocity,
        'hit_location': hit_location
    }


def structure_branch_at_bats(branch_plays: Dict[int, List[Dict]]) -> Dict[int, List[Dict]]:
    """
    Convert raw play sequences into structured at-bat summaries.

    Enriches play data with additional parsing:
    - Pitch-by-pitch count
    - Full narrative description
    - Contextual info

    Args:
        branch_plays: Output from extract_branch_plays_from_game_log()

    Returns:
        Structured at-bats dict, same format but with enriched data
    """
    structured = {}

    for player_id, plays in branch_plays.items():
        structured[player_id] = []

        for play in plays:
            # Build pitch-by-pitch narrative
            pitch_narrative = []
            for line in play['sequence']:
                count = parse_pitch_count(line)
                if count:
                    pitch_narrative.append({
                        'balls': count[0],
                        'strikes': count[1],
                        'description': line
                    })

            # Add pitch narrative to play data
            enriched_play = play.copy()
            enriched_play['pitch_sequence'] = pitch_narrative

            structured[player_id].append(enriched_play)

    return structured


def save_branch_moments_to_db(conn, game_id: int, branch_plays: Dict[int, List[Dict]]) -> int:
    """
    Store extracted moments in branch_game_moments table.

    Allows future article regeneration without re-parsing CSV.

    Args:
        conn: psycopg2 database connection
        game_id: Game ID
        branch_plays: Structured plays from structure_branch_at_bats()

    Returns:
        Number of moments saved
    """
    count = 0

    with conn.cursor() as cur:
        for player_id, plays in branch_plays.items():
            for play in plays:
                # Convert sequence to JSONB
                play_sequence = [
                    {'text': line} for line in play['sequence']
                ]

                # Build outcome summary
                outcome_parts = [play['outcome']]
                if play.get('exit_velocity'):
                    outcome_parts.append(f"EV {play['exit_velocity']} MPH")
                if play.get('hit_location'):
                    outcome_parts.append(play['hit_location'])
                outcome_summary = ', '.join(outcome_parts)

                # Insert moment
                cur.execute("""
                    INSERT INTO branch_game_moments
                    (game_id, player_id, inning, inning_half, moment_type,
                     play_sequence, outcome, exit_velocity, hit_location)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    game_id,
                    player_id,
                    play['inning'],
                    play['inning_half'],
                    'at_bat',
                    str(play_sequence),  # Convert to JSON string
                    outcome_summary,
                    play.get('exit_velocity'),
                    play.get('hit_location')
                ))
                count += 1

        conn.commit()

    logger.info(f"Saved {count} Branch moments to database for game_id={game_id}")
    return count


def get_branch_moments_from_db(conn, game_id: int, player_id: int) -> List[Dict]:
    """
    Retrieve previously saved Branch moments from database.

    Used for article regeneration without re-parsing CSV.

    Args:
        conn: psycopg2 database connection
        game_id: Game ID
        player_id: Branch player ID

    Returns:
        List of moment dicts
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                moment_id,
                inning,
                inning_half,
                moment_type,
                play_sequence,
                outcome,
                exit_velocity,
                hit_location,
                created_at
            FROM branch_game_moments
            WHERE game_id = %s AND player_id = %s
            ORDER BY inning, moment_id
        """, (game_id, player_id))

        rows = cur.fetchall()

        moments = []
        for row in rows:
            moments.append({
                'moment_id': row[0],
                'inning': row[1],
                'inning_half': row[2],
                'moment_type': row[3],
                'play_sequence': row[4],  # JSONB
                'outcome': row[5],
                'exit_velocity': float(row[6]) if row[6] else None,
                'hit_location': row[7],
                'created_at': row[8]
            })

    logger.debug(f"Retrieved {len(moments)} moments for player_id={player_id} in game_id={game_id}")
    return moments