"""Wrapper for fetch_game_data.sh script"""
import subprocess
from pathlib import Path
from loguru import logger


def fetch_game_data(dry_run=False):
    """Execute fetch_game_data.sh script"""
    script_path = Path(__file__).parent.parent.parent / "scripts" / "fetch_game_data.sh"

    if not script_path.exists:
        logger.error(f"Fetch script not found at {script_path}")
        return False

    try:
        # Make sure the scipt is executable
        script_path.chmod(0o755)

        logger.info(f"Executing fetch script: {script_path}")
        result = subprocess.run(
            [str(script_path)],
            capture_output=True,
            text=True,
            cwd=script_path.parent,
        )

        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                logger.info(f"FETCH: {line}")

        if result.stderr:
            for line in result.stderr.strip().split('\n'):
                logger.warning(f"FETCH: {line}")

        if result.returncode == 0:
            logger.success("Fetch script executed successfully")
            return True
        else:
            logger.error(f"Data fetch failed with return code {result.returncode}")
            return False
    except Exception as e:
        logger.error(f"Error executing fetch script: {e}")
        return False