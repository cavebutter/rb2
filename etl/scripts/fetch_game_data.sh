#!/usr/bin/env bash

# Exit on any error
set -e

# Load environment variables
source ../.env

# Configuration from environment
GAME_MACHINE="${OOTP_GAME_MACHINE}"
GAME_NAME="${OOTP_GAME_NAME}"

# Validate Required Variables
if [ -z "$GAME_NAME" ]; then
    echo "Error: OOTP_GAME_NAME not set in .env file"
    exit 1
fi

if [ -z "$GAME_MACHINE" ]; then
    echo "Error: OOTP_GAME_MACHINE not set in .env file"
    exit 1
fi

if [ -z "$OOTP_REMOTE_DATA_PATH" ]; then
    echo "Error: OOTP_REMOTE_DATA_PATH not set in .env file"
    exit 1
fi

# Use paths from .env
REMOTE_DATA_PATH="${OOTP_REMOTE_DATA_PATH}"
REMOTE_PICTURES_PATH="${OOTP_REMOTE_PICTURES_PATH}"
LOCAL_DATA="${OOTP_GAME_DATA_PATH}/csv"
LOCAL_PICTURES="${OOTP_IMAGES_PATH}"

# Create local directories if they don't exist
echo "Creating local directories..."
mkdir -p "${LOCAL_DATA}"
mkdir -p "${LOCAL_PICTURES}"

# Function to check if rsync succeeded
check_rsync_result() {
    local exit_code=$1
    local description="$2"
    
    case $exit_code in
        0)
            echo "✓ $description completed successfully"
            ;;
        23)
            echo "⚠ $description completed with some files not transferred (partial transfer)"
            ;;
        *)
            echo "✗ $description failed with exit code $exit_code"
            return $exit_code
            ;;
    esac
}

# Sync CSV files (always fresh export, delete removed files)
echo "Syncing OOTP CSV data from game machine..."
echo "Source: ${GAME_MACHINE}:\"${REMOTE_DATA_PATH}/\""
echo "Target: ${LOCAL_DATA}/"

rsync -avz --delete --dry-run --progress --stats \
    "${GAME_MACHINE}:${REMOTE_DATA_PATH}/" \
    "${LOCAL_DATA}/"

check_rsync_result $? "CSV data sync"

# Sync player pictures (incremental, preserve newer local files)
echo ""
echo "Syncing player pictures..."
echo "Source: ${GAME_MACHINE}:\"${REMOTE_PICTURES_PATH}/\""
echo "Target: ${LOCAL_PICTURES}/"

rsync -avz --update --progress --stats --dry-run \
    "${GAME_MACHINE}:${REMOTE_PICTURES_PATH}/" \
    "${LOCAL_PICTURES}/"

check_rsync_result $? "Player pictures sync"

echo ""
echo "🎉 Transfer complete!"
echo "CSV files: ${LOCAL_DATA}/"
echo "Pictures: ${LOCAL_PICTURES}/"
