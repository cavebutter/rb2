#!/usr/bin/env bash

# Load environment variables
source ../.env

# Configuration from environment
GAME_MACHINE="${OOTP_GAME_MACHINE:-Jayco@192.168.0.9}"
GAME_NAME="${OOTP_GAME_NAME}"

# Validate Required Variables
if [ -z "$GAME_NAME" ]; then
    echo "Error: OOTP_GAME_NAME not set in .env file"
    exit 1
fi

# Game machine paths
GAME_BASE="/Users/Jayco/Library/Containers/com.ootpdevelopments.ootp25macalm/Data/Application Support/Out of the Park Developments/OOTP Baseball 25/saved_games/${GAME_NAME}.lg"
DATA_PATH="${GAME_BASE}/import_export/csv"  # Assuming CSVs export here
PICTURES_PATH="${GAME_BASE}/news/html/person_pictures"

# Local Paths
LOCAL_DATA="../data/incoming/"
LOCAL_PICTURES="../data/images/players/"

echo "Fetching OOTP CSV data from game machine..."
scp -r ${GAME_MACHINE}:"${DATA_PATH}/*.csv" ${LOCAL_DATA}

echo "Fetching player pictures..."
mkdir -p ${LOCAL_PICTURES}
scp -r ${GAME_MACHINE}:"${PICTURES_PATH}/*" ${LOCAL_PICTURES}

echo "Transfer complete!"
