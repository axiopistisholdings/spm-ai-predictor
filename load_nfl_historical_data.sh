#!/bin/bash
# Script to load NFL historical data from nflverse into Neon database
# Usage: ./load_nfl_historical_data.sh

set -e  # Exit on error

echo "=========================================="
echo "NFL Historical Data Loader"
echo "=========================================="
echo ""

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "Error: DATABASE_URL environment variable is not set"
    echo "Please set it with: export DATABASE_URL='your_neon_connection_string'"
    exit 1
fi

echo "Step 1: Downloading NFL games data from nflverse..."
wget -q https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv -O /tmp/nfl_games.csv

if [ ! -f "/tmp/nfl_games.csv" ]; then
    echo "Error: Failed to download games.csv"
    exit 1
fi

echo "✓ Downloaded games.csv ($(wc -l < /tmp/nfl_games.csv) rows)"
echo ""

echo "Step 2: Creating temporary staging table..."
psql "$DATABASE_URL" << EOF
DROP TABLE IF EXISTS temp_nfl_games;
CREATE TEMP TABLE temp_nfl_games (
    season INTEGER,
    game_type TEXT,
    week INTEGER,
    gameday DATE,
    weekday TEXT,
    gametime TIME,
    away_team TEXT,
    away_score INTEGER,
    home_team TEXT,
    home_score INTEGER,
    location TEXT,
    result INTEGER,
    total INTEGER,
    overtime INTEGER,
    old_game_id TEXT,
    gsis TEXT,
    nfl_detail_id TEXT,
    pfr TEXT,
    pff TEXT,
    espn TEXT,
    ftn TEXT,
    away_rest INTEGER,
    home_rest INTEGER,
    away_moneyline NUMERIC,
    home_moneyline NUMERIC,
    spread_line NUMERIC,
    away_spread_odds INTEGER,
    home_spread_odds INTEGER,
    total_line NUMERIC,
    under_odds INTEGER,
    over_odds INTEGER,
    div_game INTEGER,
    roof TEXT,
    surface TEXT,
    temp NUMERIC,
    wind NUMERIC,
    away_qb_id TEXT,
    home_qb_id TEXT,
    away_qb_name TEXT,
    home_qb_name TEXT,
    away_coach TEXT,
    home_coach TEXT,
    referee TEXT,
    stadium_id TEXT,
    stadium TEXT,
    game_id TEXT
);
EOF

echo "✓ Staging table created"
echo ""

echo "Step 3: Loading CSV data into staging table..."
psql "$DATABASE_URL" -c "\COPY temp_nfl_games FROM '/tmp/nfl_games.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');"

echo "✓ Data loaded into staging table"
echo ""

echo "Step 4: Filtering and inserting into nfl_games table (2021-2025)..."
psql "$DATABASE_URL" << EOF
INSERT INTO nfl_games (game_id, season, week, game_date, home_team, away_team, home_score, away_score, status)
SELECT 
    game_id,
    season,
    week,
    gameday,
    home_team,
    away_team,
    COALESCE(home_score, 0),
    COALESCE(away_score, 0),
    CASE 
        WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN 'final'
        ELSE 'scheduled'
    END as status
FROM temp_nfl_games
WHERE season >= 2021 AND season <= 2025
  AND game_type IN ('REG', 'WC', 'DIV', 'CON', 'SB')  -- Regular season and playoffs
ON CONFLICT (game_id) DO UPDATE SET
    home_score = EXCLUDED.home_score,
    away_score = EXCLUDED.away_score,
    status = EXCLUDED.status,
    updated_at = CURRENT_TIMESTAMP;
EOF

echo "✓ Historical games inserted/updated"
echo ""

echo "Step 5: Getting statistics..."
psql "$DATABASE_URL" << EOF
SELECT season, COUNT(*) as games 
FROM nfl_games 
WHERE season >= 2021 
GROUP BY season 
ORDER BY season;
EOF

echo ""
echo "=========================================="
echo "✓ NFL Historical Data Load Complete!"
echo "=========================================="
echo ""
echo "Cleaning up..."
rm /tmp/nfl_games.csv
echo "Done!"
