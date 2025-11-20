import os
import psycopg2
from psycopg2.extras import execute_values
import requests
from datetime import datetime
import json

# Database connection using Neon
def get_db_connection():
    """
    Establish connection to Neon Postgres database
    Requires DATABASE_URL environment variable
    """
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    conn = psycopg2.connect(database_url)
    return conn

# Create tables if they don't exist
def create_tables():
    """
    Create necessary tables for NFL data
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Teams table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nfl_teams (
            id SERIAL PRIMARY KEY,
            team_name VARCHAR(100) NOT NULL,
            team_abbr VARCHAR(10) NOT NULL UNIQUE,
            conference VARCHAR(10),
            division VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Games table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nfl_games (
            id SERIAL PRIMARY KEY,
            game_id VARCHAR(50) UNIQUE,
            season INTEGER,
            week INTEGER,
            game_date DATE,
            home_team VARCHAR(10),
            away_team VARCHAR(10),
            home_score INTEGER,
            away_score INTEGER,
            status VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Players table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nfl_players (
            id SERIAL PRIMARY KEY,
            player_id VARCHAR(50) UNIQUE,
            player_name VARCHAR(100) NOT NULL,
            team VARCHAR(10),
            position VARCHAR(10),
            jersey_number INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Player stats table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nfl_player_stats (
            id SERIAL PRIMARY KEY,
            player_id VARCHAR(50),
            game_id VARCHAR(50),
            season INTEGER,
            week INTEGER,
            passing_yards INTEGER DEFAULT 0,
            passing_tds INTEGER DEFAULT 0,
            interceptions INTEGER DEFAULT 0,
            rushing_yards INTEGER DEFAULT 0,
            rushing_tds INTEGER DEFAULT 0,
            receptions INTEGER DEFAULT 0,
            receiving_yards INTEGER DEFAULT 0,
            receiving_tds INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES nfl_players(player_id),
            FOREIGN KEY (game_id) REFERENCES nfl_games(game_id)
        )
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Tables created successfully")

# Populate NFL teams
def populate_teams():
    """
    Populate NFL teams data
    """
    teams = [
        ('Arizona Cardinals', 'ARI', 'NFC', 'West'),
        ('Atlanta Falcons', 'ATL', 'NFC', 'South'),
        ('Baltimore Ravens', 'BAL', 'AFC', 'North'),
        ('Buffalo Bills', 'BUF', 'AFC', 'East'),
        ('Carolina Panthers', 'CAR', 'NFC', 'South'),
        ('Chicago Bears', 'CHI', 'NFC', 'North'),
        ('Cincinnati Bengals', 'CIN', 'AFC', 'North'),
        ('Cleveland Browns', 'CLE', 'AFC', 'North'),
        ('Dallas Cowboys', 'DAL', 'NFC', 'East'),
        ('Denver Broncos', 'DEN', 'AFC', 'West'),
        ('Detroit Lions', 'DET', 'NFC', 'North'),
        ('Green Bay Packers', 'GB', 'NFC', 'North'),
        ('Houston Texans', 'HOU', 'AFC', 'South'),
        ('Indianapolis Colts', 'IND', 'AFC', 'South'),
        ('Jacksonville Jaguars', 'JAX', 'AFC', 'South'),
        ('Kansas City Chiefs', 'KC', 'AFC', 'West'),
        ('Las Vegas Raiders', 'LV', 'AFC', 'West'),
        ('Los Angeles Chargers', 'LAC', 'AFC', 'West'),
        ('Los Angeles Rams', 'LAR', 'NFC', 'West'),
        ('Miami Dolphins', 'MIA', 'AFC', 'East'),
        ('Minnesota Vikings', 'MIN', 'NFC', 'North'),
        ('New England Patriots', 'NE', 'AFC', 'East'),
        ('New Orleans Saints', 'NO', 'NFC', 'South'),
        ('New York Giants', 'NYG', 'NFC', 'East'),
        ('New York Jets', 'NYJ', 'AFC', 'East'),
        ('Philadelphia Eagles', 'PHI', 'NFC', 'East'),
        ('Pittsburgh Steelers', 'PIT', 'AFC', 'North'),
        ('San Francisco 49ers', 'SF', 'NFC', 'West'),
        ('Seattle Seahawks', 'SEA', 'NFC', 'West'),
        ('Tampa Bay Buccaneers', 'TB', 'NFC', 'South'),
        ('Tennessee Titans', 'TEN', 'AFC', 'South'),
        ('Washington Commanders', 'WAS', 'NFC', 'East')
    ]
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO nfl_teams (team_name, team_abbr, conference, division)
        VALUES %s
        ON CONFLICT (team_abbr) DO NOTHING
    """
    
    execute_values(cur, insert_query, teams)
    conn.commit()
    print(f"Inserted {cur.rowcount} teams")
    
    cur.close()
    conn.close()

# Fetch and populate games data
def populate_games(season=2025, week=None):
    """
    Populate NFL games data for a given season and week
    This is a template - you'll need to integrate with your actual data source
    """
    # Example using ESPN API (public endpoint)
    try:
        if week:
            url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={season}&seasontype=2&week={week}"
        else:
            url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={season}"
        
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch games: {response.status_code}")
            return
        
        data = response.json()
        games = []
        
        for event in data.get('events', []):
            game_id = event.get('id')
            game_date = event.get('date', '').split('T')[0]
            
            competitions = event.get('competitions', [])
            if not competitions:
                continue
                
            competition = competitions[0]
            competitors = competition.get('competitors', [])
            
            home_team = None
            away_team = None
            home_score = None
            away_score = None
            
            for competitor in competitors:
                if competitor.get('homeAway') == 'home':
                    home_team = competitor.get('team', {}).get('abbreviation')
                    home_score = int(competitor.get('score', 0))
                else:
                    away_team = competitor.get('team', {}).get('abbreviation')
                    away_score = int(competitor.get('score', 0))
            
            status = competition.get('status', {}).get('type', {}).get('name', 'scheduled')
            
            games.append((
                game_id,
                season,
                week if week else event.get('week', {}).get('number', 1),
                game_date,
                home_team,
                away_team,
                home_score,
                away_score,
                status
            ))
        
        if games:
            conn = get_db_connection()
            cur = conn.cursor()
            
            insert_query = """
                INSERT INTO nfl_games (game_id, season, week, game_date, home_team, away_team, home_score, away_score, status)
                VALUES %s
                ON CONFLICT (game_id) DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            execute_values(cur, insert_query, games)
            conn.commit()
            print(f"Inserted/updated {len(games)} games")
            
            cur.close()
            conn.close()
    
    except Exception as e:
        print(f"Error populating games: {str(e)}")

def main():
    """
    Main function to populate all NFL data
    """
    print("Starting NFL data population...")
    
    # Create tables
    print("\n1. Creating tables...")
    create_tables()
    
    # Populate teams
    print("\n2. Populating teams...")
    populate_teams()
    
    # Populate current season games
    print("\n3. Populating games for 2025 season...")
    current_week = 11  # Update this based on current week
    for week in range(1, current_week + 1):
        print(f"   Fetching week {week}...")
        populate_games(season=2025, week=week)
    
    print("\nNFL data population completed!")

if __name__ == "__main__":
    main()
