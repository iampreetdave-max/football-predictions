import pandas as pd
import psycopg2
from psycopg2 import sql
import sys
import os

# Database credentials from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# CSV file path
CSV_FILE = "extracted_features_complete.csv"
TABLE_NAME = "soccer_features"

def create_connection():
    """Create a connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            sslmode='require'
        )
        print("✓ Connected to database successfully")
        return conn
    except Exception as e:
        print(f"✗ Error connecting to database: {e}")
        sys.exit(1)

def create_table(conn):
    """Create soccer_features table"""
    cursor = conn.cursor()
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        match_id BIGINT UNIQUE NOT NULL,
        date DATE,
        home_team_id INTEGER,
        away_team_id INTEGER,
        league_id INTEGER,
        league_name VARCHAR(255),
        home_team_name VARCHAR(255),
        away_team_name VARCHAR(255),
        CTMCL FLOAT,
        avg_goals_market FLOAT,
        team_a_xg_prematch FLOAT,
        team_b_xg_prematch FLOAT,
        pre_match_home_ppg FLOAT,
        pre_match_away_ppg FLOAT,
        home_xg_avg FLOAT,
        away_xg_avg FLOAT,
        home_xg_momentum FLOAT,
        away_xg_momentum FLOAT,
        home_goals_conceded_avg FLOAT,
        away_goals_conceded_avg FLOAT,
        o25_potential FLOAT,
        o35_potential FLOAT,
        home_shots_accuracy_avg FLOAT,
        away_shots_accuracy_avg FLOAT,
        home_dangerous_attacks_avg FLOAT,
        away_dangerous_attacks_avg FLOAT,
        h2h_total_goals_avg FLOAT,
        home_form_points FLOAT,
        away_form_points FLOAT,
        home_elo FLOAT,
        away_elo FLOAT,
        elo_diff FLOAT,
        league_avg_goals FLOAT,
        odds_ft_1_prob FLOAT,
        odds_ft_2_prob FLOAT,
        btts_potential FLOAT,
        o05_potential FLOAT,
        o15_potential FLOAT,
        o45_potential FLOAT,
        odds_ft_over25 FLOAT,
        odds_ft_under25 FLOAT,
        odds_ft_1 FLOAT,
        odds_ft_x FLOAT,
        odds_ft_2 FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print(f"✓ Table '{TABLE_NAME}' created successfully")
    except Exception as e:
        print(f"✗ Error creating table: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()

def load_csv_data(conn):
    """Load CSV data into the table, skipping duplicates based on match_id"""
    try:
        # Read CSV file
        df = pd.read_csv(CSV_FILE)
        print(f"✓ Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        cursor = conn.cursor()
        
        # Get existing match_ids from database
        cursor.execute(f"SELECT DISTINCT match_id FROM {TABLE_NAME};")
        existing_match_ids = set(row[0] for row in cursor.fetchall())
        print(f"✓ Found {len(existing_match_ids)} existing match_ids in database")
        
        inserted_count = 0
        skipped_count = 0
        
        # Insert data
        for idx, row in df.iterrows():
            match_id = row['match_id']
            
            # Check if match_id already exists
            if match_id in existing_match_ids:
                skipped_count += 1
                continue
            
            insert_query = f"""
            INSERT INTO {TABLE_NAME} (
                match_id, date, home_team_id, away_team_id, league_id, league_name,
                home_team_name, away_team_name, CTMCL, avg_goals_market, team_a_xg_prematch,
                team_b_xg_prematch, pre_match_home_ppg, pre_match_away_ppg, home_xg_avg,
                away_xg_avg, home_xg_momentum, away_xg_momentum, home_goals_conceded_avg,
                away_goals_conceded_avg, o25_potential, o35_potential, home_shots_accuracy_avg,
                away_shots_accuracy_avg, home_dangerous_attacks_avg, away_dangerous_attacks_avg,
                h2h_total_goals_avg, home_form_points, away_form_points, home_elo, away_elo,
                elo_diff, league_avg_goals, odds_ft_1_prob, odds_ft_2_prob, btts_potential,
                o05_potential, o15_potential, o45_potential, odds_ft_over25, odds_ft_under25,
                odds_ft_1, odds_ft_x, odds_ft_2
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            );
            """
            
            values = (
                row['match_id'], row['date'], row['home_team_id'], row['away_team_id'],
                row['league_id'], row['league_name'], row['home_team_name'], row['away_team_name'],
                row['CTMCL'], row['avg_goals_market'], row['team_a_xg_prematch'],
                row['team_b_xg_prematch'], row['pre_match_home_ppg'], row['pre_match_away_ppg'],
                row['home_xg_avg'], row['away_xg_avg'], row['home_xg_momentum'],
                row['away_xg_momentum'], row['home_goals_conceded_avg'], row['away_goals_conceded_avg'],
                row['o25_potential'], row['o35_potential'], row['home_shots_accuracy_avg'],
                row['away_shots_accuracy_avg'], row['home_dangerous_attacks_avg'],
                row['away_dangerous_attacks_avg'], row['h2h_total_goals_avg'], row['home_form_points'],
                row['away_form_points'], row['home_elo'], row['away_elo'], row['elo_diff'],
                row['league_avg_goals'], row['odds_ft_1_prob'], row['odds_ft_2_prob'],
                row['btts_potential'], row['o05_potential'], row['o15_potential'],
                row['o45_potential'], row['odds_ft_over25'], row['odds_ft_under25'],
                row['odds_ft_1'], row['odds_ft_x'], row['odds_ft_2']
            )
            
            cursor.execute(insert_query, values)
            existing_match_ids.add(match_id)
            inserted_count += 1
            
            if (inserted_count + skipped_count) % 10 == 0:
                print(f"  Processed {inserted_count + skipped_count} rows (inserted: {inserted_count}, skipped: {skipped_count})...")
        
        conn.commit()
        print(f"✓ Successfully loaded {inserted_count} new rows into '{TABLE_NAME}'")
        print(f"⊘ Skipped {skipped_count} duplicate match_ids")
        
    except Exception as e:
        print(f"✗ Error loading CSV data: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()

def verify_data(conn):
    """Verify data was loaded correctly"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        count = cursor.fetchone()[0]
        print(f"✓ Verification: {count} rows in table '{TABLE_NAME}'")
        
        cursor.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 3;")
        sample = cursor.fetchall()
        print(f"✓ Sample data retrieved successfully")
        
    except Exception as e:
        print(f"✗ Error verifying data: {e}")
    finally:
        cursor.close()

def main():
    print("=" * 60)
    print("Loading Soccer Features CSV to PostgreSQL")
    print("=" * 60)
    
    # Connect to database
    conn = create_connection()
    
    # Create table
    create_table(conn)
    
    # Load data
    load_csv_data(conn)
    
    # Verify
    verify_data(conn)
    
    # Close connection
    conn.close()
    print("=" * 60)
    print("✓ Process completed successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
