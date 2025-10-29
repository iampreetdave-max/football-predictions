"""
Simple script to store league_id and league_name mapping from live.csv to database
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configuration
CSV_FILE = 'live.csv'
DB_CONFIG = {
    'host': 'winbets-db.postgres.database.azure.com',
    'port': 5432,
    'dbname': 'postgres',
    'user': 'winbets',
    'password': 'deeptanshu@123'
}
TABLE_NAME = 'agility_football_pred'

def store_leagues():
    """Store league_id and league_name from CSV to database"""
    
    # Read CSV
    print(f"Reading {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)
    
    # Get unique league_id and league_name pairs
    leagues = df[['league_id', 'league_name']].drop_duplicates()
    print(f"Found {len(leagues)} unique leagues")
    
    # Connect to database
    print(f"Connecting to PostgreSQL at {DB_CONFIG['host']}...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Create table if not exists
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                league_id INTEGER PRIMARY KEY,
                league_name TEXT NOT NULL
            )
        ''')
        
        # Insert data using INSERT ... ON CONFLICT (PostgreSQL's upsert)
        print("Storing leagues...")
        data = [(int(row['league_id']), str(row['league_name'])) 
                for _, row in leagues.iterrows()]
        
        insert_query = f'''
            INSERT INTO {TABLE_NAME} (league_id, league_name)
            VALUES %s
            ON CONFLICT (league_id) 
            DO UPDATE SET league_name = EXCLUDED.league_name
        '''
        execute_values(cursor, insert_query, data)
        
        conn.commit()
        
        # Show results
        print("\nStored leagues:")
        cursor.execute(f'SELECT league_id, league_name FROM {TABLE_NAME} ORDER BY league_name')
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]}")
        
        print(f"\n✅ Done! Leagues stored in {TABLE_NAME}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    store_leagues()
