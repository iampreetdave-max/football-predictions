import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import sys

# Database configuration
DB_CONFIG = {
    'host': 'winbets-db.postgres.database.azure.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'app_user',
    'password': 'StrongPassword123!'
}

CSV_FILE = 'best_match_predictions.csv'
TABLE_NAME = 'soccer_predsv1'

def connect_db():
    """Establish database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Database connection successful")
        return conn
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        sys.exit(1)

def read_csv():
    """Read and validate CSV file"""
    try:
        df = pd.read_csv(CSV_FILE)
        print(f"✓ CSV file loaded: {len(df)} rows")
        return df
    except FileNotFoundError:
        print(f"✗ CSV file '{CSV_FILE}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        sys.exit(1)

def prepare_data(df):
    """Map CSV columns to database columns"""
    data_list = []
    
    for _, row in df.iterrows():
        data = {
            'match_id': row.get('match id'),
            'date': row.get('date'),
            'league': row.get('league id'),
            'home_team': row.get('home team name'),
            'away_team': row.get('away team name'),
            'home_odds': row.get('odds_ft_1'),
            'away_odds': row.get('odds_ft_2'),
            'draw_odds': row.get('odds_ft_x'),
            'over_2_5_odds': row.get('odds_ft_over25'),
            'under_2_5_odds': row.get('odds_ft_under25'),
            'ctmcl': row.get('CTMCL'),
            'predicted_home_goals': row.get('predicted_home_goals'),
            'predicted_away_goals': row.get('predicted_away_goals'),
            'confidence': row.get('confidence'),
            'delta': row.get('predicted_goal_diff'),
            'predicted_over_under': row.get('ctmcl_prediction'),
            'actual_over_under': None,
            'predicted_winner': row.get('outcome_label'),
            'actual_winner': None,
            'status': 'pending',
            'profit_loss_over_under': None,
            'profit_loss_moneyline': None,
            'data_source': 'footystats_API',
            'created_date': datetime.now()
        }
        data_list.append(data)
    
    return data_list

def upsert_data(conn, data_list):
    """Insert or update data in database"""
    cursor = conn.cursor()
    
    upsert_query = f"""
    INSERT INTO {TABLE_NAME} (
        match_id, date, league, home_team, away_team, 
        home_odds, away_odds, draw_odds, over_2_5_odds, under_2_5_odds,
        ctmcl, predicted_home_goals, predicted_away_goals, confidence, delta,
        predicted_over_under, actual_over_under, predicted_winner, actual_winner,
        status, profit_loss_over_under, profit_loss_moneyline, data_source, created_date
    ) VALUES %s
    ON CONFLICT (match_id) 
    DO UPDATE SET
        date = EXCLUDED.date,
        league = EXCLUDED.league,
        home_team = EXCLUDED.home_team,
        away_team = EXCLUDED.away_team,
        home_odds = EXCLUDED.home_odds,
        away_odds = EXCLUDED.away_odds,
        draw_odds = EXCLUDED.draw_odds,
        over_2_5_odds = EXCLUDED.over_2_5_odds,
        under_2_5_odds = EXCLUDED.under_2_5_odds,
        ctmcl = EXCLUDED.ctmcl,
        predicted_home_goals = EXCLUDED.predicted_home_goals,
        predicted_away_goals = EXCLUDED.predicted_away_goals,
        confidence = EXCLUDED.confidence,
        delta = EXCLUDED.delta,
        predicted_over_under = EXCLUDED.predicted_over_under,
        predicted_winner = EXCLUDED.predicted_winner,
        status = EXCLUDED.status,
        data_source = EXCLUDED.data_source,
        created_date = EXCLUDED.created_date
    """
    
    # Prepare values as tuples
    values = [
        (
            d['match_id'], d['date'], d['league'], d['home_team'], d['away_team'],
            d['home_odds'], d['away_odds'], d['draw_odds'], d['over_2_5_odds'], d['under_2_5_odds'],
            d['ctmcl'], d['predicted_home_goals'], d['predicted_away_goals'], d['confidence'], d['delta'],
            d['predicted_over_under'], d['actual_over_under'], d['predicted_winner'], d['actual_winner'],
            d['status'], d['profit_loss_over_under'], d['profit_loss_moneyline'], d['data_source'], d['created_date']
        )
        for d in data_list
    ]
    
    try:
        execute_values(cursor, upsert_query, values)
        conn.commit()
        print(f"✓ Successfully inserted/updated {len(data_list)} records")
        return True
    except Exception as e:
        conn.rollback()
        print(f"✗ Error during upsert: {e}")
        return False
    finally:
        cursor.close()

def main():
    """Main execution function"""
    print("=" * 50)
    print("CSV to PostgreSQL Data Uploader")
    print("=" * 50)
    
    # Read CSV
    df = read_csv()
    
    # Connect to database
    conn = connect_db()
    
    try:
        # Prepare data
        data_list = prepare_data(df)
        print(f"✓ Data prepared: {len(data_list)} records")
        
        # Upsert data
        success = upsert_data(conn, data_list)
        
        if success:
            print("=" * 50)
            print("✓ Upload completed successfully!")
            print("=" * 50)
        else:
            print("=" * 50)
            print("✗ Upload failed!")
            print("=" * 50)
            
    finally:
        conn.close()
        print("✓ Database connection closed")

if __name__ == "__main__":
    main()
