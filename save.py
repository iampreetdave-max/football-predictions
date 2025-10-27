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

def check_match_exists(cursor, match_id):
    """Check if match_id already exists in database"""
    cursor.execute(f"SELECT match_id FROM {TABLE_NAME} WHERE match_id = %s", (match_id,))
    return cursor.fetchone() is not None

def insert_record(cursor, data):
    """Insert new record"""
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        match_id, date, league, home_team, away_team, 
        home_odds, away_odds, draw_odds, over_2_5_odds, under_2_5_odds,
        ctmcl, predicted_home_goals, predicted_away_goals, confidence, delta,
        predicted_over_under, actual_over_under, predicted_winner, actual_winner,
        status, profit_loss_over_under, profit_loss_moneyline, data_source, created_date
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    
    cursor.execute(insert_query, (
        data['match_id'], data['date'], data['league'], data['home_team'], data['away_team'],
        data['home_odds'], data['away_odds'], data['draw_odds'], data['over_2_5_odds'], data['under_2_5_odds'],
        data['ctmcl'], data['predicted_home_goals'], data['predicted_away_goals'], data['confidence'], data['delta'],
        data['predicted_over_under'], data['actual_over_under'], data['predicted_winner'], data['actual_winner'],
        data['status'], data['profit_loss_over_under'], data['profit_loss_moneyline'], data['data_source'], data['created_date']
    ))

def update_record(cursor, data):
    """Update existing record"""
    update_query = f"""
    UPDATE {TABLE_NAME} SET
        date = %s,
        league = %s,
        home_team = %s,
        away_team = %s,
        home_odds = %s,
        away_odds = %s,
        draw_odds = %s,
        over_2_5_odds = %s,
        under_2_5_odds = %s,
        ctmcl = %s,
        predicted_home_goals = %s,
        predicted_away_goals = %s,
        confidence = %s,
        delta = %s,
        predicted_over_under = %s,
        predicted_winner = %s,
        status = %s,
        data_source = %s,
        created_date = %s
    WHERE match_id = %s
    """
    
    cursor.execute(update_query, (
        data['date'], data['league'], data['home_team'], data['away_team'],
        data['home_odds'], data['away_odds'], data['draw_odds'], data['over_2_5_odds'], data['under_2_5_odds'],
        data['ctmcl'], data['predicted_home_goals'], data['predicted_away_goals'], data['confidence'], data['delta'],
        data['predicted_over_under'], data['predicted_winner'], data['status'], data['data_source'], data['created_date'],
        data['match_id']
    ))

def process_data(conn, df):
    """Process and insert/update data"""
    cursor = conn.cursor()
    inserted = 0
    updated = 0
    errors = 0
    
    for idx, row in df.iterrows():
        try:
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
            
            # Check if match_id exists
            if check_match_exists(cursor, data['match_id']):
                update_record(cursor, data)
                updated += 1
                print(f"  Updated: match_id={data['match_id']}")
            else:
                insert_record(cursor, data)
                inserted += 1
                print(f"  Inserted: match_id={data['match_id']}")
                
        except Exception as e:
            errors += 1
            print(f"  ✗ Error processing row {idx}: {e}")
            continue
    
    conn.commit()
    cursor.close()
    
    return inserted, updated, errors

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
        # Process data
        print(f"\nProcessing {len(df)} records...")
        inserted, updated, errors = process_data(conn, df)
        
        print("\n" + "=" * 50)
        print("Summary:")
        print(f"  ✓ Inserted: {inserted} records")
        print(f"  ✓ Updated: {updated} records")
        if errors > 0:
            print(f"  ✗ Errors: {errors} records")
        print("=" * 50)
        print("✓ Upload completed successfully!")
        print("=" * 50)
            
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("✓ Database connection closed")

if __name__ == "__main__":
    main()
