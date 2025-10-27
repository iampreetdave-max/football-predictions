"""
FORCE UPDATE Best Match Predictions to PostgreSQL Database
Reads best_match_predictions.csv and UPDATES existing records in soccer_predsv1 table
Uses match_id as the reference key
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
import sys

# ==================== DATABASE CONFIGURATION ====================
DB_CONFIG = {
    'host': 'winbets-db.postgres.database.azure.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'app_user',
    'password': 'StrongPassword123!'
}

TABLE_NAME = 'soccer_predsv1'
CSV_FILE = 'best_match_predictions.csv'

print("="*80)
print("FORCE UPDATE PREDICTIONS - DATABASE UPDATE")
print("="*80)

# ==================== LOAD DATA ====================

print(f"\n[1/5] Loading CSV file: {CSV_FILE}")
try:
    df = pd.read_csv(CSV_FILE)
    print(f"✓ Loaded {len(df)} records from CSV")
except Exception as e:
    print(f"✗ Error loading CSV: {e}")
    sys.exit(1)

# ==================== TRANSFORM DATA ====================

print(f"\n[2/5] Transforming data...")

db_data = pd.DataFrame()
db_data['match_id'] = df['match_id']
db_data['date'] = df['date']
db_data['league'] = df['league_id'].astype(str)
db_data['home_team'] = df['home_team_name']
db_data['away_team'] = df['away_team_name']
db_data['home_odds'] = df['odds_ft_1']
db_data['away_odds'] = df['odds_ft_2']
db_data['draw_odds'] = df['odds_ft_x']
db_data['over_2_5_odds'] = df['odds_ft_over25']
db_data['under_2_5_odds'] = df['odds_ft_under25']
db_data['ctmcl'] = df['CTMCL']
db_data['predicted_home_goals'] = df['predicted_home_goals']
db_data['predicted_away_goals'] = df['predicted_away_goals']
db_data['confidence'] = df['confidence_category']
db_data['delta'] = df['predicted_goal_diff']
db_data['predicted_over_under'] = df['ctmcl_prediction']
db_data['predicted_winner'] = df['outcome_label']
db_data['status'] = df['status']
db_data['data_source'] = 'FootyStats_API'

# Note: We DO NOT set actual_winner, actual_over_under, profit_loss fields
# These are set by validate_predictions.py and should not be overwritten

print(f"✓ Transformed {len(db_data)} records")

# ==================== CONNECT TO DATABASE ====================

print(f"\n[3/5] Connecting to database...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✓ Connected to database")
except Exception as e:
    print(f"✗ Connection error: {e}")
    sys.exit(1)

# ==================== GET EXISTING MATCH IDS ====================

print(f"\n[4/5] Checking for existing records...")
cursor.execute(sql.SQL("SELECT match_id FROM {}").format(sql.Identifier(TABLE_NAME)))
existing_ids = set([row[0] for row in cursor.fetchall()])
print(f"✓ Found {len(existing_ids)} existing records in database")

# Separate into INSERT and UPDATE
new_records = db_data[~db_data['match_id'].isin(existing_ids)]
existing_records = db_data[db_data['match_id'].isin(existing_ids)]

print(f"✓ {len(new_records)} new records to INSERT")
print(f"✓ {len(existing_records)} existing records to UPDATE")

# ==================== UPDATE EXISTING RECORDS ====================

print(f"\n[5/5] Updating existing records...")

update_query = sql.SQL("""
    UPDATE {table}
    SET 
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
        data_source = %s
    WHERE match_id = %s
""").format(table=sql.Identifier(TABLE_NAME))

updated = 0
update_errors = 0

for idx, row in existing_records.iterrows():
    try:
        values = [
            row['date'],
            row['league'],
            row['home_team'],
            row['away_team'],
            row['home_odds'],
            row['away_odds'],
            row['draw_odds'],
            row['over_2_5_odds'],
            row['under_2_5_odds'],
            row['ctmcl'],
            row['predicted_home_goals'],
            row['predicted_away_goals'],
            row['confidence'],
            row['delta'],
            row['predicted_over_under'],
            row['predicted_winner'],
            row['status'],
            row['data_source'],
            row['match_id']  # WHERE condition
        ]
        
        # Replace NaN with None
        values = [None if pd.isna(v) else v for v in values]
        
        cursor.execute(update_query, values)
        updated += 1
        
        if updated % 50 == 0:
            conn.commit()
            print(f"  Updated {updated}/{len(existing_records)} records...")
    except Exception as e:
        update_errors += 1
        print(f"  ⚠ Error updating match_id {row['match_id']}: {e}")
        conn.rollback()

conn.commit()

print(f"\n✓ Update complete!")
print(f"  Successfully updated: {updated}")
print(f"  Update errors: {update_errors}")

# ==================== INSERT NEW RECORDS ====================

if len(new_records) > 0:
    print(f"\nInserting {len(new_records)} new records...")
    
    insert_query = sql.SQL("""
        INSERT INTO {table} (
            match_id, date, league, home_team, away_team,
            home_odds, away_odds, draw_odds, over_2_5_odds, under_2_5_odds,
            ctmcl, predicted_home_goals, predicted_away_goals, confidence, delta,
            predicted_over_under, actual_over_under, predicted_winner, actual_winner,
            status, profit_loss_over_under, profit_loss_moneyline, data_source
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """).format(table=sql.Identifier(TABLE_NAME))
    
    inserted = 0
    insert_errors = 0
    
    for idx, row in new_records.iterrows():
        try:
            values = [
                row['match_id'],
                row['date'],
                row['league'],
                row['home_team'],
                row['away_team'],
                row['home_odds'],
                row['away_odds'],
                row['draw_odds'],
                row['over_2_5_odds'],
                row['under_2_5_odds'],
                row['ctmcl'],
                row['predicted_home_goals'],
                row['predicted_away_goals'],
                row['confidence'],
                row['delta'],
                row['predicted_over_under'],
                None,  # actual_over_under
                row['predicted_winner'],
                None,  # actual_winner
                row['status'],
                None,  # profit_loss_over_under
                None,  # profit_loss_moneyline
                row['data_source']
            ]
            
            # Replace NaN with None
            values = [None if pd.isna(v) else v for v in values]
            
            cursor.execute(insert_query, values)
            inserted += 1
            
            if inserted % 50 == 0:
                conn.commit()
                print(f"  Inserted {inserted}/{len(new_records)} records...")
        except Exception as e:
            insert_errors += 1
            print(f"  ⚠ Error inserting match_id {row['match_id']}: {e}")
            conn.rollback()
    
    conn.commit()
    
    print(f"\n✓ Insertion complete!")
    print(f"  Successfully inserted: {inserted}")
    print(f"  Insert errors: {insert_errors}")
else:
    print(f"\n✓ No new records to insert")

# ==================== VERIFY ====================

cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(TABLE_NAME)))
total = cursor.fetchone()[0]
print(f"\n✓ Total records in database: {total}")

cursor.close()
conn.close()

# ==================== SUMMARY ====================

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"✓ Updated: {updated} records")
print(f"✓ Inserted: {inserted if len(new_records) > 0 else 0} new records")
print(f"✗ Errors: {update_errors + (insert_errors if len(new_records) > 0 else 0)}")
print(f"\n⚠ NOTE: This script does NOT overwrite:")
print(f"  • actual_winner")
print(f"  • actual_over_under")
print(f"  • profit_loss_over_under")
print(f"  • profit_loss_moneyline")
print(f"These are set by validate_predictions.py after matches are complete.")

print("\n" + "="*80)
print("✅ FORCE UPDATE COMPLETE!")
print("="*80)
