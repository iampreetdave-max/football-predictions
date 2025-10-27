"""
Insert Best Match Predictions to PostgreSQL Database - SIMPLE VERSION
Reads best_match_predictions.csv and inserts NEW data into soccer_predsv1 table
Skips duplicates instead of updating them
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
print("BEST MATCH PREDICTIONS - DATABASE IMPORT (SIMPLE VERSION)")
print("="*80)

# ==================== HELPER FUNCTIONS ====================

def probability_to_odds(probability):
    """Convert probability (0-1) to decimal odds"""
    if pd.isna(probability) or probability <= 0:
        return None
    return round(1 / probability, 3)

def potential_to_odds(potential):
    """Convert potential (0-100) to decimal odds"""
    if pd.isna(potential) or potential <= 0:
        return None
    probability = potential / 100
    return round(1 / probability, 3)

def calculate_draw_odds(home_prob, away_prob):
    """Calculate draw odds from home and away probabilities"""
    if pd.isna(home_prob) or pd.isna(away_prob):
        return None
    draw_prob = 1 - (home_prob + away_prob)
    if draw_prob <= 0:
        return None
    return round(1 / draw_prob, 3)

# ==================== LOAD DATA ====================

print(f"\n[1/4] Loading CSV file: {CSV_FILE}")
try:
    df = pd.read_csv(CSV_FILE)
    print(f"✓ Loaded {len(df)} records from CSV")
except Exception as e:
    print(f"✗ Error loading CSV: {e}")
    sys.exit(1)

# ==================== TRANSFORM DATA ====================

print(f"\n[2/4] Transforming data...")

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
db_data['actual_over_under'] = None
db_data['predicted_winner'] = df['outcome_label']
db_data['actual_winner'] = None
db_data['status'] = df['status']
db_data['profit_loss_over_under'] = None
db_data['profit_loss_moneyline'] = None
db_data['data_source'] = 'FootyStats_API'

print(f"✓ Transformed {len(db_data)} records")

# ==================== CONNECT TO DATABASE ====================

print(f"\n[3/4] Connecting to database...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✓ Connected to database")
except Exception as e:
    print(f"✗ Connection error: {e}")
    sys.exit(1)

# ==================== GET EXISTING MATCH IDS ====================

print(f"\n[4/4] Checking for existing records...")
cursor.execute(sql.SQL("SELECT match_id FROM {}").format(sql.Identifier(TABLE_NAME)))
existing_ids = set([row[0] for row in cursor.fetchall()])
print(f"✓ Found {len(existing_ids)} existing records")

# Filter out existing records
new_data = db_data[~db_data['match_id'].isin(existing_ids)]
print(f"✓ {len(new_data)} new records to insert")
print(f"✓ {len(db_data) - len(new_data)} records already exist (skipping)")

if len(new_data) == 0:
    print("\n✓ All records already exist in database. Nothing to insert.")
    cursor.close()
    conn.close()
    sys.exit(0)

# ==================== INSERT DATA ====================

print(f"\nInserting {len(new_data)} new records...")

insert_query = sql.SQL("""
    INSERT INTO {} (
        match_id, date, league, home_team, away_team,
        home_odds, away_odds, draw_odds, over_2_5_odds, under_2_5_odds,
        ctmcl, predicted_home_goals, predicted_away_goals, confidence, delta,
        predicted_over_under, actual_over_under, predicted_winner, actual_winner,
        status, profit_loss_over_under, profit_loss_moneyline, data_source
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
""").format(sql.Identifier(TABLE_NAME))

inserted = 0
errors = 0

for idx, row in new_data.iterrows():
    try:
        # Replace NaN with None
        values = [None if pd.isna(v) else v for v in row.values]
        cursor.execute(insert_query, values)
        inserted += 1
        
        if inserted % 50 == 0:
            conn.commit()
            print(f"  Inserted {inserted}/{len(new_data)} records...")
    except Exception as e:
        errors += 1
        print(f"  ⚠ Error inserting match_id {row['match_id']}: {e}")
        conn.rollback()

# Final commit
conn.commit()

print(f"\n✓ Insertion complete!")
print(f"  Successfully inserted: {inserted}")
print(f"  Errors: {errors}")

# ==================== VERIFY ====================

cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(TABLE_NAME)))
total = cursor.fetchone()[0]
print(f"\n✓ Total records in database: {total}")

cursor.close()
conn.close()

print("\n" + "="*80)
print("✅ IMPORT COMPLETE!")
print("="*80)
