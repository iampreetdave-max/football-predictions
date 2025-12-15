"""
Push Feature Columns to soccer_v1_features Table
Reads best_match_predictions.csv and inserts feature columns into soccer_v1_features
- Simple CSV to database insertion
- Skips duplicate match_ids
- Only inserts specified feature columns
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
import sys
from pathlib import Path
import os

# ==================== DATABASE CONFIGURATION ====================
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

TABLE_NAME = 'soccer_v1_features'
CSV_FILE = 'extracted_features_complete.csv'

# Feature columns to insert (mapped from CSV)
FEATURE_COLUMNS = [
    'CTMCL',
    'avg_goals_market',
    'pre_match_home_ppg',
    'pre_match_away_ppg',
    'home_xg_avg',
    'away_xg_avg',
    'home_xg_momentum',
    'away_xg_momentum',
    'home_goals_conceded_avg',
    'away_goals_conceded_avg',
    'o25_potential',
    'o35_potential',
    'home_shots_accuracy_avg',
    'away_shots_accuracy_avg',
    'home_dangerous_attacks_avg',
    'away_dangerous_attacks_avg',
    'h2h_total_goals_avg',
    'home_form_points',
    'away_form_points',
    'home_elo',
    'away_elo',
    'elo_diff',
    'league_avg_goals',
    'odds_ft_1_prob',
    'odds_ft_2_prob',
    'btts_potential',
    'o05_potential',
    'o15_potential',
    'o45_potential',
]

print("="*80)
print("PUSH FEATURE COLUMNS TO DATABASE")
print("="*80)
print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"Table: {TABLE_NAME}")
print(f"Total features: {len(FEATURE_COLUMNS)}")

# ==================== LOAD CSV DATA ====================
print(f"\n[1/4] Loading CSV file: {CSV_FILE}")
try:
    csv_path = Path(CSV_FILE)
    if not csv_path.exists():
        csv_path = Path(__file__).parent / CSV_FILE
    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find {CSV_FILE}")
    
    df = pd.read_csv(csv_path)
    print(f"✓ Loaded {len(df)} records from CSV")
    
except Exception as e:
    print(f"✗ Error loading CSV: {e}")
    sys.exit(1)

# ==================== VERIFY REQUIRED COLUMNS ====================
print(f"\n[2/4] Verifying required columns...")

required_cols = ['match_id'] + FEATURE_COLUMNS
missing_cols = [col for col in required_cols if col not in df.columns]

if missing_cols:
    print(f"✗ Missing columns:")
    for col in missing_cols:
        print(f"  • {col}")
    print(f"\nAvailable columns in CSV:")
    for col in df.columns:
        print(f"  • {col}")
    sys.exit(1)

print(f"✓ All required columns present")

# ==================== PREPARE DATA ====================
print(f"\n[3/4] Preparing data for insertion...")

# Select only match_id and feature columns
db_data = df[required_cols].copy()

# Replace NaN with None for proper NULL handling
db_data = db_data.where(pd.notna(db_data), None)

print(f"✓ Prepared {len(db_data)} records")

# ==================== DATABASE CONNECTION ====================
print(f"\nConnecting to database...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print(f"✓ Connected")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  Database: {DB_CONFIG['database']}")
except Exception as e:
    print(f"✗ Connection error: {e}")
    sys.exit(1)

# ==================== CHECK FOR DUPLICATES ====================
print(f"\nChecking for existing records...")
try:
    cursor.execute(sql.SQL("SELECT match_id FROM {}").format(sql.Identifier(TABLE_NAME)))
    existing_ids = set([row[0] for row in cursor.fetchall()])
    print(f"✓ Found {len(existing_ids)} existing records")
except Exception as e:
    print(f"✗ Error querying existing records: {e}")
    cursor.close()
    conn.close()
    sys.exit(1)

# Filter out duplicates
new_data = db_data[~db_data['match_id'].isin(existing_ids)]
duplicate_count = len(db_data) - len(new_data)

print(f"\n  Records breakdown:")
print(f"    • Total in CSV: {len(db_data)}")
print(f"    • Already in DB: {duplicate_count}")
print(f"    • New to insert: {len(new_data)}")

if len(new_data) == 0:
    print(f"\n✓ All records already exist. Nothing to insert.")
    cursor.close()
    conn.close()
    sys.exit(0)

# ==================== BUILD INSERT QUERY ====================
print(f"\n[4/4] Inserting {len(new_data)} records...")

# Build column list for INSERT
columns_str = ', '.join(['match_id'] + FEATURE_COLUMNS)
placeholders = ', '.join(['%s'] * len(required_cols))

insert_query = sql.SQL("""
    INSERT INTO {} ({})
    VALUES ({})
""").format(
    sql.Identifier(TABLE_NAME),
    sql.SQL(columns_str),
    sql.SQL(placeholders)
)

# ==================== INSERT RECORDS ====================
inserted = 0
errors = 0
error_details = []

for idx, row in new_data.iterrows():
    try:
        values = [row[col] for col in required_cols]
        cursor.execute(insert_query, values)
        inserted += 1
        
        if inserted % 10 == 0:
            print(f"  Progress: {inserted}/{len(new_data)} records inserted...")
            
    except Exception as e:
        errors += 1
        error_msg = f"Match ID {row['match_id']}: {str(e)[:100]}"
        error_details.append(error_msg)

# ==================== COMMIT ====================
try:
    conn.commit()
    print(f"\n✓ Successfully committed {inserted} records")
except Exception as e:
    print(f"\n✗ Error committing: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    sys.exit(1)

# ==================== VERIFY ====================
print(f"\nVerifying database...")
try:
    cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(TABLE_NAME)))
    total = cursor.fetchone()[0]
    print(f"✓ Total records in {TABLE_NAME}: {total}")
except Exception as e:
    print(f"⚠ Could not verify: {e}")

cursor.close()
conn.close()

# ==================== FINAL SUMMARY ====================
print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)
print(f"✅ Successfully inserted {inserted} records")
if errors > 0:
    print(f"⚠️  Errors encountered: {errors} records (skipped)")
    for detail in error_details[:5]:
        print(f"    • {detail}")
print("="*80)
