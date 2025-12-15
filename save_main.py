"""
Save Best Match Predictions to PostgreSQL Database
Reads best_match_predictions.csv and inserts new predictions into two tables:
- agility_soccer_v1
- soccer_v1_features
Both tables in the same database with identical columns
- Skips duplicate match_ids in each table separately
- Handles NULL values properly
- Sets initial values for fields that will be updated by validation script
- FIXED: Added confidence validation (0-1 range check)
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

TABLE_NAMES = ['agility_soccer_v1', 'soccer_v1_features']
CSV_FILE = 'best_match_predictions.csv'

# ==================== LEAGUE ID MAPPING ====================
LEAGUE_MAPPING = {
    12325: "England Premier League",
    15050: "England Premier League",
    14924: "UEFA Champions League",
   
    12316: "Spain La Liga",
    14956: "Spain La Liga",
    12530: "Italy Serie A",
    15068: "Italy Serie A",
    12529: "Germany Bundesliga",
    14968: "Germany Bundesliga",
    13973: "USA MLS",
    12337: "France Ligue 1",
    14932: "France Ligue 1",
    12322: "Netherlands Eredivisie",
    14936: "Netherlands Eredivisie",
   
    12136: "Mexico Liga MX",
    15115: "Portugal Liga NOS",
    15234: "Mexico Liga MX"
}

print("="*80)
print("AGILITY FOOTBALL PREDICTIONS - SAVE TO DUAL TABLES")
print("="*80)
print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"ℹ️  Tables: {', '.join(TABLE_NAMES)}")

# ==================== HELPER FUNCTIONS ====================
def get_league_name(league_id):
    """Get league name from league_id using the mapping"""
    try:
        league_id_int = int(league_id)
        return LEAGUE_MAPPING.get(league_id_int, "Unknown League")
    except:
        return "Unknown League"

def calculate_grade(confidence):
    """Calculate letter grade from confidence score (0-1 scale)"""
    if pd.isna(confidence):
        return None
    
    # FIXED: Validate confidence is in 0-1 range
    if confidence < 0 or confidence > 1:
        print(f"⚠️  Warning: Confidence {confidence} outside 0-1 range (clipping to valid range)")
        confidence = max(0, min(1, confidence))
    
    score = confidence * 100
    
    if score >= 90:
        return "A+"
    elif score >= 85:
        return "A"
    elif score >= 80:
        return "A-"
    elif score >= 75:
        return "B+"
    elif score >= 70:
        return "B"
    elif score >= 65:
        return "B-"
    elif score >= 60:
        return "C+"
    elif score >= 55:
        return "C"
    elif score >= 50:
        return "C-"
    else:
        return "D"

# ==================== LOAD CSV DATA ====================
print(f"\n[1/5] Loading CSV file: {CSV_FILE}")
try:
    # Try to find the CSV file in multiple locations
    csv_path = Path(CSV_FILE)
    if not csv_path.exists():
        csv_path = Path(__file__).parent / CSV_FILE
    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find {CSV_FILE}")
    
    df = pd.read_csv(csv_path)
    print(f"✓ Loaded {len(df)} records from CSV")
    print(f"  Columns found: {len(df.columns)}")
    
    # Display sample data
    print(f"\n  Sample data (first row):")
    for col in df.columns[:5]:
        print(f"    {col}: {df[col].iloc[0]}")
    
except Exception as e:
    print(f"✗ Error loading CSV: {e}")
    sys.exit(1)

# ==================== VERIFY REQUIRED COLUMNS ====================
print(f"\n[2/5] Verifying required columns...")

required_columns = {
    'match_id': 'Match ID',
    'date': 'Match Date',
    'league_id': 'League',
    'home_team_name': 'Home Team',
    'away_team_name': 'Away Team',
    'odds_ft_1': 'Home Odds',
    'odds_ft_x': 'Draw Odds',
    'odds_ft_2': 'Away Odds',
    'odds_ft_over25': 'Over 2.5 Odds',
    'odds_ft_under25': 'Under 2.5 Odds',
    'CTMCL': 'CTMCL Value',
    'predicted_home_goals': 'Predicted Home Goals',
    'predicted_away_goals': 'Predicted Away Goals',
    'confidence': 'Confidence Score',
    'predicted_goal_diff': 'Goal Difference',
    'ctmcl_prediction': 'Over/Under Prediction',
    'outcome_label': 'Winner Prediction',
    'status': 'Match Status',
    'confidence_category': 'Confidence Category'
}

missing_cols = [col for col in required_columns.keys() if col not in df.columns]
if missing_cols:
    print(f"✗ Missing required columns:")
    for col in missing_cols:
        print(f"  • {col} ({required_columns[col]})")
    sys.exit(1)

print(f"✓ All required columns present")

# ==================== TRANSFORM DATA ====================
print(f"\n[3/5] Transforming data for database...")

# Define exact column order matching INSERT statement (31 columns, NO id column)
db_columns = [
    'match_id', 'date', 'league', 'league_name', 'home_id', 'away_id', 'home_team', 'away_team',
    'home_odds', 'away_odds', 'draw_odds', 'over_2_5_odds', 'under_2_5_odds',
    'ctmcl', 'predicted_home_goals', 'predicted_away_goals', 'confidence', 'grade', 'delta',
    'predicted_outcome', 'predicted_winner',
    'status', 'data_source', 'confidence_category',
    'actual_over_under', 'actual_winner', 'profit_loss_outcome', 'profit_loss_winner',
    'actual_home_team_goals', 'actual_away_team_goals', 'actual_total_goals'
]

db_data = pd.DataFrame(index=df.index)

# Map CSV columns to database columns
db_data['match_id'] = df['match_id']
db_data['date'] = df['date']
db_data['league'] = df['league_id'].astype(str)
db_data['league_name'] = df['league_id'].apply(get_league_name)
db_data['home_id'] = df['home_team_id']
db_data['away_id'] = df['away_team_id']
db_data['home_team'] = df['home_team_name']
db_data['away_team'] = df['away_team_name']

# Betting odds
db_data['home_odds'] = df['odds_ft_1']
db_data['away_odds'] = df['odds_ft_2']
db_data['draw_odds'] = df['odds_ft_x']
db_data['over_2_5_odds'] = df['odds_ft_over25']
db_data['under_2_5_odds'] = df['odds_ft_under25']

# Prediction metrics
db_data['ctmcl'] = df['CTMCL']
db_data['predicted_home_goals'] = df['predicted_home_goals']
db_data['predicted_away_goals'] = df['predicted_away_goals']
db_data['confidence'] = df['confidence']
db_data['grade'] = df['confidence'].apply(calculate_grade)
db_data['delta'] = df['predicted_goal_diff']

# Predictions
db_data['predicted_outcome'] = df['ctmcl_prediction']
db_data['predicted_winner'] = df['outcome_label']

# Status and source
db_data['status'] = df['status']
db_data['data_source'] = 'FootyStats_API'
db_data['confidence_category'] = df['confidence_category']

# Fields to be updated later by validation script (set as NULL initially)
db_data['actual_over_under'] = None
db_data['actual_winner'] = None
db_data['profit_loss_outcome'] = None
db_data['profit_loss_winner'] = None
db_data['actual_home_team_goals'] = None
db_data['actual_away_team_goals'] = None
db_data['actual_total_goals'] = None

# Reorder to match INSERT statement exactly
db_data = db_data[db_columns]

print(f"✓ Transformed {len(db_data)} records")
print(f"  Fields mapped: {len(db_data.columns)}")
print(f"  Column order verified: {len(db_data.columns) == len(db_columns)} (31 expected)")

# Show league name mapping summary
league_counts = db_data['league_name'].value_counts()
print(f"\n  🏆 League distribution:")
for league, count in league_counts.items():
    print(f"    • {league}: {count} matches")

# ==================== INSERT TO TABLES ====================
def insert_to_table(table_name, db_data):
    """Insert data to a specific table with its own transaction"""
    print(f"\n[4/5] Processing table: {table_name}")
    print("-" * 80)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print(f"✓ Connected to database")
        print(f"  Host: {DB_CONFIG['host']}")
        print(f"  Database: {DB_CONFIG['database']}")
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return {'success': False, 'table': table_name, 'inserted': 0, 'errors': 0}

    # Check for existing records
    print(f"\nChecking for existing records in {table_name}...")
    try:
        cursor.execute(sql.SQL("SELECT match_id FROM {}").format(sql.Identifier(table_name)))
        existing_ids = set([row[0] for row in cursor.fetchall()])
        print(f"✓ Found {len(existing_ids)} existing records")
    except Exception as e:
        print(f"✗ Error querying existing records: {e}")
        cursor.close()
        conn.close()
        return {'success': False, 'table': table_name, 'inserted': 0, 'errors': 0}

    # Filter out existing records
    new_data = db_data[~db_data['match_id'].isin(existing_ids)]
    duplicate_count = len(db_data) - len(new_data)

    print(f"\n  Records breakdown:")
    print(f"    • Total in CSV: {len(db_data)}")
    print(f"    • Already in {table_name}: {duplicate_count}")
    print(f"    • New to insert: {len(new_data)}")

    if len(new_data) == 0:
        print(f"\n✓ All records already exist in {table_name}. Nothing to insert.")
        cursor.close()
        conn.close()
        return {'success': True, 'table': table_name, 'inserted': 0, 'errors': 0, 'skipped': True}

    # Insert new records
    print(f"\nInserting {len(new_data)} new records to {table_name}...")

    insert_query = sql.SQL("""
        INSERT INTO {} (
            match_id, date, league, league_name, home_id, away_id, home_team, away_team,
            home_odds, away_odds, draw_odds, over_2_5_odds, under_2_5_odds,
            ctmcl, predicted_home_goals, predicted_away_goals, confidence, grade, delta,
            predicted_outcome, predicted_winner,
            status, data_source, confidence_category,
            actual_over_under, actual_winner, profit_loss_outcome, profit_loss_winner,
            actual_home_team_goals, actual_away_team_goals, actual_total_goals
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """).format(sql.Identifier(table_name))

    inserted = 0
    errors = 0
    error_details = []

    for idx, row in new_data.iterrows():
        try:
            values = [None if pd.isna(v) else v for v in row.values]
            
            if len(values) != 31:
                raise ValueError(f"Expected 31 columns, got {len(values)}")
            
            cursor.execute(insert_query, values)
            inserted += 1
            
            if inserted % 10 == 0:
                print(f"  Progress: {inserted}/{len(new_data)} records inserted...")
                
        except Exception as e:
            errors += 1
            error_msg = f"Match ID {row['match_id']}: {str(e)[:100]}"
            error_details.append(error_msg)

    # Commit this transaction
    try:
        conn.commit()
        print(f"\n✓ Successfully committed {inserted} records to {table_name}")
    except Exception as e:
        print(f"\n✗ Error committing to {table_name}: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return {'success': False, 'table': table_name, 'inserted': 0, 'errors': len(new_data)}

    # Verify database state
    print(f"Verifying {table_name}...")
    try:
        cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        total = cursor.fetchone()[0]
        print(f"  Total records in {table_name}: {total}")
    except Exception as e:
        print(f"⚠ Could not retrieve database statistics: {e}")

    cursor.close()
    conn.close()
    
    return {
        'success': True,
        'table': table_name,
        'inserted': inserted,
        'errors': errors,
        'error_details': error_details if errors > 0 else None
    }

# ==================== EXECUTE INSERTS TO BOTH TABLES ====================
print("\n" + "="*80)
print("DUAL TABLE INSERT PROCESS")
print("="*80)

results = []

# Insert to first table
result1 = insert_to_table(TABLE_NAMES[0], db_data)
results.append(result1)

# Insert to second table (separate transaction)
result2 = insert_to_table(TABLE_NAMES[1], db_data)
results.append(result2)

# ==================== FINAL SUMMARY ====================
print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)

all_success = all(r['success'] for r in results)

for result in results:
    table_name = result['table']
    if result['success']:
        if result.get('skipped'):
            print(f"✓ {table_name}: No new records to insert (all duplicates)")
        else:
            print(f"✓ {table_name}: {result['inserted']} records inserted")
            if result['errors'] > 0:
                print(f"  ⚠️  {result['errors']} errors encountered (skipped)")
    else:
        print(f"✗ {table_name}: FAILED - 0 records inserted")

print("\n" + "="*80)
if all_success:
    print("✅ SUCCESS! Data processing complete for both tables")
else:
    print("⚠️  Some tables failed. Review details above.")

print("\nNext steps:")
print("  • Wait for matches to complete")
print("  • Run validation script to update actual results")
print("  • Both tables will be updated with match outcomes")
print("="*80)
