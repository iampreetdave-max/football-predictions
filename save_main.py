"""
Save Best Match Predictions to PostgreSQL Database
Reads best_match_predictions.csv and inserts new predictions into agility_football_pred table
- Skips duplicate match_ids
- Handles NULL values properly
- Sets initial values for fields that will be updated by validation script
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
import sys
from pathlib import Path

# ==================== DATABASE CONFIGURATION ====================
DB_CONFIG = {
    'host': 'winbets-db.postgres.database.azure.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'winbets',
    'password': 'deeptanshu@123'
}

TABLE_NAME = 'agility_football_pred'
CSV_FILE = 'best_match_predictions.csv'

# ==================== LEAGUE ID MAPPING ====================
LEAGUE_MAPPING = {
    12325: "England Premier League",
    15050: "England Premier League",
    13497: "Europe UEFA Youth League",
    16004: "Europe UEFA Youth League",
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
    12585: "Portugal LigaPro",
    15717: "Portugal LigaPro",
    12136: "Mexico Liga MX",
    15234: "Mexico Liga MX"
}

print("="*80)
print("AGILITY FOOTBALL PREDICTIONS - SAVE TO DATABASE")
print("="*80)
print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

# ==================== HELPER FUNCTIONS ====================
def get_league_name(league_id):
    """Get league name from league_id using the mapping"""
    try:
        league_id_int = int(league_id)
        return LEAGUE_MAPPING.get(league_id_int, "Unknown League")
    except:
        return "Unknown League"

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

db_data = pd.DataFrame()

# Map CSV columns to database columns
db_data['match_id'] = df['match_id']
db_data['date'] = df['date']
db_data['league'] = df['league_id'].astype(str)
db_data['league_name'] = df['league_id'].apply(get_league_name)
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

print(f"✓ Transformed {len(db_data)} records")
print(f"  Fields mapped: {len(db_data.columns)}")

# Show league name mapping summary
league_counts = db_data['league_name'].value_counts()
print(f"\n  📊 League distribution:")
for league, count in league_counts.items():
    print(f"    • {league}: {count} matches")

# Debug: Show first row mapping
if len(db_data) > 0:
    print(f"\n  Sample mapping (first record):")
    first_row = db_data.iloc[0]
    print(f"    match_id: {first_row['match_id']}")
    print(f"    league: {first_row['league']}")
    print(f"    league_name: {first_row['league_name']}")
    print(f"    home_team: {first_row['home_team']}")
    print(f"    away_team: {first_row['away_team']}")
    print(f"    status: {first_row['status']}")
    print(f"    confidence_category: {first_row['confidence_category']}")
    print(f"    predicted_outcome: {first_row['predicted_outcome']}")
    print(f"    predicted_winner: {first_row['predicted_winner']}")


# ==================== CONNECT TO DATABASE ====================
print(f"\n[4/5] Connecting to database...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✓ Connected to database successfully")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  Database: {DB_CONFIG['database']}")
except Exception as e:
    print(f"✗ Connection error: {e}")
    print(f"\nTroubleshooting:")
    print(f"  • Check if database server is running")
    print(f"  • Verify credentials are correct")
    print(f"  • Check firewall/network settings")
    sys.exit(1)

# ==================== CHECK FOR EXISTING RECORDS ====================
print(f"\nChecking for existing records...")
try:
    cursor.execute(sql.SQL("SELECT match_id FROM {}").format(sql.Identifier(TABLE_NAME)))
    existing_ids = set([row[0] for row in cursor.fetchall()])
    print(f"✓ Found {len(existing_ids)} existing records in database")
except Exception as e:
    print(f"✗ Error querying existing records: {e}")
    cursor.close()
    conn.close()
    sys.exit(1)

# Filter out existing records
new_data = db_data[~db_data['match_id'].isin(existing_ids)]
duplicate_count = len(db_data) - len(new_data)

print(f"\n  Records breakdown:")
print(f"    • Total in CSV: {len(db_data)}")
print(f"    • Already in DB: {duplicate_count}")
print(f"    • New to insert: {len(new_data)}")

if len(new_data) == 0:
    print("\n✓ All records already exist in database. Nothing to insert.")
    cursor.close()
    conn.close()
    print("\n" + "="*80)
    print("✅ SAVE COMPLETE - NO NEW RECORDS")
    print("="*80)
    sys.exit(0)

# ==================== INSERT NEW RECORDS ====================
print(f"\n[5/5] Inserting {len(new_data)} new records...")

insert_query = sql.SQL("""
    INSERT INTO {} (
        match_id, date, league, league_name, home_team, away_team,
        home_odds, away_odds, draw_odds, over_2_5_odds, under_2_5_odds,
        ctmcl, predicted_home_goals, predicted_away_goals, confidence, delta,
        predicted_outcome, predicted_winner,
        status, data_source, confidence_category,
        actual_over_under, actual_winner, profit_loss_outcome, profit_loss_winner,
        actual_home_team_goals, actual_away_team_goals, actual_total_goals
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s
    )
""").format(sql.Identifier(TABLE_NAME))

inserted = 0
errors = 0
error_details = []

for idx, row in new_data.iterrows():
    try:
        # Replace NaN with None for proper NULL handling
        values = [None if pd.isna(v) else v for v in row.values]
        cursor.execute(insert_query, values)
        inserted += 1
        
        # Commit every 10 records for better safety
        if inserted % 10 == 0:
            conn.commit()
            print(f"  Progress: {inserted}/{len(new_data)} records inserted and committed...")
            
    except Exception as e:
        errors += 1
        error_msg = f"Match ID {row['match_id']}: {str(e)[:100]}"
        error_details.append(error_msg)
        print(f"  ⚠ Error: {error_msg}")
        conn.rollback()

# Final commit
try:
    conn.commit()
    print(f"\n✓ Database commit successful")
except Exception as e:
    print(f"\n✗ Error committing to database: {e}")
    conn.rollback()

# ==================== SUMMARY ====================
print(f"\n" + "="*80)
print("INSERTION SUMMARY")
print("="*80)
print(f"✓ Successfully inserted: {inserted} records")
if errors > 0:
    print(f"⚠ Errors encountered: {errors} records")
    print(f"\nError details:")
    for i, error in enumerate(error_details[:5], 1):
        print(f"  {i}. {error}")
    if len(error_details) > 5:
        print(f"  ... and {len(error_details) - 5} more errors")

# ==================== VERIFY DATABASE STATE ====================
print(f"\n" + "="*80)
print("DATABASE STATE")
print("="*80)

try:
    # Total records
    cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(TABLE_NAME)))
    total = cursor.fetchone()[0]
    print(f"  Total records in database: {total}")
    
    # Records by league
    cursor.execute(sql.SQL("""
        SELECT league_name, COUNT(*) 
        FROM {} 
        GROUP BY league_name
        ORDER BY COUNT(*) DESC
    """).format(sql.Identifier(TABLE_NAME)))
    
    league_counts = cursor.fetchall()
    if league_counts:
        print(f"\n  Records by league:")
        for league, count in league_counts:
            print(f"    • {league}: {count}")
    
    # Records by status
    cursor.execute(sql.SQL("""
        SELECT status, COUNT(*) 
        FROM {} 
        GROUP BY status
    """).format(sql.Identifier(TABLE_NAME)))
    
    status_counts = cursor.fetchall()
    if status_counts:
        print(f"\n  Records by status:")
        for status, count in status_counts:
            print(f"    • {status}: {count}")
    
    # Records by confidence category
    cursor.execute(sql.SQL("""
        SELECT confidence_category, COUNT(*) 
        FROM {} 
        GROUP BY confidence_category
        ORDER BY 
            CASE confidence_category 
                WHEN 'High' THEN 1 
                WHEN 'Medium' THEN 2 
                WHEN 'Low' THEN 3 
                ELSE 4 
            END
    """).format(sql.Identifier(TABLE_NAME)))
    
    confidence_counts = cursor.fetchall()
    if confidence_counts:
        print(f"\n  Records by confidence:")
        for category, count in confidence_counts:
            print(f"    • {category}: {count}")
    
    # Pending validations
    cursor.execute(sql.SQL("""
        SELECT COUNT(*) 
        FROM {} 
        WHERE actual_winner IS NULL
    """).format(sql.Identifier(TABLE_NAME)))
    
    pending = cursor.fetchone()[0]
    print(f"\n  Pending validation: {pending} records")
    
except Exception as e:
    print(f"⚠ Could not retrieve database statistics: {e}")

# ==================== FINAL COMMIT ====================
print(f"\nEnsuring all changes are committed...")
try:
    conn.commit()
    print("✓ Final commit successful - all data saved!")
except Exception as e:
    print(f"✗ Final commit error: {e}")

# ==================== CLOSE CONNECTION ====================
cursor.close()
conn.close()
print(f"✓ Database connection closed")

print("\n" + "="*80)
print("✅ SAVE COMPLETE!")
print("="*80)
print(f"\nNext steps:")
print(f"  • Wait for matches to complete")
print(f"  • Run '3_validate_predictions.py' to update actual results")
print(f"  • Automated daily via GitHub Actions workflow")
print("="*80)
