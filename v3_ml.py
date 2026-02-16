"""
Save Best Match Predictions to PostgreSQL Database
Reads best_match_predictions.csv and inserts new predictions into agility_soccer_v3
- Skips: predicted_OverUnder, actual_over_under, ou_confidence, ou_grade, predicted_outcome
- Only 35 columns inserted
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime, timezone
import sys
from pathlib import Path
import os

# ==================== DATABASE CONFIGURATION ====================
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'winbets-predictions.postgres.database.azure.com'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE', 'postgres'),
    'user': os.getenv('DB_USER', 'winbets'),
    'password': os.getenv('DB_PASSWORD', 'Constantinople@1900')
}

TABLE_NAME = 'agility_soccer_v3'
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
print("AGILITY FOOTBALL PREDICTIONS - SAVE TO agility_soccer_v3")
print("="*80)
print(f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"ℹ️  Table: {TABLE_NAME}")
print(f"ℹ️  CSV File: {CSV_FILE}")
print(f"⚠️  COLUMNS SKIPPED (will be populated from different repo):")
print(f"    • predicted_OverUnder")
print(f"    • actual_over_under")
print(f"    • ou_confidence")
print(f"    • ou_grade")
print(f"    • predicted_outcome")

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
    print(f"  Original columns: {len(df.columns)}")
    
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
    'outcome_label': 'Winner Prediction',
    'status': 'Match Status',
    'confidence_category': 'Confidence Category',
    'home_team_id': 'Home Team ID',
    'away_team_id': 'Away Team ID'
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

# CRITICAL: These are the EXACT 35 columns we need to insert
# EXCLUDED: id, predicted_outcome, predicted_OverUnder, actual_over_under, ou_confidence, ou_grade
db_columns = [
    'match_id', 'date', 'league', 'home_team', 'away_team',
    'home_odds', 'away_odds', 'draw_odds', 'over_2_5_odds', 'under_2_5_odds',
    'ctmcl', 'predicted_home_goals', 'predicted_away_goals', 'confidence',
    'delta', 'predicted_winner', 'actual_winner',
    'status', 'profit_loss_over_under', 'profit_loss_winner', 'confidence_category',
    'actual_home_team_goals', 'actual_away_team_goals', 'actual_total_goals',
    'league_name', 'home_id', 'away_id', 'league_wb', 'home_teamname_wb',
    'away_teamname_wb', 'home_teamid_wb', 'away_teamid_wb',
    'ml_grade', 'ml_confidence', 'ai_moneyline', 'ai_overunder', 'ai_spreads'
]

print(f"  Expected columns: {len(db_columns)}")

db_data = pd.DataFrame()

# Map CSV columns to database columns - ONE BY ONE
db_data['match_id'] = df['match_id']
db_data['date'] = df['date']
db_data['league'] = df['league_id'].astype(str)
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

# Predictions - NO predicted_outcome
db_data['predicted_winner'] = df['outcome_label']
db_data['actual_winner'] = None

# Status
db_data['status'] = df['status']
db_data['profit_loss_over_under'] = None
db_data['profit_loss_winner'] = None
db_data['confidence_category'] = df['confidence_category']

# Actual results (NULL initially)
db_data['actual_home_team_goals'] = None
db_data['actual_away_team_goals'] = None
db_data['actual_total_goals'] = None

# League info
db_data['league_name'] = df['league_id'].apply(get_league_name)
db_data['home_id'] = df['home_team_id']
db_data['away_id'] = df['away_team_id']
db_data['league_wb'] = None
db_data['home_teamname_wb'] = None
db_data['away_teamname_wb'] = None
db_data['home_teamid_wb'] = None
db_data['away_teamid_wb'] = None

# ML/AI predictions
db_data['ml_grade'] = None
db_data['ml_confidence'] = None
db_data['ai_moneyline'] = None
db_data['ai_overunder'] = None
db_data['ai_spreads'] = None

# REORDER to match expected order EXACTLY
db_data = db_data[db_columns]

print(f"✓ Transformed {len(db_data)} records")
print(f"  Final columns in db_data: {len(db_data.columns)}")
print(f"  MATCH: {len(db_data.columns) == len(db_columns)}")

if len(db_data.columns) != len(db_columns):
    print(f"\n❌ COLUMN COUNT MISMATCH!")
    print(f"  Expected: {len(db_columns)}")
    print(f"  Got: {len(db_data.columns)}")
    sys.exit(1)

# Show league distribution
league_counts = db_data['league_name'].value_counts()
print(f"\n  🏆 League distribution:")
for league, count in league_counts.items():
    print(f"    • {league}: {count} matches")

# ==================== INSERT TO TABLE ====================
def insert_to_table(table_name, db_data):
    """Insert data to table"""
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
            match_id, date, league, home_team, away_team,
            home_odds, away_odds, draw_odds, over_2_5_odds, under_2_5_odds,
            ctmcl, predicted_home_goals, predicted_away_goals, confidence,
            delta, predicted_winner, actual_winner,
            status, profit_loss_over_under, profit_loss_winner, confidence_category,
            actual_home_team_goals, actual_away_team_goals, actual_total_goals,
            league_name, home_id, away_id, league_wb, home_teamname_wb,
            away_teamname_wb, home_teamid_wb, away_teamid_wb,
            ml_grade, ml_confidence, ai_moneyline, ai_overunder, ai_spreads
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    """).format(sql.Identifier(table_name))

    inserted = 0
    errors = 0
    error_details = []

    for idx, row in new_data.iterrows():
        try:
            values = tuple([None if pd.isna(v) else v for v in row.values])
            
            cursor.execute(insert_query, values)
            inserted += 1
            
            if inserted % 10 == 0:
                print(f"  Progress: {inserted}/{len(new_data)} records inserted...")
                
        except Exception as e:
            errors += 1
            error_msg = f"Match ID {row['match_id']}: {str(e)[:100]}"
            error_details.append(error_msg)
            if errors <= 5:
                print(f"    ⚠️  {error_msg}")

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
    print(f"\nVerifying {table_name}...")
    try:
        cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        total = cursor.fetchone()[0]
        print(f"  ✓ Total records in {table_name}: {total}")
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

# ==================== EXECUTE INSERT ====================
print("\n" + "="*80)
print("INSERT PROCESS")
print("="*80)

result = insert_to_table(TABLE_NAME, db_data)

# ==================== FINAL SUMMARY ====================
print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)

if result['success']:
    if result.get('skipped'):
        print(f"✓ {TABLE_NAME}: No new records to insert (all duplicates)")
    else:
        print(f"✓ {TABLE_NAME}: {result['inserted']} records inserted ✅")
        if result['errors'] > 0:
            print(f"  ⚠️  {result['errors']} records had errors (skipped)")
else:
    print(f"✗ {TABLE_NAME}: FAILED - 0 records inserted")

print("\n" + "="*80)
if result['success'] and result.get('inserted', 0) > 0:
    print("✅ SUCCESS! Data inserted successfully")
else:
    print("⚠️  Check details above")
print("="*80)
