"""
SIMPLIFIED MATCH VALIDATION SYSTEM
Updates: actual_winner, actual_over_under, status, profit_loss_over_under, profit_loss_moneyline
"""

import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import warnings
import psycopg2
from pathlib import Path
warnings.filterwarnings('ignore')

# API Configuration
API_KEY = "633379bdd5c4c3eb26919d8570866801e1c07f399197ba8c5311446b8ea77a49"
API_MATCH_URL = "https://api.football-data-api.com/match"

# Database Configuration
DB_CONFIG = {
    'host': 'winbets-db.postgres.database.azure.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'app_user',
    'password': 'StrongPassword123!'
}
TABLE_NAME = 'soccer_predsv1'

print("\n" + "="*80)
print("FOOTBALL MATCH VALIDATION - DATABASE UPDATE")
print("="*80)

# ========== DATABASE CONNECTION ==========
print("\n[1/5] Connecting to PostgreSQL Database...")
print("="*80)

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print(f"✓ Connected to database: {DB_CONFIG['database']}")
    print(f"✓ Table: {TABLE_NAME}")
except Exception as e:
    print(f"✗ Database connection failed: {e}")
    exit(1)

# ========== CONFIGURATION ==========
VALIDATION_DATE = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"\n📅 Validation Date: {VALIDATION_DATE}")

# ========== LOAD PREDICTIONS ==========
print("\n[2/5] Loading predictions from CSV...")
print("="*80)

try:
    base_dir = Path(__file__).resolve().parent
    predictions_csv_path = base_dir / 'best_match_predictions.csv'
    predictions_df = pd.read_csv(predictions_csv_path)
    print(f"✓ Loaded {len(predictions_df)} predictions")
    
    # Verify required columns exist
    required_columns = [
        'CTMCL', 'ctmcl_prediction', 'outcome_label',
        'odds_ft_over25', 'odds_ft_under25', 
        'odds_ft_1', 'odds_ft_x', 'odds_ft_2'
    ]
    
    missing_columns = [col for col in required_columns if col not in predictions_df.columns]
    if missing_columns:
        print(f"✗ Error: Missing required columns: {missing_columns}")
        cursor.close()
        conn.close()
        exit(1)
    
    print(f"✓ All required columns found")
    
except Exception as e:
    print(f"✗ Error loading predictions: {e}")
    cursor.close()
    conn.close()
    exit(1)

# ========== FILTER BY DATE ==========
print("\n[3/5] Filtering predictions...")
print("="*80)

predictions_df['date'] = pd.to_datetime(predictions_df['date']).dt.date
validation_date_obj = pd.to_datetime(VALIDATION_DATE).date()
predictions_to_validate = predictions_df[predictions_df['date'] == validation_date_obj].copy()

if len(predictions_to_validate) == 0:
    print(f"ℹ No predictions found for {VALIDATION_DATE}")
    cursor.close()
    conn.close()
    exit(0)

print(f"✓ Found {len(predictions_to_validate)} predictions to validate")

# ========== FETCH & UPDATE ==========
print("\n[4/5] Fetching match results and updating database...")
print("="*80)

successful_updates = 0
failed_fetches = 0

for idx, row in predictions_to_validate.iterrows():
    match_id = row['match_id']
    ctmcl = row['CTMCL']
    
    # Get prediction data
    predicted_ou = row['ctmcl_prediction']  # "Over 2.5" or "Under 2.5"
    predicted_winner = row['outcome_label']  # "Home Win", "Away Win", or "Draw"
    
    # Get odds data
    odds_over = row['odds_ft_over25']
    odds_under = row['odds_ft_under25']
    odds_home = row['odds_ft_1']
    odds_away = row['odds_ft_2']
    odds_draw = row['odds_ft_x']
    
    try:
        # Fetch match details from API
        url = f"{API_MATCH_URL}?key={API_KEY}&match_id={match_id}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('success') and data.get('data'):
                match_data = data['data']
                status = match_data.get('status', '')
                
                if status == 'complete':
                    # Get actual scores
                    home_score = match_data.get('homeGoalCount', 0)
                    away_score = match_data.get('awayGoalCount', 0)
                    total_goals = home_score + away_score
                    
                    # Determine actual winner
                    if home_score > away_score:
                        actual_winner = row['home_team_name']
                    elif away_score > home_score:
                        actual_winner = row['away_team_name']
                    else:
                        actual_winner = 'Draw'
                    
                    # Determine actual over/under based on 2.5 (not CTMCL)
                    if total_goals > 2.5:
                        actual_over_under = 'over'
                    else:
                        actual_over_under = 'under'
                    
                    # ========== CALCULATE PROFIT/LOSS FOR OVER/UNDER ==========
                    if 'Over' in predicted_ou:
                        # We bet on over 2.5
                        if actual_over_under == 'over':
                            profit_loss_ou = round(odds_over - 1, 2)
                        else:
                            profit_loss_ou = -1.0
                    else:
                        # We bet on under 2.5
                        if actual_over_under == 'under':
                            profit_loss_ou = round(odds_under - 1, 2)
                        else:
                            profit_loss_ou = -1.0
                    
                    # ========== CALCULATE PROFIT/LOSS FOR MONEYLINE ==========
                    if predicted_winner == 'Home Win':
                        # We bet on home win
                        if actual_winner == row['home_team_name']:
                            profit_loss_ml = round(odds_home - 1, 2)
                        else:
                            profit_loss_ml = -1.0
                    elif predicted_winner == 'Away Win':
                        # We bet on away win
                        if actual_winner == row['away_team_name']:
                            profit_loss_ml = round(odds_away - 1, 2)
                        else:
                            profit_loss_ml = -1.0
                    elif predicted_winner == 'Draw':
                        # We bet on draw
                        if actual_winner == 'Draw':
                            profit_loss_ml = round(odds_draw - 1, 2)
                        else:
                            profit_loss_ml = -1.0
                    else:
                        profit_loss_ml = 0.0
                    
                    # Update database - 5 fields
                    update_query = f"""
                    UPDATE {TABLE_NAME}
                    SET 
                        actual_winner = %s,
                        actual_over_under = %s,
                        status = %s,
                        profit_loss_over_under = %s,
                        profit_loss_moneyline = %s
                    WHERE match_id = %s
                    """
                    
                    cursor.execute(update_query, (
                        actual_winner,
                        actual_over_under,
                        'true',
                        profit_loss_ou,
                        profit_loss_ml,
                        match_id
                    ))
                    
                    successful_updates += 1
                    print(f"✓ {match_id}: {row['home_team_name']} {home_score}-{away_score} {row['away_team_name']}")
                    print(f"  → Winner: {actual_winner} | O/U: {actual_over_under} (2.5)")
                    print(f"  → P/L O/U: ${profit_loss_ou:.2f} | P/L Moneyline: ${profit_loss_ml:.2f}")
                    
                else:
                    print(f"⏳ {match_id}: Match not complete (status: {status})")
                    failed_fetches += 1
            else:
                print(f"⚠ {match_id}: No data from API")
                failed_fetches += 1
        else:
            print(f"✗ {match_id}: HTTP {response.status_code}")
            failed_fetches += 1
        
        # Rate limiting
        time.sleep(0.2)
        
    except Exception as e:
        print(f"✗ {match_id}: Error - {str(e)}")
        failed_fetches += 1

# Commit all updates
try:
    conn.commit()
    print(f"\n✓ Database updates committed!")
except Exception as e:
    conn.rollback()
    print(f"\n✗ Database commit failed: {e}")

# ========== SUMMARY ==========
print("\n[5/5] SUMMARY")
print("="*80)
print(f"✓ Successfully updated: {successful_updates} matches")
print(f"✗ Failed/Incomplete: {failed_fetches} matches")
print(f"\n📊 Updated fields per match:")
print(f"  • actual_winner")
print(f"  • actual_over_under (based on 2.5, not CTMCL)")
print(f"  • status = 'true'")
print(f"  • profit_loss_over_under (based on odds_ft_over25/under25)")
print(f"  • profit_loss_moneyline (based on odds_ft_1/x/2)")

# Close connection
cursor.close()
conn.close()
print(f"\n✓ Database connection closed")

print("\n" + "="*80)
print("✅ VALIDATION COMPLETE!")
print("="*80)
print(f"⏰ Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("\n💡 Tip: Change VALIDATION_DATE in script to check other dates")
print("="*80)
