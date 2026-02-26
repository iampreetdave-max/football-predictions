"""
VALIDATION SCRIPT - predictions_soccer_v3_ourmodel - PROFIT/LOSS CALCULATION
This script reads PENDING match_ids from predictions_soccer_v3_ourmodel and validates match results
Calculates profit/loss for both moneyline and over/under predictions
Syncs to PRIMARY database ONLY

FEATURES:
1. ✓ Fetches match_ids from predictions_soccer_v3_ourmodel WHERE status = 'PENDING'
2. ✓ Converts numeric(10,2) match_id to integer format
3. ✓ Uses correct column names for v3 table
4. ✓ Normalizes team names with .strip()
5. ✓ Calculates profit/loss for moneyline and over/under
6. ✓ Updates PRIMARY database only
7. ✓ Better error handling and logging
"""

import pandas as pd
import requests
import time
from datetime import datetime
import warnings
import psycopg2
from psycopg2 import sql
import json
import os
warnings.filterwarnings('ignore')

# ==================== API CONFIGURATION ====================
API_KEY = os.getenv("FOOTYSTATSAPI")

# Try multiple API endpoint configurations
API_CONFIGS = [
    {"url": "https://api.football-data-api.com/match", "param": "match_id"},
    {"url": "https://api.footystats.org/match", "param": "id"},
    {"url": "https://api.footystats.org/match", "param": "match_id"},
]

# ==================== DATABASE CONFIGURATION ====================
# Primary database
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

TABLE_NAME = 'predictions_soccer_v3_ourmodel'

print("\n" + "="*80)
print("AGILITY FOOTBALL PREDICTIONS - VALIDATION WITH PROFIT/LOSS")
print("="*80)
print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"ℹ️  Table: {TABLE_NAME}")
print(f"ℹ️  This version fetches PENDING match_ids and calculates P/L")
print(f"ℹ️  Updates PRIMARY database only")

# ==================== HELPER FUNCTION FOR DATABASE OPERATIONS ====================
def connect_database(db_config, db_name):
    """Connect to a specific database"""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print(f"✓ Connected to {db_name}")
        return conn, cursor
    except Exception as e:
        print(f"✗ Failed to connect to {db_name}: {e}")
        return None, None

# ==================== CONNECT TO PRIMARY DATABASE & FETCH PENDING MATCH_IDS ====================
print("\n[1/4] Connecting to PRIMARY database and fetching PENDING match_ids...")
print("="*80)

try:
    conn_primary, cursor_primary = connect_database(DB_CONFIG, "PRIMARY")
    
    if not conn_primary or not cursor_primary:
        print(f"\n✗ CRITICAL: Cannot connect to PRIMARY database!")
        exit(1)
    
    # Fetch all PENDING match_ids from predictions_soccer_v3_ourmodel
    fetch_query = sql.SQL("""
        SELECT match_id, date, home_team, away_team, 
               predicted_winner,
               home_odds, away_odds, draw_odds,
               over_2_5_odds, under_2_5_odds
        FROM {}
        WHERE status = %s
        ORDER BY date DESC
    """).format(sql.Identifier(TABLE_NAME))
    
    cursor_primary.execute(fetch_query, ('PENDING',))
    rows = cursor_primary.fetchall()
    
    if len(rows) == 0:
        print(f"ℹ No PENDING predictions found in {TABLE_NAME}")
        conn_primary.close()
        exit(0)
    
    # Get column names from cursor description
    column_names = [desc[0] for desc in cursor_primary.description]
    
    # Convert to DataFrame
    predictions_df = pd.DataFrame(rows, columns=column_names)
    
    # Convert numeric(10,2) match_id to integer
    predictions_df['match_id'] = predictions_df['match_id'].astype(float).astype(int)
    
    print(f"✓ Fetched {len(predictions_df)} PENDING predictions from {TABLE_NAME}")
    print(f"✓ Converted match_id from numeric(10,2) to integer format")
    
    # Prepare data for validation
    predictions_to_validate = predictions_df.copy()
    
except Exception as e:
    print(f"✗ Error fetching from database: {e}")
    if conn_primary:
        conn_primary.close()
    exit(1)

# ==================== TEST API FIRST ====================
print("\n[2/4] Testing API configurations...")
print("="*80)

working_api_config = None
test_match_id = predictions_to_validate.iloc[0]['match_id']

print(f"Testing with match ID: {test_match_id}\n")

for i, config in enumerate(API_CONFIGS, 1):
    try:
        print(f"[{i}/{len(API_CONFIGS)}] Testing: {config['url']} with {config['param']}=...")
        
        response = requests.get(config['url'], 
                               params={'key': API_KEY, config['param']: test_match_id},
                               timeout=30)
        
        if response.status_code == 200 and response.text:
            try:
                data = response.json()
                if data.get('success') and data.get('data'):
                    print(f"✓ SUCCESS! This configuration works")
                    working_api_config = config
                    break
                else:
                    print(f"✗ API returned success=false")
            except:
                print(f"✗ Invalid JSON")
        else:
            print(f"✗ HTTP {response.status_code}")
            
    except Exception as e:
        print(f"✗ Error: {str(e)[:50]}")
    
    time.sleep(0.3)

if not working_api_config:
    print(f"\n✗ ERROR: No working API configuration found!")
    if conn_primary:
        conn_primary.close()
    exit(1)

print(f"\n✓ Using: {working_api_config['url']} with parameter '{working_api_config['param']}'")

# ==================== FETCH & UPDATE PRIMARY DATABASE ====================
print("\n[3/4] Fetching match results and updating PRIMARY database...")
print("="*80)

successful_updates = 0
failed_fetches = 0

for idx, row in predictions_to_validate.iterrows():
    match_id = row['match_id']
    
    # Read prediction data
    predicted_winner = str(row.get('predicted_winner', '')).strip()
    
    odds_over = float(row.get('over_2_5_odds', 0)) if row.get('over_2_5_odds') else 0
    odds_under = float(row.get('under_2_5_odds', 0)) if row.get('under_2_5_odds') else 0
    odds_home = float(row.get('home_odds', 0)) if row.get('home_odds') else 0
    odds_away = float(row.get('away_odds', 0)) if row.get('away_odds') else 0
    odds_draw = float(row.get('draw_odds', 0)) if row.get('draw_odds') else 0
    
    home_team = str(row.get('home_team', '')).strip()
    away_team = str(row.get('away_team', '')).strip()
    
    try:
        # Fetch match details
        response = requests.get(
            working_api_config['url'],
            params={'key': API_KEY, working_api_config['param']: match_id},
            timeout=30
        )
        
        if response.status_code == 200 and response.text:
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"✗ {match_id}: JSON error")
                failed_fetches += 1
                continue
            
            if data.get('success') and data.get('data'):
                match_data = data['data']
                status = match_data.get('status', '')
                
                if status == 'complete':
                    # Get scores
                    home_score = int(match_data.get('homeGoalCount', 0))
                    away_score = int(match_data.get('awayGoalCount', 0))
                    total_goals = home_score + away_score
                    
                    # Determine winner
                    if home_score > away_score:
                        actual_winner = home_team
                    elif away_score > home_score:
                        actual_winner = away_team
                    else:
                        actual_winner = 'Draw'
                    
                    # Determine O/U
                    actual_over_under = 'Over 2.5' if total_goals > 2.5 else 'Under 2.5'
                    
                    # =============== PROFIT/LOSS CALCULATION ===============
                    
                    profit_loss_outcome = None
                    
                    # For Moneyline (predicted_winner)
                    if predicted_winner == 'Home Win' and actual_winner == home_team:
                        profit_loss_winner = round(odds_home - 1, 2)
                    elif predicted_winner == 'Away Win' and actual_winner == away_team:
                        profit_loss_winner = round(odds_away - 1, 2)
                    elif predicted_winner == 'Draw' and actual_winner == 'Draw':
                        profit_loss_winner = round(odds_draw - 1, 2)
                    else:
                        profit_loss_winner = -1.0
                    
                    # =============== UPDATE PRIMARY DATABASE ===============
                    try:
                        update_query = sql.SQL("""
                            UPDATE {}
                            SET 
                                actual_winner = %s,
                                actual_home_team_goals = %s,
                                actual_away_team_goals = %s,
                                actual_total_goals = %s,
                                status = %s,
                                profit_loss_outcome = %s,
                                profit_loss_winner = %s
                            WHERE match_id = %s
                        """).format(sql.Identifier(TABLE_NAME))
                        
                        cursor_primary.execute(update_query, (
                            actual_winner,
                            float(home_score),
                            float(away_score),
                            float(total_goals),
                            'SETTLED',
                            profit_loss_outcome,
                            profit_loss_winner,
                            match_id
                        ))
                        
                        conn_primary.commit()
                    except Exception as e:
                        print(f"⚠ Error updating PRIMARY DB for {match_id}: {str(e)[:50]}")
                        conn_primary.rollback()
                    
                    successful_updates += 1
                    
                    print(f"✓ {match_id}: {home_team} {home_score}-{away_score} {away_team}")
                    print(f"  → Actual Winner: {actual_winner} | O/U: {actual_over_under}")
                    print(f"  → Predicted: {predicted_winner}")
                    print(f"  → P/L Moneyline: ${profit_loss_winner:.2f}")
                    
                else:
                    # Match not complete yet, keep as PENDING
                    print(f"⏳ {match_id}: Not complete (status: {status}) → Still PENDING")
                    failed_fetches += 1
            else:
                print(f"⚠ {match_id}: No data from API")
                failed_fetches += 1
        else:
            print(f"✗ {match_id}: HTTP {response.status_code}")
            failed_fetches += 1
        
        time.sleep(0.25)
        
    except Exception as e:
        print(f"✗ {match_id}: {str(e)[:50]}")
        failed_fetches += 1

# ==================== SUMMARY ====================
print("\n" + "="*80)
print("[4/4] VALIDATION SUMMARY")
print("="*80)
print(f"✓ Successfully updated: {successful_updates} matches (SETTLED)")
print(f"⏳ Still pending: {failed_fetches} matches (PENDING)")

if successful_updates == 0:
    print(f"\n⚠️  WARNING: No matches were successfully validated")

# Close connection
if conn_primary:
    cursor_primary.close()
    conn_primary.close()
    print(f"\n✓ PRIMARY database connection closed")

print("\n" + "="*80)
print("✅ PRIMARY DATABASE VALIDATION COMPLETE!")
print("="*80)
print(f"⏰ Completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"\n📊 PRIMARY database updated with match results")
print(f"📈 Profit/Loss calculated for moneyline predictions")
print("="*80)
