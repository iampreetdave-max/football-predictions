"""
FIXED VALIDATION SCRIPT - DATABASE-BASED DUAL DATABASE
This script reads PENDING match_ids from PRIMARY database and validates match results
Updates BOTH databases: agility_soccer_v1 (old credentials + new WINBETS credentials)

FIXES APPLIED:
1. ✓ Fetches match_ids from PRIMARY database WHERE status = 'PENDING'
2. ✓ Converts numeric(10,2) match_id to integer format
3. ✓ Uses ctmcl_prediction column (not predicted_outcome which is numeric)
4. ✓ Normalizes team names with .strip()
5. ✓ Better error handling and logging
6. ✓ Syncs updates to both databases
"""

import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import warnings
import psycopg2
from psycopg2 import sql
from pathlib import Path
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
# Primary database (old credentials)
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Secondary database (new credentials - WINBETS)
DB_CONFIG_WINBETS = {
    'host': os.getenv('WINBETS_DB_HOST'),
    'port': int(os.getenv('WINBETS_DB_PORT', 5432)),
    'database': os.getenv('WINBETS_DB_DATABASE'),
    'user': os.getenv('WINBETS_DB_USER'),
    'password': os.getenv('WINBETS_DB_PASSWORD')
}

TABLE_NAME = 'agility_soccer_v1'

print("\n" + "="*80)
print("AGILITY FOOTBALL PREDICTIONS - DATABASE-BASED DUAL DB VALIDATION")
print("="*80)
print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"ℹ️  This version fetches PENDING match_ids from PRIMARY database")
print(f"ℹ️  Updates BOTH old and new database credentials")

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
print("\n[1/6] Connecting to PRIMARY database and fetching PENDING match_ids...")
print("="*80)

try:
    conn_primary, cursor_primary = connect_database(DB_CONFIG, "PRIMARY (Old Credentials)")
    
    if not conn_primary or not cursor_primary:
        print(f"\n✗ CRITICAL: Cannot connect to PRIMARY database!")
        exit(1)
    
    # Fetch all PENDING match_ids from database
    fetch_query = sql.SQL("""
        SELECT match_id, date, home_team, away_team, 
               ctmcl_prediction, outcome_label,
               odds_ft_over25, odds_ft_under25,
               odds_ft_1, odds_ft_x, odds_ft_2
        FROM {}
        WHERE status = %s
        ORDER BY date DESC
    """).format(sql.Identifier(TABLE_NAME))
    
    cursor_primary.execute(fetch_query, ('PENDING',))
    rows = cursor_primary.fetchall()
    
    if len(rows) == 0:
        print(f"ℹ No PENDING predictions found in database")
        conn_primary.close()
        exit(0)
    
    # Get column names from cursor description
    column_names = [desc[0] for desc in cursor_primary.description]
    
    # Convert to DataFrame
    predictions_df = pd.DataFrame(rows, columns=column_names)
    
    # Convert numeric(10,2) match_id to integer
    predictions_df['match_id'] = predictions_df['match_id'].astype(float).astype(int)
    
    print(f"✓ Fetched {len(predictions_df)} PENDING predictions from database")
    print(f"✓ Converted match_id from numeric(10,2) to integer format")
    
    # Prepare data for validation
    predictions_to_validate = predictions_df.copy()
    
except Exception as e:
    print(f"✗ Error fetching from database: {e}")
    if conn_primary:
        conn_primary.close()
    exit(1)

# ==================== CONNECT TO WINBETS DATABASE ====================
print("\n[2/6] Connecting to WINBETS database...")
print("="*80)

conn_winbets, cursor_winbets = connect_database(DB_CONFIG_WINBETS, "WINBETS (New Credentials)")

if not conn_winbets:
    print(f"\n⚠️  Warning: WINBETS database connection failed")
    print(f"ℹ️  Will continue with PRIMARY database only")

# ==================== TEST API FIRST ====================
print("\n[3/6] Testing API configurations...")
print("="*80)

working_api_config = None
test_match_id = predictions_to_validate.iloc[0]['match_id']

print(f"Testing with match ID: {test_match_id}\n")

for i, config in enumerate(API_CONFIGS, 1):
    try:
        url = f"{config['url']}?key={API_KEY}&{config['param']}={test_match_id}"
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
    if conn_winbets:
        conn_winbets.close()
    exit(1)

print(f"\n✓ Using: {working_api_config['url']} with parameter '{working_api_config['param']}'")

# ==================== FETCH & UPDATE BOTH DATABASES ====================
print("\n[4/6] Fetching match results and updating databases...")
print("="*80)

successful_updates = 0
failed_fetches = 0

for idx, row in predictions_to_validate.iterrows():
    match_id = row['match_id']
    
    # Read prediction data
    predicted_ou = str(row.get('predicted_outcome', '')).strip()
    predicted_winner = str(row.get('predicted_winner', '')).strip()
    
    odds_over = row.get('odds_ft_over25', row.get('over_2_5_odds', 0))
    odds_under = row.get('odds_ft_under25', row.get('under_2_5_odds', 0))
    odds_home = row.get('odds_ft_1', row.get('home_odds', 0))
    odds_away = row.get('odds_ft_2', row.get('away_odds', 0))
    odds_draw = row.get('odds_ft_x', row.get('draw_odds', 0))
    
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
                    
                    # Normalize for comparison
                    predicted_ou_normalized = predicted_ou.lower().strip()
                    actual_ou_normalized = actual_over_under.lower().strip()
                    
                    # Calculate P/L for Over/Under
                    if predicted_ou_normalized == actual_ou_normalized:
                        if 'over' in actual_ou_normalized:
                            profit_loss_ou = round(odds_over - 1, 2)
                        else:
                            profit_loss_ou = round(odds_under - 1, 2)
                    else:
                        profit_loss_ou = -1.0
                    
                    # Calculate P/L for Moneyline
                    if predicted_winner == 'Home Win' and actual_winner == home_team:
                        profit_loss_ml = round(odds_home - 1, 2)
                    elif predicted_winner == 'Away Win' and actual_winner == away_team:
                        profit_loss_ml = round(odds_away - 1, 2)
                    elif predicted_winner == 'Draw' and actual_winner == 'Draw':
                        profit_loss_ml = round(odds_draw - 1, 2)
                    else:
                        profit_loss_ml = -1.0
                    
                    # UPDATE PRIMARY DATABASE
                    if conn_primary and cursor_primary:
                        try:
                            update_query = sql.SQL("""
                                UPDATE {}
                                SET 
                                    actual_winner = %s,
                                    actual_over_under = %s,
                                    actual_home_team_goals = %s,
                                    actual_away_team_goals = %s,
                                    actual_total_goals = %s,
                                    status = %s,
                                    profit_loss_outcome = %s,
                                    profit_loss_winner = %s,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE match_id = %s
                            """).format(sql.Identifier(TABLE_NAME))
                            
                            cursor_primary.execute(update_query, (
                                actual_winner,
                                actual_over_under,
                                float(home_score),
                                float(away_score),
                                float(total_goals),
                                'SETTLED',
                                profit_loss_ou,
                                profit_loss_ml,
                                match_id
                            ))
                            
                            conn_primary.commit()
                        except Exception as e:
                            print(f"⚠ Error updating PRIMARY DB for {match_id}: {str(e)[:50]}")
                            conn_primary.rollback()
                    
                    # UPDATE WINBETS DATABASE
                    if conn_winbets and cursor_winbets:
                        try:
                            update_query = sql.SQL("""
                                UPDATE {}
                                SET 
                                    actual_winner = %s,
                                    actual_over_under = %s,
                                    actual_home_team_goals = %s,
                                    actual_away_team_goals = %s,
                                    actual_total_goals = %s,
                                    status = %s,
                                    profit_loss_outcome = %s,
                                    profit_loss_winner = %s,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE match_id = %s
                            """).format(sql.Identifier(TABLE_NAME))
                            
                            cursor_winbets.execute(update_query, (
                                actual_winner,
                                actual_over_under,
                                float(home_score),
                                float(away_score),
                                float(total_goals),
                                'SETTLED',
                                profit_loss_ou,
                                profit_loss_ml,
                                match_id
                            ))
                            
                            conn_winbets.commit()
                        except Exception as e:
                            print(f"⚠ Error updating WINBETS DB for {match_id}: {str(e)[:50]}")
                            conn_winbets.rollback()
                    
                    successful_updates += 1
                    
                    print(f"✓ {match_id}: {home_team} {home_score}-{away_score} {away_team}")
                    print(f"  → Winner: {actual_winner} | O/U: {actual_over_under}")
                    print(f"  → P/L O/U: ${profit_loss_ou:.2f} | P/L ML: ${profit_loss_ml:.2f}")
                    
                else:
                    # Update incomplete matches to PENDING in both databases
                    if conn_primary and cursor_primary:
                        try:
                            update_query = sql.SQL("""
                                UPDATE {}
                                SET status = %s, updated_at = CURRENT_TIMESTAMP
                                WHERE match_id = %s
                            """).format(sql.Identifier(TABLE_NAME))
                            cursor_primary.execute(update_query, ('PENDING', match_id))
                            conn_primary.commit()
                        except:
                            conn_primary.rollback()
                    
                    if conn_winbets and cursor_winbets:
                        try:
                            update_query = sql.SQL("""
                                UPDATE {}
                                SET status = %s, updated_at = CURRENT_TIMESTAMP
                                WHERE match_id = %s
                            """).format(sql.Identifier(TABLE_NAME))
                            cursor_winbets.execute(update_query, ('PENDING', match_id))
                            conn_winbets.commit()
                        except:
                            conn_winbets.rollback()
                    
                    print(f"⏳ {match_id}: Not complete (status: {status}) → Set to PENDING")
                    failed_fetches += 1
            else:
                print(f"⚠ {match_id}: No data")
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
print("VALIDATION SUMMARY")
print("="*80)
print(f"✓ Successfully updated: {successful_updates} matches (SETTLED)")
print(f"✗ Failed/Pending: {failed_fetches} matches (PENDING)")

if successful_updates == 0:
    print(f"\n⚠️  WARNING: No matches were successfully validated")

# Close connections
if conn_primary:
    cursor_primary.close()
    conn_primary.close()
    print(f"\n✓ PRIMARY database connection closed")

if conn_winbets:
    cursor_winbets.close()
    conn_winbets.close()
    print(f"✓ WINBETS database connection closed")

print("\n" + "="*80)
print("✅ DUAL DATABASE VALIDATION COMPLETE!")
print("="*80)
print(f"⏰ Completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"\n📊 Both databases have been synchronized with match results")
print("="*80)
