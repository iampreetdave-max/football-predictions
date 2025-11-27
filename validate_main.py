"""
FIXED VALIDATION SCRIPT - CSV-BASED
This script reads from CSV and validates match results
Updates database: agility_soccer_v1

FIXES APPLIED:
1. ✓ Uses ctmcl_prediction column (not predicted_outcome which is numeric)
2. ✓ Normalizes team names with .strip()
3. ✓ Better error handling and logging
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
API_KEY = "633379bdd5c4c3eb26919d8570866801e1c07f399197ba8c5311446b8ea77a49"

# Try multiple API endpoint configurations
API_CONFIGS = [
    {"url": "https://api.football-data-api.com/match", "param": "match_id"},
    {"url": "https://api.footystats.org/match", "param": "id"},
    {"url": "https://api.footystats.org/match", "param": "match_id"},
]

# ==================== DATABASE CONFIGURATION ====================
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

TABLE_NAME = 'agility_soccer_v1'

print("\n" + "="*80)
print("AGILITY FOOTBALL PREDICTIONS - CSV-BASED VALIDATION (FIXED VERSION)")
print("="*80)
print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"\n⚠️  IMPORTANT: This is the FIXED version with correct column mapping")
print(f"   Original issue: Was reading predicted_outcome (numeric) instead of ctmcl_prediction")

# ==================== DATABASE CONNECTION ====================
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

# ==================== CONFIGURATION ====================
VALIDATION_DATE = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"\n📅 Validation Date: {VALIDATION_DATE}")

# ==================== LOAD PREDICTIONS FROM CSV ====================
print("\n[2/5] Loading predictions from CSV...")
print("="*80)

try:
    # Look for CSV file in the same directory or specify path
    csv_path = Path('best_match_predictions.csv')
    
    # Try different possible locations
    possible_paths = [
        csv_path,
        Path('best_match_predictions.csv'),
        Path('/home/claude/best_match_predictions.csv'),
        Path(__file__).resolve().parent / 'best_match_predictions.csv'
    ]
    
    predictions_df = None
    for path in possible_paths:
        if path.exists():
            predictions_df = pd.read_csv(path)
            print(f"✓ Loaded CSV from: {path}")
            break
    
    if predictions_df is None:
        print(f"✗ Could not find CSV file. Tried:")
        for p in possible_paths:
            print(f"  - {p}")
        cursor.close()
        conn.close()
        exit(1)
    
    print(f"✓ Loaded {len(predictions_df)} total predictions")
    
    # Verify required columns
    required_columns = [
        'match_id', 'date', 'home_team_name', 'away_team_name',
        'ctmcl_prediction', 'outcome_label',
        'odds_ft_over25', 'odds_ft_under25',
        'odds_ft_1', 'odds_ft_x', 'odds_ft_2'
    ]
    
    missing_columns = [col for col in required_columns if col not in predictions_df.columns]
    if missing_columns:
        print(f"⚠️  Missing columns: {missing_columns}")
        print(f"Available columns: {list(predictions_df.columns)}")
        # Don't exit - some columns might have alternate names
    else:
        print(f"✓ All required columns present")
    
except Exception as e:
    print(f"✗ Error loading CSV: {e}")
    cursor.close()
    conn.close()
    exit(1)

# ==================== FILTER BY DATE ====================
print("\n[3/5] Filtering predictions by date...")
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

# ==================== TEST API FIRST ====================
print("\n[4/5] Testing API configurations...")
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
    print(f"\n❌ ERROR: No working API configuration found!")
    print(f"\n💡 SOLUTIONS:")
    print(f"   1. Your match IDs ({test_match_id}) are not compatible with these APIs")
    print(f"   2. Check if match IDs are from a different source (RapidAPI, etc.)")
    print(f"   3. Verify your API key has access to match data")
    print(f"   4. The matches might be too old or not yet in the API")
    cursor.close()
    conn.close()
    exit(1)

print(f"\n✓ Using: {working_api_config['url']} with parameter '{working_api_config['param']}'")

# ==================== FETCH & UPDATE ====================
print("\n[5/5] Fetching match results and updating database...")
print("="*80)

successful_updates = 0
failed_fetches = 0

for idx, row in predictions_to_validate.iterrows():
    match_id = row['match_id']
    
    # FIXED: Read from ctmcl_prediction (correct column for O/U)
    predicted_ou = str(row.get('ctmcl_prediction', '')).strip()
    predicted_winner = str(row.get('outcome_label', '')).strip()
    
    # Get odds data with fallbacks
    odds_over = row.get('odds_ft_over25', row.get('over_2_5_odds', 0))
    odds_under = row.get('odds_ft_under25', row.get('under_2_5_odds', 0))
    odds_home = row.get('odds_ft_1', row.get('home_odds', 0))
    odds_away = row.get('odds_ft_2', row.get('away_odds', 0))
    odds_draw = row.get('odds_ft_x', row.get('draw_odds', 0))
    
    # FIXED: Normalize team names with .strip()
    home_team = str(row.get('home_team_name', row.get('home_team', ''))).strip()
    away_team = str(row.get('away_team_name', row.get('away_team', ''))).strip()
    
    try:
        # Fetch match details using working config
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
                    
                    # Determine winner - FIXED: Strip whitespace
                    if home_score > away_score:
                        actual_winner = home_team
                    elif away_score > home_score:
                        actual_winner = away_team
                    else:
                        actual_winner = 'Draw'
                    
                    # Determine O/U (based on 2.5) - standardized format
                    actual_over_under = 'Over 2.5' if total_goals > 2.5 else 'Under 2.5'
                    
                    # FIXED: Normalize predicted_ou for comparison
                    # predicted_ou should already be from ctmcl_prediction which has "Over 2.5" or "Under 2.5"
                    predicted_ou_normalized = predicted_ou.lower().strip()
                    actual_ou_normalized = actual_over_under.lower().strip()
                    
                    # Calculate P/L for Over/Under
                    # IF predicted == actual, use odds; ELSE -1.0
                    if predicted_ou_normalized == actual_ou_normalized:
                        if 'over' in actual_ou_normalized:
                            profit_loss_ou = round(odds_over - 1, 2)
                        else:  # Under
                            profit_loss_ou = round(odds_under - 1, 2)
                    else:
                        profit_loss_ou = -1.0
                    
                    # Calculate P/L for Moneyline (Winner)
                    # IF predicted == actual, use odds; ELSE -1.0
                    if predicted_winner == 'Home Win' and actual_winner == home_team:
                        profit_loss_ml = round(odds_home - 1, 2)
                    elif predicted_winner == 'Away Win' and actual_winner == away_team:
                        profit_loss_ml = round(odds_away - 1, 2)
                    elif predicted_winner == 'Draw' and actual_winner == 'Draw':
                        profit_loss_ml = round(odds_draw - 1, 2)
                    else:
                        profit_loss_ml = -1.0
                    
                    # Update database
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
                    
                    cursor.execute(update_query, (
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
                    
                    conn.commit()
                    successful_updates += 1
                    
                    print(f"✓ {match_id}: {home_team} {home_score}-{away_score} {away_team}")
                    print(f"  → Winner: {actual_winner} | O/U: {actual_over_under}")
                    print(f"  → Pred O/U: {predicted_ou} | Profit O/U: ${profit_loss_ou:.2f}")
                    print(f"  → P/L ML: ${profit_loss_ml:.2f}")
                    
                else:
                    # Update incomplete matches to PENDING
                    update_query = sql.SQL("""
                        UPDATE {}
                        SET status = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE match_id = %s
                    """).format(sql.Identifier(TABLE_NAME))
                    
                    cursor.execute(update_query, ('PENDING', match_id))
                    conn.commit()
                    
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
print("SUMMARY")
print("="*80)
print(f"✓ Successfully updated: {successful_updates} matches (SETTLED)")
print(f"✗ Failed/Pending: {failed_fetches} matches (PENDING)")

if successful_updates == 0:
    print(f"\n⚠️  WARNING: No matches were successfully validated")
    print(f"   This suggests the match IDs are incompatible with the API")

cursor.close()
conn.close()
print(f"\n✓ Database connection closed")

print("\n" + "="*80)
print("✅ VALIDATION COMPLETE!")
print("="*80)
print(f"⏰ Completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("="*80)
print(f"\n📊 KEY FIXES APPLIED:")
print(f"   1. ✓ Now reads ctmcl_prediction (O/U text) instead of predicted_outcome (numeric)")
print(f"   2. ✓ Team names normalized with .strip() for accurate comparisons")
print(f"   3. ✓ Better error logging and messaging")
print("="*80)
