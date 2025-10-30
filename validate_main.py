"""
Validate Match Predictions and Update Database
Fetches actual match results from API and updates:
- actual_winner
- actual_over_under
- actual_home_team_goals (NEW)
- actual_away_team_goals (NEW)
- actual_total_goals (NEW)
- status
- profit_loss_outcome
- profit_loss_winner
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
warnings.filterwarnings('ignore')

# ==================== API CONFIGURATION ====================
API_KEY = "633379bdd5c4c3eb26919d8570866801e1c07f399197ba8c5311446b8ea77a49"
# Using the SAME API endpoint as the working validate_predictions.py
API_MATCH_URL = "https://api.football-data-api.com/match"

# ==================== DATABASE CONFIGURATION ====================
DB_CONFIG = {
    'host': 'winbets-db.postgres.database.azure.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'winbets',
    'password': 'deeptanshu@123'
}

TABLE_NAME = 'agility_football_pred'

print("\n" + "="*80)
print("AGILITY FOOTBALL PREDICTIONS - VALIDATION & UPDATE")
print("="*80)
print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

# ==================== DATABASE CONNECTION ====================
print("\n[1/6] Connecting to PostgreSQL Database...")
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
# Validate matches from yesterday by default
VALIDATION_DATE = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"\n📅 Validation Date: {VALIDATION_DATE}")

# ==================== LOAD PREDICTIONS FROM DATABASE ====================
print("\n[2/6] Loading predictions from database...")
print("="*80)

try:
    # Get predictions for the validation date that haven't been validated yet
    query = sql.SQL("""
        SELECT 
            match_id, date, home_team, away_team,
            ctmcl, predicted_outcome, predicted_winner,
            over_2_5_odds, under_2_5_odds,
            home_odds, away_odds, draw_odds,
            status, actual_winner
        FROM {}
        WHERE date = %s
        ORDER BY match_id
    """).format(sql.Identifier(TABLE_NAME))
    
    cursor.execute(query, [VALIDATION_DATE])
    predictions = cursor.fetchall()
    
    if len(predictions) == 0:
        print(f"ℹ No predictions found for {VALIDATION_DATE}")
        cursor.close()
        conn.close()
        exit(0)
    
    print(f"✓ Found {len(predictions)} predictions for {VALIDATION_DATE}")
    
    # Create DataFrame for easier handling
    predictions_df = pd.DataFrame(predictions, columns=[
        'match_id', 'date', 'home_team', 'away_team',
        'ctmcl', 'predicted_outcome', 'predicted_winner',
        'over_2_5_odds', 'under_2_5_odds',
        'home_odds', 'away_odds', 'draw_odds',
        'status', 'actual_winner'
    ])
    
    # Show sample match IDs for debugging
    print(f"\n🔍 Sample match IDs from database:")
    for i, match_id in enumerate(predictions_df['match_id'].head(3)):
        print(f"    {i+1}. {match_id} (type: {type(match_id).__name__})")
    
    # Filter only PENDING or not yet validated matches
    pending_matches = predictions_df[
        (predictions_df['status'] == 'PENDING') | 
        (predictions_df['actual_winner'].isna())
    ]
    
    print(f"\n✓ {len(pending_matches)} matches pending validation")
    print(f"✓ {len(predictions_df) - len(pending_matches)} matches already validated")
    
    if len(pending_matches) == 0:
        print(f"\n✓ All matches for {VALIDATION_DATE} have been validated")
        cursor.close()
        conn.close()
        exit(0)
    
except Exception as e:
    print(f"✗ Error loading predictions: {e}")
    cursor.close()
    conn.close()
    exit(1)

# ==================== FETCH MATCH RESULTS ====================
print("\n[3/6] Fetching match results from API...")
print("="*80)
print("📡 API Endpoint:", API_MATCH_URL)
print("="*80)

successful_updates = 0
failed_fetches = 0
api_errors = {'422': 0, 'empty': 0, 'json_error': 0, 'other': 0}

# Test with first match to see response format
if len(pending_matches) > 0:
    first_match_id = pending_matches.iloc[0]['match_id']
    test_url = f"{API_MATCH_URL}?key={API_KEY}&match_id={first_match_id}"
    print(f"\n🧪 Testing API with first match ID: {first_match_id}")
    print(f"📍 URL: {test_url[:80]}...")
    
    try:
        test_response = requests.get(test_url, timeout=30)
        print(f"📊 Status Code: {test_response.status_code}")
        if test_response.text:
            print(f"📄 Response length: {len(test_response.text)} bytes")
            print(f"📝 Response preview: {test_response.text[:200]}")
        else:
            print(f"⚠️  Response is empty")
    except Exception as e:
        print(f"⚠️  Test request failed: {e}")
    print("="*80)

for idx, row in pending_matches.iterrows():
    match_id = row['match_id']
    
    # Get prediction data
    predicted_ou = row['predicted_outcome']  # "Over 2.5" or "Under 2.5"
    predicted_winner = row['predicted_winner']  # "Home Win", "Away Win", or "Draw"
    
    # Get odds data
    odds_over = row['over_2_5_odds']
    odds_under = row['under_2_5_odds']
    odds_home = row['home_odds']
    odds_away = row['away_odds']
    odds_draw = row['draw_odds']
    
    try:
        # Fetch match details from API - USING SAME FORMAT AS WORKING SCRIPT
        url = f"{API_MATCH_URL}?key={API_KEY}&match_id={match_id}"
        response = requests.get(url, timeout=30)
        
        # Check status code
        if response.status_code == 200:
            # Check if response has content
            if not response.text or response.text.strip() == '':
                print(f"✗ {match_id}: Empty response from API")
                api_errors['empty'] += 1
                failed_fetches += 1
                continue
            
            # Try to parse JSON
            try:
                data = response.json()
            except json.JSONDecodeError as je:
                print(f"✗ {match_id}: Invalid JSON - {str(je)[:50]}")
                api_errors['json_error'] += 1
                failed_fetches += 1
                continue
            
            # Check if API returned success
            if data.get('success') and data.get('data'):
                match_data = data['data']
                status = match_data.get('status', '')
                
                if status == 'complete':
                    # ==================== GET ACTUAL SCORES ====================
                    home_score = match_data.get('homeGoalCount', 0)
                    away_score = match_data.get('awayGoalCount', 0)
                    
                    # Convert to integers
                    home_score = int(home_score)
                    away_score = int(away_score)
                    total_goals = home_score + away_score
                    
                    # ==================== DETERMINE ACTUAL WINNER ====================
                    if home_score > away_score:
                        actual_winner = row['home_team']
                    elif away_score > home_score:
                        actual_winner = row['away_team']
                    else:
                        actual_winner = 'Draw'
                    
                    # ==================== DETERMINE ACTUAL OVER/UNDER ====================
                    if total_goals > 2.5:
                        actual_over_under = 'Over 2.5'
                    else:
                        actual_over_under = 'Under 2.5'
                    
                    # ==================== CALCULATE PROFIT/LOSS FOR OVER/UNDER ====================
                    if 'Over' in str(predicted_ou):
                        if 'Over' in actual_over_under:
                            profit_loss_ou = round(odds_over - 1, 2) if not pd.isna(odds_over) else 0
                        else:
                            profit_loss_ou = -1.0
                    else:
                        if 'Under' in actual_over_under:
                            profit_loss_ou = round(odds_under - 1, 2) if not pd.isna(odds_under) else 0
                        else:
                            profit_loss_ou = -1.0
                    
                    # ==================== CALCULATE PROFIT/LOSS FOR WINNER ====================
                    if predicted_winner == 'Home Win':
                        if actual_winner == row['home_team']:
                            profit_loss_ml = round(odds_home - 1, 2) if not pd.isna(odds_home) else 0
                        else:
                            profit_loss_ml = -1.0
                    elif predicted_winner == 'Away Win':
                        if actual_winner == row['away_team']:
                            profit_loss_ml = round(odds_away - 1, 2) if not pd.isna(odds_away) else 0
                        else:
                            profit_loss_ml = -1.0
                    elif predicted_winner == 'Draw':
                        if actual_winner == 'Draw':
                            profit_loss_ml = round(odds_draw - 1, 2) if not pd.isna(odds_draw) else 0
                        else:
                            profit_loss_ml = -1.0
                    else:
                        profit_loss_ml = 0.0
                    
                    # ==================== UPDATE DATABASE ====================
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
                        'COMPLETE',
                        profit_loss_ou,
                        profit_loss_ml,
                        match_id
                    ))
                    
                    conn.commit()
                    successful_updates += 1
                    
                    # Display result
                    print(f"✓ {match_id}: {row['home_team']} {home_score}-{away_score} {row['away_team']}")
                    print(f"  → Winner: {actual_winner} | O/U: {actual_over_under}")
                    print(f"  → P/L O/U: ${profit_loss_ou:.2f} | P/L Winner: ${profit_loss_ml:.2f}")
                    
                elif status in ['cancelled', 'postponed', 'abandoned']:
                    cursor.execute(
                        sql.SQL("UPDATE {} SET status = %s WHERE match_id = %s").format(
                            sql.Identifier(TABLE_NAME)
                        ),
                        [status.upper(), match_id]
                    )
                    conn.commit()
                    print(f"⚠ {match_id}: Match {status}")
                    failed_fetches += 1
                    
                else:
                    print(f"⏳ {match_id}: Not complete (status: {status})")
                    failed_fetches += 1
            else:
                error_msg = data.get('error', 'No data from API')
                print(f"⚠ {match_id}: {error_msg}")
                api_errors['other'] += 1
                failed_fetches += 1
                
        elif response.status_code == 422:
            print(f"✗ {match_id}: HTTP 422 - Invalid match ID or API parameter format")
            api_errors['422'] += 1
            failed_fetches += 1
        else:
            print(f"✗ {match_id}: HTTP {response.status_code}")
            api_errors['other'] += 1
            failed_fetches += 1
        
        # Rate limiting
        time.sleep(0.25)
        
    except requests.exceptions.Timeout:
        print(f"✗ {match_id}: Timeout")
        api_errors['other'] += 1
        failed_fetches += 1
    except Exception as e:
        print(f"✗ {match_id}: Error - {str(e)}")
        api_errors['other'] += 1
        failed_fetches += 1

# ==================== ERROR ANALYSIS ====================
print("\n" + "="*80)
print("📊 API ERROR BREAKDOWN:")
print("="*80)
if api_errors['422'] > 0:
    print(f"  • HTTP 422 errors: {api_errors['422']}")
    print(f"    → This usually means the match_id format is not recognized by the API")
    print(f"    → Your match IDs might be from a different data source")
if api_errors['empty'] > 0:
    print(f"  • Empty responses: {api_errors['empty']}")
if api_errors['json_error'] > 0:
    print(f"  • JSON parse errors: {api_errors['json_error']}")
if api_errors['other'] > 0:
    print(f"  • Other errors: {api_errors['other']}")

# ==================== COMMIT UPDATES ====================
print("\n[4/6] Committing database updates...")
print("="*80)

try:
    conn.commit()
    print(f"✓ Database updates committed!")
except Exception as e:
    conn.rollback()
    print(f"✗ Commit failed: {e}")

# ==================== CALCULATE STATISTICS ====================
print("\n[5/6] Calculating performance statistics...")
print("="*80)

try:
    cursor.execute(sql.SQL("""
        SELECT 
            COUNT(*) as total_validated,
            SUM(CASE WHEN actual_winner IS NOT NULL THEN 1 ELSE 0 END) as completed,
            SUM(profit_loss_outcome) as total_profit_ou,
            SUM(profit_loss_winner) as total_profit_winner,
            AVG(profit_loss_outcome) as avg_profit_ou,
            AVG(profit_loss_winner) as avg_profit_winner
        FROM {}
        WHERE date = %s AND actual_winner IS NOT NULL
    """).format(sql.Identifier(TABLE_NAME)), [VALIDATION_DATE])
    
    stats = cursor.fetchone()
    
    if stats and stats[0] > 0:
        print(f"\n  Performance for {VALIDATION_DATE}:")
        print(f"    • Total Validated: {stats[1]}")
        print(f"    • Total P/L (O/U): ${stats[2]:.2f}" if stats[2] else "    • Total P/L (O/U): $0.00")
        print(f"    • Total P/L (Winner): ${stats[3]:.2f}" if stats[3] else "    • Total P/L (Winner): $0.00")
        print(f"    • Avg P/L (O/U): ${stats[4]:.2f}" if stats[4] else "    • Avg P/L (O/U): $0.00")
        print(f"    • Avg P/L (Winner): ${stats[5]:.2f}" if stats[5] else "    • Avg P/L (Winner): $0.00")
    
    cursor.execute(sql.SQL("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE 
                WHEN predicted_outcome = actual_over_under THEN 1 
                ELSE 0 
            END) as correct_ou,
            SUM(CASE 
                WHEN (predicted_winner = 'Home Win' AND actual_winner = home_team) OR
                     (predicted_winner = 'Away Win' AND actual_winner = away_team) OR
                     (predicted_winner = 'Draw' AND actual_winner = 'Draw')
                THEN 1 
                ELSE 0 
            END) as correct_winner
        FROM {}
        WHERE date = %s AND actual_winner IS NOT NULL
    """).format(sql.Identifier(TABLE_NAME)), [VALIDATION_DATE])
    
    accuracy = cursor.fetchone()
    
    if accuracy and accuracy[0] > 0:
        ou_accuracy = (accuracy[1] / accuracy[0]) * 100
        winner_accuracy = (accuracy[2] / accuracy[0]) * 100
        
        print(f"\n  Prediction Accuracy:")
        print(f"    • Over/Under: {accuracy[1]}/{accuracy[0]} ({ou_accuracy:.1f}%)")
        print(f"    • Winner: {accuracy[2]}/{accuracy[0]} ({winner_accuracy:.1f}%)")
    
    cursor.execute(sql.SQL("""
        SELECT 
            confidence_category,
            COUNT(*) as total,
            SUM(CASE 
                WHEN (predicted_winner = 'Home Win' AND actual_winner = home_team) OR
                     (predicted_winner = 'Away Win' AND actual_winner = away_team) OR
                     (predicted_winner = 'Draw' AND actual_winner = 'Draw')
                THEN 1 
                ELSE 0 
            END) as correct
        FROM {}
        WHERE date = %s AND actual_winner IS NOT NULL
        GROUP BY confidence_category
        ORDER BY 
            CASE confidence_category 
                WHEN 'High' THEN 1 
                WHEN 'Medium' THEN 2 
                WHEN 'Low' THEN 3 
                ELSE 4 
            END
    """).format(sql.Identifier(TABLE_NAME)), [VALIDATION_DATE])
    
    confidence_stats = cursor.fetchall()
    
    if confidence_stats:
        print(f"\n  Accuracy by Confidence:")
        for category, total, correct in confidence_stats:
            accuracy_pct = (correct / total) * 100 if total > 0 else 0
            print(f"    • {category}: {correct}/{total} ({accuracy_pct:.1f}%)")

except Exception as e:
    print(f"⚠ Statistics error: {e}")

# ==================== SUMMARY ====================
print("\n[6/6] SUMMARY")
print("="*80)
print(f"✓ Successfully updated: {successful_updates} matches")
print(f"✗ Failed/Pending: {failed_fetches} matches")

if successful_updates == 0 and api_errors['422'] > 0:
    print("\n⚠️  WARNING: All requests returned HTTP 422")
    print("   This suggests the match IDs in your database are not compatible")
    print("   with the football-data-api.com API.")
    print("\n💡 POSSIBLE SOLUTIONS:")
    print("   1. Check if match IDs are from a different API/source")
    print("   2. Verify the API key is valid and has access to these matches")
    print("   3. Compare match_id format with the old working database")

print(f"\n📊 Updated fields per match:")
print(f"  • actual_winner, actual_over_under")
print(f"  • actual_home_team_goals, actual_away_team_goals, actual_total_goals")
print(f"  • status, profit_loss_outcome, profit_loss_winner")

# ==================== CLOSE CONNECTION ====================
cursor.close()
conn.close()
print(f"\n✓ Database connection closed")

print("\n" + "="*80)
print("✅ VALIDATION COMPLETE!")
print("="*80)
print(f"⏰ Completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("="*80)
