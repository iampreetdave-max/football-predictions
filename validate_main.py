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
warnings.filterwarnings('ignore')

# ==================== API CONFIGURATION ====================
API_KEY = "633379bdd5c4c3eb26919d8570866801e1c07f399197ba8c5311446b8ea77a49"
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
    # ORDER BY match_id ensures consistent processing order
    # NOTE: Records maintain their original insertion order (id/created_at)
    # even after validation updates updated_at timestamp
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
    
    # Filter only PENDING or not yet validated matches
    pending_matches = predictions_df[
        (predictions_df['status'] == 'PENDING') | 
        (predictions_df['actual_winner'].isna())
    ]
    
    print(f"✓ {len(pending_matches)} matches pending validation")
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

successful_updates = 0
failed_fetches = 0
already_complete = 0

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
        # Fetch match details from API
        url = f"{API_MATCH_URL}?key={API_KEY}&match_id={match_id}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('success') and data.get('data'):
                match_data = data['data']
                status = match_data.get('status', '')
                
                if status == 'complete':
                    # ==================== GET ACTUAL SCORES ====================
                    home_score = match_data.get('homeGoalCount', 0)
                    away_score = match_data.get('awayGoalCount', 0)
                    total_goals = home_score + away_score
                    
                    # ==================== DETERMINE ACTUAL WINNER ====================
                    if home_score > away_score:
                        actual_winner = row['home_team']
                    elif away_score > home_score:
                        actual_winner = row['away_team']
                    else:
                        actual_winner = 'Draw'
                    
                    # ==================== DETERMINE ACTUAL OVER/UNDER ====================
                    # Based on 2.5 goals
                    if total_goals > 2.5:
                        actual_over_under = 'Over 2.5'
                    else:
                        actual_over_under = 'Under 2.5'
                    
                    # ==================== CALCULATE PROFIT/LOSS FOR OVER/UNDER ====================
                    if 'Over' in predicted_ou:
                        # We predicted over 2.5
                        if 'Over' in actual_over_under:
                            profit_loss_ou = round(odds_over - 1, 2) if not pd.isna(odds_over) else 0
                        else:
                            profit_loss_ou = -1.0
                    else:
                        # We predicted under 2.5
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
                    
                    # Commit this update immediately
                    conn.commit()
                    
                    successful_updates += 1
                    
                    # Display result
                    print(f"\n✓ Match {successful_updates}: {row['home_team']} vs {row['away_team']}")
                    print(f"  Score: {home_score} - {away_score}")
                    print(f"  Winner: {actual_winner}")
                    print(f"  Total Goals: {total_goals} ({actual_over_under})")
                    print(f"  P/L Over/Under: ${profit_loss_ou:.2f}")
                    print(f"  P/L Winner: ${profit_loss_ml:.2f}")
                    
                elif status in ['cancelled', 'postponed', 'abandoned']:
                    # Update status for cancelled/postponed matches
                    cursor.execute(
                        sql.SQL("UPDATE {} SET status = %s WHERE match_id = %s").format(
                            sql.Identifier(TABLE_NAME)
                        ),
                        [status.upper(), match_id]
                    )
                    conn.commit()  # Commit status update
                    print(f"⚠ {match_id}: Match {status}")
                    failed_fetches += 1
                    
                else:
                    # Match not complete yet
                    print(f"⏳ {match_id}: Match not complete (status: {status})")
                    failed_fetches += 1
            else:
                print(f"⚠ {match_id}: No data from API")
                failed_fetches += 1
        else:
            print(f"✗ {match_id}: HTTP {response.status_code}")
            failed_fetches += 1
        
        # Rate limiting to avoid overwhelming the API
        time.sleep(0.3)
        
    except Exception as e:
        print(f"✗ {match_id}: Error - {str(e)}")
        failed_fetches += 1

# ==================== COMMIT UPDATES ====================
print("\n[4/6] Committing database updates...")
print("="*80)

try:
    conn.commit()
    print(f"✓ Database updates committed successfully!")
except Exception as e:
    conn.rollback()
    print(f"✗ Database commit failed: {e}")

# ==================== CALCULATE STATISTICS ====================
print("\n[5/6] Calculating performance statistics...")
print("="*80)

try:
    # Overall statistics
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
    
    # Accuracy by prediction type
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
    
    # Accuracy by confidence category
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
    print(f"⚠ Could not calculate statistics: {e}")

# ==================== SUMMARY ====================
print("\n[6/6] SUMMARY")
print("="*80)
print(f"✓ Successfully updated: {successful_updates} matches")
print(f"⏳ Pending/Failed: {failed_fetches} matches")

print(f"\n📊 Updated fields per match:")
print(f"  • actual_winner")
print(f"  • actual_over_under")
print(f"  • actual_home_team_goals (NEW)")
print(f"  • actual_away_team_goals (NEW)")
print(f"  • actual_total_goals (NEW)")
print(f"  • status")
print(f"  • profit_loss_outcome")
print(f"  • profit_loss_winner")

# ==================== FINAL COMMIT ====================
print(f"\nEnsuring all updates are committed...")
try:
    conn.commit()
    print("✓ Final commit successful - all updates saved!")
except Exception as e:
    print(f"✗ Final commit error: {e}")

# ==================== CLOSE CONNECTION ====================
cursor.close()
conn.close()
print(f"✓ Database connection closed")

print("\n" + "="*80)
print("✅ VALIDATION COMPLETE!")
print("="*80)
print(f"⏰ Completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("\n💡 Tip: This script runs automatically via GitHub Actions daily")
print("="*80)
