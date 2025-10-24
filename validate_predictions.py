"""
MATCH PREDICTION VALIDATION SYSTEM
Validates predictions from best_match_predictions.csv against actual match results
Uses FootyStats API to fetch match results and generates validation report
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta
import warnings
from pathlib import Path
warnings.filterwarnings('ignore')

# API Configuration
API_KEY = "633379bdd5c4c3eb26919d8570866801e1c07f399197ba8c5311446b8ea77a49"
API_MATCH_URL = "https://api.football-data-api.com/match"

print("\n" + "="*80)
print("FOOTBALL MATCH PREDICTION VALIDATION SYSTEM")
print("="*80)

# ========== CONFIGURATION ==========
# Set the date you want to validate (default: yesterday)
# You can change this to any date in YYYY-MM-DD format
VALIDATION_DATE = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"\n📅 Validation Date: {VALIDATION_DATE}")

# ========== STEP 1: LOAD PREDICTIONS ==========
print("\n" + "="*80)
print("[1/5] Loading predictions from best_match_predictions.csv...")
print("="*80)

try:
    base_dir = Path(__file__).resolve().parent
    predictions_csv_path = base_dir / 'best_match_predictions.csv'
    predictions_df = pd.read_csv(predictions_csv_path)
    print(f"✓ Loaded {len(predictions_df)} total predictions")
    
    # Show available columns
    print(f"✓ Available columns: {list(predictions_df.columns)}")
    
except FileNotFoundError:
    print("✗ Error: best_match_predictions.csv not found!")
    print(f"  Looked for: {predictions_csv_path}")
    print("  Please run predict.py first to generate predictions.")
    exit(1)
except Exception as e:
    print(f"✗ Error loading predictions: {e}")
    exit(1)

# ========== STEP 2: FILTER PREDICTIONS FOR VALIDATION DATE ==========
print("\n" + "="*80)
print(f"[2/5] Filtering predictions for {VALIDATION_DATE}...")
print("="*80)

# Convert date column to datetime for proper filtering
predictions_df['date'] = pd.to_datetime(predictions_df['date']).dt.date
validation_date_obj = pd.to_datetime(VALIDATION_DATE).date()

# Filter predictions for the validation date
predictions_to_validate = predictions_df[predictions_df['date'] == validation_date_obj].copy()

if len(predictions_to_validate) == 0:
    print(f"ℹ No predictions found for {VALIDATION_DATE}")
    print(f"  Available dates in predictions:")
    available_dates = predictions_df['date'].value_counts().sort_index()
    for date, count in available_dates.head(10).items():
        print(f"    - {date}: {count} matches")
    print("\n💡 Tip: Modify VALIDATION_DATE at the top of this script to check other dates")
    exit(0)

print(f"✓ Found {len(predictions_to_validate)} predictions to validate")
print(f"  Matches: {predictions_to_validate['home_team_name'].nunique() + predictions_to_validate['away_team_name'].nunique()} unique teams")

# ========== STEP 3: FETCH ACTUAL MATCH RESULTS FROM API ==========
print("\n" + "="*80)
print("[3/5] Fetching actual match results from FootyStats API...")
print("="*80)

validation_results = []
successful_fetches = 0
failed_fetches = 0

for idx, row in predictions_to_validate.iterrows():
    match_id = row['match_id']
    
    try:
        # Fetch match details from API
        url = f"{API_MATCH_URL}?key={API_KEY}&match_id={match_id}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('success') and data.get('data'):
                match_data = data['data']
                
                # Check if match is complete
                status = match_data.get('status', '')
                
                if status == 'complete':
                    # Extract actual scores
                    home_score = match_data.get('homeGoalCount', 0)
                    away_score = match_data.get('awayGoalCount', 0)
                    total_goals = home_score + away_score
                    
                    # Determine actual outcome
                    if home_score > away_score:
                        actual_outcome = '1'  # Home Win
                        actual_outcome_label = 'Home Win'
                    elif away_score > home_score:
                        actual_outcome = '2'  # Away Win
                        actual_outcome_label = 'Away Win'
                    else:
                        actual_outcome = 'X'  # Draw
                        actual_outcome_label = 'Draw'
                    
                    # Determine actual over/under results
                    actual_over_1_5 = 1 if total_goals > 1.5 else 0
                    actual_over_2_5 = 1 if total_goals > 2.5 else 0
                    actual_over_3_5 = 1 if total_goals > 3.5 else 0
                    
                    # Determine actual BTTS
                    actual_btts = 1 if home_score > 0 and away_score > 0 else 0
                    
                    # Compare predictions with actuals
                    outcome_correct = 1 if row['predicted_outcome'] == actual_outcome else 0
                    over_1_5_correct = 1 if row['predicted_over_1.5'] == actual_over_1_5 else 0
                    over_2_5_correct = 1 if row['predicted_over_2.5'] == actual_over_2_5 else 0
                    over_3_5_correct = 1 if row['predicted_over_3.5'] == actual_over_3_5 else 0
                    btts_correct = 1 if row['predicted_btts'] == actual_btts else 0
                    
                    # Calculate goal prediction accuracy
                    home_goal_error = abs(row['predicted_home_goals'] - home_score)
                    away_goal_error = abs(row['predicted_away_goals'] - away_score)
                    total_goal_error = abs(row['predicted_total_goals'] - total_goals)
                    
                    # Create validation record
                    validation_record = {
                        # Match Information
                        'match_id': match_id,
                        'date': row['date'],
                        'home_team_name': row['home_team_name'],
                        'away_team_name': row['away_team_name'],
                        
                        # Actual Results
                        'actual_home_score': home_score,
                        'actual_away_score': away_score,
                        'actual_total_goals': total_goals,
                        'actual_outcome': actual_outcome,
                        'actual_outcome_label': actual_outcome_label,
                        'actual_over_1.5': actual_over_1_5,
                        'actual_over_2.5': actual_over_2_5,
                        'actual_over_3.5': actual_over_3_5,
                        'actual_btts': actual_btts,
                        
                        # Predicted Results
                        'predicted_home_goals': row['predicted_home_goals'],
                        'predicted_away_goals': row['predicted_away_goals'],
                        'predicted_total_goals': row['predicted_total_goals'],
                        'predicted_outcome': row['predicted_outcome'],
                        'predicted_outcome_label': row['outcome_label'],
                        'predicted_over_1.5': row['predicted_over_1.5'],
                        'predicted_over_2.5': row['predicted_over_2.5'],
                        'predicted_over_3.5': row['predicted_over_3.5'],
                        'predicted_btts': row['predicted_btts'],
                        
                        # Validation Results
                        'outcome_correct': outcome_correct,
                        'over_1.5_correct': over_1_5_correct,
                        'over_2.5_correct': over_2_5_correct,
                        'over_3.5_correct': over_3_5_correct,
                        'btts_correct': btts_correct,
                        
                        # Error Metrics
                        'home_goal_error': round(home_goal_error, 2),
                        'away_goal_error': round(away_goal_error, 2),
                        'total_goal_error': round(total_goal_error, 2),
                        
                        # Additional Info
                        'confidence_category': row.get('confidence_category', 'Unknown'),
                        'moneyline_profit': row.get('moneyline_profit', 0),
                        
                        # Status
                        'validation_status': 'Complete'
                    }
                    
                    validation_results.append(validation_record)
                    successful_fetches += 1
                    
                    print(f"✓ Match {match_id}: {row['home_team_name']} {home_score}-{away_score} {row['away_team_name']} - Outcome: {'✓' if outcome_correct else '✗'}")
                    
                else:
                    # Match not yet complete
                    print(f"⏳ Match {match_id}: Status = {status} (Not complete yet)")
                    failed_fetches += 1
            else:
                print(f"⚠ Match {match_id}: No data returned from API")
                failed_fetches += 1
        else:
            print(f"✗ Match {match_id}: HTTP {response.status_code}")
            failed_fetches += 1
        
        # Rate limiting
        time.sleep(0.2)
        
    except Exception as e:
        print(f"✗ Match {match_id}: Error - {str(e)}")
        failed_fetches += 1

print(f"\n✓ API fetch complete!")
print(f"  - Successful: {successful_fetches} matches")
print(f"  - Failed/Incomplete: {failed_fetches} matches")

# ========== STEP 4: CREATE VALIDATION DATAFRAME AND SAVE ==========
print("\n" + "="*80)
print("[4/5] Creating validation report...")
print("="*80)

if len(validation_results) == 0:
    print("⚠ No completed matches to validate")
    print("  Matches may not have been played yet or API data not available")
    exit(0)

validation_df = pd.DataFrame(validation_results)

# Generate output filename with date
output_filename = f'validation_results_{VALIDATION_DATE}.csv'
validation_df.to_csv(output_filename, index=False)

print(f"✓ Validation results saved to: {output_filename}")
print(f"✓ Total validated matches: {len(validation_df)}")

# ========== STEP 5: GENERATE VALIDATION STATISTICS ==========
print("\n" + "="*80)
print("[5/5] VALIDATION STATISTICS & ANALYSIS")
print("="*80)

# Overall accuracy metrics
outcome_accuracy = (validation_df['outcome_correct'].sum() / len(validation_df)) * 100
over_1_5_accuracy = (validation_df['over_1.5_correct'].sum() / len(validation_df)) * 100
over_2_5_accuracy = (validation_df['over_2.5_correct'].sum() / len(validation_df)) * 100
over_3_5_accuracy = (validation_df['over_3.5_correct'].sum() / len(validation_df)) * 100
btts_accuracy = (validation_df['btts_correct'].sum() / len(validation_df)) * 100

print(f"\n🎯 PREDICTION ACCURACY:")
print(f"  • Match Outcome (1X2): {outcome_accuracy:.1f}% ({validation_df['outcome_correct'].sum()}/{len(validation_df)})")
print(f"  • Over 1.5 Goals: {over_1_5_accuracy:.1f}% ({validation_df['over_1.5_correct'].sum()}/{len(validation_df)})")
print(f"  • Over 2.5 Goals: {over_2_5_accuracy:.1f}% ({validation_df['over_2.5_correct'].sum()}/{len(validation_df)})")
print(f"  • Over 3.5 Goals: {over_3_5_accuracy:.1f}% ({validation_df['over_3.5_correct'].sum()}/{len(validation_df)})")
print(f"  • Both Teams to Score: {btts_accuracy:.1f}% ({validation_df['btts_correct'].sum()}/{len(validation_df)})")

# Goal prediction accuracy
print(f"\n⚽ GOAL PREDICTION ERROR:")
print(f"  • Average Home Goal Error: {validation_df['home_goal_error'].mean():.2f} goals")
print(f"  • Average Away Goal Error: {validation_df['away_goal_error'].mean():.2f} goals")
print(f"  • Average Total Goal Error: {validation_df['total_goal_error'].mean():.2f} goals")

# Confidence-based analysis
if 'confidence_category' in validation_df.columns:
    print(f"\n💪 ACCURACY BY CONFIDENCE:")
    for conf in ['High', 'Medium', 'Low']:
        conf_matches = validation_df[validation_df['confidence_category'] == conf]
        if len(conf_matches) > 0:
            conf_accuracy = (conf_matches['outcome_correct'].sum() / len(conf_matches)) * 100
            print(f"  • {conf} Confidence: {conf_accuracy:.1f}% ({conf_matches['outcome_correct'].sum()}/{len(conf_matches)} matches)")

# Outcome distribution analysis
print(f"\n📊 OUTCOME DISTRIBUTION:")
print(f"\nPredicted vs Actual:")
predicted_dist = validation_df['predicted_outcome_label'].value_counts()
actual_dist = validation_df['actual_outcome_label'].value_counts()
for outcome in ['Home Win', 'Draw', 'Away Win']:
    pred_count = predicted_dist.get(outcome, 0)
    actual_count = actual_dist.get(outcome, 0)
    print(f"  • {outcome}: Predicted {pred_count}, Actual {actual_count}")

# Profit/Loss Analysis (if available)
if 'moneyline_profit' in validation_df.columns:
    print(f"\n💰 BETTING SIMULATION (Based on predictions):")
    total_matches = len(validation_df)
    successful_bets = validation_df['outcome_correct'].sum()
    
    # Calculate theoretical profit
    theoretical_total_profit = validation_df[validation_df['outcome_correct'] == 1]['moneyline_profit'].sum()
    theoretical_total_loss = total_matches - successful_bets  # Lost $1 per failed bet
    theoretical_net_profit = theoretical_total_profit - theoretical_total_loss
    
    print(f"  • Total Bets: {total_matches}")
    print(f"  • Successful Bets: {successful_bets}")
    print(f"  • Win Rate: {(successful_bets/total_matches)*100:.1f}%")
    print(f"  • Theoretical Profit: ${theoretical_total_profit:.2f}")
    print(f"  • Theoretical Loss: ${theoretical_total_loss:.2f}")
    print(f"  • Net Profit/Loss: ${theoretical_net_profit:.2f}")
    print(f"  • ROI: {(theoretical_net_profit/total_matches)*100:.1f}%")

# Show sample results
print(f"\n" + "="*80)
print("SAMPLE VALIDATION RESULTS")
print("="*80)

display_cols = ['home_team_name', 'away_team_name', 
                'actual_home_score', 'actual_away_score',
                'predicted_home_goals', 'predicted_away_goals',
                'outcome_correct', 'total_goal_error']

if len(validation_df) > 0:
    print("\n" + validation_df[display_cols].head(10).to_string(index=False))

# Show incorrect predictions for analysis
incorrect_predictions = validation_df[validation_df['outcome_correct'] == 0]
if len(incorrect_predictions) > 0:
    print(f"\n" + "="*80)
    print(f"⚠️  INCORRECT PREDICTIONS ({len(incorrect_predictions)} matches)")
    print("="*80)
    
    incorrect_display = incorrect_predictions[['home_team_name', 'away_team_name', 
                                               'actual_home_score', 'actual_away_score',
                                               'predicted_outcome_label', 'actual_outcome_label']].head(10)
    print("\n" + incorrect_display.to_string(index=False))

print("\n" + "="*80)
print("✅ VALIDATION COMPLETE!")
print("="*80)
print(f"\n📄 Validation report saved to: {output_filename}")
print(f"📊 Date validated: {VALIDATION_DATE}")
print(f"✓ Total matches validated: {len(validation_df)}")
print(f"🎯 Overall accuracy: {outcome_accuracy:.1f}%")
print(f"⏰ Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

print("\n💡 Tips:")
print("  • Run this script daily to track your model's performance")
print("  • Modify VALIDATION_DATE variable to check other dates")
print("  • Use validation results to improve your prediction model")
print("  • Track accuracy trends over time to assess model stability")

print("\n" + "="*80)
