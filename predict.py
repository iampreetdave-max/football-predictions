"""
FOOTBALL MATCH OUTCOME PREDICTION - INCREMENTAL VERSION
Uses trained Ridge models (home & away) with scaler to predict match outcomes
Only predicts NEW matches that haven't been predicted yet
Based on extracted_features_complete.csv
"""

import pandas as pd
import numpy as np
import joblib
import warnings
import os
from datetime import datetime
warnings.filterwarnings('ignore')

print("\n" + "="*80)
print("FOOTBALL MATCH PREDICTION SYSTEM - INCREMENTAL")
print("Using Ridge Regression Models")
print("="*80)

# ========== STEP 1: LOAD DATA ==========
print("\n[1/6] Loading extracted features...")
df = pd.read_csv('extracted_features_complete.csv')
print(f"✓ Loaded {len(df)} matches")
print(f"✓ Columns: {list(df.columns)}")

# ========== STEP 2: CHECK FOR EXISTING PREDICTIONS ==========
print("\n[2/6] Checking for existing predictions...")

existing_predictions_file = 'best_match_predictions.csv'
predicted_match_ids = set()
existing_df = None

if os.path.exists(existing_predictions_file):
    try:
        existing_df = pd.read_csv(existing_predictions_file)
        predicted_match_ids = set(existing_df['match_id'].values)
        print(f"✓ Found {len(predicted_match_ids)} previously predicted matches")
    except Exception as e:
        print(f"⚠ Could not load existing predictions: {e}")
        print("  Will create new predictions file")
else:
    print(f"ℹ No existing predictions file found")
    print("  Will create new predictions file")

# Filter for new matches only
new_matches_mask = ~df['match_id'].isin(predicted_match_ids)
new_matches_df = df[new_matches_mask].copy()

if len(new_matches_df) == 0:
    print("\n" + "="*80)
    print("✓ NO NEW MATCHES TO PREDICT")
    print("="*80)
    print(f"All {len(df)} matches have already been predicted.")
    print(f"Total predictions in database: {len(predicted_match_ids)}")
    print("\n✓ Predictions are up to date!")
    exit(0)

print(f"\n✓ Found {len(new_matches_df)} NEW matches to predict")
print(f"  Already predicted: {len(predicted_match_ids)} matches")
print(f"  New predictions: {len(new_matches_df)} matches")

# Use new matches for prediction
df = new_matches_df

# ========== STEP 3: LOAD MODELS AND SCALER ==========
print("\n[3/6] Loading trained models and scaler...")

try:
    ridge_home_model = joblib.load('ridge_home_model.pkl')
    print("✓ Home goals model loaded")
except Exception as e:
    print(f"✗ Error loading home model: {e}")
    exit(1)

try:
    ridge_away_model = joblib.load('ridge_away_model.pkl')
    print("✓ Away goals model loaded")
except Exception as e:
    print(f"✗ Error loading away model: {e}")
    exit(1)

try:
    scaler = joblib.load('scaler.pkl')
    print("✓ Feature scaler loaded")
except Exception as e:
    print(f"✗ Error loading scaler: {e}")
    exit(1)

# ========== STEP 4: PREPARE FEATURES ==========
print("\n[4/6] Preparing features...")

# Define exact feature columns used during model training (21 features)
feature_columns = [
    'CTMCL',
    'avg_goals_market',
    'team_a_xg_prematch', 'team_b_xg_prematch',
    'pre_match_home_ppg', 'pre_match_away_ppg',
    'home_xg_avg', 'away_xg_avg',
    'home_goals_conceded_avg', 'away_goals_conceded_avg',
    'o25_potential', 'o35_potential',
    'home_shots_accuracy_avg', 'away_shots_accuracy_avg',
    'home_dangerous_attacks_avg', 'away_dangerous_attacks_avg',
    'home_form_points', 'away_form_points',
    'league_avg_goals',
]

# Add odds features if they exist
for col in ['odds_ft_1_prob', 'odds_ft_2_prob']:
    if col in df.columns:
        feature_columns.append(col)

# Check for missing features
missing_features = [f for f in feature_columns if f not in df.columns]
if missing_features:
    print(f"⚠ Warning: Missing features: {missing_features}")
    feature_columns = [f for f in feature_columns if f in df.columns]

print(f"✓ Feature columns identified: {len(feature_columns)} features")
print(f"  Features: {', '.join(feature_columns)}")

# Extract features
X = df[feature_columns].copy()

# Handle any missing values (fill with 0 or median)
if X.isnull().any().any():
    print(f"⚠ Warning: Found {X.isnull().sum().sum()} missing values, filling with 0")
    X = X.fillna(0)

print(f"✓ Feature matrix shape: {X.shape}")

# ========== STEP 5: SCALE FEATURES AND MAKE PREDICTIONS ==========
print("\n[5/6] Scaling features and making predictions...")

try:
    # Define feature weights (matching model training)
    feature_weights_dict = {
        'CTMCL': 2.0,
        'avg_goals_market': 1.4,
        'odds_ft_1_prob': 1.3,
        'odds_ft_2_prob': 1.3,
        'team_a_xg_prematch': 1.3,
        'team_b_xg_prematch': 1.3,
        'home_xg_avg': 1.2,
        'away_xg_avg': 1.2,
        'pre_match_home_ppg': 1.2,
        'pre_match_away_ppg': 1.2,
        'home_form_points': 1.1,
        'away_form_points': 1.1,
        'home_goals_conceded_avg': 1.0,
        'away_goals_conceded_avg': 1.0,
        'home_shots_accuracy_avg': 1.1,
        'away_shots_accuracy_avg': 1.1,
        'home_dangerous_attacks_avg': 1.1,
        'away_dangerous_attacks_avg': 1.1,
        'o25_potential': 1.1,
        'o35_potential': 1.0,
        'league_avg_goals': 0.9,
    }
    
    # Create weight array matching feature columns
    weights = np.array([feature_weights_dict.get(feat, 1.0) for feat in feature_columns])
    print(f"✓ Feature weights applied")
    
    # Apply weights to features (matching training process)
    X_weighted = X.values * weights
    
    # Scale features using the loaded scaler
    X_scaled = scaler.transform(X_weighted)
    print("✓ Features scaled successfully")
    
    # Make predictions
    home_goals_pred = ridge_home_model.predict(X_scaled)
    away_goals_pred = ridge_away_model.predict(X_scaled)
    total_goals_pred = home_goals_pred + away_goals_pred
    
    print("✓ Predictions generated successfully")
    
except Exception as e:
    print(f"✗ Error during prediction: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# ========== STEP 6: CREATE RESULTS AND SAVE ==========
print("\n[6/6] Creating results dataframe...")

# Create comprehensive results
results = pd.DataFrame({
    # Match identifiers
    'match_id': df['match_id'],
    'home_team_id': df['home_team_id'],
    'away_team_id': df['away_team_id'],
    'league_id': df['league_id'],
    'home_team_name': df['home_team_name'],
    'away_team_name': df['away_team_name'],
    
    # Predictions
    'predicted_home_goals': home_goals_pred,
    'predicted_away_goals': away_goals_pred,
    'predicted_total_goals': total_goals_pred,
})

# Round predictions to 2 decimal places
results['predicted_home_goals'] = results['predicted_home_goals'].round(2)
results['predicted_away_goals'] = results['predicted_away_goals'].round(2)
results['predicted_total_goals'] = results['predicted_total_goals'].round(2)

# Add goal difference
results['predicted_goal_diff'] = (results['predicted_home_goals'] - 
                                   results['predicted_away_goals']).round(2)

# Predict match outcome (1=Home Win, X=Draw, 2=Away Win)
def predict_outcome(home_goals, away_goals, threshold=0.15):
    """Predict match outcome with draw threshold"""
    diff = home_goals - away_goals
    if diff > threshold:
        return '1'  # Home Win
    elif diff < -threshold:
        return '2'  # Away Win
    else:
        return 'X'  # Draw

results['predicted_outcome'] = results.apply(
    lambda row: predict_outcome(row['predicted_home_goals'], 
                                row['predicted_away_goals']), 
    axis=1
)

# Add outcome labels for clarity
outcome_labels = {
    '1': 'Home Win',
    'X': 'Draw', 
    '2': 'Away Win'
}
results['outcome_label'] = results['predicted_outcome'].map(outcome_labels)

# Add over/under predictions
results['predicted_over_2.5'] = (results['predicted_total_goals'] > 2.5).astype(int)
results['predicted_over_1.5'] = (results['predicted_total_goals'] > 1.5).astype(int)
results['predicted_over_3.5'] = (results['predicted_total_goals'] > 3.5).astype(int)

# Add BTTS prediction (both teams to score)
results['predicted_btts'] = ((results['predicted_home_goals'] >= 0.75) & 
                              (results['predicted_away_goals'] >= 0.75)).astype(int)

# Add confidence score (based on goal difference)
results['confidence'] = np.abs(results['predicted_goal_diff'])
results['confidence_category'] = pd.cut(results['confidence'], 
                                         bins=[0, 0.3, 0.7, 10],
                                         labels=['Low', 'Medium', 'High'])

# Add prediction timestamp
results['prediction_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')

# ========== ADD BETTING PROFIT CALCULATIONS ==========
print("✓ Calculating betting profits...")

# Calculate moneyline profit (based on predicted outcome and odds)
# Formula: If bet $1, profit = 1 / probability (implied odds payout)
results['moneyline_profit'] = 0.0

for idx, row in results.iterrows():
    outcome = row['predicted_outcome']
    
    # Get odds probabilities from original dataframe
    if 'odds_ft_1_prob' in df.columns and 'odds_ft_2_prob' in df.columns:
        match_row = df[df['match_id'] == row['match_id']]
        
        if len(match_row) > 0:
            odds_home = match_row['odds_ft_1_prob'].values[0]
            odds_away = match_row['odds_ft_2_prob'].values[0]
            
            # Calculate profit based on predicted outcome
            if outcome == '1':  # Home Win
                # Profit = (1 / odds_home) if we bet $1 on home
                if odds_home > 0:
                    results.at[idx, 'moneyline_profit'] = round(1 / odds_home, 2)
            elif outcome == '2':  # Away Win
                # Profit = (1 / odds_away) if we bet $1 on away
                if odds_away > 0:
                    results.at[idx, 'moneyline_profit'] = round(1 / odds_away, 2)
            elif outcome == 'X':  # Draw
                # For draws, use average of home and away odds
                if odds_home > 0 and odds_away > 0:
                    avg_odds = (odds_home + odds_away) / 2
                    results.at[idx, 'moneyline_profit'] = round(1 / avg_odds, 2)

# Calculate over 2.5 profit (based on o25_potential)
# Formula: If bet $1, profit = 1 / (o25_potential/100) if we predict over 2.5
results['over_profit'] = 0.0

for idx, row in results.iterrows():
    # Get o25_potential from original dataframe
    if 'o25_potential' in df.columns:
        match_row = df[df['match_id'] == row['match_id']]
        
        if len(match_row) > 0:
            o25_pot = match_row['o25_potential'].values[0]
            
            # Calculate profit if we predict over 2.5 goals
            if row['predicted_over_2.5'] == 1:  # We predict over 2.5
                # Convert o25_potential (0-100) to probability (0-1)
                o25_prob = o25_pot / 100 if o25_pot > 0 else 0.5
                
                # Profit = (1 / probability) if we bet $1
                if o25_prob > 0:
                    results.at[idx, 'over_profit'] = round(1 / o25_prob, 2)
            else:  # We predict under 2.5
                # Use inverse probability (under 2.5)
                under_prob = (100 - o25_pot) / 100 if o25_pot < 100 else 0.5
                
                if under_prob > 0:
                    results.at[idx, 'over_profit'] = round(1 / under_prob, 2)

print(f"✓ Betting profits calculated")
print(f"  Average moneyline profit: ${results['moneyline_profit'].mean():.2f}")
print(f"  Average over profit: ${results['over_profit'].mean():.2f}")

print("✓ Results dataframe created")

# ========== APPEND OR CREATE CSV ==========
output_file = 'best_match_predictions.csv'

if existing_df is not None and len(predicted_match_ids) > 0:
    # Append new predictions to existing
    combined_results = pd.concat([existing_df, results], ignore_index=True)
    combined_results.to_csv(output_file, index=False)
    print(f"✓ Appended {len(results)} new predictions to existing file")
    print(f"  Total predictions in file: {len(combined_results)}")
else:
    # Create new file
    results.to_csv(output_file, index=False)
    print(f"✓ Created new predictions file with {len(results)} predictions")

# Use combined results for display if appending
display_results = results  # Only show new predictions in summary

# ========== DISPLAY SUMMARY STATISTICS ==========
print("\n" + "="*80)
print("NEW PREDICTIONS SUMMARY")
print("="*80)

print(f"\n📊 New matches predicted: {len(display_results)}")
if existing_df is not None:
    print(f"📊 Total predictions in database: {len(predicted_match_ids) + len(results)}")

print(f"\n⚽ Goal Predictions (New Matches):")
print(f"  • Average predicted home goals: {display_results['predicted_home_goals'].mean():.2f}")
print(f"  • Average predicted away goals: {display_results['predicted_away_goals'].mean():.2f}")
print(f"  • Average predicted total goals: {display_results['predicted_total_goals'].mean():.2f}")
print(f"  • Min total goals: {display_results['predicted_total_goals'].min():.2f}")
print(f"  • Max total goals: {display_results['predicted_total_goals'].max():.2f}")

print(f"\n🏆 Outcome Distribution (New Matches):")
outcome_counts = display_results['outcome_label'].value_counts()
for outcome, count in outcome_counts.items():
    percentage = (count / len(display_results)) * 100
    print(f"  • {outcome}: {count} ({percentage:.1f}%)")

print(f"\n📈 Over/Under Predictions (New Matches):")
print(f"  • Over 1.5 goals: {display_results['predicted_over_1.5'].sum()} ({display_results['predicted_over_1.5'].mean()*100:.1f}%)")
print(f"  • Over 2.5 goals: {display_results['predicted_over_2.5'].sum()} ({display_results['predicted_over_2.5'].mean()*100:.1f}%)")
print(f"  • Over 3.5 goals: {display_results['predicted_over_3.5'].sum()} ({display_results['predicted_over_3.5'].mean()*100:.1f}%)")

print(f"\n🎯 Both Teams to Score (BTTS) (New Matches):")
print(f"  • Yes: {display_results['predicted_btts'].sum()} ({display_results['predicted_btts'].mean()*100:.1f}%)")
print(f"  • No: {(1-display_results['predicted_btts']).sum()} ({(1-display_results['predicted_btts']).mean()*100:.1f}%)")

print(f"\n💪 Prediction Confidence (New Matches):")
confidence_counts = display_results['confidence_category'].value_counts()
for conf, count in confidence_counts.items():
    percentage = (count / len(display_results)) * 100
    print(f"  • {conf}: {count} ({percentage:.1f}%)")

print(f"\n💰 Betting Profit Analysis (New Matches):")
print(f"  • Average Moneyline Profit: ${display_results['moneyline_profit'].mean():.2f}")
print(f"  • Max Moneyline Profit: ${display_results['moneyline_profit'].max():.2f}")
print(f"  • Average Over 2.5 Profit: ${display_results['over_profit'].mean():.2f}")
print(f"  • Max Over 2.5 Profit: ${display_results['over_profit'].max():.2f}")
print(f"  • Total Potential Moneyline Profit: ${display_results['moneyline_profit'].sum():.2f}")
print(f"  • Total Potential Over Profit: ${display_results['over_profit'].sum():.2f}")

# ========== DISPLAY DETAILED PREDICTIONS (Sample) ==========
print("\n" + "="*80)
print("SAMPLE OF NEW PREDICTIONS")
print("="*80)

display_cols = ['match_id', 'home_team_name', 'away_team_name', 
                'predicted_home_goals', 'predicted_away_goals', 
                'predicted_total_goals', 'outcome_label', 'confidence_category',
                'moneyline_profit', 'over_profit']

print("\n" + display_results[display_cols].head(10).to_string(index=False))

# ========== HIGH-PROFIT OPPORTUNITIES ==========
print("\n" + "="*80)
print("💰 HIGH-PROFIT OPPORTUNITIES")
print("="*80)

# Moneyline high-profit bets
high_moneyline = display_results[display_results['moneyline_profit'] > display_results['moneyline_profit'].quantile(0.75)].copy()
if len(high_moneyline) > 0:
    high_moneyline_sorted = high_moneyline.sort_values('moneyline_profit', ascending=False)
    print(f"\n🎯 Top Moneyline Profit Opportunities ({len(high_moneyline)} matches):")
    print(f"{'Home Team':<20} {'Away Team':<20} {'Outcome':<10} {'Profit':<8}")
    print("-" * 80)
    for _, row in high_moneyline_sorted.head(5).iterrows():
        print(f"{row['home_team_name'][:19]:<20} {row['away_team_name'][:19]:<20} {row['outcome_label']:<10} ${row['moneyline_profit']:.2f}")
else:
    print("\nℹ️  No high-profit moneyline opportunities found")

# Over 2.5 high-profit bets
high_over = display_results[display_results['over_profit'] > display_results['over_profit'].quantile(0.75)].copy()
if len(high_over) > 0:
    high_over_sorted = high_over.sort_values('over_profit', ascending=False)
    print(f"\n📈 Top Over 2.5 Profit Opportunities ({len(high_over)} matches):")
    print(f"{'Home Team':<20} {'Away Team':<20} {'Total Goals':<12} {'Profit':<8}")
    print("-" * 80)
    for _, row in high_over_sorted.head(5).iterrows():
        print(f"{row['home_team_name'][:19]:<20} {row['away_team_name'][:19]:<20} {row['predicted_total_goals']:<12.2f} ${row['over_profit']:.2f}")
else:
    print("\nℹ️  No high-profit over 2.5 opportunities found")

print("\n" + "="*80)
print("✅ PREDICTION COMPLETE!")
print("="*80)
print(f"\n📄 Full results saved to: {output_file}")
print(f"🆕 New predictions: {len(results)}")
if existing_df is not None:
    print(f"📊 Total predictions: {len(predicted_match_ids) + len(results)}")
print(f"⏰ Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

print("\n" + "="*80)
