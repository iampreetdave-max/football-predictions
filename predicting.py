"""
FOOTBALL MATCH OUTCOME PREDICTION - ENHANCED VERSION
Uses trained Ridge models (home & away) with scaler to predict match outcomes
Only predicts NEW matches that haven't been predicted yet
Based on extracted_features_complete.csv

ENHANCEMENTS:
1. Added new odds columns: odds_ft_over25, odds_ft_under25, odds_ft_1, odds_ft_x, odds_ft_2
2. Over/Under predictions use fixed 2.5 (not CTMCL)
3. Removed all profit calculations (moneyline_profit, over_profit, ctmcl_profit)
4. Automatic cleanup of old predictions for matches no longer in extracted_features
"""

import pandas as pd
import numpy as np
import joblib
import warnings
import os
from datetime import datetime
warnings.filterwarnings('ignore')

print("\n" + "="*80)
print("FOOTBALL MATCH PREDICTION SYSTEM - ENHANCED VERSION")
print("Using Ridge Regression Models")
print("="*80)

# ========== STEP 1: LOAD DATA ==========
print("\n[1/7] Loading extracted features...")
df = pd.read_csv('extracted_features_complete.csv')
print(f"✓ Loaded {len(df)} matches")
print(f"✓ Columns: {list(df.columns)}")

# Store all valid match IDs from extracted_features
valid_match_ids = set(df['match_id'].values)

# ========== STEP 2: CHECK FOR EXISTING PREDICTIONS ==========
print("\n[2/7] Checking for existing predictions...")

existing_predictions_file = 'best_match_predictions.csv'
predicted_match_ids = set()
existing_df = None

if os.path.exists(existing_predictions_file):
    try:
        existing_df = pd.read_csv(existing_predictions_file)
        print(f"⚠ Found {len(existing_df)} previously predicted matches")
        
        # CLEANUP: Remove predictions for matches that are no longer in extracted_features
        old_predictions = existing_df[~existing_df['match_id'].isin(valid_match_ids)]
        if len(old_predictions) > 0:
            print(f"🧹 Removing {len(old_predictions)} old predictions for matches no longer in extracted_features")
            existing_df = existing_df[existing_df['match_id'].isin(valid_match_ids)]
            print(f"✓ Cleaned predictions: {len(existing_df)} remaining")
        
        predicted_match_ids = set(existing_df['match_id'].values)
        print(f"✓ Valid predictions: {len(predicted_match_ids)} matches")
        
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
    
    # Save cleaned existing predictions if cleanup was performed
    if existing_df is not None and len(old_predictions) > 0:
        existing_df.to_csv(existing_predictions_file, index=False)
        print(f"✓ Saved cleaned predictions to {existing_predictions_file}")
    
    exit(0)

print(f"\n✓ Found {len(new_matches_df)} NEW matches to predict")
print(f"  Already predicted: {len(predicted_match_ids)} matches")
print(f"  New predictions: {len(new_matches_df)} matches")

# Use new matches for prediction
df = new_matches_df

# ========== STEP 3: LOAD MODELS AND SCALER ==========
print("\n[3/7] Loading trained models and scaler...")

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
print("\n[4/7] Preparing features...")

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
print("\n[5/7] Scaling features and making predictions...")

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
print("\n[6/7] Creating results dataframe...")

# Create comprehensive results
results = pd.DataFrame({
    # Match identifiers
    'match_id': df['match_id'],
    'date': df['date'] if 'date' in df.columns else None,
    'home_team_id': df['home_team_id'],
    'away_team_id': df['away_team_id'],
    'league_id': df['league_id'],
    'home_team_name': df['home_team_name'],
    'away_team_name': df['away_team_name'],
    'league_name': df['league_name'] if 'league_name' in df.columns else None,
    
    # NEW: Additional columns from extracted_features
    'CTMCL': df['CTMCL'].values,
    'odds_ft_1_prob': df['odds_ft_1_prob'].values if 'odds_ft_1_prob' in df.columns else 0,
    'odds_ft_2_prob': df['odds_ft_2_prob'].values if 'odds_ft_2_prob' in df.columns else 0,
    'o25_potential': df['o25_potential'].values if 'o25_potential' in df.columns else 0,
    'odds_ft_over25': df['odds_ft_over25'].values if 'odds_ft_over25' in df.columns else 0,
    'odds_ft_under25': df['odds_ft_under25'].values if 'odds_ft_under25' in df.columns else 0,
    'odds_ft_1': df['odds_ft_1'].values if 'odds_ft_1' in df.columns else 0,
    'odds_ft_x': df['odds_ft_x'].values if 'odds_ft_x' in df.columns else 0,
    'odds_ft_2': df['odds_ft_2'].values if 'odds_ft_2' in df.columns else 0,
    
    # Predictions
    'predicted_home_goals': home_goals_pred,
    'predicted_away_goals': away_goals_pred,
    'predicted_total_goals': total_goals_pred,
})

# NEW: Calculate u25_potential as (1 - o25_potential)
# o25_potential is typically in range 0-100, so u25_potential = 100 - o25_potential
if 'o25_potential' in df.columns:
    results['u25_potential'] = 100 - df['o25_potential'].values
else:
    results['u25_potential'] = 0

# NEW: Add status column (default to PENDING)
results['status'] = 'PENDING'

# Round predictions to 2 decimal places
results['predicted_home_goals'] = results['predicted_home_goals'].round(2)
results['predicted_away_goals'] = results['predicted_away_goals'].round(2)
results['predicted_total_goals'] = results['predicted_total_goals'].round(2)

# Add goal difference
results['predicted_goal_diff'] = (results['predicted_home_goals'] - 
                                   results['predicted_away_goals']).round(2)

# Predict match outcome (1=Home Win, X=Draw, 2=Away Win)
def predict_outcome(home_goals, away_goals):
    """Predict match outcome with draw threshold"""
    #diff = home_goals - away_goals
    if home_goals > away_goals:
        return '1'  # Home Win
    elif home_goals < away_goals:
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

# ========== ENHANCED OVER/UNDER PREDICTIONS ==========
print("✓ Creating CTMCL-based over/under predictions...")

# Traditional over/under predictions (keep for compatibility)
results['predicted_over_1.5'] = (results['predicted_total_goals'] > 1.5).astype(int)
results['predicted_over_2.5'] = (results['predicted_total_goals'] > 2.5).astype(int)
results['predicted_over_3.5'] = (results['predicted_total_goals'] > 3.5).astype(int)

# NEW: Over/Under 2.5 predictions (fixed at 2.5 instead of CTMCL)
results['predicted_over_CTMCL'] = (results['predicted_total_goals'] > 2.5).astype(int)
results['predicted_under_CTMCL'] = (results['predicted_total_goals'] < 2.5).astype(int)

# Add labels for 2.5 predictions
results['ctmcl_prediction'] = results.apply(
    lambda row: "Over 2.5" if row['predicted_over_CTMCL'] == 1 
    else "Under 2.5", 
    axis=1
)

print(f"✓ Over/Under 2.5 predictions created (fixed at 2.5)")
print(f"  Over 2.5: {results['predicted_over_CTMCL'].sum()} matches")
print(f"  Under 2.5: {results['predicted_under_CTMCL'].sum()} matches")

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

# ========== REMOVED: BETTING PROFIT CALCULATIONS ==========
# Profit calculations have been removed as requested


print("✓ Results dataframe created")

# ========== STEP 7: APPEND OR CREATE CSV ==========
print("\n[7/7] Saving predictions...")

output_file = 'best_match_predictions.csv'

if existing_df is not None and len(predicted_match_ids) > 0:
    # Append new predictions to existing (already cleaned)
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
print(f"  • Average CTMCL: {display_results['CTMCL'].mean():.2f}")
print(f"  • Min total goals: {display_results['predicted_total_goals'].min():.2f}")
print(f"  • Max total goals: {display_results['predicted_total_goals'].max():.2f}")

print(f"\n🏆 Outcome Distribution (New Matches):")
outcome_counts = display_results['outcome_label'].value_counts()
for outcome, count in outcome_counts.items():
    percentage = (count / len(display_results)) * 100
    print(f"  • {outcome}: {count} ({percentage:.1f}%)")

print(f"\n📈 Traditional Over/Under Predictions (New Matches):")
print(f"  • Over 1.5 goals: {display_results['predicted_over_1.5'].sum()} ({display_results['predicted_over_1.5'].mean()*100:.1f}%)")
print(f"  • Over 2.5 goals: {display_results['predicted_over_2.5'].sum()} ({display_results['predicted_over_2.5'].mean()*100:.1f}%)")
print(f"  • Over 3.5 goals: {display_results['predicted_over_3.5'].sum()} ({display_results['predicted_over_3.5'].mean()*100:.1f}%)")

print(f"\n🎯 Over/Under 2.5 Predictions (New Matches):")
print(f"  • Over 2.5: {display_results['predicted_over_CTMCL'].sum()} ({display_results['predicted_over_CTMCL'].mean()*100:.1f}%)")
print(f"  • Under 2.5: {display_results['predicted_under_CTMCL'].sum()} ({display_results['predicted_under_CTMCL'].mean()*100:.1f}%)")

print(f"\n🎯 Both Teams to Score (BTTS) (New Matches):")
print(f"  • Yes: {display_results['predicted_btts'].sum()} ({display_results['predicted_btts'].mean()*100:.1f}%)")
print(f"  • No: {(1-display_results['predicted_btts']).sum()} ({(1-display_results['predicted_btts']).mean()*100:.1f}%)")

print(f"\n💪 Prediction Confidence (New Matches):")
confidence_counts = display_results['confidence_category'].value_counts()
for conf, count in confidence_counts.items():
    percentage = (count / len(display_results)) * 100
    print(f"  • {conf}: {count} ({percentage:.1f}%)")

# ========== DISPLAY DETAILED PREDICTIONS (Sample) ==========
print("\n" + "="*80)
print("SAMPLE OF NEW PREDICTIONS")
print("="*80)

display_cols = ['match_id', 'home_team_name', 'away_team_name', 
                'predicted_home_goals', 'predicted_away_goals', 
                'predicted_total_goals', 'CTMCL', 'ctmcl_prediction',
                'outcome_label', 'confidence_category', 'status']

print("\n" + display_results[display_cols].head(10).to_string(index=False))

print("\n" + "="*80)
print("✅ PREDICTION COMPLETE!")
print("="*80)
print(f"\n📄 Full results saved to: {output_file}")
print(f"🆕 New predictions: {len(results)}")
if existing_df is not None:
    print(f"📊 Total predictions: {len(predicted_match_ids) + len(results)}")
print(f"⏰ Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

print("\n🆕 FEATURES:")
print("  ✓ CTMCL column from extracted_features")
print("  ✓ odds_ft_1_prob, odds_ft_2_prob columns from extracted_features")
print("  ✓ odds_ft_over25, odds_ft_under25 columns from extracted_features")
print("  ✓ odds_ft_1, odds_ft_x, odds_ft_2 columns from extracted_features")
print("  ✓ o25_potential column from extracted_features")
print("  ✓ u25_potential column (calculated as 100 - o25_potential)")
print("  ✓ status column (default: PENDING)")
print("  ✓ Over/Under 2.5 predictions (fixed at 2.5, not CTMCL)")
print("  ✓ Automatic cleanup of old predictions")
print("  ✓ Profit calculations removed")

print("\n" + "="*80)

