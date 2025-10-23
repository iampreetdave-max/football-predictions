"""
FOOTBALL MATCH OUTCOME PREDICTION
Uses trained Ridge models (home & away) with scaler to predict match outcomes
Based on extracted_features_complete.csv
"""

import pandas as pd
import numpy as np
import joblib
import warnings
warnings.filterwarnings('ignore')

print("\n" + "="*80)
print("FOOTBALL MATCH PREDICTION SYSTEM")
print("Using Ridge Regression Models")
print("="*80)

# ========== STEP 1: LOAD DATA ==========
print("\n[1/5] Loading extracted features...")
df = pd.read_csv('extracted_features_complete.csv')
print(f"✓ Loaded {len(df)} matches")
print(f"✓ Columns: {list(df.columns)}")

# ========== STEP 2: LOAD MODELS AND SCALER ==========
print("\n[2/5] Loading trained models and scaler...")

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

# ========== STEP 3: PREPARE FEATURES ==========
print("\n[3/5] Preparing features...")

# Define exact feature columns used during model training (21 features)
# Based on model_generator.py FeaturePreparator class
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
    print(f"⚠️ Warning: Missing features: {missing_features}")
    feature_columns = [f for f in feature_columns if f in df.columns]

print(f"✓ Feature columns identified: {len(feature_columns)} features")
print(f"  Features: {', '.join(feature_columns)}")

# Extract features
X = df[feature_columns].copy()

# Handle any missing values (fill with 0 or median)
if X.isnull().any().any():
    print(f"⚠️ Warning: Found {X.isnull().sum().sum()} missing values, filling with 0")
    X = X.fillna(0)

print(f"✓ Feature matrix shape: {X.shape}")

# ========== STEP 4: SCALE FEATURES AND MAKE PREDICTIONS ==========
print("\n[4/5] Scaling features and making predictions...")

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

# ========== STEP 5: CREATE RESULTS DATAFRAME ==========
print("\n[5/5] Creating results dataframe...")

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

print("✓ Results dataframe created")

# ========== SAVE TO CSV ==========
output_file = 'best_match_predictions.csv'
results.to_csv(output_file, index=False)
print(f"✓ Predictions saved to: {output_file}")

# ========== DISPLAY SUMMARY STATISTICS ==========
print("\n" + "="*80)
print("PREDICTION SUMMARY")
print("="*80)

print(f"\n📊 Total matches predicted: {len(results)}")

print(f"\n⚽ Goal Predictions:")
print(f"  • Average predicted home goals: {results['predicted_home_goals'].mean():.2f}")
print(f"  • Average predicted away goals: {results['predicted_away_goals'].mean():.2f}")
print(f"  • Average predicted total goals: {results['predicted_total_goals'].mean():.2f}")
print(f"  • Min total goals: {results['predicted_total_goals'].min():.2f}")
print(f"  • Max total goals: {results['predicted_total_goals'].max():.2f}")

print(f"\n🏆 Outcome Distribution:")
outcome_counts = results['outcome_label'].value_counts()
for outcome, count in outcome_counts.items():
    percentage = (count / len(results)) * 100
    print(f"  • {outcome}: {count} ({percentage:.1f}%)")

print(f"\n📈 Over/Under Predictions:")
print(f"  • Over 1.5 goals: {results['predicted_over_1.5'].sum()} ({results['predicted_over_1.5'].mean()*100:.1f}%)")
print(f"  • Over 2.5 goals: {results['predicted_over_2.5'].sum()} ({results['predicted_over_2.5'].mean()*100:.1f}%)")
print(f"  • Over 3.5 goals: {results['predicted_over_3.5'].sum()} ({results['predicted_over_3.5'].mean()*100:.1f}%)")

print(f"\n🎯 Both Teams to Score (BTTS):")
print(f"  • Yes: {results['predicted_btts'].sum()} ({results['predicted_btts'].mean()*100:.1f}%)")
print(f"  • No: {(1-results['predicted_btts']).sum()} ({(1-results['predicted_btts']).mean()*100:.1f}%)")

print(f"\n💪 Prediction Confidence:")
confidence_counts = results['confidence_category'].value_counts()
for conf, count in confidence_counts.items():
    percentage = (count / len(results)) * 100
    print(f"  • {conf}: {count} ({percentage:.1f}%)")

# ========== DISPLAY DETAILED PREDICTIONS ==========
print("\n" + "="*80)
print("DETAILED MATCH PREDICTIONS")
print("="*80)

# Display all predictions
display_cols = ['match_id', 'home_team_name', 'away_team_name', 
                'predicted_home_goals', 'predicted_away_goals', 
                'predicted_total_goals', 'outcome_label', 'confidence_category']

print("\n" + results[display_cols].to_string(index=False))

# ========== HIGH CONFIDENCE PREDICTIONS ==========
print("\n" + "="*80)
print("HIGH CONFIDENCE PREDICTIONS (Goal Diff > 0.7)")
print("="*80)

high_conf = results[results['confidence'] > 0.7].copy()
if len(high_conf) > 0:
    print(f"\nFound {len(high_conf)} high confidence predictions:")
    print("\n" + high_conf[display_cols].to_string(index=False))
else:
    print("\nNo high confidence predictions found.")

# ========== BETTING INSIGHTS ==========
print("\n" + "="*80)
print("BETTING INSIGHTS")
print("="*80)

print("\n🎲 Recommended Bets (High Confidence Home Wins):")
home_wins = results[(results['predicted_outcome'] == '1') & 
                    (results['confidence'] > 0.5)].copy()
if len(home_wins) > 0:
    print(f"\nFound {len(home_wins)} strong home win predictions:")
    for _, row in home_wins.iterrows():
        print(f"  • {row['home_team_name']} vs {row['away_team_name']}: "
              f"{row['predicted_home_goals']}-{row['predicted_away_goals']} "
              f"(Confidence: {row['confidence_category']})")
else:
    print("  No strong home win predictions")

print("\n🎲 Recommended Bets (High Confidence Away Wins):")
away_wins = results[(results['predicted_outcome'] == '2') & 
                    (results['confidence'] > 0.5)].copy()
if len(away_wins) > 0:
    print(f"\nFound {len(away_wins)} strong away win predictions:")
    for _, row in away_wins.iterrows():
        print(f"  • {row['home_team_name']} vs {row['away_team_name']}: "
              f"{row['predicted_home_goals']}-{row['predicted_away_goals']} "
              f"(Confidence: {row['confidence_category']})")
else:
    print("  No strong away win predictions")

print("\n🎲 High-Scoring Matches (Over 3.5 goals):")
high_scoring = results[results['predicted_total_goals'] > 3.5].copy()
if len(high_scoring) > 0:
    print(f"\nFound {len(high_scoring)} high-scoring match predictions:")
    for _, row in high_scoring.iterrows():
        print(f"  • {row['home_team_name']} vs {row['away_team_name']}: "
              f"Total {row['predicted_total_goals']:.2f} goals")
else:
    print("  No high-scoring match predictions")

print("\n" + "="*80)
print("✅ PREDICTION COMPLETE!")
print("="*80)
print(f"\n📄 Full results saved to: {output_file}")
print("\nYou can now use this CSV file for:")
print("  • Match outcome analysis")
print("  • Betting strategy development")
print("  • Performance tracking")
print("  • Further statistical analysis")

print("\n" + "="*80)