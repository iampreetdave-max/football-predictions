# Predict match outcome (1=Home Win, X=Draw, 2=Away Win)
def predict_outcome(home_goals, away_goals):
    """Predict match outcome based on simple comparison"""
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
