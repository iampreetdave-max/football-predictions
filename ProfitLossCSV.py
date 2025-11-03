import pandas as pd
import numpy as np

# Load the CSV file
df = pd.read_csv('agility_football_pred_202511031316.csv')

# Filter only completed matches
df_complete = df[df['status'] == 'COMPLETE'].copy()

print("=" * 80)
print("FOOTBALL PREDICTIONS VALIDATION REPORT")
print("=" * 80)
print(f"\nTotal Predictions: {len(df)}")
print(f"Completed Matches: {len(df_complete)}")
print(f"Pending Matches: {len(df[df['status'] == 'PENDING'])}")
print("\n" + "=" * 80)

# ============================================================================
# OVERALL PROFIT/LOSS AND ACCURACY
# ============================================================================

def calculate_metrics(column_name, data):
    """Calculate profit/loss and accuracy for a given column"""
    # Remove NaN values
    valid_data = data.dropna()
    
    if len(valid_data) == 0:
        return {
            'total_profit_loss': 0,
            'correct_predictions': 0,
            'wrong_predictions': 0,
            'total_predictions': 0,
            'accuracy': 0,
            'avg_profit_per_bet': 0
        }
    
    # Calculate metrics
    total_profit_loss = valid_data.sum()
    wrong_predictions = (valid_data == -1).sum()
    correct_predictions = (valid_data != -1).sum()
    total_predictions = len(valid_data)
    accuracy = (correct_predictions / total_predictions * 100) if total_predictions > 0 else 0
    avg_profit_per_bet = total_profit_loss / total_predictions if total_predictions > 0 else 0
    
    return {
        'total_profit_loss': total_profit_loss,
        'correct_predictions': correct_predictions,
        'wrong_predictions': wrong_predictions,
        'total_predictions': total_predictions,
        'accuracy': accuracy,
        'avg_profit_per_bet': avg_profit_per_bet
    }

# Calculate for profit_loss_outcome
print("\n📊 PROFIT/LOSS OUTCOME ANALYSIS")
print("-" * 80)
outcome_metrics = calculate_metrics('profit_loss_outcome', df_complete['profit_loss_outcome'])

print(f"Total Profit/Loss: {outcome_metrics['total_profit_loss']:.2f} units")
print(f"Correct Predictions: {outcome_metrics['correct_predictions']}")
print(f"Wrong Predictions: {outcome_metrics['wrong_predictions']}")
print(f"Total Predictions: {outcome_metrics['total_predictions']}")
print(f"Accuracy: {outcome_metrics['accuracy']:.2f}%")
print(f"Average Profit per Bet: {outcome_metrics['avg_profit_per_bet']:.4f} units")

# Calculate for profit_loss_winner
print("\n📊 PROFIT/LOSS WINNER ANALYSIS")
print("-" * 80)
winner_metrics = calculate_metrics('profit_loss_winner', df_complete['profit_loss_winner'])

print(f"Total Profit/Loss: {winner_metrics['total_profit_loss']:.2f} units")
print(f"Correct Predictions: {winner_metrics['correct_predictions']}")
print(f"Wrong Predictions: {winner_metrics['wrong_predictions']}")
print(f"Total Predictions: {winner_metrics['total_predictions']}")
print(f"Accuracy: {winner_metrics['accuracy']:.2f}%")
print(f"Average Profit per Bet: {winner_metrics['avg_profit_per_bet']:.4f} units")

# ============================================================================
# CONFIDENCE CATEGORY BREAKDOWN
# ============================================================================

print("\n\n" + "=" * 80)
print("CONFIDENCE CATEGORY BREAKDOWN")
print("=" * 80)

confidence_levels = ['High', 'Medium', 'Low']

for confidence in confidence_levels:
    df_conf = df_complete[df_complete['confidence_category'] == confidence]
    
    print(f"\n{'🔥' if confidence == 'High' else '⚡' if confidence == 'Medium' else '💡'} {confidence.upper()} CONFIDENCE PREDICTIONS")
    print("-" * 80)
    print(f"Total Matches: {len(df_conf)}")
    
    if len(df_conf) == 0:
        print("No data available for this confidence level.")
        continue
    
    # Outcome Analysis
    print(f"\n  ➤ Outcome Predictions:")
    outcome_conf_metrics = calculate_metrics('profit_loss_outcome', df_conf['profit_loss_outcome'])
    print(f"     • Total Profit/Loss: {outcome_conf_metrics['total_profit_loss']:.2f} units")
    print(f"     • Correct: {outcome_conf_metrics['correct_predictions']} | Wrong: {outcome_conf_metrics['wrong_predictions']}")
    print(f"     • Accuracy: {outcome_conf_metrics['accuracy']:.2f}%")
    print(f"     • Avg Profit/Bet: {outcome_conf_metrics['avg_profit_per_bet']:.4f} units")
    
    # Winner Analysis
    print(f"\n  ➤ Winner Predictions:")
    winner_conf_metrics = calculate_metrics('profit_loss_winner', df_conf['profit_loss_winner'])
    print(f"     • Total Profit/Loss: {winner_conf_metrics['total_profit_loss']:.2f} units")
    print(f"     • Correct: {winner_conf_metrics['correct_predictions']} | Wrong: {winner_conf_metrics['wrong_predictions']}")
    print(f"     • Accuracy: {winner_conf_metrics['accuracy']:.2f}%")
    print(f"     • Avg Profit/Bet: {winner_conf_metrics['avg_profit_per_bet']:.4f} units")

# ============================================================================
# SUMMARY TABLE
# ============================================================================

print("\n\n" + "=" * 80)
print("SUMMARY TABLE")
print("=" * 80)

summary_data = []

# Overall
summary_data.append({
    'Category': 'OVERALL',
    'Type': 'Outcome',
    'Predictions': outcome_metrics['total_predictions'],
    'Correct': outcome_metrics['correct_predictions'],
    'Wrong': outcome_metrics['wrong_predictions'],
    'Accuracy': f"{outcome_metrics['accuracy']:.2f}%",
    'Profit/Loss': f"{outcome_metrics['total_profit_loss']:.2f}",
    'Avg P/L': f"{outcome_metrics['avg_profit_per_bet']:.4f}"
})

summary_data.append({
    'Category': 'OVERALL',
    'Type': 'Winner',
    'Predictions': winner_metrics['total_predictions'],
    'Correct': winner_metrics['correct_predictions'],
    'Wrong': winner_metrics['wrong_predictions'],
    'Accuracy': f"{winner_metrics['accuracy']:.2f}%",
    'Profit/Loss': f"{winner_metrics['total_profit_loss']:.2f}",
    'Avg P/L': f"{winner_metrics['avg_profit_per_bet']:.4f}"
})

# By confidence
for confidence in confidence_levels:
    df_conf = df_complete[df_complete['confidence_category'] == confidence]
    
    if len(df_conf) > 0:
        outcome_conf = calculate_metrics('profit_loss_outcome', df_conf['profit_loss_outcome'])
        winner_conf = calculate_metrics('profit_loss_winner', df_conf['profit_loss_winner'])
        
        summary_data.append({
            'Category': confidence.upper(),
            'Type': 'Outcome',
            'Predictions': outcome_conf['total_predictions'],
            'Correct': outcome_conf['correct_predictions'],
            'Wrong': outcome_conf['wrong_predictions'],
            'Accuracy': f"{outcome_conf['accuracy']:.2f}%",
            'Profit/Loss': f"{outcome_conf['total_profit_loss']:.2f}",
            'Avg P/L': f"{outcome_conf['avg_profit_per_bet']:.4f}"
        })
        
        summary_data.append({
            'Category': confidence.upper(),
            'Type': 'Winner',
            'Predictions': winner_conf['total_predictions'],
            'Correct': winner_conf['correct_predictions'],
            'Wrong': winner_conf['wrong_predictions'],
            'Accuracy': f"{winner_conf['accuracy']:.2f}%",
            'Profit/Loss': f"{winner_conf['total_profit_loss']:.2f}",
            'Avg P/L': f"{winner_conf['avg_profit_per_bet']:.4f}"
        })

summary_df = pd.DataFrame(summary_data)
print("\n" + summary_df.to_string(index=False))

# ============================================================================
# KEY INSIGHTS
# ============================================================================

print("\n\n" + "=" * 80)
print("KEY INSIGHTS")
print("=" * 80)

# Best performing category
best_outcome_conf = None
best_outcome_profit = float('-inf')
best_winner_conf = None
best_winner_profit = float('-inf')

for confidence in confidence_levels:
    df_conf = df_complete[df_complete['confidence_category'] == confidence]
    if len(df_conf) > 0:
        outcome_profit = df_conf['profit_loss_outcome'].sum()
        winner_profit = df_conf['profit_loss_winner'].sum()
        
        if outcome_profit > best_outcome_profit:
            best_outcome_profit = outcome_profit
            best_outcome_conf = confidence
        
        if winner_profit > best_winner_profit:
            best_winner_profit = winner_profit
            best_winner_conf = confidence

print(f"\n✅ Best Confidence Level for Outcome Predictions: {best_outcome_conf} ({best_outcome_profit:.2f} units)")
print(f"✅ Best Confidence Level for Winner Predictions: {best_winner_conf} ({best_winner_profit:.2f} units)")

# Overall profitability
total_combined_profit = outcome_metrics['total_profit_loss'] + winner_metrics['total_profit_loss']
print(f"\n💰 Combined Total Profit/Loss: {total_combined_profit:.2f} units")

if outcome_metrics['total_profit_loss'] > 0:
    print(f"✅ Outcome predictions are PROFITABLE")
else:
    print(f"❌ Outcome predictions are in LOSS")

if winner_metrics['total_profit_loss'] > 0:
    print(f"✅ Winner predictions are PROFITABLE")
else:
    print(f"❌ Winner predictions are in LOSS")

print("\n" + "=" * 80)
print("END OF REPORT")
print("=" * 80)
