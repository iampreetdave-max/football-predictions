"""
AI Soccer Predictions using Mistral API
This script reads match predictions from CSV, calls Mistral API for analysis,
and updates the PostgreSQL database with AI predictions.

Usage: python ai_predictions_local.py <csv_file_path>
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
import json
import time
import sys
from typing import Dict, Optional

# Database configuration
DB_CONFIG = {
    'host': 'winbets-predictions.postgres.database.azure.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'winbets',
    'password': 'Constantinople@1900'
}

# Mistral API configuration
MISTRAL_API_KEY = 'pS5gro9f1FeKKZQS3gFKONyaCBAjJnDh'
MISTRAL_API_URL = 'https://api.mistral.ai/v1/chat/completions'

# League ID to Name mapping
LEAGUE_ID_TO_NAME = {
    12325: "England Premier League",
    15050: "England Premier League",
    14924: "UEFA Champions League",
    12316: "Spain La Liga",
    14956: "Spain La Liga",
    12530: "Italy Serie A",
    15068: "Italy Serie A",
    12529: "Germany Bundesliga",
    14968: "Germany Bundesliga",
    13973: "USA MLS",
    12337: "France Ligue 1",
    14932: "France Ligue 1",
    12322: "Netherlands Eredivisie",
    14936: "Netherlands Eredivisie",
    15115: "Portugal Liga NOS",
    16504: "USA MLS",
    12136: "Mexico Liga MX",
    15234: "Mexico Liga MX"
}

def call_mistral_api(prompt: str) -> str:
    """Call Mistral API for match analysis"""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {MISTRAL_API_KEY}'
    }
    
    payload = {
        'model': 'mistral-large-latest',
        'messages': [
            {
                'role': 'system',
                'content': 'You are a professional soccer analyst with deep knowledge of global soccer leagues, teams, and players. Provide detailed match analysis based on recent form, statistics, and tactical considerations.'
            },
            {
                'role': 'user',
                'content': prompt
            }
        ],
        'temperature': 0.7,
        'max_tokens': 1500
    }
    
    try:
        response = requests.post(MISTRAL_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        return result['choices'][0]['message']['content']
    except requests.exceptions.RequestException as e:
        print(f"Error calling Mistral API: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def parse_mistral_response(response: str, home_team: str, away_team: str) -> Dict[str, Optional[str]]:
    """Parse Mistral's response to extract predictions"""
    predictions = {
        'ai_moneyline': None,
        'ai_overunder': None,
        'ai_spreads': None
    }
    
    if not response:
        return predictions
    
    response_lower = response.lower()
    lines = response.split('\n')
    
    # Parse Moneyline - look for explicit prediction
    for line in lines:
        line_lower = line.lower()
        if 'moneyline' in line_lower or 'winner' in line_lower or 'result' in line_lower:
            if 'home win' in line_lower or home_team.lower() in line_lower and 'win' in line_lower:
                predictions['ai_moneyline'] = 'Home Win'
            elif 'away win' in line_lower or away_team.lower() in line_lower and 'win' in line_lower:
                predictions['ai_moneyline'] = 'Away Win'
            elif 'draw' in line_lower or 'tie' in line_lower:
                predictions['ai_moneyline'] = 'Draw'
    
    # Fallback moneyline detection
    if not predictions['ai_moneyline']:
        if 'home win' in response_lower or 'home team win' in response_lower:
            predictions['ai_moneyline'] = 'Home Win'
        elif 'away win' in response_lower or 'away team win' in response_lower:
            predictions['ai_moneyline'] = 'Away Win'
        elif 'draw' in response_lower or 'likely to draw' in response_lower:
            predictions['ai_moneyline'] = 'Draw'
    
    # Parse Over/Under
    for line in lines:
        line_lower = line.lower()
        if 'over/under' in line_lower or 'total goals' in line_lower or 'o/u' in line_lower:
            if 'over 2.5' in line_lower or 'over 3' in line_lower:
                predictions['ai_overunder'] = 'Over 2.5'
            elif 'under 2.5' in line_lower or 'under 3' in line_lower:
                predictions['ai_overunder'] = 'Under 2.5'
    
    # Fallback over/under detection
    if not predictions['ai_overunder']:
        if 'over 2.5' in response_lower:
            predictions['ai_overunder'] = 'Over 2.5'
        elif 'under 2.5' in response_lower:
            predictions['ai_overunder'] = 'Under 2.5'
    
    # Parse Spreads - look for team name with spread value
    for line in lines:
        if 'spread' in line.lower() and ('(' in line or '+' in line or '-' in line):
            # Extract team name and spread
            if home_team in line:
                spread_match = line[line.find(home_team):line.find(home_team)+len(home_team)+10]
                predictions['ai_spreads'] = home_team + ' ' + spread_match.split('(')[-1].split(')')[0] if '(' in spread_match else home_team
            elif away_team in line:
                spread_match = line[line.find(away_team):line.find(away_team)+len(away_team)+10]
                predictions['ai_spreads'] = away_team + ' ' + spread_match.split('(')[-1].split(')')[0] if '(' in spread_match else away_team
    
    # Fallback: use moneyline winner for spread
    if not predictions['ai_spreads'] and predictions['ai_moneyline']:
        if predictions['ai_moneyline'] == 'Home Win':
            predictions['ai_spreads'] = home_team + ' (-1.5)'
        elif predictions['ai_moneyline'] == 'Away Win':
            predictions['ai_spreads'] = away_team + ' (-1.5)'
    
    return predictions

def create_match_prompt(row: pd.Series) -> str:
    """Create a detailed prompt for Mistral to analyze the match"""
    league_name = LEAGUE_ID_TO_NAME.get(row['league_id'], f"League ID {row['league_id']}")
    
    prompt = f"""You are an expert soccer analyst tasked with INDEPENDENTLY analyzing this match. DO NOT simply agree with the model predictions - conduct your own analysis.

**Match Details:**
- League: {league_name}
- Date: {row['date']}
- Home Team: {row['home_team_name']}
- Away Team: {row['away_team_name']}

**Baseline Model Predictions (for reference only):**
- Predicted Outcome: {row['outcome_label']}
- Predicted Score: {row['home_team_name']} {row['predicted_home_goals']:.1f} - {row['predicted_away_goals']:.1f} {row['away_team_name']}
- Total Goals: {row['predicted_total_goals']:.1f}
- Over/Under 2.5: {row['ctmcl_prediction']}
- Model Confidence: {row['confidence_category']}

**YOUR INDEPENDENT ANALYSIS MUST INCLUDE:**

1. Research the CURRENT form of both teams (last 5 matches minimum)
2. Check for KEY INJURIES, suspensions, or tactical changes
3. Analyze HEAD-TO-HEAD history and recent meetings
4. Consider HOME vs AWAY performance statistics
5. Review league standings and motivation factors
6. Identify any TACTICAL MISMATCHES or advantages

**CRITICAL INSTRUCTION:** 
Do NOT simply validate the model's predictions. Your job is to identify potential DIVERGENCES where your analysis disagrees with the model. Look for:
- Overvalued favorites
- Underrated underdogs
- Form reversals the model might miss
- Tactical factors affecting goals
- Situational contexts (injuries, rotation, schedule)

**REQUIRED OUTPUT FORMAT (be specific):**

Moneyline: [Home Win OR Away Win OR Draw]
Over/Under 2.5: [Over 2.5 OR Under 2.5]
Spreads: [Exact team name] (-X.X or +X.X)

**Then explain:** 
Where you AGREE with the model and why (1-2 sentences)
Where you DISAGREE with the model and why (1-2 sentences)
Your key differentiating insight (1 sentence)

Be decisive. Pick definitive predictions even if uncertain.
"""
    return prompt

def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected to database successfully")
        return conn
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return None

def check_predictions_exist(conn, match_id: int) -> bool:
    """Check if AI predictions already exist for this match"""
    cursor = conn.cursor()
    
    query = """
        SELECT ai_moneyline, ai_overunder, ai_spreads
        FROM agility_soccer_v1
        WHERE match_id = %s
    """
    
    try:
        cursor.execute(query, (match_id,))
        result = cursor.fetchone()
        
        if result is None:
            return False  # Match doesn't exist in DB
        
        # Check if all three columns are NULL
        ai_moneyline, ai_overunder, ai_spreads = result
        return not (ai_moneyline is None and ai_overunder is None and ai_spreads is None)
    except Exception as e:
        print(f"Error checking match {match_id}: {e}")
        return False
    finally:
        cursor.close()

def update_predictions(conn, match_id: int, predictions: Dict[str, str]):
    """Update database with AI predictions"""
    cursor = conn.cursor()
    
    update_query = """
        UPDATE agility_soccer_v1
        SET ai_moneyline = %s,
            ai_overunder = %s,
            ai_spreads = %s
        WHERE match_id = %s
    """
    
    try:
        cursor.execute(update_query, (
            predictions['ai_moneyline'],
            predictions['ai_overunder'],
            predictions['ai_spreads'],
            match_id
        ))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error updating match {match_id}: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def process_predictions(csv_file: str):
    """Main function to process all predictions"""
    # Read CSV
    print("="*60)
    print("AI SOCCER PREDICTIONS - MISTRAL ANALYSIS")
    print("="*60)
    print(f"\nReading CSV file: {csv_file}")
    
    try:
        df = pd.read_csv(csv_file)
        print(f"✓ Found {len(df)} total matches in CSV")
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        return
    
    # Connect to database
    print("\nConnecting to database...")
    conn = get_db_connection()
    
    if not conn:
        print("✗ Cannot proceed without database connection")
        return
    
    # Filter matches that need AI predictions
    print("\nChecking which matches need AI predictions...")
    matches_to_process = []
    matches_skipped = 0
    
    for idx, row in df.iterrows():
        match_id = row['match_id']
        if not check_predictions_exist(conn, match_id):
            matches_to_process.append(row)
        else:
            matches_skipped += 1
            print(f"  Skipping match {match_id} - AI predictions already exist")
    
    print(f"\n✓ Matches to process: {len(matches_to_process)}")
    print(f"✓ Matches skipped (already have AI predictions): {matches_skipped}")
    
    if len(matches_to_process) == 0:
        print("\n✓ All matches already have AI predictions. Nothing to do!")
        conn.close()
        return
    
    success_count = 0
    error_count = 0
    
    print(f"\n{'='*60}")
    print("Starting match analysis...")
    print(f"{'='*60}\n")
    
    # Process each match
    for idx, row in enumerate(matches_to_process):
        match_id = row['match_id']
        home_team = row['home_team_name']
        away_team = row['away_team_name']
        league_id = row['league_id']
        league_name = LEAGUE_ID_TO_NAME.get(league_id, f"League ID {league_id}")
        
        print(f"\n[{idx+1}/{len(matches_to_process)}] {home_team} vs {away_team}")
        print(f"League: {league_name} | Date: {row['date']}")
        print("-" * 60)
        
        # Create prompt and call Mistral
        prompt = create_match_prompt(row)
        print("Analyzing with Mistral AI...", end=" ", flush=True)
        
        response = call_mistral_api(prompt)
        
        if response:
            print("✓")
            
            # Parse predictions
            predictions = parse_mistral_response(response, home_team, away_team)
            
            print(f"AI Predictions:")
            print(f"  • Moneyline: {predictions['ai_moneyline']}")
            print(f"  • Over/Under: {predictions['ai_overunder']}")
            print(f"  • Spreads: {predictions['ai_spreads']}")
            
            # Update database
            if update_predictions(conn, match_id, predictions):
                success_count += 1
                print(f"✓ Database updated for match {match_id}")
            else:
                error_count += 1
                print(f"✗ Failed to update database for match {match_id}")
        else:
            error_count += 1
            print("✗")
            print(f"✗ Failed to get Mistral response for match {match_id}")
        
        # Rate limiting - wait between API calls to avoid hitting limits
        if idx < len(matches_to_process) - 1:
            print("Waiting before next request...", end=" ", flush=True)
            time.sleep(3)  # 3 second delay between requests
            print("✓")
    
    # Close connection
    conn.close()
    
    # Final Summary
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f"Total matches in CSV: {len(df)}")
    print(f"Matches skipped (already processed): {matches_skipped}")
    print(f"Matches analyzed: {len(matches_to_process)}")
    print(f"✓ Successfully updated: {success_count}")
    print(f"✗ Errors: {error_count}")
    if len(matches_to_process) > 0:
        print(f"Success rate: {(success_count/len(matches_to_process)*100):.1f}%")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = "best_match_predictions.csv"
    
    process_predictions(csv_file)
