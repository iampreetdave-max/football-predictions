"""
AI Soccer Predictions using Mistral Conversations API with Web Search
This script reads match predictions from CSV, calls Mistral's Conversations API
(with built-in web search for real-time team data), and updates PostgreSQL with AI predictions.

Usage: python ai_predictions_local.py <csv_file_path>
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
import json
import time
import sys
import re
from typing import Dict, Optional
import os

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Mistral API configuration
MISTRAL_API_KEY = os.getenv('MISTRAL_API_KEY')
MISTRAL_CONVERSATIONS_URL = 'https://api.mistral.ai/v1/conversations'

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


def call_mistral_conversations_api(prompt: str, system_prompt: str) -> str:
    """
    Call Mistral Conversations API with web search enabled.
    Uses the beta /v1/conversations endpoint which supports built-in web_search tool.
    The model will automatically search the web for current team form, injuries, etc.
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {MISTRAL_API_KEY}'
    }

    payload = {
        'model': 'mistral-medium-latest',  # Best model for web search (82% SimpleQA score)
        'inputs': prompt,
        'store': False,  # Stateless - no conversation persistence needed
        'tools': [
            {'type': 'web_search'}  # Enable built-in web search connector
        ],
        'instructions': system_prompt,
        'completion_args': {
            'temperature': 0.3,  # Lower temp = more factual, less creative/hallucination
            'max_tokens': 2000
        }
    }

    try:
        response = requests.post(
            MISTRAL_CONVERSATIONS_URL,
            headers=headers,
            json=payload,
            timeout=60  # Longer timeout since web search takes extra time
        )
        response.raise_for_status()
        result = response.json()

        # Parse the Conversations API response format
        # Response has 'outputs' array with entries of type 'message.output' and 'tool.execution'
        full_text = ""
        citations = []

        outputs = result.get('outputs', [])
        for entry in outputs:
            entry_type = entry.get('type', '')

            if entry_type == 'message.output':
                content_parts = entry.get('content', [])
                for part in content_parts:
                    if part.get('type') == 'text':
                        full_text += part.get('text', '')
                    elif part.get('type') == 'tool_reference':
                        # Collect citation URLs for logging
                        url = part.get('url', '')
                        title = part.get('title', '')
                        if url:
                            citations.append(f"{title}: {url}")

        if citations:
            print(f"  📰 Sources used: {len(citations)}")
            for c in citations[:3]:  # Show top 3 sources
                print(f"     - {c[:80]}")

        return full_text if full_text else None

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error calling Mistral API: {e}")
        print(f"Response body: {e.response.text if e.response else 'N/A'}")
        # Fallback: try standard chat completions without web search
        print("  ⚠ Falling back to standard chat completions (no web search)...")
        return call_mistral_chat_fallback(prompt, system_prompt)
    except requests.exceptions.RequestException as e:
        print(f"Error calling Mistral API: {e}")
        return call_mistral_chat_fallback(prompt, system_prompt)
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def call_mistral_chat_fallback(prompt: str, system_prompt: str) -> str:
    """
    Fallback to standard /v1/chat/completions if Conversations API fails.
    No web search available here - purely model knowledge.
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {MISTRAL_API_KEY}'
    }

    payload = {
        'model': 'mistral-large-latest',
        'messages': [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': prompt}
        ],
        'temperature': 0.3,
        'max_tokens': 2000
    }

    try:
        response = requests.post(
            'https://api.mistral.ai/v1/chat/completions',
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        return result['choices'][0]['message']['content']
    except Exception as e:
        print(f"Fallback also failed: {e}")
        return None


def parse_mistral_response(response: str, home_team: str, away_team: str) -> Dict[str, Optional[str]]:
    """
    Parse Mistral's response to extract predictions.
    Uses regex-based extraction on the structured output lines.
    """
    predictions = {
        'ai_moneyline': None,
        'ai_overunder': None,
        'ai_spreads': None
    }

    if not response:
        return predictions

    response_lower = response.lower()
    lines = response.split('\n')

    home_lower = home_team.lower()
    away_lower = away_team.lower()

    # ---- MONEYLINE PARSING ----
    # Step 1: Look for the structured "MONEYLINE:" output line
    for line in lines:
        line_lower = line.lower().strip()
        # Match lines like "MONEYLINE: Home Win" or "**Moneyline:** Away Win"
        if re.search(r'\bmoneyline\b', line_lower):
            cleaned = re.sub(r'[*#\-]', '', line_lower).strip()
            if 'home win' in cleaned:
                predictions['ai_moneyline'] = 'Home Win'
                break
            elif 'away win' in cleaned:
                predictions['ai_moneyline'] = 'Away Win'
                break
            elif 'draw' in cleaned:
                predictions['ai_moneyline'] = 'Draw'
                break
            # Check for team name directly on the moneyline line
            elif home_lower in cleaned:
                predictions['ai_moneyline'] = 'Home Win'
                break
            elif away_lower in cleaned:
                predictions['ai_moneyline'] = 'Away Win'
                break

    # Step 2: Fallback - scan for "winner" or "result" lines
    if not predictions['ai_moneyline']:
        for line in lines:
            line_lower = line.lower().strip()
            if any(kw in line_lower for kw in ['winner:', 'result:', 'prediction:']):
                if 'home win' in line_lower or (home_lower in line_lower and 'win' in line_lower):
                    predictions['ai_moneyline'] = 'Home Win'
                    break
                elif 'away win' in line_lower or (away_lower in line_lower and 'win' in line_lower):
                    predictions['ai_moneyline'] = 'Away Win'
                    break
                elif 'draw' in line_lower or 'tie' in line_lower:
                    predictions['ai_moneyline'] = 'Draw'
                    break

    # Step 3: Last resort - count keyword occurrences to determine dominant prediction
    if not predictions['ai_moneyline']:
        home_win_signals = len(re.findall(r'\bhome\s+win\b', response_lower))
        away_win_signals = len(re.findall(r'\baway\s+win\b', response_lower))
        draw_signals = len(re.findall(r'\bdraw\b', response_lower))

        # Also count team name + "win" mentions (with proper grouping)
        home_win_signals += len(re.findall(re.escape(home_lower) + r'.*?\bwin\b', response_lower))
        away_win_signals += len(re.findall(re.escape(away_lower) + r'.*?\bwin\b', response_lower))

        if home_win_signals > away_win_signals and home_win_signals > draw_signals:
            predictions['ai_moneyline'] = 'Home Win'
        elif away_win_signals > home_win_signals and away_win_signals > draw_signals:
            predictions['ai_moneyline'] = 'Away Win'
        elif draw_signals > 0:
            predictions['ai_moneyline'] = 'Draw'

    # ---- OVER/UNDER PARSING ----
    # Step 1: Look for structured output line
    for line in lines:
        line_lower = line.lower().strip()
        if re.search(r'over\s*/\s*under|total\s+goals|o/u', line_lower):
            if re.search(r'over\s+2\.5', line_lower):
                predictions['ai_overunder'] = 'Over 2.5'
                break
            elif re.search(r'under\s+2\.5', line_lower):
                predictions['ai_overunder'] = 'Under 2.5'
                break
            # Handle "Over 3" or "Under 2" as proxies
            elif re.search(r'over\s+[23](\.\d)?', line_lower):
                predictions['ai_overunder'] = 'Over 2.5'
                break
            elif re.search(r'under\s+[23](\.\d)?', line_lower):
                predictions['ai_overunder'] = 'Under 2.5'
                break

    # Step 2: Fallback - find any "over 2.5" or "under 2.5" in the response
    if not predictions['ai_overunder']:
        over_count = len(re.findall(r'over\s+2\.5', response_lower))
        under_count = len(re.findall(r'under\s+2\.5', response_lower))
        if over_count > under_count:
            predictions['ai_overunder'] = 'Over 2.5'
        elif under_count > over_count:
            predictions['ai_overunder'] = 'Under 2.5'
        elif over_count == under_count and over_count > 0:
            # Both mentioned equally - look at which appears last (final verdict)
            last_over = response_lower.rfind('over 2.5')
            last_under = response_lower.rfind('under 2.5')
            predictions['ai_overunder'] = 'Over 2.5' if last_over > last_under else 'Under 2.5'

    # ---- SPREADS PARSING ----
    # Step 1: Look for structured spread line with team name and number
    for line in lines:
        line_lower = line.lower().strip()
        if 'spread' in line_lower:
            # Try to extract: TeamName (-1.5) or TeamName (+0.5) patterns
            spread_match = re.search(
                r'([\w\s.]+?)\s*\(?\s*([+-]\d+\.?\d*)\s*\)?',
                line, re.IGNORECASE
            )
            if spread_match:
                team_part = spread_match.group(1).strip()
                spread_val = spread_match.group(2).strip()
                # Determine which team
                if home_team.lower() in team_part.lower():
                    predictions['ai_spreads'] = f"{home_team} ({spread_val})"
                    break
                elif away_team.lower() in team_part.lower():
                    predictions['ai_spreads'] = f"{away_team} ({spread_val})"
                    break
                else:
                    # Team name might be abbreviated - use as-is
                    predictions['ai_spreads'] = f"{team_part} ({spread_val})"
                    break

    # Step 2: Fallback - derive spread from moneyline prediction
    if not predictions['ai_spreads'] and predictions['ai_moneyline']:
        if predictions['ai_moneyline'] == 'Home Win':
            predictions['ai_spreads'] = f"{home_team} (-1.5)"
        elif predictions['ai_moneyline'] == 'Away Win':
            predictions['ai_spreads'] = f"{away_team} (-1.5)"
        elif predictions['ai_moneyline'] == 'Draw':
            predictions['ai_spreads'] = f"{home_team} (+0.5)"

    return predictions


def create_system_prompt() -> str:
    """System prompt that defines the AI analyst's role and behavior."""
    return """You are a soccer match analyst. Your job is to provide match predictions grounded ONLY in verifiable facts you find through web search.

RULES:
1. You MUST search the web for EACH team's recent results, injury news, and head-to-head record before making any prediction.
2. Base your predictions ONLY on facts you find. If you cannot find information, say so - do NOT make up results or statistics.
3. You are given a baseline model prediction for reference. Treat it as one data point. If the web evidence supports it, agree with it. If the web evidence contradicts it, disagree. Do NOT default to disagreeing or agreeing.
4. Draws are rare in soccer (roughly 25% of matches). Only predict a draw if the evidence STRONGLY suggests it (e.g., both teams in poor scoring form, strong defensive records, nothing to play for, or historical pattern of draws in this fixture).
5. Be specific with facts: mention actual recent scores, actual injured player names, actual league positions.
6. You must ALWAYS provide all three predictions in the exact format specified - never skip any."""


def create_match_prompt(row: pd.Series) -> str:
    """Create a fact-driven prompt that forces web search and structured output."""
    league_name = LEAGUE_ID_TO_NAME.get(row['league_id'], f"League ID {row['league_id']}")

    prompt = f"""Analyze this soccer match and provide your predictions.

MATCH: {row['home_team_name']} (Home) vs {row['away_team_name']} (Away)
LEAGUE: {league_name}
DATE: {row['date']}

STEP 1 - SEARCH FOR FACTS:
Search the web for ALL of the following. Report what you find with specific details:

a) {row['home_team_name']} last 5 match results (scores and opponents)
b) {row['away_team_name']} last 5 match results (scores and opponents)
c) Key injuries or suspensions for BOTH teams ahead of this match
d) Head-to-head record between these two teams in recent seasons
e) Current league standings / position of both teams

STEP 2 - BASELINE MODEL PREDICTION (for reference):
Our statistical model predicts:
- Winner: {row['outcome_label']}
- Predicted Score: {row['home_team_name']} {row['predicted_home_goals']:.1f} - {row['predicted_away_goals']:.1f} {row['away_team_name']}
- Total Goals: {row['predicted_total_goals']:.1f}
- Over/Under 2.5: {row['ctmcl_prediction']}
- Model Confidence: {row['confidence_category']}
- Home Win Probability: {row['odds_ft_1_prob']:.1%}
- Away Win Probability: {row['odds_ft_2_prob']:.1%}

STEP 3 - YOUR VERDICT:
Based on the FACTS you found (not assumptions), provide your predictions in EXACTLY this format:

MONEYLINE: [Home Win OR Away Win OR Draw]
OVER/UNDER 2.5: [Over 2.5 OR Under 2.5]
SPREADS: [Full Team Name] ([+/-X.X])

REASONING: [2-3 sentences explaining your prediction based on the specific facts you found. Mention actual results, actual injuries, actual standings.]

IMPORTANT:
- Only predict Draw if evidence strongly supports it. Most matches have a winner.
- Your predictions should reflect what the web search evidence shows, whether that agrees or disagrees with the baseline model.
- If you found no relevant recent data for a team, lean toward the baseline model prediction rather than guessing."""

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
            return False

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
    print("=" * 60)
    print("AI SOCCER PREDICTIONS - MISTRAL + WEB SEARCH")
    print("=" * 60)
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

    # Prepare system prompt once (reused for all matches)
    system_prompt = create_system_prompt()

    success_count = 0
    error_count = 0
    parse_failures = 0

    print(f"\n{'=' * 60}")
    print("Starting match analysis (with web search)...")
    print(f"{'=' * 60}\n")

    for idx, row in enumerate(matches_to_process):
        match_id = row['match_id']
        home_team = row['home_team_name']
        away_team = row['away_team_name']
        league_id = row['league_id']
        league_name = LEAGUE_ID_TO_NAME.get(league_id, f"League ID {league_id}")

        print(f"\n[{idx + 1}/{len(matches_to_process)}] {home_team} vs {away_team}")
        print(f"  League: {league_name} | Date: {row['date']}")
        print(f"  Model says: {row['outcome_label']} | {row['ctmcl_prediction']} | Confidence: {row['confidence_category']}")
        print("-" * 60)

        # Create prompt and call Mistral with web search
        prompt = create_match_prompt(row)
        print("  🔍 Searching web + analyzing with Mistral...", end=" ", flush=True)

        response = call_mistral_conversations_api(prompt, system_prompt)

        if response:
            print("✓")

            # Parse predictions
            predictions = parse_mistral_response(response, home_team, away_team)

            # Validate all fields were parsed
            missing = [k for k, v in predictions.items() if v is None]
            if missing:
                parse_failures += 1
                print(f"  ⚠ Could not parse: {', '.join(missing)}")
                # Log a snippet of the raw response for debugging
                print(f"  Raw response snippet: {response[:200]}...")

            print(f"  AI Predictions:")
            print(f"    • Moneyline:   {predictions['ai_moneyline'] or '⚠ UNPARSED'}")
            print(f"    • Over/Under:  {predictions['ai_overunder'] or '⚠ UNPARSED'}")
            print(f"    • Spreads:     {predictions['ai_spreads'] or '⚠ UNPARSED'}")

            # Compare with model prediction
            if predictions['ai_moneyline']:
                agrees_ml = "✅ agrees" if predictions['ai_moneyline'] == row['outcome_label'] else "❌ disagrees"
                print(f"    → Moneyline vs model: {agrees_ml}")
            if predictions['ai_overunder']:
                agrees_ou = "✅ agrees" if predictions['ai_overunder'] == row['ctmcl_prediction'] else "❌ disagrees"
                print(f"    → Over/Under vs model: {agrees_ou}")

            # Update database
            if update_predictions(conn, match_id, predictions):
                success_count += 1
                print(f"  ✓ Database updated for match {match_id}")
            else:
                error_count += 1
                print(f"  ✗ Failed to update database for match {match_id}")
        else:
            error_count += 1
            print("✗")
            print(f"  ✗ Failed to get Mistral response for match {match_id}")

        # Rate limiting - web search calls cost $0.03 each, plus need to avoid rate limits
        if idx < len(matches_to_process) - 1:
            print("  ⏳ Waiting 5s before next request...", end=" ", flush=True)
            time.sleep(5)  # 5 second delay for web search rate limits
            print("✓")

    # Close connection
    conn.close()

    # Final Summary
    print(f"\n{'=' * 60}")
    print("PROCESSING COMPLETE")
    print(f"{'=' * 60}")
    print(f"Total matches in CSV:                {len(df)}")
    print(f"Matches skipped (already processed): {matches_skipped}")
    print(f"Matches analyzed:                    {len(matches_to_process)}")
    print(f"✓ Successfully updated:              {success_count}")
    print(f"✗ API/DB Errors:                     {error_count}")
    print(f"⚠ Partial parse failures:            {parse_failures}")
    if len(matches_to_process) > 0:
        print(f"Success rate: {(success_count / len(matches_to_process) * 100):.1f}%")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = "best_match_predictions.csv"

    process_predictions(csv_file)
