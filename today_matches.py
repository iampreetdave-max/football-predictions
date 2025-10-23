"""
FootyStats API - Optimized Multi-Day Matches Fetcher
Fetches ONLY essential fields with data (removes NaN columns automatically)
"""

import requests
import csv
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time

# Configuration
API_KEY = "633379bdd5c4c3eb26919d8570866801e1c07f399197ba8c5311446b8ea77a49"
BASE_URL = "https://api.footystats.org"

class FootyStatsAPI:
    """FootyStats API Client"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = BASE_URL
        self.request_count = 0
        self.max_retries = 3

    def fetch_todays_matches(self, timezone: str = "Etc/UTC", date: Optional[str] = None, page: int = 1) -> Optional[Dict]:
        """Fetch matches from FootyStats API for a given date"""
        url = f"{self.base_url}/todays-matches"
        params = {"key": self.api_key, "timezone": timezone, "page": page}
        if date:
            params["date"] = date
        
        for attempt in range(self.max_retries):
            try:
                self.request_count += 1
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                if not data.get("success", False):
                    print(f"✗ API error: {data.get('error', 'Unknown')}")
                    return None
                return data
            except requests.exceptions.Timeout:
                print(f"⚠ Timeout (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    time.sleep(2)
            except requests.exceptions.RequestException as e:
                print(f"✗ Error (attempt {attempt + 1}/{self.max_retries}): {str(e)[:100]}")
                if attempt < self.max_retries - 1:
                    time.sleep(2)
        return None


def format_datetime(unix_timestamp: int) -> str:
    """Convert Unix timestamp to readable datetime"""
    try:
        if unix_timestamp and unix_timestamp > 0:
            dt = datetime.fromtimestamp(unix_timestamp)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        pass
    return ""


def safe_get(data: Dict, key: str, default=""):
    """Safely get value from dictionary"""
    value = data.get(key, default)
    if value in [None, "", -1, "N/A", "null"]:
        return default
    return value


def calculate_features(match: Dict) -> Dict:
    """Calculate additional features for prediction model"""
    features = {}
    
    # Implied probabilities
    odds_ft_1 = safe_get(match, "odds_ft_1", 0)
    odds_ft_x = safe_get(match, "odds_ft_x", 0)
    odds_ft_2 = safe_get(match, "odds_ft_2", 0)
    
    probs = []
    if odds_ft_1 and float(odds_ft_1) > 0:
        features["odds_ft_1_prob"] = 1 / float(odds_ft_1)
        probs.append(features["odds_ft_1_prob"])
    else:
        features["odds_ft_1_prob"] = ""
    
    if odds_ft_x and float(odds_ft_x) > 0:
        features["odds_ft_x_prob"] = 1 / float(odds_ft_x)
        probs.append(features["odds_ft_x_prob"])
    else:
        features["odds_ft_x_prob"] = ""
    
    if odds_ft_2 and float(odds_ft_2) > 0:
        features["odds_ft_2_prob"] = 1 / float(odds_ft_2)
        probs.append(features["odds_ft_2_prob"])
    else:
        features["odds_ft_2_prob"] = ""
    
    # Normalize probabilities
    if probs:
        total = sum(probs)
        if features["odds_ft_1_prob"]:
            features["odds_ft_1_prob"] = features["odds_ft_1_prob"] / total
        if features["odds_ft_x_prob"]:
            features["odds_ft_x_prob"] = features["odds_ft_x_prob"] / total
        if features["odds_ft_2_prob"]:
            features["odds_ft_2_prob"] = features["odds_ft_2_prob"] / total
    
    # CTMCL calculation
    odds_over25 = safe_get(match, "odds_ft_over25", 0)
    if odds_over25 and float(odds_over25) > 0:
        features["IP_OVER"] = 1 / float(odds_over25)
        features["CTMCL"] = max(0.5, min(6.0, 2.5 + (features["IP_OVER"] - 0.5)))
    else:
        features["IP_OVER"] = ""
        features["CTMCL"] = 2.5
    
    # Average goals market
    o25_pot = safe_get(match, "o25_potential", 0)
    o15_pot = safe_get(match, "o15_potential", 0)
    if o25_pot and o15_pot:
        features["avg_goals_market"] = max(0.5, min(6.0, 2.5 + (float(o25_pot) - float(o15_pot)) / 100))
    else:
        features["avg_goals_market"] = 2.5
    
    # Pre-match total xG
    team_a_xg = safe_get(match, "team_a_xg_prematch", 0)
    team_b_xg = safe_get(match, "team_b_xg_prematch", 0)
    if team_a_xg and team_b_xg:
        features["pre_total_xg"] = float(team_a_xg) + float(team_b_xg)
    else:
        features["pre_total_xg"] = ""
    
    return features


def extract_match_data(matches: List[Dict]) -> pd.DataFrame:
    """Extract match data into DataFrame with only populated fields"""
    
    all_rows = []
    
    for match in matches:
        calc_features = calculate_features(match)
        
        # Build row with ONLY essential fields that are typically populated
        row = {
            # Match Identification
            "match_id": safe_get(match, "id"),
            "date": format_datetime(match.get("date_unix", 0)),
            "date_unix": safe_get(match, "date_unix"),
            "status": safe_get(match, "status"),
            "game_week": safe_get(match, "game_week"),
            "season": safe_get(match, "season"),
            "fetch_date": match.get("fetch_date", ""),
            
            # League
            "league_id": safe_get(match, "competition_id"),
            "league_name": safe_get(match, "competition_name", safe_get(match, "league_name")),
            
            # Teams
            "homeID": safe_get(match, "homeID"),
            "home_name": safe_get(match, "home_name"),
            "awayID": safe_get(match, "awayID"),
            "away_name": safe_get(match, "away_name"),
            
            # Pre-match Stats (CRITICAL)
            "team_a_xg_prematch": safe_get(match, "team_a_xg_prematch"),
            "team_b_xg_prematch": safe_get(match, "team_b_xg_prematch"),
            "pre_match_teamA_ppg": safe_get(match, "pre_match_teamA_ppg"),
            "pre_match_teamB_ppg": safe_get(match, "pre_match_teamB_ppg"),
            
            # Match Winner Odds
            "odds_ft_1": safe_get(match, "odds_ft_1"),
            "odds_ft_x": safe_get(match, "odds_ft_x"),
            "odds_ft_2": safe_get(match, "odds_ft_2"),
            "odds_ft_1_prob": calc_features.get("odds_ft_1_prob", ""),
            "odds_ft_x_prob": calc_features.get("odds_ft_x_prob", ""),
            "odds_ft_2_prob": calc_features.get("odds_ft_2_prob", ""),
            
            # Over/Under Odds (Most Common)
            "odds_ft_over05": safe_get(match, "odds_ft_over05"),
            "odds_ft_under05": safe_get(match, "odds_ft_under05"),
            "odds_ft_over15": safe_get(match, "odds_ft_over15"),
            "odds_ft_under15": safe_get(match, "odds_ft_under15"),
            "odds_ft_over25": safe_get(match, "odds_ft_over25"),
            "odds_ft_under25": safe_get(match, "odds_ft_under25"),
            "odds_ft_over35": safe_get(match, "odds_ft_over35"),
            "odds_ft_under35": safe_get(match, "odds_ft_under35"),
            "odds_ft_over45": safe_get(match, "odds_ft_over45"),
            "odds_ft_under45": safe_get(match, "odds_ft_under45"),
            
            # BTTS
            "odds_btts_yes": safe_get(match, "odds_btts_yes"),
            "odds_btts_no": safe_get(match, "odds_btts_no"),
            
            # Double Chance
            "odds_doublechance_1x": safe_get(match, "odds_doublechance_1x"),
            "odds_doublechance_12": safe_get(match, "odds_doublechance_12"),
            "odds_doublechance_x2": safe_get(match, "odds_doublechance_x2"),
            
            # Market Potentials (Usually Available)
            "btts_potential": safe_get(match, "btts_potential"),
            "o05_potential": safe_get(match, "o05_potential"),
            "o15_potential": safe_get(match, "o15_potential"),
            "o25_potential": safe_get(match, "o25_potential"),
            "o35_potential": safe_get(match, "o35_potential"),
            "o45_potential": safe_get(match, "o45_potential"),
            
            # Calculated Features
            "CTMCL": calc_features.get("CTMCL", ""),
            "IP_OVER": calc_features.get("IP_OVER", ""),
            "avg_goals_market": calc_features.get("avg_goals_market", ""),
            "pre_total_xg": calc_features.get("pre_total_xg", ""),
            
            # Additional common odds
            "odds_1st_half_1": safe_get(match, "odds_1st_half_1"),
            "odds_1st_half_x": safe_get(match, "odds_1st_half_x"),
            "odds_1st_half_2": safe_get(match, "odds_1st_half_2"),
            
            # Corners (if available)
            "odds_corners_over_85": safe_get(match, "odds_corners_over_85"),
            "odds_corners_over_95": safe_get(match, "odds_corners_over_95"),
            
            # Common statistics
            "team_a_shots": safe_get(match, "team_a_shots"),
            "team_b_shots": safe_get(match, "team_b_shots"),
            "team_a_shotsOnTarget": safe_get(match, "team_a_shotsOnTarget"),
            "team_b_shotsOnTarget": safe_get(match, "team_b_shotsOnTarget"),
            "team_a_corners": safe_get(match, "team_a_corners"),
            "team_b_corners": safe_get(match, "team_b_corners"),
            "team_a_yellow_cards": safe_get(match, "team_a_yellow_cards"),
            "team_b_yellow_cards": safe_get(match, "team_b_yellow_cards"),
            "team_a_red_cards": safe_get(match, "team_a_red_cards"),
            "team_b_red_cards": safe_get(match, "team_b_red_cards"),
            "team_a_attacks": safe_get(match, "team_a_attacks"),
            "team_b_attacks": safe_get(match, "team_b_attacks"),
            "team_a_dangerous_attacks": safe_get(match, "team_a_dangerous_attacks"),
            "team_b_dangerous_attacks": safe_get(match, "team_b_dangerous_attacks"),
            "team_a_penalties_won": safe_get(match, "team_a_penalties_won"),
            "team_b_penalties_won": safe_get(match, "team_b_penalties_won"),
        }
        
        all_rows.append(row)
    
    df = pd.DataFrame(all_rows)
    return df


def remove_empty_columns(df: pd.DataFrame, threshold: float = 0.95) -> pd.DataFrame:
    """Remove columns where more than threshold% of values are empty/NaN"""
    
    initial_cols = len(df.columns)
    
    # Calculate percentage of empty values per column
    empty_pct = df.isnull().sum() / len(df)
    
    # Keep columns with less than threshold empty values
    cols_to_keep = empty_pct[empty_pct < threshold].index.tolist()
    
    # Also check for empty strings
    for col in df.columns:
        if col not in cols_to_keep:
            continue
        if df[col].dtype == 'object':
            non_empty = df[col].apply(lambda x: x not in ["", None, "N/A"]).sum()
            if non_empty / len(df) < (1 - threshold):
                cols_to_keep.remove(col)
    
    df_cleaned = df[cols_to_keep]
    
    removed_cols = initial_cols - len(cols_to_keep)
    if removed_cols > 0:
        print(f"   ✓ Removed {removed_cols} empty columns (>{threshold*100:.0f}% NaN)")
    
    return df_cleaned


def main():
    """Main execution function"""
    print("=" * 80)
    print("FootyStats API - Optimized Multi-Day Matches Fetcher")
    print("Fetches ONLY populated fields (auto-removes NaN columns)")
    print("=" * 80)
    print()
    
    api_client = FootyStatsAPI(API_KEY)
    
    all_matches_combined = []
    dates_info = []
    base_day = datetime.now()
    day_labels = ['Today', 'Tomorrow', 'Day After Tomorrow']
    
    for offset_day, label in enumerate(day_labels):
        fetch_date = (base_day + timedelta(days=offset_day)).strftime('%Y-%m-%d')
        dates_info.append(f"{label} ({fetch_date})")
        print(f"📅 Fetching {label} ({fetch_date})")
        print('-' * 80)
        
        page = 1
        day_matches = []
        
        while True:
            print(f"   Page {page}...", end=" ")
            data = api_client.fetch_todays_matches("Etc/UTC", fetch_date, page)
            if not data:
                print("Failed")
                break
            matches = data.get("data", [])
            if not matches:
                print("No more matches")
                break
            
            # Add fetch_date to each match
            for match in matches:
                match['fetch_date'] = fetch_date
            
            day_matches.extend(matches)
            print(f"✓ {len(matches)} matches")
            
            pager = data.get("pager", {})
            if pager.get('current_page', 0) >= pager.get('max_page', 0):
                break
            page += 1
            time.sleep(0.5)
        
        all_matches_combined.extend(day_matches)
        print(f"   Total: {len(day_matches)} matches for {label}\n")
    
    if all_matches_combined:
        filename = "live.csv"
        print("\n" + "=" * 80)
        print(f"💾 Processing and saving to: {filename}")
        print("=" * 80)
        
        # Extract data to DataFrame
        print(f"\n   Extracting {len(all_matches_combined)} matches...")
        df = extract_match_data(all_matches_combined)
        
        # Remove empty columns
        print(f"   Cleaning data (removing NaN columns)...")
        df = remove_empty_columns(df, threshold=0.95)
        
        # Save to CSV
        df.to_csv(filename, index=False)
        
        print(f"\n✓ Saved {len(df)} matches to {filename}")
        print(f"✓ Columns: {len(df.columns)} (kept only populated fields)")
        print(f"✓ API requests: {api_client.request_count}")
        
        # Show summary
        print(f"\n📊 Summary:")
        print(f"   Total matches: {len(df)}")
        print(f"   Date range: {', '.join(dates_info)}")
        print(f"   Leagues: {df['league_name'].nunique() if 'league_name' in df.columns else 'N/A'}")
        
        # Show key fields status
        print(f"\n📋 Key Fields Availability:")
        key_fields = ['competition_id', 'team_a_xg_prematch', 'team_b_xg_prematch', 
                     'pre_match_teamA_ppg', 'pre_match_teamB_ppg', 'odds_ft_1', 
                     'odds_ft_over25', 'CTMCL']
        for field in key_fields:
            if field in df.columns:
                non_empty = df[field].notna().sum()
                pct = (non_empty / len(df)) * 100
                status = "✓" if pct > 50 else "⚠"
                print(f"   {status} {field}: {non_empty}/{len(df)} ({pct:.1f}%)")
        
        print(f"\n✅ Ready for prediction model!")
        
    else:
        print("✗ No matches found")
    
    print("\n" + "=" * 80)
    print("✅ Complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()