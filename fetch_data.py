import pandas as pd
import requests
import json
import time
import numpy as np

API_KEY = "633379bdd5c4c3eb26919d8570866801e1c07f399197ba8c5311446b8ea77a49"
API_BASE_URL = "https://api.football-data-api.com/lastx"
API_LEAGUE_URL = "https://api.football-data-api.com/league-season"

def normalize_probability(value, expected_range_0_100=True):
    """
    Normalize probability values to be in the range 0-100.
    Handles cases where values might be stored as decimals (0-1) or percentages (0-100).
    """
    if pd.isna(value):
        return 50.0  # Default neutral value
    
    # If value appears to be a decimal probability (0-1), convert to percentage
    if expected_range_0_100 and value <= 1.0:
        return value * 100
    # If value appears to be percentage but stored as decimal
    elif not expected_range_0_100 and value > 1.0:
        return value / 100
    
    return value

def normalize_odds_probability(value):
    """
    Normalize odds probability values (should be 0-1 range representing probabilities).
    """
    if pd.isna(value):
        return 0.33  # Default neutral value
    
    # If value is in percentage range (0-100), convert to decimal
    if value > 1.0:
        return value / 100
    
    return value

# Read live.csv
print("=" * 80)
print("LOADING LIVE.CSV")
print("=" * 80)
df = pd.read_csv("live.csv")
print(f"✓ Loaded {len(df)} matches from live.csv")

# Check and display sample values to identify scaling issues
print("\n" + "=" * 80)
print("CHECKING DATA SCALING")
print("=" * 80)
if len(df) > 0:
    sample = df.iloc[0]
    print("Sample values from first row:")
    for col in ['o25_potential', 'o15_potential', 'odds_ft_1_prob', 'odds_ft_2_prob', 'btts_potential']:
        if col in df.columns:
            val = sample.get(col, 'N/A')
            print(f"  {col}: {val}")

# Collect unique team IDs and league IDs
unique_team_ids = set()
unique_team_ids.update(df['homeID'].unique())
unique_team_ids.update(df['awayID'].unique())
unique_league_ids = set(df['league_id'].unique())

print(f"\n✓ Found {len(unique_team_ids)} unique teams")
print(f"✓ Found {len(unique_league_ids)} unique leagues")

# Fetch team data
print("\n" + "=" * 80)
print("FETCHING TEAM DATA FROM API")
print("=" * 80)

team_data_cache = {}
count = 0

for team_id in unique_team_ids:
    try:
        url = f"{API_BASE_URL}?key={API_KEY}&team_id={team_id}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success') and data.get('data'):
                team_data_cache[team_id] = data['data'][0]
                count += 1
                print(f"✓ Team {team_id}: Success ({count}/{len(unique_team_ids)})")
            else:
                print(f"✗ Team {team_id}: No data returned")
        else:
            print(f"✗ Team {team_id}: HTTP {response.status_code}")
        
        time.sleep(0.1)  # Rate limiting
        
    except Exception as e:
        print(f"✗ Team {team_id}: {str(e)}")

print(f"\n✓ Successfully fetched data for {len(team_data_cache)} teams")

# Fetch league data
print("\n" + "=" * 80)
print("FETCHING LEAGUE DATA FROM API")
print("=" * 80)

league_data_cache = {}
league_count = 0

for league_id in unique_league_ids:
    try:
        # Get current season (2025/2026 based on live.csv)
        url = f"{API_LEAGUE_URL}?key={API_KEY}&league_id={league_id}&season_id=2025/2026"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success') and data.get('data'):
                league_data_cache[league_id] = data['data']
                league_count += 1
                print(f"✓ League {league_id}: Success ({league_count}/{len(unique_league_ids)})")
            else:
                print(f"✗ League {league_id}: No data returned")
        else:
            print(f"✗ League {league_id}: HTTP {response.status_code}")
        
        time.sleep(0.5)
        
    except Exception as e:
        print(f"✗ League {league_id}: {str(e)}")

print(f"\n✓ Successfully fetched data for {len(league_data_cache)} leagues")

# Process each match and extract features
print("\n" + "=" * 80)
print("EXTRACTING FEATURES FOR ALL MATCHES")
print("=" * 80)

all_features = []
processed = 0
skipped = 0

for idx, row in df.iterrows():
    match_id = row['match_id']
    home_id = row['homeID']
    away_id = row['awayID']
    league_id = row['league_id']
    
    # Skip if we don't have data for both teams
    if home_id not in team_data_cache or away_id not in team_data_cache:
        print(f"⚠ Match {match_id}: Missing team data (home: {home_id}, away: {away_id})")
        skipped += 1
        continue
    
    home_data = team_data_cache[home_id]
    away_data = team_data_cache[away_id]
    
    try:
        # Normalize potential values (should be 0-100)
        o25_pot = normalize_probability(row.get('o25_potential', 50), expected_range_0_100=True)
        o15_pot = normalize_probability(row.get('o15_potential', 50), expected_range_0_100=True)
        o35_pot = normalize_probability(row.get('o35_potential', 50), expected_range_0_100=True)
        o05_pot = normalize_probability(row.get('o05_potential', 50), expected_range_0_100=True)
        o45_pot = normalize_probability(row.get('o45_potential', 50), expected_range_0_100=True)
        btts_pot = normalize_probability(row.get('btts_potential', 50), expected_range_0_100=True)
        
        # Normalize odds probabilities (should be 0-1)
        odds_ft_1_prob = normalize_odds_probability(row.get('odds_ft_1_prob', 0.33))
        odds_ft_2_prob = normalize_odds_probability(row.get('odds_ft_2_prob', 0.33))
        
        # Calculate avg_goals_market from o25_potential and o15_potential
        # Using normalized values (0-100 scale)
        avg_goals_market = 2.5 + (o25_pot - o15_pot) / 100
        avg_goals_market = np.clip(avg_goals_market, 0.5, 6.0)
        
        # Get league average goals
        league_avg_goals = 2.5
        if league_id in league_data_cache:
            league_stats = league_data_cache[league_id]
            if isinstance(league_stats, dict) and 'seasonAVG_overall' in league_stats:
                league_avg_goals = league_stats['seasonAVG_overall']
        
        # Calculate shots accuracy
        home_shots_avg = home_data['stats'].get('shotsAVG_home', 1)
        home_sot_avg = home_data['stats'].get('shotsOnTargetAVG_home', 0)
        home_shots_accuracy = home_sot_avg / max(home_shots_avg, 1) if home_shots_avg > 0 else 0.33
        
        away_shots_avg = away_data['stats'].get('shotsAVG_away', 1)
        away_sot_avg = away_data['stats'].get('shotsOnTargetAVG_away', 0)
        away_shots_accuracy = away_sot_avg / max(away_shots_avg, 1) if away_shots_avg > 0 else 0.33
        
        # Extract features
        features = {
            # IDs
            'match_id': match_id,
            'home_team_id': home_id,
            'away_team_id': away_id,
            'league_id': league_id,
            
            # Team names (optional, for reference)
            'home_team_name': home_data.get('name', ''),
            'away_team_name': away_data.get('name', ''),
            
            # CTMCL and market data (from live.csv)
            'CTMCL': row.get('CTMCL', 2.5),
            'avg_goals_market': avg_goals_market,
            
            # Pre-match xG
            'team_a_xg_prematch': row.get('team_a_xg_prematch', home_data['stats'].get('xg_for_avg_home', 0)),
            'team_b_xg_prematch': row.get('team_b_xg_prematch', away_data['stats'].get('xg_for_avg_away', 0)),
            
            # Pre-match PPG
            'pre_match_home_ppg': home_data['stats'].get('seasonPPG_home', 0),
            'pre_match_away_ppg': away_data['stats'].get('seasonPPG_away', 0),
            
            # XG averages
            'home_xg_avg': home_data['stats'].get('xg_for_avg_home', 0),
            'away_xg_avg': away_data['stats'].get('xg_for_avg_away', 0),
            
            # XG momentum (approximation: recent - overall)
            'home_xg_momentum': 0,  # Would need historical data
            'away_xg_momentum': 0,
            
            # Goals conceded averages
            'home_goals_conceded_avg': home_data['stats'].get('seasonConcededAVG_home', 0),
            'away_goals_conceded_avg': away_data['stats'].get('seasonConcededAVG_away', 0),
            
            # Over/under potentials (normalized to 0-100)
            'o25_potential': o25_pot,
            'o35_potential': o35_pot,
            
            # Shots accuracy
            'home_shots_accuracy_avg': home_shots_accuracy,
            'away_shots_accuracy_avg': away_shots_accuracy,
            
            # Dangerous attacks
            'home_dangerous_attacks_avg': home_data['stats'].get('dangerous_attacks_avg_home', 0),
            'away_dangerous_attacks_avg': away_data['stats'].get('dangerous_attacks_avg_away', 0),
            
            # H2H (not available in lastx endpoint, default to 0)
            'h2h_total_goals_avg': 0,
            
            # Form points
            'home_form_points': home_data['stats'].get('seasonPPG_home', 0) * 5,
            'away_form_points': away_data['stats'].get('seasonPPG_away', 0) * 5,
            
            # Elo diff (approximation using performance rank)
            'home_elo': 1500 + (home_data.get('performance_rank', 0) * 10),
            'away_elo': 1500 + (away_data.get('performance_rank', 0) * 10),
            'elo_diff': (1500 + (home_data.get('performance_rank', 0) * 10)) - (1500 + (away_data.get('performance_rank', 0) * 10)),
            
            # League average goals
            'league_avg_goals': league_avg_goals,
            
            # Additional useful features from live.csv (normalized)
            'odds_ft_1_prob': odds_ft_1_prob,
            'odds_ft_2_prob': odds_ft_2_prob,
            'btts_potential': btts_pot,
            'o05_potential': o05_pot,
            'o15_potential': o15_pot,
            'o45_potential': o45_pot,
        }
        
        all_features.append(features)
        processed += 1
        
        if processed % 10 == 0:
            print(f"✓ Processed {processed}/{len(df)} matches...")
        
    except Exception as e:
        print(f"✗ Match {match_id}: Error - {str(e)}")
        import traceback
        traceback.print_exc()
        skipped += 1

print(f"\n✓ Processing complete!")
print(f"  - Processed: {processed} matches")
print(f"  - Skipped: {skipped} matches")

# Convert to DataFrame and save
print("\n" + "=" * 80)
print("SAVING FEATURES TO CSV")
print("=" * 80)

if all_features:
    features_df = pd.DataFrame(all_features)
    
    # Reorder columns to match best_version.py requirements
    column_order = [
        'match_id', 'home_team_id', 'away_team_id', 'league_id',
        'home_team_name', 'away_team_name',
        'CTMCL', 'avg_goals_market',
        'team_a_xg_prematch', 'team_b_xg_prematch',
        'pre_match_home_ppg', 'pre_match_away_ppg',
        'home_xg_avg', 'away_xg_avg',
        'home_xg_momentum', 'away_xg_momentum',
        'home_goals_conceded_avg', 'away_goals_conceded_avg',
        'o25_potential', 'o35_potential',
        'home_shots_accuracy_avg', 'away_shots_accuracy_avg',
        'home_dangerous_attacks_avg', 'away_dangerous_attacks_avg',
        'h2h_total_goals_avg',
        'home_form_points', 'away_form_points',
        'home_elo', 'away_elo', 'elo_diff',
        'league_avg_goals',
        'odds_ft_1_prob', 'odds_ft_2_prob',
        'btts_potential', 'o05_potential', 'o15_potential', 'o45_potential'
    ]
    
    # Ensure all columns exist
    for col in column_order:
        if col not in features_df.columns:
            features_df[col] = 0
    
    features_df = features_df[column_order]
    
    features_df.to_csv('extracted_features_complete.csv', index=False)
    print(f"✓ Saved {len(features_df)} matches to 'extracted_features_complete.csv'")
    
    print("\n" + "=" * 80)
    print("FEATURE SUMMARY")
    print("=" * 80)
    print(f"Total columns: {len(features_df.columns)}")
    print(f"Total matches with features: {len(features_df)}")
    
    # Show sample statistics
    print(f"\nSample statistics:")
    print(f"  CTMCL: mean={features_df['CTMCL'].mean():.2f}, std={features_df['CTMCL'].std():.2f}")
    print(f"  Home xG: mean={features_df['team_a_xg_prematch'].mean():.2f}, std={features_df['team_a_xg_prematch'].std():.2f}")
    print(f"  Away xG: mean={features_df['team_b_xg_prematch'].mean():.2f}, std={features_df['team_b_xg_prematch'].std():.2f}")
    print(f"  Home PPG: mean={features_df['pre_match_home_ppg'].mean():.2f}, std={features_df['pre_match_home_ppg'].std():.2f}")
    print(f"  Away PPG: mean={features_df['pre_match_away_ppg'].mean():.2f}, std={features_df['pre_match_away_ppg'].std():.2f}")
    print(f"  o25_potential: mean={features_df['o25_potential'].mean():.2f}, std={features_df['o25_potential'].std():.2f}")
    print(f"  odds_ft_1_prob: mean={features_df['odds_ft_1_prob'].mean():.3f}, std={features_df['odds_ft_1_prob'].std():.3f}")
    
    # Show value ranges to verify normalization
    print(f"\nValue ranges (to verify normalization):")
    print(f"  o25_potential: [{features_df['o25_potential'].min():.2f}, {features_df['o25_potential'].max():.2f}]")
    print(f"  o15_potential: [{features_df['o15_potential'].min():.2f}, {features_df['o15_potential'].max():.2f}]")
    print(f"  btts_potential: [{features_df['btts_potential'].min():.2f}, {features_df['btts_potential'].max():.2f}]")
    print(f"  odds_ft_1_prob: [{features_df['odds_ft_1_prob'].min():.3f}, {features_df['odds_ft_1_prob'].max():.3f}]")
    print(f"  odds_ft_2_prob: [{features_df['odds_ft_2_prob'].min():.3f}, {features_df['odds_ft_2_prob'].max():.3f}]")
    
    print(f"\nFirst match preview:")
    first_match = features_df.iloc[0]
    print(f"  Match ID: {first_match['match_id']}")
    print(f"  {first_match['home_team_name']} (xG: {first_match['team_a_xg_prematch']:.2f}) vs {first_match['away_team_name']} (xG: {first_match['team_b_xg_prematch']:.2f})")
    print(f"  CTMCL: {first_match['CTMCL']:.2f}")
    print(f"  Home form: {first_match['home_form_points']:.2f} | Away form: {first_match['away_form_points']:.2f}")
    print(f"  o25_potential: {first_match['o25_potential']:.2f} | btts_potential: {first_match['btts_potential']:.2f}")
    print(f"  odds_ft_1_prob: {first_match['odds_ft_1_prob']:.3f} | odds_ft_2_prob: {first_match['odds_ft_2_prob']:.3f}")
    
else:
    print("⚠ No features extracted. Check API responses and data availability.")

print("\n" + "=" * 80)
print("COMPLETED!")
print("=" * 80)
print(f"✓ Output file: extracted_features_complete.csv")
print(f"✓ Ready for use with best_version.py")