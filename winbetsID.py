"""
DUAL DATABASE ID MAPPING SCRIPT
Maps team IDs and league data between databases
Updates BOTH: Primary database (old credentials) AND WINBETS database (new credentials)
"""

import psycopg2
import pandas as pd
from psycopg2 import Error
import os

# ==================== DATABASE CONFIGURATION ====================
# Primary database (old credentials)
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Secondary database (new credentials - WINBETS)
DB_CONFIG_WINBETS = {
    'host': os.getenv('WINBETS_DB_HOST'),
    'port': int(os.getenv('WINBETS_DB_PORT', 5432)),
    'database': os.getenv('WINBETS_DB_DATABASE'),
    'user': os.getenv('WINBETS_DB_USER'),
    'password': os.getenv('WINBETS_DB_PASSWORD')
}

TABLE_NAME = 'agility_soccer_v1'

print("="*80)
print("DUAL DATABASE ID MAPPING SCRIPT")
print("="*80)

# ==================== LOAD CSV MAPPING FILE ====================
print("\n[1/4] Loading CSV mapping file...")

try:
    csv_path = 'map.csv'
    mapping_df = pd.read_csv(csv_path, encoding='utf-8-sig')
    print(f"✓ Loaded mapping file with {len(mapping_df)} team entries")
except Exception as e:
    print(f"✗ Error loading CSV: {e}")
    exit(1)

# Create lookup dictionaries
team_name_lookup = {}
team_id_lookup = {}
league_lookup = {}

for _, row in mapping_df.iterrows():
    team_name_clean = row['TeamName_Agility'].strip()
    league_clean = row['League_Agility'].strip()
    
    team_id_lookup[(row['TeamId_Agility'], league_clean)] = row['TeamId_Wb']
    team_name_lookup[(team_name_clean, league_clean)] = row['TeamName_Wb']
    
    if league_clean not in league_lookup:
        league_lookup[league_clean] = row['League_Wb']

print(f"✓ Created lookup dictionaries:")
print(f"  - Teams: {len(team_name_lookup)}")
print(f"  - Team IDs: {len(team_id_lookup)}")
print(f"  - Leagues: {len(league_lookup)}")

# ==================== HELPER FUNCTION FOR DATABASE OPERATIONS ====================
def process_database(db_config, db_name):
    """Process ID mapping for a specific database"""
    print(f"\n[2/4] Connecting to {db_name}...")
    
    connection = None
    
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        print(f"✓ Connected to {db_name}")
        print(f"  Host: {db_config['host']}")
        print(f"  Database: {db_config['database']}")
        
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False
    
    # Fetch all rows from database
    print(f"\n[3/4] Fetching records from {db_name}...")
    
    try:
        select_query = """
        SELECT match_id, home_team, away_team, home_id, away_id, league_name, 
               home_TeamName_Wb, away_TeamName_Wb, home_TeamId_Wb, away_TeamId_Wb, league_wb
        FROM agility_soccer_v1
        """
        cursor.execute(select_query)
        rows = cursor.fetchall()
        
        print(f"✓ Fetched {len(rows)} rows from {db_name}")
        
    except Exception as e:
        print(f"✗ Error fetching rows: {e}")
        cursor.close()
        connection.close()
        return False
    
    # Process and update records
    print(f"\n[4/4] Processing ID mappings for {db_name}...")
    
    updated_count = 0
    error_count = 0
    
    for row in rows:
        match_id, home_team, away_team, home_id, away_id, league_name, \
        home_TeamName_Wb, away_TeamName_Wb, home_TeamId_Wb, away_TeamId_Wb, league_wb = row
        
        updates = {}
        
        # Map home team name (only if NULL)
        if home_TeamName_Wb is None and home_team:
            home_team_clean = home_team.strip()
            league_clean = league_name.strip()
            wb_home_name = team_name_lookup.get((home_team_clean, league_clean))
            if wb_home_name:
                updates['home_TeamName_Wb'] = wb_home_name
        
        # Map away team name (only if NULL)
        if away_TeamName_Wb is None and away_team:
            away_team_clean = away_team.strip()
            league_clean = league_name.strip()
            wb_away_name = team_name_lookup.get((away_team_clean, league_clean))
            if wb_away_name:
                updates['away_TeamName_Wb'] = wb_away_name
        
        # Map home team ID (only if NULL)
        if home_TeamId_Wb is None and home_id:
            league_clean = league_name.strip()
            wb_home_id = team_id_lookup.get((home_id, league_clean))
            if wb_home_id:
                updates['home_TeamId_Wb'] = wb_home_id
        
        # Map away team ID (only if NULL)
        if away_TeamId_Wb is None and away_id:
            league_clean = league_name.strip()
            wb_away_id = team_id_lookup.get((away_id, league_clean))
            if wb_away_id:
                updates['away_TeamId_Wb'] = wb_away_id
        
        # Map league (only if NULL)
        if league_wb is None and league_name:
            league_name_clean = league_name.strip()
            wb_league = league_lookup.get(league_name_clean)
            if wb_league:
                updates['league_wb'] = wb_league
        
        # Update database if there are values to update
        if updates:
            try:
                set_clause = ", ".join([f"{key} = %s" for key in updates.keys()])
                values = list(updates.values()) + [match_id]
                update_query = f"UPDATE agility_soccer_v1 SET {set_clause} WHERE match_id = %s"
                cursor.execute(update_query, values)
                updated_count += 1
                
            except Exception as e:
                error_count += 1
                print(f"⚠ Error updating match {match_id}: {str(e)[:50]}")
    
    # Commit changes
    try:
        connection.commit()
        print(f"\n✓ Successfully updated {updated_count} rows in {db_name}")
        if error_count > 0:
            print(f"⚠ Errors encountered: {error_count} rows")
        
    except Exception as e:
        print(f"✗ Commit error: {e}")
        connection.rollback()
        cursor.close()
        connection.close()
        return False
    
    # Close connection
    cursor.close()
    connection.close()
    print(f"✓ {db_name} connection closed")
    
    return True

# ==================== PROCESS BOTH DATABASES ====================
print("\n" + "="*80)
print("PROCESSING BOTH DATABASES")
print("="*80)

success_primary = process_database(DB_CONFIG, "PRIMARY (Old Credentials)")
success_winbets = process_database(DB_CONFIG_WINBETS, "WINBETS (New Credentials)")

# ==================== FINAL SUMMARY ====================
print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)

if success_primary and success_winbets:
    print("✅ SUCCESS! ID mapping completed for BOTH databases")
    print("  ✓ Primary database (old credentials) updated")
    print("  ✓ WINBETS database (new credentials) updated")
elif success_primary:
    print("⚠️  PRIMARY database OK, but WINBETS database FAILED")
elif success_winbets:
    print("⚠️  WINBETS database OK, but PRIMARY database FAILED")
else:
    print("❌ Both databases FAILED")

print("\n" + "="*80)
