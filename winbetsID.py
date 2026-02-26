"""
DUAL DATABASE ID MAPPING SCRIPT - DUAL TABLES
Maps team IDs and league data between databases
Updates BOTH tables AND both databases:
- predictions_soccer_v1_ourmodel (Primary)
- predictions_soccer_v3_ourmodel (New)

For:
- Primary database (old credentials)
- WINBETS database (new credentials)
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

TABLE_NAMES = ['predictions_soccer_v1_ourmodel', 'predictions_soccer_v3_ourmodel']

print("="*80)
print("DUAL DATABASE ID MAPPING SCRIPT - DUAL TABLES")
print("="*80)
print(f"Tables: {', '.join(TABLE_NAMES)}")

# ==================== LOAD CSV MAPPING FILE ====================
print("\n[1/4] Loading CSV mapping file...")

try:
    csv_path = 'map2026.csv'
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
    """Process ID mapping for a specific database and both tables"""
    print(f"\n{'='*80}")
    print(f"Processing {db_name}")
    print(f"{'='*80}")
    
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
    
    # Process each table
    all_success = True
    
    for table_name in TABLE_NAMES:
        print(f"\n{'='*40}")
        print(f"Table: {table_name}")
        print(f"{'='*40}")
        
        # Fetch all rows from database
        print(f"[3/4] Fetching records from {table_name}...")
        
        try:
            select_query = f"""
            SELECT match_id, home_team, away_team, home_id, away_id, league_name, 
                   home_teamname_wb, away_teamname_wb, home_teamid_wb, away_teamid_wb, league_wb
            FROM {table_name}
            """
            cursor.execute(select_query)
            rows = cursor.fetchall()
            
            print(f"✓ Fetched {len(rows)} rows from {table_name}")
            
        except Exception as e:
            print(f"✗ Error fetching rows from {table_name}: {e}")
            all_success = False
            continue
        
        # Process and update records
        print(f"[4/4] Processing ID mappings for {table_name}...")
        
        updated_count = 0
        error_count = 0
        
        for row in rows:
            match_id, home_team, away_team, home_id, away_id, league_name, \
            home_teamname_wb, away_teamname_wb, home_teamid_wb, away_teamid_wb, league_wb = row
            
            updates = {}
            
            # Map home team name (only if NULL)
            if home_teamname_wb is None and home_team:
                home_team_clean = home_team.strip()
                league_clean = league_name.strip()
                wb_home_name = team_name_lookup.get((home_team_clean, league_clean))
                if wb_home_name:
                    updates['home_teamname_wb'] = wb_home_name
            
            # Map away team name (only if NULL)
            if away_teamname_wb is None and away_team:
                away_team_clean = away_team.strip()
                league_clean = league_name.strip()
                wb_away_name = team_name_lookup.get((away_team_clean, league_clean))
                if wb_away_name:
                    updates['away_teamname_wb'] = wb_away_name
            
            # Map home team ID (only if NULL)
            if home_teamid_wb is None and home_id:
                league_clean = league_name.strip()
                wb_home_id = team_id_lookup.get((home_id, league_clean))
                if wb_home_id:
                    updates['home_teamid_wb'] = wb_home_id
            
            # Map away team ID (only if NULL)
            if away_teamid_wb is None and away_id:
                league_clean = league_name.strip()
                wb_away_id = team_id_lookup.get((away_id, league_clean))
                if wb_away_id:
                    updates['away_teamid_wb'] = wb_away_id
            
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
                    update_query = f"UPDATE {table_name} SET {set_clause} WHERE match_id = %s"
                    cursor.execute(update_query, values)
                    updated_count += 1
                    
                except Exception as e:
                    error_count += 1
                    print(f"⚠ Error updating match {match_id}: {str(e)[:50]}")
        
        # Commit changes for this table
        try:
            connection.commit()
            print(f"\n✓ Successfully updated {updated_count} rows in {table_name}")
            if error_count > 0:
                print(f"⚠ Errors encountered: {error_count} rows")
            
        except Exception as e:
            print(f"✗ Commit error for {table_name}: {e}")
            connection.rollback()
            all_success = False
    
    # Close connection
    cursor.close()
    connection.close()
    print(f"\n✓ {db_name} connection closed")
    
    return all_success

# ==================== PROCESS BOTH DATABASES ====================
print("\n" + "="*80)
print("PROCESSING BOTH DATABASES AND BOTH TABLES")
print("="*80)

success_primary = process_database(DB_CONFIG, "PRIMARY (Old Credentials)")
success_winbets = process_database(DB_CONFIG_WINBETS, "WINBETS (New Credentials)")

# ==================== FINAL SUMMARY ====================
print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)

if success_primary and success_winbets:
    print("✅ SUCCESS! ID mapping completed for:")
    print("  ✓ PRIMARY database - BOTH tables")
    print("    • predictions_soccer_v1_ourmodel")
    print("    • predictions_soccer_v3_ourmodel")
    print("  ✓ WINBETS database - BOTH tables")
    print("    • predictions_soccer_v1_ourmodel")
    print("    • predictions_soccer_v3_ourmodel")
elif success_primary:
    print("⚠️  PRIMARY database OK, but WINBETS database FAILED")
elif success_winbets:
    print("⚠️  WINBETS database OK, but PRIMARY database FAILED")
else:
    print("❌ Both databases FAILED")

print("\n" + "="*80)
