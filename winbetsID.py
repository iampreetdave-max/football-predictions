import psycopg2
import pandas as pd
from psycopg2 import Error

# Database credentials
db_config = {
    'host': 'winbets-db.postgres.database.azure.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'winbets',
    'password': 'deeptanshu@123'
}

# Load CSV mapping file
csv_path = 'map_winbets.csv'
mapping_df = pd.read_csv(csv_path)

# Create lookup dictionaries
team_name_lookup = dict(zip(mapping_df['TeamName_Agility'].str.strip(), mapping_df['TeamName_Wb']))
team_id_lookup = dict(zip(mapping_df['TeamId_Agility'], mapping_df['TeamId_Wb']))
league_lookup = dict(zip(mapping_df['League_Agility'], mapping_df['League_Wb']))

print("✓ CSV loaded. Created lookup dictionaries")
print(f"  - Teams: {len(team_name_lookup)}")
print(f"  - Team IDs: {len(team_id_lookup)}")
print(f"  - Leagues: {len(league_lookup)}")

try:
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    
    # Fetch all rows from database including WB columns
    select_query = """
    SELECT id, home_team, away_team, home_id, away_id, league_name, 
           home_TeamName_Wb, away_TeamName_Wb, home_TeamId_Wb, away_TeamId_Wb, league_wb
    FROM agility_soccer_v1
    """
    cursor.execute(select_query)
    rows = cursor.fetchall()
    
    print(f"\n✓ Fetched {len(rows)} rows from database")
    
    updated_count = 0
    
    for row in rows:
        row_id, home_team, away_team, home_id, away_id, league_name, \
        home_TeamName_Wb, away_TeamName_Wb, home_TeamId_Wb, away_TeamId_Wb, league_wb = row
        
        updates = {}
        
        # Map home team name (only if NULL)
        if home_TeamName_Wb is None and home_team:
            home_team_clean = home_team.strip()
            wb_home_name = team_name_lookup.get(home_team_clean)
            if wb_home_name:
                updates['home_TeamName_Wb'] = wb_home_name
        
        # Map away team name (only if NULL)
        if away_TeamName_Wb is None and away_team:
            away_team_clean = away_team.strip()
            wb_away_name = team_name_lookup.get(away_team_clean)
            if wb_away_name:
                updates['away_TeamName_Wb'] = wb_away_name
        
        # Map home team ID (only if NULL)
        if home_TeamId_Wb is None and home_id:
            wb_home_id = team_id_lookup.get(home_id)
            if wb_home_id:
                updates['home_TeamId_Wb'] = wb_home_id
        
        # Map away team ID (only if NULL)
        if away_TeamId_Wb is None and away_id:
            wb_away_id = team_id_lookup.get(away_id)
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
            set_clause = ", ".join([f"{key} = %s" for key in updates.keys()])
            values = list(updates.values()) + [row_id]
            update_query = f"UPDATE agility_soccer_v1 SET {set_clause} WHERE id = %s"
            cursor.execute(update_query, values)
            updated_count += 1
    
    connection.commit()
    print(f"\n✓ Updated {updated_count} rows successfully!")
    
except Error as e:
    print(f"Error: {e}")
    connection.rollback()
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Database connection closed.")
