# ETL script for World Cup 2022 data.
# We have two data files to process: 'WorldCupMatches2022.csv' and 'WorldCupMatches2022-venue.csv'
# We will follow theses steps:
# - Extract relevant data
# - Clean it
# - Transform it
# - Export it to a pd dataframe

import pandas as pd
import numpy as np
import os

def get_cleaned_2022_data() -> pd.DataFrame:
    """
    ETL pipeline for World Cup 2022 data.
    Reads from ./data, cleans teams/dates, merges match info with venues,
    and returns a clean DataFrame.
    """
    print("--- STARTING 2022 ETL PROCESS ---")

    # --- 1. CONFIGURATION (Relative Paths) ---
    # We assume the script is run from a root directory that contains a 'data' folder
    data_base_dir = "./../data"
    
    file1_path = os.path.join(data_base_dir, "WorldCupMatches2022.csv")
    file2_path = os.path.join(data_base_dir, "WorldCupMatches2022-venue.csv")
    mapping_path = os.path.join(data_base_dir, "stadium_city_mapping2022.csv")

    # Verify files exist
    for p in [file1_path, file2_path, mapping_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"CRITICAL: Required file not found at {p}")

    # --- 2. LOAD DATA ---
    stadium_df = pd.read_csv(mapping_path)
    # Assumes col 0 = Stadium, col 1 = City
    stadium_mapping = stadium_df.set_index(stadium_df.columns[0])[stadium_df.columns[1]].to_dict()

    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    # --- 3. CLEAN TEAM NAMES ---
    team_name_mapping = {
        "ir iran": "iran",
        "korea republic": "south korea",
        # Add more mappings here based on data inconsistencies
    }
    
    def clean_names(series):
        return series.str.lower().str.strip().replace(team_name_mapping)

    df1['team1'] = clean_names(df1['team1'])
    df1['team2'] = clean_names(df1['team2'])
    df2['home_team'] = clean_names(df2['home_team'])
    df2['away_team'] = clean_names(df2['away_team'])

    # --- 4. CLEAN DATES & ROUNDS (FILE 1) ---
    # A. Dates
    df1['date_clean'] = pd.to_datetime(df1['date'], dayfirst=True, errors='coerce')
    
    # B. Rounds / Stages
    stage_map = {
        **{f'group {x}': 'group' for x in 'abcdefgh'},
        'round of 16': 'round of 16',
        'quarter-final': 'quarter-final',
        'semi-final': 'semi-final',
        'play-off for third place': 'play-off for third place',
        'final': 'final'
    }
    
    if 'category' in df1.columns:
        df1['round_clean'] = df1['category'].str.lower().str.strip().map(stage_map).fillna('unknown')
    else:
        print("Warning: Column 'category' missing in File 1. Setting round to unknown.")
        df1['round_clean'] = 'unknown'

    # --- 5. CLEAN DATES (FILE 2) ---
    df2['date_clean'] = pd.to_datetime(df2['match_time'], dayfirst=True, errors='coerce')

    # --- 6. CREATE JOIN KEYS ---
    def create_merge_key(row, t1_col, t2_col, date_col):
        # 1. Date: YYYY-MM-DD only
        if pd.isna(row[date_col]):
            d_str = "NAT"
        else:
            d_str = row[date_col].strftime('%Y-%m-%d')
        
        # 2. Teams: Sorted alphabetically to handle Home/Away swaps
        teams = [str(row[t1_col]), str(row[t2_col])]
        teams.sort()
        
        return f"{teams[0]}_{teams[1]}_{d_str}"

    df1['join_key'] = df1.apply(lambda x: create_merge_key(x, 'team1', 'team2', 'date_clean'), axis=1)
    df2['join_key'] = df2.apply(lambda x: create_merge_key(x, 'home_team', 'away_team', 'date_clean'), axis=1)

    # --- 7. MERGE ---
    merged = pd.merge(df1, df2, on='join_key', how='inner', suffixes=('_f1', '_f2'))

    if merged.empty:
        print("Error: Merge resulted in 0 rows. Check team names or date formats.")
        return pd.DataFrame()

    print(f"Successfully merged {len(merged)} rows.")

    # --- 8. FINAL CONSTRUCTION ---
    merged['Home Team Goals'] = pd.to_numeric(merged['number of goals team1'], errors='coerce').fillna(0).astype(int)
    merged['Away Team Goals'] = pd.to_numeric(merged['number of goals team2'], errors='coerce').fillna(0).astype(int)

    final_df = pd.DataFrame({
        'Datetime': merged['date_clean_f2'], 
        'Stage': merged['round_clean'], 
        'City': merged['venue'].map(stadium_mapping).fillna('Unknown'),
        'Home Team Name': merged['team1'],
        'Home Team Goals': merged['Home Team Goals'],
        'Away Team Goals': merged['Away Team Goals'],
        'Away Team Name': merged['team2']
    })

    # Results Calculation
    conditions = [
        (final_df['Home Team Goals'] > final_df['Away Team Goals']),
        (final_df['Home Team Goals'] < final_df['Away Team Goals'])
    ]
    final_df['Home Result'] = np.select(conditions, ['win', 'loss'], default='draw')
    final_df['Away Result'] = final_df['Home Result'].map({'win': 'loss', 'loss': 'win', 'draw': 'draw'})

    return final_df.drop_duplicates()