# etl_2022.py
import pandas as pd # type: ignore
import numpy as np # type: ignore
import os

# --- 1. GLOBAL CONSTANTS (Configuration) ---
# Moved here so they are accessible for inspection or modification
TEAM_MAPPING = {
    "ir iran": "iran",
    "korea republic": "south korea"
    # Add other mapping if necessary
}

STAGE_MAP = {
    **{f'group {x}': 'group' for x in 'abcdefgh'},
    'round of 16': 'round of 16',
    'quarter-final': 'quarter-final',
    'semi-final': 'semi-final',
    'play-off for third place': 'play-off for third place',
    'final': 'final'
}

# --- 2. HELPER FUNCTIONS (Pure Logic - Unit Testable) ---

def clean_team_name(name) -> str:
    """
    Unit-testable helper to normalize a single team name.
    """
    if pd.isna(name):
        return "unknown"
    
    # Convert to string, lowercase, strip
    clean_val = str(name).lower().strip()
    
    # Replace using the dictionary, or return original if not in dict
    return TEAM_MAPPING.get(clean_val, clean_val)

def create_merge_key(t1: str, t2: str, date_val) -> str:
    """
    Unit-testable helper to generate the join key.
    Logic: Sort team names alphabetically + YYYY-MM-DD date.
    """
    # 1. Handle Date Formatting
    if pd.isna(date_val):
        d_str = "NAT"
    else:
        # Check if it's already a string or a datetime object
        if isinstance(date_val, str):
            d_str = date_val
        else:
            d_str = date_val.strftime('%Y-%m-%d')
            
    # 2. Sort teams (Crucial: 'France' vs 'Peru' == 'Peru' vs 'France')
    teams = [str(t1), str(t2)]
    teams.sort()
    
    return f"{teams[0]}_{teams[1]}_{d_str}"

# --- 3. MAIN PIPELINE (Orchestrator) ---

def get_cleaned_2022_data() -> pd.DataFrame:
    """
    Main ETL orchestrator. 
    Handles File I/O and applies the helper functions.
    """
    print("--- STARTING 2022 ETL PROCESS ---")

    # A. Configuration
    data_base_dir = "./../data" # Adjust this path as needed
    
    file1_path = os.path.join(data_base_dir, "WorldCupMatches2022.csv")
    file2_path = os.path.join(data_base_dir, "WorldCupMatches2022-venue.csv")
    mapping_path = os.path.join(data_base_dir, "stadium_city_mapping2022.csv")

    # Verify files
    for p in [file1_path, file2_path, mapping_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"CRITICAL: Required file not found at {p}")

    # B. Load Data
    stadium_df = pd.read_csv(mapping_path)
    stadium_mapping = stadium_df.set_index(stadium_df.columns[0])[stadium_df.columns[1]].to_dict()

    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    # C. Transform Team Names (Using Helper)
    # We use .apply() here to utilize the unit-testable function
    df1['team1'] = df1['team1'].apply(clean_team_name)
    df1['team2'] = df1['team2'].apply(clean_team_name)
    df2['home_team'] = df2['home_team'].apply(clean_team_name)
    df2['away_team'] = df2['away_team'].apply(clean_team_name)

    # D. Transform Dates (Pandas native is fine here, typically tested via integration)
    df1['date_clean'] = pd.to_datetime(df1['date'], dayfirst=True, errors='coerce')
    df2['date_clean'] = pd.to_datetime(df2['match_time'], dayfirst=True, errors='coerce')

    # E. Transform Rounds
    if 'category' in df1.columns:
        df1['round_clean'] = df1['category'].str.lower().str.strip().map(STAGE_MAP).fillna('unknown')
    else:
        print("Warning: 'category' column missing in File 1")
        df1['round_clean'] = 'unknown'

    # F. Generate Join Keys (Using Helper)
    df1['join_key'] = df1.apply(
        lambda x: create_merge_key(x['team1'], x['team2'], x['date_clean']), axis=1
    )
    df2['join_key'] = df2.apply(
        lambda x: create_merge_key(x['home_team'], x['away_team'], x['date_clean']), axis=1
    )

    # G. Merge
    merged = pd.merge(df1, df2, on='join_key', how='inner', suffixes=('_f1', '_f2'))

    if merged.empty:
        print("Error: Merge resulted in 0 rows.")
        return pd.DataFrame()

    print(f"Successfully merged {len(merged)} rows.")

    # H. Final Construction
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

if __name__ == "__main__":
    # Allow running directly for manual testing
    try:
        df = get_cleaned_2022_data()
        print(df.head())
    except Exception as e:
        print(f"Execution failed: {e}")