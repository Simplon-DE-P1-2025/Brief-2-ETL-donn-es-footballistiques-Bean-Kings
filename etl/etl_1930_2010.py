import pandas as pd
from unidecode import unidecode

# =========================
# CONFIGURATION
# =========================
INPUT_FILE = "./../data/WorldCupMatches1930-2010.csv"
DATETIME_FILE = "./../data/WorldCupMatches1930-2022-datetime.csv"

# =========================
# FONCTIONS UTILITAIRES
# =========================
def normalize_round(round_str):
    """Normalise les noms de rounds pour matcher entre les deux DataFrames"""
    if pd.isna(round_str):
        return ""
    
    r = str(round_str).strip().upper()
    
    if "GROUP" in r and "STAGE" in r:
        return "group stage"
    elif "1/8" in r or "FIRST" in r or "ROUND OF 16" in r:
        return "round of 16"
    elif "1/4" in r or "QUARTER" in r:
        return "quarter-finals"
    elif "1/2" in r or "SEMI" in r:
        return "semi-finals"
    elif "PLACES_3" in r or "3RD" in r or "THIRD" in r:
        return "third-place match"
    elif "QUARTERFINAL_STAGE" in r or "SEMIFINAL_STAGE" in r:
        return "second group stage"
    elif "FINAL_ROUND" in r:
        return "final round"
    elif r == "FINAL":
        return "final"
    
    return r.lower()

def normalize_team(team_str):
    """Normalise les noms d'équipes"""
    if pd.isna(team_str):
        return ""
    
    # Enlever les parenthèses
    team = str(team_str).strip()
    team = team.split("(")[0].strip()
    
    # Remplacements spécifiques
    replacements = {
        "USA": "United States",
        "FRG": "West Germany",
        "GDR": "East Germany",
        "Serbia-Montenegro": "Serbia and Montenegro",
        "Ireland": "Republic of Ireland",
    }
    
    for old, new in replacements.items():
        if team.upper() == old.upper():
            return new
    
    return team

def city_to_english(city):
    """Normalise les noms de villes"""
    if pd.isna(city) or str(city).strip() == "":
        return "unknown"
    return unidecode(str(city)).lower().strip().replace(".", "").replace("_", " ")

def get_match_date(row, df_dt):
    """Trouve la date/heure d'un match avec plusieurs niveaux de fallback"""
    
    # Niveau 1 : Match exact (année + round + équipes + replay)
    filt = (
        (df_dt["_year"] == row["_year"]) &
        (df_dt["round"] == row["round"]) &
        (df_dt["Replay"] == row["Replay"]) &
        (
            ((df_dt["team1"] == row["team1"]) & (df_dt["team2"] == row["team2"])) |
            ((df_dt["team1"] == row["team2"]) & (df_dt["team2"] == row["team1"]))
        )
    )
    match = df_dt[filt]
    if not match.empty:
        return match.iloc[0]["Match Date"], match.iloc[0]["Match Time"]
    
    # Niveau 2 : Sans vérifier Replay
    filt = (
        (df_dt["_year"] == row["_year"]) &
        (df_dt["round"] == row["round"]) &
        (
            ((df_dt["team1"] == row["team1"]) & (df_dt["team2"] == row["team2"])) |
            ((df_dt["team1"] == row["team2"]) & (df_dt["team2"] == row["team1"]))
        )
    )
    match = df_dt[filt]
    if not match.empty:
        if len(match) > 1:
            replay_match = match[match["Replay"] == row["Replay"]]
            if not replay_match.empty:
                match = replay_match
        return match.iloc[0]["Match Date"], match.iloc[0]["Match Time"]
    
    # Niveau 3 : Juste année + équipes
    filt = (
        (df_dt["_year"] == row["_year"]) &
        (
            ((df_dt["team1"] == row["team1"]) & (df_dt["team2"] == row["team2"])) |
            ((df_dt["team1"] == row["team2"]) & (df_dt["team2"] == row["team1"]))
        )
    )
    match = df_dt[filt]
    if not match.empty:
        return match.iloc[0]["Match Date"], match.iloc[0]["Match Time"]
    
    return None, None

def create_datetime(row):
    """Combine Match Date et Match Time en datetime"""
    try:
        if pd.isna(row['Match Date']) or pd.isna(row['Match Time']):
            return pd.NaT
        
        date_str = str(row['Match Date']).strip()
        time_str = str(row['Match Time']).strip()
        
        # Si les valeurs sont 'None' ou vides en string
        if date_str.lower() == 'none' or time_str.lower() == 'none':
            return pd.NaT
        if date_str == '' or time_str == '':
            return pd.NaT
        
        datetime_str = f"{date_str} {time_str}"
        
        # Essayer plusieurs formats de date
        for fmt in ['%m/%d/%Y %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S']:
            try:
                return pd.to_datetime(datetime_str, format=fmt)
            except:
                continue
        
        # Fallback sur parsing automatique
        return pd.to_datetime(datetime_str, errors='coerce')
    except Exception as e:
        return pd.NaT

# =========================
# PIPELINE PRINCIPAL
# =========================
def load_and_clean_data():
    """
    Charge et nettoie les données de 1930 à 2010.
    Retourne un DataFrame avec la colonne Datetime.
    """
    print("📥 Chargement des données 1930-2010...")
    
    # 1️⃣ Charger les CSV
    df = pd.read_csv(INPUT_FILE)
    df_datetime = pd.read_csv(DATETIME_FILE, encoding="latin1")
    
    # 2️⃣ Nettoyage préalable
    df = df[
        (~df["round"].str.contains("PRELIMINARY", case=False, na=False)) &
        (~df["edition"].astype(str).str.contains("2014", na=False))
    ].copy()
    
    # 3️⃣ Normaliser round
    df["round"] = df["round"].apply(normalize_round)
    
    # 4️⃣ Normaliser équipes
    df["team1"] = df["team1"].apply(normalize_team)
    df["team2"] = df["team2"].apply(normalize_team)
    
    # 5️⃣ Extraire année
    df["year"] = (
        df["edition"]
        .astype(str)
        .str.extract(r"(\d{4})", expand=False)
        .astype("Int64")
    )
    
    # Correction Slovakia → Slovenia en 2002
    df.loc[
        (df["team2"].str.lower() == "slovakia") & (df["year"] == 2002),
        "team2"
    ] = "Slovenia"
    
    # 6️⃣ Colonne Replay
    df["Replay"] = 0
    duplicates = df.duplicated(subset=["edition", "round", "team1", "team2"], keep="first")
    df.loc[duplicates, "Replay"] = 1
    
    # 7️⃣ Préparer df_datetime
    df_datetime = df_datetime.copy()
    df_datetime = df_datetime.rename(columns={
        "Stage Name": "round",
        "Home Team Name": "team1",
        "Away Team Name": "team2"
    })
    
    df_datetime["round"] = df_datetime["round"].apply(normalize_round)
    df_datetime["team1"] = df_datetime["team1"].apply(normalize_team)
    df_datetime["team2"] = df_datetime["team2"].apply(normalize_team)
    
    df["_year"] = df["edition"].astype(str).str.extract(r"(\d{4})", expand=False)
    df_datetime["_year"] = df_datetime["Tournament Id"].astype(str).str.extract(r"(\d{4})", expand=False)
    
    df_datetime["Replay"] = (
        pd.to_numeric(df_datetime.get("Replay", 0), errors="coerce")
        .fillna(0)
        .astype(int)
    )
    
    # 8️⃣ Récupérer dates et heures
    print("🔄 Récupération des dates et heures...")
    df[["Match Date", "Match Time"]] = df.apply(
        lambda r: pd.Series(get_match_date(r, df_datetime)), axis=1
    )
    
    # 🐛 DEBUG : Vérifier ce qu'on a récupéré
    print(f"\n🔍 DEBUG - Échantillon de dates récupérées:")
    sample = df[df['Match Date'].notna()].head(3)
    for idx, row in sample.iterrows():
        print(f"  Match Date: '{row['Match Date']}' | Match Time: '{row['Match Time']}'")
    
    missing_dates = df['Match Date'].isna().sum()
    print(f"\n⚠️  {missing_dates} matches sans date après récupération")
    
    # 9️⃣ Corrections manuelles matches spécifiques
    specific_matches = [
        ("Hardturm Stadium", "West Germany", "Turkey"),
        ("St. Jakob Stadium", "Switzerland", "Italy"),
        ("Malmö Stadion", "Northern Ireland", "Czechoslovakia"),
    ]
    
    for stadium, team1_name, team2_name in specific_matches:
        match_row = df_datetime[
            (df_datetime["Stadium Name"] == stadium) &
            (((df_datetime["team1"] == team1_name) & (df_datetime["team2"] == team2_name)) |
             ((df_datetime["team1"] == team2_name) & (df_datetime["team2"] == team1_name)))
        ]
        if not match_row.empty:
            match_date = match_row.iloc[0]['Match Date']
            match_time = match_row.iloc[0]['Match Time']
            
            mask = (
                (df['venue'].str.contains(stadium.split()[0], case=False, na=False)) &
                (((df['team1'] == team1_name) & (df['team2'] == team2_name)) |
                 ((df['team1'] == team2_name) & (df['team2'] == team1_name)))
            )
            df.loc[mask, 'Replay'] = 1
            df.loc[mask, 'Match Date'] = match_date
            df.loc[mask, 'Match Time'] = match_time
    
    # 🔟 Corrections manuelles 1994
    matches_1994 = [
        ("Norway", "Mexico", "06/19/1994", "13:00:00"),
        ("Netherlands", "Saudi Arabia", "06/20/1994", "16:30:00"),
        ("Italy", "Mexico", "06/28/1994", "17:30:00"),
        ("Belgium", "Saudi Arabia", "06/29/1994", "17:30:00"),
        ("Spain", "Switzerland", "06/18/1994", "13:00:00"),
    ]
    
    for team1, team2, d, t in matches_1994:
        mask = (
            (df["edition"] == "1994-USA") &
            (df["team1"] == team1) &
            (df["team2"] == team2)
        )
        df.loc[mask, ["Match Date", "Match Time"]] = [d, t]
    
    # 1️⃣1️⃣ Normaliser venue
    df["venue"] = df["venue"].apply(city_to_english)
    
    # 1️⃣2️⃣ Créer colonne Datetime
    print("🔄 Création de la colonne Datetime...")
    df['Datetime'] = df.apply(create_datetime, axis=1)
    
    # 🐛 DEBUG : Vérifier le parsing
    print(f"\n🔍 DEBUG - Échantillon de Datetime créées:")
    sample_dt = df[df['Datetime'].notna()].head(3)
    for idx, row in sample_dt.iterrows():
        print(f"  {row['team1']} vs {row['team2']}: {row['Datetime']}")
    
    failed_parsing = df[df['Match Date'].notna() & df['Datetime'].isna()]
    if len(failed_parsing) > 0:
        print(f"\n⚠️  {len(failed_parsing)} dates n'ont PAS pu être parsées!")
        print("Exemple de format problématique:")
        for idx, row in failed_parsing.head(2).iterrows():
            print(f"  Date: '{row['Match Date']}' | Time: '{row['Match Time']}'")
    
    
    # 1️⃣3️⃣ Nettoyage final
    df.drop(columns="_year", inplace=True, errors='ignore')
    
    # ✅ Statistiques
    print("\n" + "="*50)
    print("📊 STATISTIQUES 1930-2010")
    print("="*50)
    print(f"✅ Total matches: {len(df)}")
    print(f"✅ Matches avec Datetime: {df['Datetime'].notna().sum()}")
    print(f"❌ Matches sans Datetime: {df['Datetime'].isna().sum()}")
    print(f"📈 Taux de couverture: {df['Datetime'].notna().sum() / len(df) * 100:.1f}%")
    
    if df['Datetime'].isna().sum() > 0:
        print("\n🔍 Matches manquants par édition:")
        missing = df[df['Datetime'].isna()].groupby('edition').size()
        print(missing)
    
    print("✅ Pipeline 1930-2010 terminé\n")
    
    return df

# =========================
# FONCTION D'EXPORT PRINCIPALE
# =========================
def get_cleaned_1930_data():
    """
    Fonction à appeler depuis main.py
    """
    return load_and_clean_data()

# =========================
# EXÉCUTION DIRECTE (pour tests)
# =========================
if __name__ == "__main__":
    df = load_and_clean_data()
    
    # Export optionnel
    # df.to_csv("data/WorldCupMatches1930-2010_clean.csv", index=False, encoding="utf-8")