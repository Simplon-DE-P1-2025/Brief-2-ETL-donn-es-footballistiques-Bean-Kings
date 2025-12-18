import json
import pandas as pd
import numpy as np
from unidecode import unidecode
import os

# --- FONCTIONS UTILITAIRES (HELPERS) ---

def clean_text_field(text):
    """Nettoie le texte : minuscule, sans accent, sans espaces inutiles."""
    if pd.isna(text) or text == "":
        return "unknown"
    return unidecode(str(text)).lower().strip()

def standardize_stage_name(val):
    """Standardise les noms des étapes (Round -> Stage)."""
    val = clean_text_field(val)
    if 'group' in val: return 'group'
    if 'round of 16' in val: return 'round of 16'
    if 'quarter' in val: return 'quarter-final'
    if 'semi' in val: return 'semi-final'
    if 'third' in val: return 'play-off for third place'
    if 'final' in val: return 'final'
    return val

# --- FONCTION PRINCIPALE ETL ---

def get_cleaned_2018_data(json_file_path):
    """
    Extrait, Transforme et Nettoie les données du JSON 2018.
    Retourne un DataFrame Pandas prêt pour l'analyse.
    """
    print(f"Traitement du fichier : {json_file_path}")
    
    # 1. EXTRACTION
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Le fichier {json_file_path} est introuvable.")

    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Création des Lookups (Dictionnaires)
    teams_map = {t['id']: t['name'] for t in data['teams']}
    stadiums_map = {s['id']: s['city'] for s in data['stadiums']}

    all_matches = []

    # Fonction interne pour éviter de répéter le code (Groupes & Knockout)
    def _extract_matches(match_list, round_label):
        for m in match_list:
            all_matches.append({
                'raw_date': m['date'],
                'raw_round': round_label,
                'raw_city': stadiums_map.get(m['stadium']),
                'raw_home_team': teams_map.get(m['home_team']),
                'raw_away_team': teams_map.get(m['away_team']),
                'home_goals': m['home_result'],
                'away_goals': m['away_result']
            })

    # Boucle sur les Groupes
    for k, v in data['groups'].items():
        _extract_matches(v['matches'], v['name'])

    # Boucle sur les Phases Finales
    for k, v in data['knockout'].items():
        _extract_matches(v['matches'], v['name'])

    # Création du DataFrame initial
    df = pd.DataFrame(all_matches)

    # 2. TRANSFORMATION & NETTOYAGE
    
    # Date : ISO 8601
    df['Date'] = pd.to_datetime(df['raw_date'], utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Texte : Villes et Équipes (Clean text)
    df['City'] = df['raw_city'].apply(clean_text_field)
    df['Home Team Name'] = df['raw_home_team'].apply(clean_text_field)
    df['Away Team Name'] = df['raw_away_team'].apply(clean_text_field)

    # Stage : Standardisation
    df['Stage'] = df['raw_round'].apply(standardize_stage_name)

    # Buts : Conversion en Entiers
    df['Home Team Goals'] = df['home_goals'].fillna(0).astype(int)
    df['Away Team Goals'] = df['away_goals'].fillna(0).astype(int)

    # Résultats : Calcul Winner/Loser/Draw
    conditions = [
        (df['Home Team Goals'] > df['Away Team Goals']),
        (df['Home Team Goals'] < df['Away Team Goals']),
        (df['Home Team Goals'] == df['Away Team Goals'])
    ]
    choices_home = ['winner', 'loser', 'draw']
    choices_away = ['loser', 'winner', 'draw']

    df['Home Result'] = np.select(conditions, choices_home, default='draw')
    df['Away Result'] = np.select(conditions, choices_away, default='draw')

    # 3. SÉLECTION FINALE
    final_cols = [
        'Date', 
        'Stage', 
        'City', 
        'Home Team Name', 
        'Away Team Name', 
        'Home Team Goals', 
        'Away Team Goals', 
        'Home Result', 
        'Away Result'
    ]
    
    df_final = df[final_cols]
    
    print(f"✅ Succès : {len(df_final)} matchs traités.")
    return df_final

# --- TEST DU SCRIPT (S'exécute seulement si on lance ce fichier directement) ---
if __name__ == "__main__":
    # Chemin vers ton fichier (à adapter si besoin)
    path = 'data/data_2018.json'
    
    try:
        df_2018 = get_cleaned_2018_data(path)
        print("\n--- Aperçu du résultat ---")
        print(df_2018.head())
        print("\n--- Types des colonnes ---")
        print(df_2018.dtypes)
    except Exception as e:
        print(f"❌ Erreur : {e}")