import pandas as pd
from unidecode import unidecode
import logging
import re
from etl_1930_2010 import load_and_clean_data

# =========================
# CONFIG
# =========================
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    filename="etl_worldcup.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =========================
# 0️⃣ UTILISER LE DATAFRAME NETTOYÉ DU PREMIER SCRIPT
# Ici df_clean est le DataFrame final du premier script
# =========================
df_etl = load_and_clean_data()
  # df_clean vient du premier script ETL

# =========================
# UTILS
# =========================
def normalize_text(val, field_name="value"):
    if pd.isna(val) or str(val).strip() == "":
        logging.warning(f"{field_name} missing → replaced by 'unknown'")
        return "unknown"
    val = unidecode(str(val)).lower().strip()
    val = re.sub(r"[^a-z0-9\s]", "", val)
    val = re.sub(r"\s+", " ", val)
    return val

def normalize_round(val):
    if pd.isna(val):
        return None
    val = normalize_text(val, "round")
    key = val.replace(" ", "")
    ROUND_MAPPING = {
        "groupstage": "group",
        "group": "group",
        "roundof16": "round of 16",
        "round16": "round of 16",
        "quarterfinal": "quarter-final",
        "quarterfinals": "quarter-final",
        "semifinal": "semi-final",
        "semifinals": "semi-final",
        "thirdplacematch": "play-off for third place",
        "finalround": "final-round",
        "final": "final"
    }
    return ROUND_MAPPING.get(key, val)

def normalize_city(val):
    if pd.isna(val) or str(val).strip() == "":
        logging.warning("City missing → replaced by 'unknown'")
        return "unknown"
    val = unidecode(str(val)).lower().strip()
    val = val.replace(".", "").replace("_", " ")
    val = re.sub(r"[^a-z\s-]", "", val)
    val = re.sub(r"\s+", " ", val)
    return val

def compute_home_result(row):
    if pd.isna(row["Home Team Goals"]) or pd.isna(row["Away Team Goals"]):
        return None
    if row["Home Team Goals"] > row["Away Team Goals"]:
        return "winner"
    if row["Home Team Goals"] < row["Away Team Goals"]:
        return "loser"
    return "draw"

def compute_away_result(row):
    if pd.isna(row["Home Team Goals"]) or pd.isna(row["Away Team Goals"]):
        return None
    if row["Away Team Goals"] > row["Home Team Goals"]:
        return "winner"
    if row["Away Team Goals"] < row["Home Team Goals"]:
        return "loser"
    return "draw"

def get_cleaned_1930_data():
    # =========================
    # 1️⃣ DATETIME
    # =========================
    df_etl["Match Date"] = pd.to_datetime(df_etl["Match Date"], errors="coerce")
    df_etl["Match Time"] = pd.to_datetime(df_etl["Match Time"], errors="coerce").dt.time
    df_etl["Datetime"] = pd.to_datetime(
        df_etl["Match Date"].astype(str) + " " + df_etl["Match Time"].astype(str),
        errors="coerce"
    )

    # =========================
    # 2️⃣ NORMALISATION ROUND / STAGE
    # =========================
    df_etl["round"] = df_etl["round"].apply(normalize_round)

    # =========================
    # 3️⃣ NORMALISATION CITY
    # =========================
    df_etl["venue"] = df_etl["venue"].apply(normalize_city)

    # =========================
    # 4️⃣ NORMALISATION TEAM NAMES
    # =========================
    df_etl["team1"] = df_etl["team1"].apply(lambda x: normalize_text(x, "home team"))
    df_etl["team2"] = df_etl["team2"].apply(lambda x: normalize_text(x, "away team"))

    # =========================
    # 5️⃣ GOALS (robuste)
    # =========================
    if "Home Team Goals" not in df_etl.columns or "Away Team Goals" not in df_etl.columns:
        if "score" not in df_etl.columns:
            raise ValueError("❌ Impossible de calculer les buts : colonne 'score' absente")
        goals = df_etl["score"].astype(str).str.extract(r"(\d+)\s*[-–]\s*(\d+)")
        df_etl["Home Team Goals"] = goals[0].astype("Int64")
        df_etl["Away Team Goals"] = goals[1].astype("Int64")

    # =========================
    # 6️⃣ CALCUL DES RESULTATS
    # =========================
    df_etl["Home Result"] = df_etl.apply(compute_home_result, axis=1)
    df_etl["Away Result"] = df_etl.apply(compute_away_result, axis=1)

    # =========================
    # 7️⃣ SELECTION COLONNES FINALES
    # =========================
    FINAL_COLUMNS = {
        "Datetime": "Datetime",
        "round": "Stage",
        "venue": "City",
        "team1": "Home Team Name",
        "Home Team Goals": "Home Team Goals",
        "Away Team Goals": "Away Team Goals",
        "team2": "Away Team Name",
        "Home Result": "Home Result",
        "Away Result": "Away Result"
    }

    df_final = df_etl[list(FINAL_COLUMNS.keys())].rename(columns=FINAL_COLUMNS)
    return df_final

print(get_cleaned_1930_data())
  
   
