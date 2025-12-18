import pandas as pd
from unidecode import unidecode

# =========================
# Fichiers
# =========================
input_file = "../data/WorldCupMatches1930-2010.csv"
datetime_file = "../data/WorldCupMatches1930-2022-datetime.csv"

# =========================
# 1️⃣ Charger les CSV (UNE SEULE FOIS)
# =========================
df = pd.read_csv(input_file)
df_datetime = pd.read_csv(datetime_file, encoding="latin1")

# =========================
# 2️⃣ Nettoyage préalable
# =========================
df = df[
    (~df["round"].str.contains("PRELIMINARY", case=False, na=False)) &
    (~df["edition"].astype(str).str.contains("2014", na=False))
]

# =========================
# 3️⃣ Normalisation de 'round'
# =========================
df["round"] = df["round"].astype(str).str.strip().str.upper()

df.loc[df["round"].str.contains("GROUP_STAGE"), "round"] = "group stage"
df.loc[df["round"].str.contains("1/8|FIRST"), "round"] = "round of 16"
df.loc[df["round"].str.contains("1/4"), "round"] = "quarter-finals"
df.loc[df["round"].str.contains("1/2"), "round"] = "semi-finals"
df.loc[df["round"].str.contains("PLACES_3"), "round"] = "third-place match"
df.loc[df["round"].str.contains("QUARTERFINAL_STAGE|SEMIFINAL_STAGE"), "round"] = "second group stage"
df.loc[df["round"].str.contains("FINAL_ROUND"), "round"] = "final round"
df.loc[df["round"].str.contains("FINAL"), "round"] = "final"

# =========================
# 4️⃣ Nettoyage team1 / team2
# =========================
for col in ["team1", "team2"]:
    df[col] = (
        df[col]
        .astype(str)
        .str.replace(r"\s*\(.*\)", "", regex=True)
        .str.strip()
    )

# Normalisations pays
replacements = {
    r"(?i)^usa$": "United States",
    r"(?i)^frg$": "West Germany",
    r"(?i)^gdr$": "East Germany",
    r"(?i)^serbia-montenegro$": "Serbia and Montenegro",
    r"(?i)^ireland$": "Republic of Ireland",
}

for pattern, value in replacements.items():
    df["team1"] = df["team1"].replace(regex={pattern: value})
    df["team2"] = df["team2"].replace(regex={pattern: value})

# =========================
# 5️⃣ Année
# =========================
df["year"] = pd.to_datetime(df["edition"], errors="coerce").dt.year

# Correction Slovakia → Slovenia uniquement en 2002
df.loc[
    (df["team2"].str.lower() == "slovakia") & (df["year"] == 2002),
    "team2"
] = "Slovenia"

# =========================
# 6️⃣ Colonne Replay
# =========================
df["Replay"] = "0"
duplicates = df.duplicated(subset=["edition", "round", "team1", "team2"], keep="first")
df.loc[duplicates, "Replay"] = "1"

# =========================
# 7️⃣ Préparer df_datetime
# =========================
df["_year_join"] = df["edition"].astype(str).str.extract(r"(\d{4})")
df_datetime["_year_join"] = df_datetime["Tournament Id"].astype(str).str.extract(r"(\d{4})")

df_datetime = df_datetime.rename(columns={
    "Stage Name": "round",
    "Home Team Name": "team1",
    "Away Team Name": "team2"
})

for col in ["round", "team1", "team2"]:
    df[col] = df[col].astype(str).str.strip()
    df_datetime[col] = df_datetime[col].astype(str).str.strip()

df_datetime["Replay"] = (
    pd.to_numeric(df_datetime.get("Replay", 0), errors="coerce")
    .fillna(0)
    .astype(int)
    .astype(str)
)

# =========================
# 8️⃣ Récupération Match Date / Time
# =========================
def get_match_date(row, df_dt):
    filt = (
        (df_dt["_year_join"] == row["_year_join"]) &
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
    return None, None

df["Match Date"], df["Match Time"] = zip(
    *df.apply(lambda r: get_match_date(r, df_datetime), axis=1)
)

df.drop(columns="_year_join", inplace=True)

# =========================
# 9️⃣ Corrections manuelles 1994
# =========================
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

# =========================
# 🔟 Normalisation venue
# =========================
def city_to_english(city):
    if pd.isna(city) or str(city).strip() == "":
        return "unknown"
    return unidecode(str(city)).lower().strip().replace(".", "").replace("_", " ")

df["venue"] = df["venue"].apply(city_to_english)

# =========================
# ✅ DATAFRAME FINAL PRÊT
# =========================

print(df.head(50))

def load_and_clean_data():
    # ... tout ton code actuel pour créer df ...
    print("✅ Pipeline terminé — DataFrame prêt")
    return df
print(load_and_clean_data())
# 🔴 Export final OPTIONNEL
# df.to_csv("data/WorldCupMatches1930-2010_clean.csv", index=False, encoding="utf-8")





