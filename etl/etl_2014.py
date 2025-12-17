import duckdb
import etl.etl_inserter_2014 as inserter
import pandas as pd
from unidecode import unidecode
import geonamescache
import logging

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

gc = geonamescache.GeonamesCache()
df = pd.read_csv('./data/WorldCupMatches2014.csv', sep=";", encoding='iso-8859-1') 
logger = logging.getLogger("ETL")

cities =  gc.get_cities()
countries = gc.get_countries()

STAGE_MAP = {
    "group a": "group",
    "group b": "group",
    "group c": "group",
    "group d": "group",
    "group e": "group",
    "group f": "group",
    "group g": "group",
    "group h": "group",
    "round16": "round of 16",
    "round of 16": "round of 16",
    "quarter-finals": "quarter-final",
    "quarter-final": "quarter-final",
    "semi-finals": "semi-final",
    "semi-final": "semi-final",
    "final": "final",
    "play-off for third place": "play-off for third place",
}

COUNTRY_FIX_MAP = {
    'rn">bosnia and herzegovina': "bosnia and herzegovina",
    "ci? 1/2te d'ivoire": "ivory coast",
    "ir iran": "iran",
    "korea republic": "south korea",
    "usa": "united states",
    "china pr": "china"
}

def clean_text(value, default="Unknown"):
    if pd.isna(value) or str(value).strip() == "":
        logger.warning("Valeur manquante remplacée par %s", default)
        return default

    value = str(value).strip().lower()
    value = unidecode(value)
    return value

def city_to_english(city):
    if pd.isna(city) or str(city).strip() == "":
        logger.warning("Ville manquante")
        return "unknown"

    city_clean = unidecode(str(city)).lower().strip()

    for c in cities.values():
        c_name = unidecode(c["name"]).lower().strip()
        if c_name == city_clean:
            return c_name

    return city_clean 

def normalize_stage(stage):
    if pd.isna(stage):
        return "unknown"

    key = unidecode(stage).lower().strip()
    key = key.replace("_", " ").replace("-", " ")

    return STAGE_MAP.get(key, key)

def normalize_country(name):
    if pd.isna(name) or str(name).strip() == "":
        logger.warning("Pays manquant → unknown")
        return "unknown"

    key = unidecode(str(name)).lower().strip()

    if key in COUNTRY_FIX_MAP:
        return COUNTRY_FIX_MAP[key]

    logger.warning("Pays hors référentiel officiel : %s", name)
    return key

print(df.describe(include='all'))

df = df.drop(columns=["Year", "Stadium", "Attendance", "Half-time Home Goals", "Half-time Away Goals", "Referee", "Assistant 1", "Assistant 2","RoundID"  ,  "MatchID","Home Team Initials", "Away Team Initials"])

df["Datetime"] = pd.to_datetime(
    df["Datetime"],
    errors="coerce")

df["Home Team Goals"] = pd.to_numeric(
    df["Home Team Goals"],
    errors="coerce"
)
df["Away Team Goals"] = pd.to_numeric(
    df["Away Team Goals"],
    errors="coerce"
)

df = df.drop_duplicates(df)

df["Win conditions"] = df["Win conditions"].str.strip().str.replace(" ", "")
df["Win conditions"] = df["Win conditions"].str.extract(r"(\d+-\d+)")
df[["Score home", "Score away"]] = df["Win conditions"].str.split("-", expand=True).astype("Int64") 
df_win = df[
    df["Win conditions"]
      .notna()
    & df["Win conditions"].str.strip().ne("")
]

df["Home result"] = "draw"
df["Away result"] = "draw"

home_win = df["Home Team Goals"] > df["Away Team Goals"]
away_win = df["Away Team Goals"] > df["Home Team Goals"]

df.loc[home_win, ["Home result", "Away result"]] = ["winner", "loser"]
df.loc[away_win, ["Home result", "Away result"]] = ["loser", "winner"]

draw = df["Home Team Goals"] == df["Away Team Goals"]

df.loc[draw & (df["Score home"] > df["Score away"]),
       ["Home result", "Away result"]] = ["winner", "loser"]

df.loc[draw & (df["Score away"] > df["Score home"]),
       ["Home result", "Away result"]] = ["loser", "winner"]
df = df.drop(columns=["Win conditions", "Score home", "Score away"])

df["Stage"] = df["Stage"].apply(normalize_stage)

df["City"] = df["City"].apply(city_to_english)

df["Home Team Name"] = df["Home Team Name"].apply(normalize_country)
df["Away Team Name"] = df["Away Team Name"].apply(normalize_country)

print(df)
inserter.load_matches(df, db_path="./db/db.duckdb")