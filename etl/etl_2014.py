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