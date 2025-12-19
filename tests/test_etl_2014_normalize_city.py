import pandas as pd
from etl.etl_2014 import city_to_english

def test_city_to_english_known_city():
    assert city_to_english("Rio de Janeiro") == "rio de janeiro"

def test_city_to_english_unknown_city():
    assert city_to_english("SomeUnknownCity") == "someunknowncity"

def test_city_to_english_nan():
    assert city_to_english(pd.NA) == "unknown"
