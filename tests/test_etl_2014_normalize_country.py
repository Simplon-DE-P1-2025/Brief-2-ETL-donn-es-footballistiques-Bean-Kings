import pandas as pd
from etl.etl_2014 import normalize_country

def test_normalize_country_standard():
    assert normalize_country("France") == "france"

def test_normalize_country_fix_map():
    assert normalize_country("USA") == "united states"

def test_normalize_country_nan():
    assert normalize_country(pd.NA) == "unknown"

def test_normalize_country_empty():
    assert normalize_country("") == "unknown"
