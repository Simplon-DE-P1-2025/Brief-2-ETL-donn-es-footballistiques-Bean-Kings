import pandas as pd
from etl.etl_2014 import clean_text

def test_clean_text_normal_case():
    assert clean_text("São Paulo") == "sao paulo"

def test_clean_text_strips_and_lowercases():
    assert clean_text("  France ") == "france"

def test_clean_text_nan_returns_default():
    assert clean_text(pd.NA) == "Unknown"

def test_clean_text_empty_string_returns_default():
    assert clean_text("") == "Unknown"
