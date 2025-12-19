import pytest # type: ignore
import pandas as pd # type: ignore
from datetime import datetime
from etl.etl_2022 import create_merge_key

def test_merge_key_sorting():
    """
    CRITIQUE : Vérifie que l'ordre des équipes n'importe pas.
    'France vs Peru' doit donner la même clé que 'Peru vs France'.
    """
    date_str = "2022-12-18"
    key1 = create_merge_key("france", "peru", date_str)
    key2 = create_merge_key("peru", "france", date_str)
    
    assert key1 == "france_peru_2022-12-18"
    assert key1 == key2

def test_merge_key_dates():
    """Vérifie le support des objets datetime et des strings."""
    dt_obj = datetime(2022, 1, 1)
    key = create_merge_key("A", "B", dt_obj)
    assert "2022-01-01" in key

def test_merge_key_nat():
    """Vérifie le comportement si la date est manquante."""
    key = create_merge_key("A", "B", pd.NaT)
    assert key.endswith("NAT")