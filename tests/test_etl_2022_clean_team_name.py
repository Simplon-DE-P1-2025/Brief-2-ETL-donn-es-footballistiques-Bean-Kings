import pytest # type: ignore
import numpy as np # type: ignore
from etl.etl_2022 import clean_team_name

# Configuration du chemin pour trouver etl_2022.py
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.mark.parametrize("input_name, expected_output", [
    ("France", "france"),             # Cas standard
    ("  BRAZIL  ", "brazil"),         # Espaces inutiles
    ("IR Iran", "iran"),              # Mapping dictionnaire
    ("Korea Republic", "south korea"),# Mapping dictionnaire
    (np.nan, "unknown"),              # Gestion NaN
    (None, "unknown")                 # Gestion None
])
def test_clean_team_name(input_name, expected_output):
    """Vérifie que le nettoyage et le mapping fonctionnent correctement."""
    assert clean_team_name(input_name) == expected_output