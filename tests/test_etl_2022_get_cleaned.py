import pytest # type: ignore
import pandas as pd # type: ignore
from unittest.mock import patch
# Assure-toi que ton dossier racine est bien dans le PYTHONPATH ou que tu lances pytest depuis la racine
from etl.etl_2022 import get_cleaned_2022_data

@pytest.fixture
def mock_csv_data():
    """Crée des faux DataFrames pour simuler les fichiers CSV."""
    df_matches = pd.DataFrame({
        'date': ['18 DEC 2022'],
        'team1': ['ARGENTINA'], 'team2': ['FRANCE'],
        'number of goals team1': [3], 'number of goals team2': [3],
        'category': ['Final']
    })
    
    df_venues = pd.DataFrame({
        'match_time': ['18/12/2022 18:00'],
        'home_team': ['Argentina'], 'away_team': ['France'],
        'venue': ['Lusail Iconic Stadium'] 
    })
    
    df_mapping = pd.DataFrame({
        'stadium_name': ['Lusail Iconic Stadium'], 'city': ['lusail']
    })
    
    return df_matches, df_venues, df_mapping

@patch('etl.etl_2022.os.path.exists')
@patch('etl.etl_2022.pd.read_csv')
def test_get_cleaned_2022_data(mock_read_csv, mock_exists, mock_csv_data):
    """Teste le pipeline complet sans toucher au disque dur."""
    # 1. Setup Mocks
    mock_exists.return_value = True
    df_matches, df_venues, df_mapping = mock_csv_data
    
    def side_effect(filepath, *args, **kwargs):
        path_str = str(filepath)
        # On utilise 'in' pour matcher même si le chemin est absolu/relatif
        if "WorldCupMatches2022.csv" in path_str: return df_matches
        if "WorldCupMatches2022-venue.csv" in path_str: return df_venues
        if "stadium_city_mapping2022.csv" in path_str: return df_mapping
        return pd.DataFrame()
    
    mock_read_csv.side_effect = side_effect

    # 2. Exécution
    result = get_cleaned_2022_data()

    # 3. Validations
    assert len(result) == 1
    # Vérifie que le mapping a fonctionné (si mismatch nom stade -> Unknown)
    assert result.iloc[0]['City'] == 'lusail'       
    assert result.iloc[0]['Home Result'] == 'draw'