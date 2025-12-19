import pandas as pd
from unittest.mock import patch
from etl.etl_2014 import get_cleaned_2014_data

@patch("etl.etl_2014.pd.read_csv")
def test_get_cleaned_2014_data_minimal(mock_read_csv):
    mock_read_csv.return_value = pd.DataFrame({
    "Year": [2014],
    "Stadium": ["Maracana"],
    "Attendance": [70000],
    "Half-time Home Goals": [1],
    "Half-time Away Goals": [0],
    "Referee": ["X"],
    "Assistant 1": ["Y"],
    "Assistant 2": ["Z"],
    "RoundID": [1],
    "MatchID": [1],
    "Home Team Initials": ["BRA"],
    "Away Team Initials": ["CRO"],

    "Datetime": ["2014-06-12"],
    "Home Team Goals": ["3"],
    "Away Team Goals": ["1"],
    "Win conditions": ["4-3"],
    "Stage": ["Group A"],
    "City": ["Rio de Janeiro"],
    "Home Team Name": ["Brazil"],
    "Away Team Name": ["Croatia"],
})


    df = get_cleaned_2014_data()

    assert len(df) == 1
    assert df["Home Result"].iloc[0] == "winner"
    assert df["Away Result"].iloc[0] == "loser"
    assert df["Stage"].iloc[0] == "group"
    assert df["City"].iloc[0] == "rio de janeiro"
