import pytest
from etl.etl_2014 import normalize_stage

@pytest.mark.parametrize(
    "input_stage,expected",
    [
        ("Group A", "group"),
        ("group_b", "group"),
        ("ROUND16", "round of 16"),
        ("Quarter-Finals", "quarter finals"),
        ("Semi-Final", "semi final"),
        ("Final", "final"),
    ]
)
def test_normalize_stage_known(input_stage, expected):
    assert normalize_stage(input_stage) == expected


def test_normalize_stage_unknown():
    assert normalize_stage("CrazyStage") == "crazystage"

def test_normalize_stage_nan():
    assert normalize_stage(None) == "unknown"
