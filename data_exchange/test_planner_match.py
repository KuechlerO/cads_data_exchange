import pytest
from . import planner_match

scenarios = [
    ("ACADS Patient", False),
    (None, False),
    ("", False),
    ("CADS/Selektivvertrag", True),
    ("Selektivertrag", True),
    ("CADS adsfsdfsf", True),
    ("(CADS) asdfs", True),
]

@pytest.mark.parametrize("sequence, expected", scenarios)
def test_cads_match(sequence, expected):
    assert planner_match.is_cads(sequence) == expected
