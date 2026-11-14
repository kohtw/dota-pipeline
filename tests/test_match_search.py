"""Check match IDs without depending on a warehouse or Streamlit session."""

from importlib import import_module

import pytest


@pytest.fixture
def parse_match_id(dashboard_database):
    """Use the dashboard's existing import path for the search module."""
    return import_module("match_search").parse_match_id


@pytest.mark.parametrize(("text", "expected"), [
    ("9079738443", 9079738443),
    (" \t9079738443\n", 9079738443),
    ("0009079738443", 9079738443),
    ("1", 1),
    (str(2**63 - 1), 2**63 - 1),
])
def test_valid_match_ids_preserve_integer_precision(parse_match_id, text, expected):
    assert parse_match_id(text) == expected


@pytest.mark.parametrize(("text", "message"), [
    ("", "Enter a match ID"),
    (" \t ", "Enter a match ID"),
    ("abc", "digits only"),
    ("9079.0", "digits only"),
    ("-1", "digits only"),
    ("+1", "digits only"),
    ("1e10", "digits only"),
    ("9079 738443", "digits only"),
    ("１２３", "digits only"),
    ("https://example.com/matches/9079738443", "digits only"),
    ("0", "positive match ID"),
    (str(2**63), "supported range"),
    ("9" * 5000, "supported range"),
])
def test_invalid_match_ids_report_validation_errors(parse_match_id, text, message):
    with pytest.raises(ValueError, match=message):
        parse_match_id(text)
