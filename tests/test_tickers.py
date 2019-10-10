import analyze.tickers as tr
import pytest


@pytest.fixture(scope='module')
def get_tickers():
    return {"F", "FF", "FFF"}


@pytest.mark.parametrize("regex_match,expected",
                         [("F is great", "F"),
                          (" F", "F"),
                          (" $F", "F"),
                          ("F", "F"),
                          ("a F", "F"),
                          ("F a", "F"),
                          ("a F a", "F"),
                          ("F ", "F"),
                          (" F", "F"),
                          (" F ", "F"),
                          ("\nF", "F"),
                          ("F\n", "F"),
                          ("\nF\n", "F"),
                          ("FF", "FF"),
                          ("a FFF", "FFF"),
                          ("a FFF a", "FFF"),
                          (" FFF ", "FFF"),
                          ("FFF ", "FFF"),
                          ("$FFF ", "FFF"),
                          ("FFF\n", "FFF"),
                          ("FFF", "FFF")])
def test_find_tickers_regex_match(get_tickers, regex_match, expected):
    content = {'id_0': regex_match}
    expected = {'id_0': {expected}}
    assert tr.find_tickers(get_tickers, content) == expected


@pytest.mark.parametrize("regex_no_match",
                         ["aF",
                          "Fa",
                          "FFa",
                          "aFF",
                          "FaF",
                          "aFaF",
                          "aFFaFF",
                          "$Fa",
                          "F$a",
                          "$aF",
                          "F$FaFFa$$Fa"])
def test_find_tickers_no_regex_match(get_tickers, regex_no_match):
    content = {'id_0': regex_no_match}
    expected = {'id_0': {'UNKNOWN'}}
    assert tr.find_tickers(get_tickers, content) == expected
