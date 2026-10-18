from fetch_technical import canonicalize_yahoo_fetch_candidates, is_probably_non_equity_ticker, normalize_yahoo_ticker


def test_normalize_yahoo_ticker_for_nasdaq_class_shares_and_preferreds():
    assert normalize_yahoo_ticker("BRK.B") == "BRK-B"
    assert normalize_yahoo_ticker("BF.B") == "BF-B"
    assert normalize_yahoo_ticker("C$R") == "C-R"
    assert normalize_yahoo_ticker("BAC$K") == "BAC-K"
    assert normalize_yahoo_ticker("AXS$E") == "AXS-E"
    assert normalize_yahoo_ticker("JPM.L") == "JPM-L"
    assert normalize_yahoo_ticker("AAPL") == "AAPL"


def test_non_equity_symbols_are_filtered_out():
    assert is_probably_non_equity_ticker("AAC.U") is True
    assert is_probably_non_equity_ticker("ACHR.W") is True
    assert is_probably_non_equity_ticker("AIIA.R") is True
    assert is_probably_non_equity_ticker("BRK.B") is False
    assert is_probably_non_equity_ticker("C$R") is False


def test_canonical_fetch_candidates_dedupe_yahoo_variants():
    assert canonicalize_yahoo_fetch_candidates("JPM.L", "JPM-L") == ["JPM-L"]
    assert canonicalize_yahoo_fetch_candidates("AXS$E", "AXS-E") == ["AXS-E"]
    assert canonicalize_yahoo_fetch_candidates("AAPL", None) == ["AAPL"]
