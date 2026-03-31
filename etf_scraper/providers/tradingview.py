"""
TradingView ETF shares outstanding scraper.
Extracts shares_outstanding from embedded JSON in TradingView symbol pages.
"""

import logging
import re
import requests

log = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    ),
}

# Ticker → (exchange, slug)  — slug = exchange-TICKER for TradingView URL
TRADINGVIEW_FUNDS: dict[str, tuple[str, str]] = {
    # Invesco — NASDAQ
    "QQQ":  ("NASDAQ", "NASDAQ-QQQ"),
    # Vanguard — AMEX
    "VOO":  ("AMEX", "AMEX-VOO"),
    "VTI":  ("AMEX", "AMEX-VTI"),
    "VEA":  ("AMEX", "AMEX-VEA"),
    "VWO":  ("AMEX", "AMEX-VWO"),
    "VNQ":  ("AMEX", "AMEX-VNQ"),
    "BND":  ("NASDAQ", "NASDAQ-BND"),
    # iShares Gold
    "IAU":  ("AMEX", "AMEX-IAU"),
    # Ark
    "ARKK": ("AMEX", "AMEX-ARKK"),
    # VanEck
    "GDX":  ("AMEX", "AMEX-GDX"),
    "GDXJ": ("AMEX", "AMEX-GDXJ"),
    # Fixed Income / Credit
    "BKLN": ("AMEX", "AMEX-BKLN"),
    "JAAA": ("AMEX", "AMEX-JAAA"),
}

_BASE = "https://www.tradingview.com/symbols"


def fetch_shares(ticker: str, session: requests.Session | None = None) -> int | None:
    """
    Fetch current shares outstanding for a TradingView-listed ETF.
    Returns shares as integer, or None on failure.
    """
    fund = TRADINGVIEW_FUNDS.get(ticker)
    if not fund:
        log.debug(f"tradingview: {ticker} not in TRADINGVIEW_FUNDS")
        return None

    _, slug = fund
    url = f"{_BASE}/{slug}/"
    sess = session or requests.Session()
    try:
        r = sess.get(url, headers=_HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as exc:
        log.warning(f"tradingview: {ticker} fetch error — {exc}")
        return None

    m = re.search(r'"shares_outstanding":(\d+\.?\d*)', r.text)
    if not m:
        log.warning(f"tradingview: {ticker} — shares_outstanding not found in page")
        return None

    shares = int(float(m.group(1)))
    log.info(f"tradingview: {ticker} shares outstanding = {shares:,}")
    return shares


def fetch_all(tickers: list[str]) -> dict[str, int]:
    """Fetch shares outstanding for all TradingView tickers. Returns {ticker: shares}."""
    sess = requests.Session()
    results: dict[str, int] = {}
    for tk in tickers:
        if tk not in TRADINGVIEW_FUNDS:
            continue
        sh = fetch_shares(tk, session=sess)
        if sh is not None:
            results[tk] = sh
    return results
